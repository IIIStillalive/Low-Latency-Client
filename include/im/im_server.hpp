#pragma once

// im_server —— v2 用户层多会话 IM（类飞书）业务层。挂在 TcpServer 之上，核心零改动。
//
// 帧协议：message_proto.hpp 的 imEncode*/imDecode（全新 type 0x10–0x9F，与 v1 聊天室零复用）。
// 持久化：kv-engine 五族键 = user/conv/member/cursor/h（schema 与边界见
//         plan/task_plan_im_v2.md §1）。--kv-path 必给（用户/会话/未读/历史全要盘）。
//
// 多线程纪律（沿用 v1，见 plan/task_plan_im_v2.md §3）：
//   - 消息回调跑在各连接归属 ioLoop 线程，天然串行；跨 loop 广播一律 runInLoop 投递到
//     接收者「自己的」loop 再 send（绝不跨线程碰 outputBuffer_）。
//   - 三层互斥、锁从不嵌套（本层保证）：convsMtx_ 保护会话注册表（含成员集/next_seq），
//     onlineMtx_ 保护在线表 + connUid_ 映射；kv 内部两锁 group-commit 自洽，不加新锁。
//     取成员集与查在线严格分两步、各自持锁即放，避免 convsMtx_→onlineMtx_ 顺序问题。
//   - 锁序只可能是（顺序持有，从不同时）：convsMtx_ → (kv) 或 onlineMtx_ → (kv)；
//     广播路径先复制成员集（convsMtx_ 内）、放锁后再查在线，二者永不嵌套。
//
// 连接生命周期（presence）：TcpServer::setConnectionCallback 在 ioLoop 线程回调
//   up=true 建连（本层无动作，认证后才绑定 uid）/ up=false 断连（移除在线表 + 广播 0x9A 离线）。
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace server { class TcpConnection; class TcpServer; }
namespace kv    { class KvStore; }

namespace im {

struct ImConv;   // 会话注册表条目（成员 uid 集 + next_seq），定义见 .cpp

// 错误码（S2C_ERROR frame 的 code 字段；客户端主要看 reason 文案）
enum : uint8_t {
    kErrBad        = 1,   // 非法请求/参数
    kErrNotAuthed  = 2,   // 未认证即操作
    kErrUserExists = 3,   // 注册：uid 已存在
    kErrAuthFail   = 4,   // 登录：未知用户 / 口令不符
    kErrConvNone   = 5,   // 会话不存在
    kErrNotMember  = 6,   // 非会话成员
    kErrDmNoLeave  = 7,   // dm 不支持退群
    kErrUnavail    = 8,   // 持久化未开（kv 未挂）
};

class ImServer{
public:
    explicit ImServer(server::TcpServer* srv);   // 注册 setConnectionCallback（presence）
    void setStore(kv::KvStore* kv);              // 挂 kv-engine（main 里 --kv-path 打开后调）
    void onMessage(server::TcpConnection* conn, const std::string& payload);  // 消息入口
    void stop();                                 // runloop 返回后调：kv flush+close

private:
    // ---- 处理函数（全部在 conn 的 ioLoop 线程）----
    void handleRegister(server::TcpConnection* conn, const std::string& uid,
                        const std::string& nick, const std::string& pwd32);
    void handleAuth(server::TcpConnection* conn, const std::string& uid, const std::string& pwd32);
    void handleUserLookup(server::TcpConnection* conn, const std::string& uid);
    void handleConvCreate(server::TcpConnection* conn, uint8_t type, const std::string& name,
                          const std::vector<std::string>& members);
    void handleConvList(server::TcpConnection* conn);
    void handleConvOpen(server::TcpConnection* conn, const std::string& convId);
    void handleHistory(server::TcpConnection* conn, const std::string& convId,
                       uint32_t count, uint32_t before);
    void handleMsg(server::TcpConnection* conn, const std::string& convId, const std::string& msg);
    void handleRead(server::TcpConnection* conn, const std::string& convId, uint64_t seq);
    void handleConvLeave(server::TcpConnection* conn, const std::string& convId);
    void handleMembers(server::TcpConnection* conn, const std::string& convId);
    void handleConvJoin(server::TcpConnection* conn, const std::string& convId);

    void onDisconnect(server::TcpConnection* conn);   // up=false：移除在线 + 广播离线

    // ---- 基础设施 ----
    std::string uidOf(server::TcpConnection* conn);      // 空 = 未认证（onlineMtx_ 内查）
    std::shared_ptr<ImConv> getConv(const std::string& convId);   // 注册表；缺则从 kv 加载（nullptr=不存在）
    bool isMember(server::TcpConnection* conn, const std::string& convId,
                  const std::shared_ptr<ImConv>& c);
    std::string displayName(const std::string& convId, const std::shared_ptr<ImConv>& c,
                            const std::string& meUid);
    std::string userNick(const std::string& uid);        // kv 读昵称；空=未知
    std::vector<std::string> convsOfUser(const std::string& uid);   // 反查会话集（kv member: 前缀扫描）
    uint64_t readCursor(const std::string& uid, const std::string& convId);   // 0=从未读
    uint32_t unreadFor(const std::string& uid, const std::string& convId);    // range 计数（免 seed）
    bool onlineOf(const std::string& uid);               // 在线表是否有当前连接
    void broadcastPresence(server::TcpConnection* conn, const std::string& uid, bool online);
    void broadcastToConv(const std::shared_ptr<ImConv>& c, const std::string& convId,
                         const std::string& frame, server::TcpConnection* exclude);
    void sendTo(server::TcpConnection* conn, const std::string& frame);   // 本线程直发
    void deliver(const std::shared_ptr<server::TcpConnection>& conn, const std::string& frame);
    void sendError(server::TcpConnection* conn, uint8_t code, const std::string& reason);
    // 群 id 单调计数（重启由既有 grp: 键扫描推导 max+1）
    std::string takeGroupId();

    server::TcpServer* srv_ = nullptr;
    kv::KvStore* kv_ = nullptr;                 // 必给（im 无历史关闭路径）

    mutable std::mutex convsMtx_;
    std::map<std::string, std::shared_ptr<ImConv>> convs_;   // conv_id → 注册表条目（含成员集/next_seq）
    uint64_t grpCounter_ = 0;                   // 下可用群号；缺省 0 = 未推导
    bool     grpSeeded_  = false;

    mutable std::mutex onlineMtx_;
    std::map<server::TcpConnection*, std::string> connUid_;          // conn → uid（断连清理用）
    std::map<std::string, std::shared_ptr<server::TcpConnection>> online_;  // uid → 当前连接
};

} // namespace im