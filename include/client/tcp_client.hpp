#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <netinet/in.h>   // struct sockaddr_in

#include "net/length_header_codec.hpp"
#include "net/noncopyable.hpp"
#include "net/socket.hpp"
#include "net/timer.hpp"   // TimerId：重连 / 握手兜底 timer 的可取消句柄

namespace server {

class EventLoop;
class Channel;
class TcpConnection;

// 客户端门面（Muduo TcpClient 同构；C1 迁移后常驻 include/client/）
//
// 职责：只负责「把一条 TCP 连接建立起来，再交回既有的传输 / 解析链」。
//   - 建立后一切复用 TcpConnection / LengthHeaderCodec——客户端不发明任何新传输逻辑。
//   - 与 TcpServer 唯一的差异：accept() 拿到的是已连接 fd，而 connect() 在非阻塞 fd 上
//     立即返回 EINPROGRESS（握手在途），必须等 EPOLLOUT 再 getsockopt(SO_ERROR) 裁决，
//     确认成功之后才能构造 TcpConnection。因此存在一个 server 没有的「connecting 阶段」。
//
// 线程模型：单 EventLoop，另约定析构也必须发生在 loop 线程（Muduo TcpClient 同款约束）——
//   析构要拆连接，依赖 loop 线程的 runInLoop 内联执行；跨线程析构把 [this] 任务排队到
//   已死/将死的 loop → 悬垂。程序退出顺序：先确保 loop 停止、本对象仍存活，再析构它。
//   connect() / disconnect() 允许任意线程调用（内部 runInLoop 转交 loop 线程）。
//
// 生命周期（状态机，loop 线程内全串行）：
//   Idle ──connect()──▶ Connecting ──SO_ERROR==0──▶ Connected（conn_ 接管 fd）
//      ▲                   │（临时 Channel 挂 EPOLLOUT，5s 兜底超时） │
//      │                   └── SO_ERROR!=0 / 超时 / disconnect ────────┘
//      └────────────── 对端关 / 超时──removeAndReset────────────────────┘
class TcpClient : public Noncopyable {

public:
    // peer：服务器地址（sockaddr_in，语义同 Acceptor 的构造参数）。
    // 构造不自动发起连接；需显式调 connect()。
    TcpClient(EventLoop* loop, const struct sockaddr_in& peer);
    ~TcpClient();                      // 主动下线：拆连接并置 active_=false；须 loop 线程析构

    // 发起（或再次发起）连接。可从任意线程调用；内部 runInLoop 转 loop 线程。
    // 已 connecting / 已 connected 时本次不重复建 socket。
    void connect();

    // 主动下线：走完整关闭链（下树 + cancel 心跳 + 通知上层按「断开」处理）。
    // 此后 connect() 变 no-op —— 重连与用户退出互斥开关（Day2 指数退避重连据此判断）。
    void disconnect();

    bool isConnected() const;          // 是否有活动连接（仅 loop 线程读；调试 / 日志用）

    // 连接回调：conn != nullptr → 连接建立，可用此 conn 挂在 ProtocolCodec 上发送；
    //           conn == nullptr → 连接失败或断开（区分靠之前是否收到过成功回调）。
    // ⚠️ 该回调在 loop 线程执行，且回调期间 TcpClient 一定是活的（析构前先拆连接撤销回调）。
    void setConnectionCallback(std::function<void(TcpConnection*)> cb);

    // 业务层：收到一条「完整消息」时的回调（语义同 TcpServer::setMessageCallback）。
    using MessageCallback = LengthHeaderCodec::MessageCallback;
    void setMessageCallback(const MessageCallback& cb);

    // 透传给活动连接：空闲超时（秒）；0 或未调用 = 不启用。
    void setConnectionTimeout(double seconds);

    // ---- Day2：指数退避自动重连 ----
    // 开关 + 退避参数（秒）。默认关闭：Day1 行为不变（断开即通知上层，由上层决定去留）。
    // enable 后：任何「非主动下线」的失败（握手失败/超时/连接断开）都会按退避序列
    //   initial*2^n（≥max 封顶）自动重连；重连成功 retry 计数清零。
    // 参数建议在配置期（连接建立前）设置；loop 线程随用随读，跨线程调用自身 runInLoop 转交。
    void setAutoReconnect(bool enable, double initialBackoff = 1.0, double maxBackoff = 30.0);

    // 可选：第 n 次重连尝试「被调度」前回调（n 从 1 起，成功重新从 1 计）。
    // 仅当 autoReconnect 开启且启用了重连才触发；业务可打日志或按次数决定放弃。
    void setAttemptCallback(std::function<void(int n)> cb);

private:
    void connectInLoop();   // [loop 线程] socket + 非阻塞 + connect(EINPROGRESS) + 挂 EPOLLOUT + 5s兜底超时
    void handleConnect();   // [loop 线程] EPOLLOUT/ERR/HUP 回调：SO_ERROR 最终裁决
    void onConnected(int fd);   // [loop 线程] 握手确认成功：建 TcpConnection 接管 fd + 通知上层
    void teardownConnecting();  // [loop 线程] 拆握手通道（仅摘 epoll + 关 fd；对象留到下次 connect/析构回收，杜绝批内 UAF）
    void removeAndReset();  // [loop 线程] 活动连接断开回调 → 先调度重连再通知上层 nullptr（Day2）
    void fail();            // [loop 线程] 连接失败/中止 → 先调度重连再通知上层 nullptr（Day2）
    void scheduleReconnect();   // [loop 线程] 任一失败收尾：按退避调度一次自动重连（主动下线不调度）
    void onReconnectTick();     // [loop 线程] 重连 timer 到期 → 幂等 guard → connectInLoop() 复用完整握手链

    EventLoop* loop_;
    struct sockaddr_in peer_;

    // ---- connecting 阶段（Idle→Connected 之间短暂存在）----
    Socket connSock_;                        // 握手期 fd；成功后 release() 移交 TcpConnection（防双 close）
    std::shared_ptr<Channel> connectingCh_;  // 握手期临时通道（唯一挂 EPOLLOUT 的地方）。
                                             // teardown 只摘 epoll + 关 fd，对象经 runAfter 延迟到下一轮循环回收：
                                             // 可能仍在本批 readys 待分发，批内销毁 = UAF

    std::shared_ptr<TcpConnection> conn_;    // Connected 后唯一所有权（含 fd）

    std::function<void(TcpConnection*)> connectionCallback_;
    LengthHeaderCodec codec_;                // 无状态切帧刀（与 TcpServer 同款；一实例服务各类连接）
    double connectionTimeout_ = 0;

    // ---- Day2 重连状态（loop 线程内全串行读写）----
    TimerId reconnectTimerId_ = 0;   // 非 0 = 有一条 pending 重连 timer（到期→onReconnectTick）；disconnect 时 cancel
    TimerId handshakeTimerId_ = 0;   // 握手 5s 兜底 timer 句柄（Day1 洞补修：可 cancel，防 Connecting 期析构后悬垂）
    int retryCount_ = 0;             // 连续失败次数（onConnected 成功清零）
    double initialBackoff_ = 1.0;    // 退避首跳（秒）
    double maxBackoff_ = 30.0;       // 退避封顶（秒）
    bool autoReconnect_ = false;     // Day2 开关：默认关闭（决策点），setAutoReconnect(true) 显式开启
    std::function<void(int)> attemptCallback_;

    std::atomic<bool> active_     { true };  // disconnect()/析构 → false；此后 connect() no-op（重连互斥开关）
    std::atomic<bool> connecting_ { false }; // EINPROGRESS 在途标记；disconnect 竞态时据此拆握手通道
};

} // namespace server