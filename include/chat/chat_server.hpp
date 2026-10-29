#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace server { class TcpConnection; class TcpServer; }

// 聊天室业务层（Day10）：挂在 TcpServer::setMessageCallback / sendMessage 之上，不改任何核心。
//   帧协议：include/chat/message_proto.hpp（1B type，无 protobuf）。
//
// 多线程安全（设计见 plan/task_plan_chat_room.md §2/§3）：
//   - 房间 registry / 成员表各有互斥锁；连接的消息回调跑在各自 ioLoop，天然串行。
//   - 跨 loop 广播一律 runInLoop 到成员「自己的」loop 再 send：TcpConnection::send 非本
//     loop 线程会落入直写 fallback，与批末 flush 竞争 outputBuffer_ → 数据竞争；只在
//     归属 loop 线程内调用才安全。
//   - 关闭清理：成员持 weak_ptr，广播前弱引用过期即修剪。closeCallback 槽已被 TcpServer
//     占用（移除 conns_），这里不再加第二个钩子；连接释放后残余 weak 自动过期，不阻塞
//     关闭、不悬垂。send 首行 closed_/isremove 守卫兜底「投递到已关闭连接」。
namespace chat {

class ChatRoom;

// 业务门面：一个进程一个实例，挂在 TcpServer 上。
class ChatServer{
public:
    explicit ChatServer(server::TcpServer* srv);
    void onMessage(server::TcpConnection* conn, const std::string& payload);  // 消息回调入口

private:
    void handleJoin(server::TcpConnection* conn, const std::string& room, const std::string& name);
    void handleMsg(server::TcpConnection* conn, const std::string& room, const std::string& msg);
    void handleLeave(server::TcpConnection* conn, const std::string& room);
    void handleList(server::TcpConnection* conn, const std::string& room);

    std::shared_ptr<ChatRoom> findRoom(const std::string& room);      // 只读 registry；不存在=nullptr
    std::shared_ptr<ChatRoom> getOrCreate(const std::string& room);   // 缺则建（空房保留，诚实边界）
    void sendError(server::TcpConnection* conn, const std::string& reason);
    void sendTo(server::TcpConnection* conn, const std::string& frame);              // 本线程直发（批末合并）
    void deliver(const std::shared_ptr<server::TcpConnection>& conn,
                 const std::string& frame);                                          // 跨 loop：runInLoop 投递
    void broadcast(const std::shared_ptr<ChatRoom>& room, const std::string& frame); // 扇出（含发送者 echo）

    server::TcpServer* srv_;           // 只持有指针；生命周期随 TcpServer（同栈/同进程）
    mutable std::mutex roomsMtx_;
    std::map<std::string, std::shared_ptr<ChatRoom>> rooms_;   // 房间号 → 房间（registry）
};

// 单房间：成员 = weak_ptr<连接> + 昵称；全部读写都在房间锁内。
class ChatRoom{
public:
    // 同连接重复 join → 更新昵称并返回 false；新成员追加返回 true。
    bool join(const std::shared_ptr<server::TcpConnection>& conn, const std::string& name);
    // 移除成员并返回其昵称（用于 LEFT 通知帧）；非成员返回空串。
    std::string leave(server::TcpConnection* conn);
    // 昵称；非成员返回空串（顺带修剪已析构成员）。
    std::string nameOf(server::TcpConnection* conn);
    // 快照：锁内 weak->lock 收集活连接（顺带修剪），锁外自由使用 → 广播不持锁。
    std::vector<std::shared_ptr<server::TcpConnection>> alive();
    // 成员昵称列表（MEMBERS 帧用）。
    std::vector<std::string> names();

private:
    mutable std::mutex mtx_;
    std::vector<std::pair<std::weak_ptr<server::TcpConnection>, std::string>> members_;
};

} // namespace chat