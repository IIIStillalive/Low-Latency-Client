#pragma once

// RpcConnection：单连接 RPC 门面（把既有四层栈 + RpcCodec 组装成一个可调用的对象）
//
//   业务 → RpcConnection::call
//            └─ RpcCodec(req_id 关联 + Future + 超时/取消)
//                 └─ ReliableCodec(seq/Ack/重传/断线重发)
//                      └─ ProtocolCodec(5B 头)
//                           └─ LengthHeaderCodec(4B 长度) ← TcpClient 内部也有一个切帧实例
//                                └─ TcpClient(非阻塞连接 + 指数退避重连)
//
// 成员声明次序 = 构造次序；析构逆序 → client_ 先拆连接、撤回调，再析构各 codec（防回调碰已析构状态）。
//
// Day6 的 RpcChannelPool 会把本类实例化 N 份，每份绑定一个 EventLoopThread 的 loop；
// 本类自身不拥有线程，ioLoop 由调用方提供。
//
// 收尾（硬约束）：
//   1) loop.runloop() 退出后（仍在 IO 线程）调用 stop() —— 完成所有 in-flight（Unavailable）；
//   2) 再停 biz loop 线程（若业务 loop 是外部 EventLoopThread）；
//   3) 最后析构本对象。

#include <atomic>
#include <functional>   // std::function：连接状态回调
#include <netinet/in.h>   // struct sockaddr_in
#include <utility>      // std::move

#include "client/protocol.hpp"
#include "client/rpc_codec.hpp"
#include "client/rpc_future.hpp"
#include "client/reliable_codec.hpp"
#include "client/tcp_client.hpp"
#include "net/length_header_codec.hpp"
#include "net/noncopyable.hpp"

namespace server {

class EventLoop;

class RpcConnection : public Noncopyable {
public:
    // ioLoop：本连接的网络事件循环（TcpClient 与各 codec 的归属线程）。
    // bizLoop：Future 续体的执行线程；nullptr → 退化为 ioLoop。
    RpcConnection(EventLoop* ioLoop, const struct sockaddr_in& peer, EventLoop* bizLoop = nullptr);
    ~RpcConnection() = default;

    void start();                                   // 发起连接（任意线程）
    RpcFuture call(const std::string& method, const std::string& payload,
                   const RpcCallOptions& opts = {});   // 任意线程

    void setMaxInflight(size_t n) { rpcCodec_.setMaxInflight(n); }
    void setReqIdSequence(std::shared_ptr<std::atomic<ReqId>> seq) {
        rpcCodec_.setReqIdSequence(std::move(seq));
    }
    void setAutoReconnect(bool enable, double initialBackoff = 0.5, double maxBackoff = 5.0);

    // 连接状态回调（Day7 D3，供 RpcChannelPool 维护 Slot::available 快照）：
    //   connected == true  → 刚建立/重连成功，在 relCodec_.onConnected 之后触发
    //   connected == false → 连接已断开，在 relCodec_.onDisconnected 之后触发
    // 始终在本连接的 IO 线程调用 —— 调用方只准往原子/无锁状态里写，不准在回调里做阻塞事。
    void setStateCallback(std::function<void(bool connected)> cb){ stateCb_ = std::move(cb); }

    size_t              inflight() const { return rpcCodec_.inflight(); }
    RpcCodec::Stats     stats()    const { return rpcCodec_.stats(); }

    void stop();                                    // 收尾：loop 停止后于 IO 线程调用一次

private:
    std::function<void(bool)> stateCb_;   // 空则跳过；回调在 IO 线程触发（见 setStateCallback 注释）
    EventLoop*        ioLoop_;
    LengthHeaderCodec lengthCodec_;   // 仅供 ProtocolCodec 发送时补 4B 长度头
    ProtocolCodec     protoCodec_;
    ReliableCodec     relCodec_;
    RpcCodec          rpcCodec_;
    TcpClient         client_;        // 最后声明 → 最先析构（先拆连接，后析构 codec）
};

} // namespace server
