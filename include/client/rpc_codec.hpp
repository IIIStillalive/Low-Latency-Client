#pragma once

// RpcCodec：RPC 关联层（req_id ↔ Future），夹在 ReliableCodec 与业务之间
//
// 发: call(method,payload) ──req_id→ RpcRequest(protobuf) ──ReliableCodec::sendFrame(Request)→ 线
// 收: ReliableCodec::onFrame(Response) ──protobuf→ 按 req_id 找 in-flight → 完成 Future
//
// 线程模型（与 Day4 一致）：
//   - call() 任意线程可调：req_id 在调用方分配（原子），其余全部 runInLoop 转 IO 线程。
//   - in-flight 表 / 超时定时器 / 发送 只在 IO 线程读写 —— 无锁。
//   - Future 完成后的续体统一投递到 biz loop（RpcFuture 内部负责）。
//
// 语义：
//   - 响应可以乱序到达（服务端并发/延迟返回），按 req_id 关联，不假设顺序。
//   - 超时从 call() 发起时刻起算；到期 → DeadlineExceeded 并从 in-flight 移除。
//     之后迟到的响应按「未知 req_id」丢弃（计数 lateResponses），不算错误。
//   - 取消：cancel() → IO 线程移除 in-flight + 发 Cancel 帧（best-effort）+ Cancelled。
//   - 背压：in-flight 达到 maxInflight（默认 1024）→ 新调用立即 ResourceExhausted（fail-fast）。
//   - 连接断开：请求在 ReliableCodec 里挂起重发，in-flight 不动；恢复后服务端按 req_id
//     命中缓存重放响应 —— 调用方无感（Day7 完整验证）。
//
// 生命周期：与 ReliableCodec 同款硬约束 —— stop() 必须在 loop 停止后、本线程（IO loop 线程）
// 调用，用于完成所有 in-flight（Unavailable）并清空取消钩子；之后才允许析构。
// 禁止在析构后调用任何 Future 的 cancel()。

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "client/protocol.hpp"
#include "client/rpc_future.hpp"
#include "client/rpc_types.hpp"
#include "net/timer.hpp"

namespace server {

class ReliableCodec;
class EventLoop;
class TcpConnection;

class RpcCodec {
public:
    struct Stats {
        uint64_t calls        = 0;  // call() 次数
        uint64_t ok           = 0;  // 成功响应
        uint64_t timeouts     = 0;  // 本地超时
        uint64_t cancelled    = 0;  // 主动取消
        uint64_t exhausted    = 0;  // 背压拒绝
        uint64_t badResponses = 0;  // protobuf 解析失败
        uint64_t lateResponses= 0;  // 超时/取消之后迟到的响应（已丢弃）
        size_t   inflight     = 0;  // 当前 in-flight 数
    };

    RpcCodec(ReliableCodec& rel, EventLoop* ioLoop, EventLoop* bizLoop);
    ~RpcCodec() = default;

    // 背压闸：in-flight 上限（默认 1024）；超限 fail-fast 返回 ResourceExhausted。
    void setMaxInflight(size_t n){ maxInflight_ = n; }

    // 共享 req_id 分配器：连接池内所有连接共用一个计数器，保证跨连接唯一
    // （服务端幂等缓存以 req_id 为键）。不调用则使用本实例内部计数器。
    void setReqIdSequence(std::shared_ptr<std::atomic<uint32_t>> seq){ reqIdSeq_ = std::move(seq); }

    // 发起调用（任意线程）。返回的 Future 已完成时也可安全 then()。
    RpcFuture call(const std::string& method, const std::string& payload,
                   const RpcCallOptions& opts = {});

    // 收：挂到 ReliableCodec 的 messageCallback（IO 线程）。
    void onFrame(TcpConnection* conn, MsgType type, uint32_t seq, const std::string& body);

    // 收尾：完成全部 in-flight（Unavailable）。须在 loop 停止后于 IO 线程调用一次。
    void stop();

    size_t inflight() const { return inflightCount_.load(); }
    Stats  stats() const;

private:
    struct InflightCall {
        RpcFuture future;
        TimerId   timerId = 0;    // deadline 定时器；0 = 未设置（不超时）
    };

    void startCallInLoop(uint32_t reqId, std::string body,
                         const RpcCallOptions& opts, RpcFuture fut);   // [IO 线程]
    void finish(uint32_t reqId, RpcResult r);                          // [IO 线程] 统一收口
    void onTimeout(uint32_t reqId);                                    // [IO 线程]
    void cancelInLoop(uint32_t reqId);                                 // [IO 线程]

    ReliableCodec& rel_;
    EventLoop*     ioLoop_;
    EventLoop*     bizLoop_;

    std::shared_ptr<std::atomic<uint32_t>> reqIdSeq_;

    std::unordered_map<uint32_t, InflightCall> inflight_;   // 仅 IO 线程读写
    std::atomic<size_t> inflightCount_{0};

    size_t maxInflight_ = 1024;
    bool   stopped_     = false;   // 仅 IO 线程写（stop() 在 loop 停止后调用）

    std::atomic<uint64_t> calls_{0}, ok_{0}, timeouts_{0}, cancelled_{0},
                          exhausted_{0}, bad_{0}, late_{0};
};

} // namespace server
