#pragma once

// RpcChannelPool：N 连接 × 每连接多路复用（multi-reactor 版，Day6）
//
//   业务 → RpcChannelPool::call
//            ├─ 选连接（RoundRobin / LeastInflight）
//            └─ RpcConnection[i]
//                 └─ RpcCodec(req_id 关联 + Future + 超时/取消)
//                      └─ ReliableCodec(seq/Ack/重传/断线重发)
//                           └─ ProtocolCodec → LengthHeaderCodec → TcpClient
//
// 与 Day5 的关系：RpcConnection 的设计前提是「不拥有线程、ioLoop 由调用方提供」。
// 本类正是那个「调用方」——每条连接配一个 EventLoopThread，池持有线程，
// 连接对象只持 EventLoop*。**Day5 已有文件零改动**（唯一例外见文末 A1）。
//
// 线程模型：
//   - 每连接一个 IO loop 线程（各连接有自己的 ReliableCodec/RpcCodec 实例与 epoll，
//     连接之间无共享可变状态，无需锁）。
//   - Future 续体统一投递到 bizLoop（全池共享，由调用方创建并保证活得比池久）。
//   - call() 任意线程可调：req_id 在调用线程原子分配，随后转目标连接的 IO loop。
//   - 选连接用的 inflight 计数是 atomic 快照 —— 只作「避开最忙连接」的启发式，
//     不保证严格均衡（并发下多个调用可能同时选中同一条连接）。
//
// req_id 共享：
//   池内所有连接共用一个 atomic 递增器（RpcCodec::setReqIdSequence），
//   因此 req_id 跨连接唯一 —— 服务端幂等缓存（按 req_id）在 Day7 的
//   「断线重发 / 跨连接重放」场景下才能命中。若每连接各自计数，同一 req_id
//   会在不同连接上重复出现，服务端缓存随即失效。
//
// 就绪同步（Day7 D3 起）：
//   本类维护每条连接的可用性快照 Slot::available（原子 bool，IO 线程写、池线程读），
//   pickSlot 会跳过断线中的连接、把新请求改道到可用连接。请求不会因此失败——
//   断线连接上的 in-flight 帧进 ReliableCodec 的 pending_ 等重连重发，新请求只是不派给它。
//   这是**启发式快照**，不是同步原语：真实可用性仍由 pending_ 兜底（帧不会丢）。
//   为什么池不能直接读 TcpClient::isConnected()：它仅允许 loop 线程读，池线程跨线程读
//   是数据竞争，只能靠 available 这个原子快照。连接未就绪时 call() 照常可用（见上文）。
//
// ---------------------------------------------------------------------------
// 收尾（硬约束，读一遍再改代码）
//
//   RpcConnection 必须在**自己的 IO 线程上、loop 尚未退出时**销毁，因为
//   ~TcpClient 断言 loop 线程（tcp_client.cpp），~TcpConnection 还要操作
//   本 loop 的 poller/timerQueue。而 ~EventLoopThread 会 quit()+join()，
//   绝不能在该线程自身执行（自 join）。
//
//   所以 shutdown() 的时序是：
//     ① 池线程：把每条连接搬进一个 lambda，投递到它的 IO loop（所有权随之转移）
//     ② IO 线程（任务 1）：conn->stop() —— 完成全部 in-flight（Unavailable）、
//        清空 Future 的 cancelFn、主动断开；随后在**本 loop 线程内**用
//        runAfter(1ms) 排任务 2（runAfter 只能由 loop 线程调用）
//     ③ IO 线程（任务 2，下一轮循环）：conn.reset() —— conn 的最后一次引用
//        在此释放，~TcpClient / ~TcpConnection 都在 IO 线程执行；随后 loop->quit()
//     ④ 池线程：reset 每条连接的 EventLoopThread（内部 quit + join）
//
//   为什么任务 2 要隔一轮：TcpConnection 的关闭链把对象回收挂在 runAfter 上，
//   同一轮 readys 批内销毁会出现「本批仍持有该 Channel*」的 UAF（Day4 已踩）。
//
//   兜底：即使任务 2 未执行，队列/定时器随 ~EventLoop 释构时，conn 的最后一个
//   引用仍在 IO 线程释放 —— 仍然不违反①。
//
//   调用约束：
//     - shutdown() 幂等，但**不得在池内任一 IO 线程调用**（会 join 自己）；
//       典型误用：在连接回调里判断失败就销毁池。
//     - bizLoop 必须比 shutdown() 活得久：stop() 完成 Future 时会向 bizLoop
//       投递续体。
//     - shutdown() 返回后，池内 call() 一律返回已完成的 Unavailable（见 A1）。
// ---------------------------------------------------------------------------

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <netinet/in.h>   // struct sockaddr_in
#include <string>

#include "client/rpc_codec.hpp"        // RpcCodec::Stats（池级聚合的元素类型）
#include "client/rpc_connection.hpp"
#include "client/rpc_future.hpp"
#include "client/rpc_types.hpp"
#include "net/noncopyable.hpp"

namespace server {

class EventLoop;
class EventLoopThread;

class RpcChannelPool : public Noncopyable {
public:
    // 选连接策略：
    //   RoundRobin     —— 严格轮转，忽略负载（便于压测时观察每条连接的分布）
    //   LeastInflight  —— 选当前 in-flight 最少的连接，平局按轮转打破
    enum class Policy { RoundRobin, LeastInflight };

    // 池级聚合统计（各连接 RpcCodec::Stats 求和 + 池自身计数）。
    // 不含 retransmits：ReliableCodec 目前未暴露重传计数；若 Day8 压测需要，
    // 再在可靠层加一个 atomic 计数器（不为统计去动已稳定的传输层）。
    struct Stats {
        uint64_t calls         = 0;
        uint64_t ok            = 0;
        uint64_t timeouts      = 0;
        uint64_t cancelled     = 0;
        uint64_t exhausted     = 0;
        uint64_t badResponses  = 0;
        uint64_t lateResponses = 0;
        size_t   inflight      = 0;   // 全池当前 in-flight 总数
        size_t   connections   = 0;   // 池内连接数（构造时确定）
    };

    // peer       : 服务端地址（语义同 TcpClient 构造参数）
    // connCount  : 连接数；0 按 1 处理。每条连接独占一个 EventLoopThread
    // bizLoop    : Future 续体的执行线程（**不得为 nullptr**：池模式下若用各连接
    //              自己的 IO loop 跑续体，慢回调会卡死该连接的传输，Day4-B 白做）
    // policy     : 选连接策略（运行期可用 setPolicy 切换）
    //
    // 构造期间会启动 connCount 个线程并建立各连接的 codec 栈，但不发起 TCP 连接
    // （需显式 start()）。因此不得在任一池内 IO 线程上构造本对象。
    RpcChannelPool(const struct sockaddr_in& peer, size_t connCount,
                   EventLoop* bizLoop, Policy policy = Policy::LeastInflight);

    // 等价于 shutdown()；返回后所有线程已 join、连接对象已在其 IO 线程销毁。
    ~RpcChannelPool();

    // 对全部连接发起 TCP 连接（任意线程；幂等——已连接/在连的连接不会重复建 socket）。
    void start();

    // 发起一次调用（任意线程）。选连接 → 该连接的 RpcCodec::call。
    // 池已 shutdown → 返回已完成的 Unavailable；不抛异常、不阻塞。
    RpcFuture call(const std::string& method, const std::string& payload,
                   const RpcCallOptions& opts = {});

    // 收尾（见文件头时序）。幂等；任意非池内 IO 线程可调。
    // 完成所有 in-flight（Unavailable）→ 断开 → 销毁连接 → join 线程。
    // 与 call() 并发安全：内部会等在途 call() 全部退出后才搬走连接（call() 只多一次
    // 原子自增/自减）；shutdown 返回后 call() 一律得到已完成的 Unavailable。
    void shutdown();

    // 每条连接的 in-flight 上限（默认 1024）。任意线程可调、运行期即时生效
    // （背压闸是原子的，切换策略/负载阶段时不需要重建连接）。
    void setMaxInflight(size_t perConn);

    // 配置期设置：透传给每条连接的 TcpClient 自动重连参数。
    void setAutoReconnect(bool enable, double initialBackoff = 0.5, double maxBackoff = 5.0);

    Policy policy() const { return policy_.load(); }
    void   setPolicy(Policy p) { policy_.store(p); }

    size_t connCount() const { return slots_.size(); }   // 构造时确定，不随 shutdown 变化
    size_t inflight()  const;                            // 全池 in-flight 总数

    Stats          stats()   const;                      // 池级聚合
    RpcCodec::Stats statsAt(size_t i) const;             // 单连接明细；越界返回空 Stats

private:
    // 一条连接的全部资源。thread 必须比 conn 活得久（先于 conn 释放 = 自 join 风险），
    // 因此声明顺序为 thread → loop → conn，池负责编排实际释放时序（见文件头）。
    struct Slot {
        std::unique_ptr<EventLoopThread> thread;
        EventLoop*                       loop = nullptr;
        std::unique_ptr<RpcConnection>   conn;
        // 连接可用性快照（Day7 D3）：由 conn 的状态回调在 IO 线程写、pickSlot 读。
        // 容器用 std::deque（元素不可移动，见 ctor）：地址稳定 → 构造期绑状态回调
        // 时捕获 &available 是安全的，无需 reserve/移动。
        std::atomic<bool>                available{false};
    };

    static constexpr size_t kNoSlot = static_cast<size_t>(-1);   // 「无可用槽」

    size_t pickSlot();   // 按 policy 选下标；slots_ 为空返回 kNoSlot

    std::deque<Slot>                            slots_;
    EventLoop*                                  bizLoop_;
    std::shared_ptr<std::atomic<ReqId>>     reqIdSeq_;   // 全池共享（见文件头）
    std::atomic<size_t>                     rr_{0};      // RoundRobin 游标 / LeastInflight 平局打破
    std::atomic<Policy>                     policy_;
    std::atomic<bool>                       stopped_{false};
    std::atomic<size_t>                     callers_{0};   // 在途 call() 数；shutdown 等它归零
    std::atomic<size_t>                     maxInflight_{1024};   // 与各连接的背压闸同步的默认值
};

} // namespace server
