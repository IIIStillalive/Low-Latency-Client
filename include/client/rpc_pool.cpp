#include "client/rpc_pool.hpp"

#include <cassert>
#include <thread>      // shutdown 等待在途 call() 退出
#include <utility>

#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "server/eventloopthread.hpp"

namespace server {

namespace {
// 关闭链会把 TcpConnection 的回收挂到后续一轮循环；连接对象的销毁必须晚于它，
// 所以任务 2 用定时器排到下一轮（详见 rpc_pool.hpp 文件头的收尾时序）。
constexpr double kSettleDelaySec = 0.001;
} // namespace

RpcChannelPool::RpcChannelPool(const struct sockaddr_in& peer, size_t connCount,
                               EventLoop* bizLoop, Policy policy)
    : bizLoop_(bizLoop),
      reqIdSeq_(RpcCodec::makeReqIdSequence()),
      policy_(policy) {
    assert(bizLoop_ != nullptr && "池模式必须提供 bizLoop：续体不能跑在各连接的 IO 线程上");
    if(connCount == 0) connCount = 1;

    slots_.reserve(connCount);                         // 之后不再扩容：Slot 地址稳定
    for(size_t i = 0; i < connCount; ++i){
        Slot s;
        s.thread = std::make_unique<EventLoopThread>();
        s.loop   = s.thread->startLoop();              // 阻塞到子线程把 EventLoop 建好
        s.conn   = std::make_unique<RpcConnection>(s.loop, peer, bizLoop_);
        s.conn->setReqIdSequence(reqIdSeq_);           // 全池共享：req_id 跨连接唯一
        s.conn->setMaxInflight(maxInflight_);
        slots_.push_back(std::move(s));
    }
}

RpcChannelPool::~RpcChannelPool(){
    shutdown();
}

void RpcChannelPool::start(){
    if(stopped_.load()) return;
    for(auto& s : slots_){
        if(s.conn) s.conn->start();
    }
}

RpcFuture RpcChannelPool::call(const std::string& method, const std::string& payload,
                               const RpcCallOptions& opts){
    if(stopped_.load()) return RpcFuture::makeFailed(RpcStatus::Unavailable, "pool stopped");

    // 登记在途调用：shutdown() 会等 callers_ 归零后才搬走连接对象。
    // 双重检查：若 shutdown 在本线程自增前已置 stopped_，这里直接失败，不碰 slots_。
    callers_.fetch_add(1, std::memory_order_seq_cst);
    if(stopped_.load(std::memory_order_seq_cst)){
        callers_.fetch_sub(1, std::memory_order_seq_cst);
        return RpcFuture::makeFailed(RpcStatus::Unavailable, "pool stopped");
    }

    const size_t idx = pickSlot();
    if(idx == kNoSlot || slots_[idx].conn == nullptr){
        callers_.fetch_sub(1, std::memory_order_seq_cst);
        return RpcFuture::makeFailed(RpcStatus::Unavailable, "no connection in pool");
    }
    RpcFuture fut = slots_[idx].conn->call(method, payload, opts);   // 线程安全（Day5）
    callers_.fetch_sub(1, std::memory_order_seq_cst);
    return fut;
}

size_t RpcChannelPool::pickSlot(){
    const size_t n = slots_.size();
    if(n == 0) return kNoSlot;

    // 先取一个轮转起点：RoundRobin 直接用它；LeastInflight 用它做平局打破
    const size_t start = rr_.fetch_add(1, std::memory_order_relaxed) % n;
    if(policy_.load() == Policy::RoundRobin) return start;

    // LeastInflight：inflight() 是 atomic 快照，只作「避开最忙连接」的启发式。
    // 注意 startCallInLoop 在 IO 线程才登记 in-flight，刚 call() 完的连接可能仍显示 0，
    // 所以并发下不保证严格均衡 —— 这也是它只作启发式、平局按轮转打破的原因。
    bool   found    = false;
    size_t best     = kNoSlot;
    size_t bestLoad = 0;
    for(size_t k = 0; k < n; ++k){
        const size_t idx = (start + k) % n;
        if(slots_[idx].conn == nullptr) continue;
        const size_t load = slots_[idx].conn->inflight();
        if(!found || load < bestLoad){
            found    = true;
            best     = idx;
            bestLoad = load;
            if(load == 0) break;                       // 0 已是最小，无需继续扫
        }
    }
    return found ? best : kNoSlot;
}

size_t RpcChannelPool::inflight() const{
    size_t sum = 0;
    for(const auto& s : slots_){
        if(s.conn) sum += s.conn->inflight();
    }
    return sum;
}

RpcChannelPool::Stats RpcChannelPool::stats() const{
    Stats out;
    out.connections = slots_.size();
    for(const auto& s : slots_){
        if(!s.conn) continue;
        const RpcCodec::Stats c = s.conn->stats();
        out.calls         += c.calls;
        out.ok            += c.ok;
        out.timeouts      += c.timeouts;
        out.cancelled     += c.cancelled;
        out.exhausted     += c.exhausted;
        out.badResponses  += c.badResponses;
        out.lateResponses += c.lateResponses;
        out.inflight      += c.inflight;
    }
    return out;
}

RpcCodec::Stats RpcChannelPool::statsAt(size_t i) const{
    if(i >= slots_.size() || slots_[i].conn == nullptr) return RpcCodec::Stats{};
    return slots_[i].conn->stats();
}

void RpcChannelPool::setMaxInflight(size_t perConn){
    maxInflight_ = perConn;
    for(auto& s : slots_){
        if(s.conn) s.conn->setMaxInflight(perConn);
    }
}

void RpcChannelPool::setAutoReconnect(bool enable, double initialBackoff, double maxBackoff){
    for(auto& s : slots_){
        if(s.conn) s.conn->setAutoReconnect(enable, initialBackoff, maxBackoff);
    }
}

void RpcChannelPool::shutdown(){
    if(stopped_.exchange(true)) return;

    // 契约：不得在池内任一 IO 线程调用（否则下面 join 的是自己）。
    // 这一刻所有 loop 都还活着，读 loop 指针是安全的。
    for(auto& s : slots_){
        if(s.loop != nullptr && s.loop->isInLoopThread()){
            Logger::instance().log(Level::ERROR,
                "[rpc_pool] shutdown() called on a pool IO thread; refusing");
            assert(false && "RpcChannelPool::shutdown() must not run on a pool IO thread");
            return;
        }
    }

    // 等在途 call() 全部退出后，slots_ 才允许被搬空（与 call() 的 seq_cst 双重检查配对：
    // 「自增 → 读 stopped_」与「写 stopped_ → 读 callers_」两个序列在总序里必有交集）。
    while(callers_.load(std::memory_order_seq_cst) != 0){
        std::this_thread::yield();
    }

    for(auto& s : slots_){
        if(!s.conn) continue;
        // std::function 要求目标是可拷贝的，unique_ptr 不满足 → 用 shared_ptr 承载所有权。
        // 这不影响「最后一次引用在 IO 线程释放」：池线程这份在投递后立即析构，队列/定时器里的
        // 副本都归所属 loop 线程所有，最终在任务 2 执行或 ~EventLoop 清理时于 loop 线程释放。
        std::shared_ptr<RpcConnection> conn(std::move(s.conn));
        EventLoop* loop = s.loop;

        // ① 跨线程投递：连接所有权随 lambda 进入它自己的 IO 线程
        loop->runInLoop([conn, loop]() mutable {
            // ② IO 线程：完成全部 in-flight（Unavailable）+ 清空 cancelFn + 主动断开
            conn->stop();

            // 关闭链的对象回收挂在后续一轮；用定时器排到下一轮再销毁。
            // runAfter 只能由 loop 线程调用，所以必须在这里排，不能由池线程排。
            loop->runAfter(kSettleDelaySec, [conn, loop]() mutable {
                // ③ conn 的最后一次引用在此释放：
                //    ~TcpClient / ~TcpConnection 都在本 loop 线程、loop 仍存活时执行
                conn.reset();
                loop->quit();                      // 引导本线程退出（join 在池线程等）
            });
        });
    }

    // ④ 等各 loop 由任务 2 引导退出，再释放 EventLoopThread。
    //    此时 threadFunc 已把 loop_ 置空，dtor 内的 quit()/join() 都是 no-op。
    for(auto& s : slots_){
        if(s.thread) s.thread->join();
    }
    for(auto& s : slots_){
        s.thread.reset();
    }
}

} // namespace server
