// Day6 连接池负载测试：流水线 / 背压 / 取消（配合 ./build/rpc_server）
//
// Phase A 10k 纯流水线：4 连接 × 2500 个 echo，全部发出后不等待结果
//          → Ok=10000、exhausted=0、inflight 归零；顺带打印墙钟时间与 QPS
// Phase B 背压：每连接上限降到 64，发 296 个 sleep(300ms)
//          → 每连接前 64 个被接受、后 10 个立即 ResourceExhausted（共 40 个）
// Phase C 取消：8 个 sleep(400ms)，50ms 后统一 cancel()
//          → 8 个都拿到 Cancelled、续体各回一次；服务端 demo 的 Cancel 只确认传输帧、
//            不中断执行，所以 400ms 后响应仍会到，客户端按「未知 req_id」丢弃并计 lateResponses
//
// 线程：
//   - main：空转 EventLoop，承载看门狗、取消定时器、迟到响应观察点
//   - 4 条池内 IO 线程（池自建，每连接一个 poller）
//   - biz loop：EventLoopThread（续体现场；阶段推进也在这里）
//
// 阶段间为什么要改 maxInflight：Phase A 需要高上限（2500/连接 > 默认 1024），
// Phase B 需要低上限才能触发背压；setMaxInflight 是原子闸，运行期切换安全。
//
// 捕获纪律：所有 lambda 一律捕获 shared_ptr<Ctx>（按值）+ 需要的字符串/指针，
// 不用 [&]。原因是 runInLoop/runAfter/then 都会把 lambda 拷进队列或定时器，
// 「按引用捕获外层闭包成员」会在临时闭包析构后悬垂 —— 这是本文件最容易踩的坑。
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdlib>   // std::atoi
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <vector>

#include "client/rpc_pool.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "server/eventloopthread.hpp"

using namespace server;

namespace {
constexpr int    kConns        = 4;       // 与 pool 构造参数一致
constexpr int    kPipeline     = 10000;   // Phase A（4 的倍数：RoundRobin 起点对齐 slot 0）
constexpr size_t kCapB         = 64;      // Phase B：每连接 in-flight 上限
constexpr int    kPerConnB     = static_cast<int>(kCapB) + 10;   // 74：64 进、10 拒
constexpr int    kBackpressure = kConns * kPerConnB;             // 296
constexpr int    kAcceptedB    = kConns * static_cast<int>(kCapB);   // 256
constexpr int    kCancel       = 8;       // Phase C
constexpr int    kSleepMsB     = 300;     // Phase B 服务端延迟
constexpr int    kSleepMsC     = 400;     // Phase C 服务端延迟
constexpr double kCancelAfterSec = 0.05;  // 取消时刻（远早于 400ms 响应）
constexpr double kLateWaitSec    = 0.60;  // 迟到响应观察点（> 400ms 响应 + 余量）
constexpr double kWarmupSec      = 0.20;  // 等连接建立后再发 Phase A

// steady_clock 纳秒：作为原子计数器存，避免跨线程读一个普通 time_point
inline long long nowNs(){
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
} // namespace

// 全部测试状态集中在这里：每个 lambda 按值捕获 shared_ptr，生命周期不依赖任何闭包
struct Ctx {
    std::atomic<int>  aDone{0}, aOk{0};
    std::atomic<int>  bDone{0}, bOk{0}, bExhausted{0};
    std::atomic<int>  cCb{0}, cCancelled{0};
    std::atomic<long long> aStartNs{0}, aElapsedUs{0};
    std::atomic<bool> aPass{false}, bPass{false}, cCbPass{false}, cLatePass{false};

    EventLoop*      loop = nullptr;
    RpcChannelPool* pool = nullptr;

    std::function<void()> phaseA, phaseB, phaseC;
};

int main(int argc, char** argv){
    int port = 8899;
    for(int i = 1; i + 1 < argc; ++i){
        if(std::string(argv[i]) == "--port") port = std::atoi(argv[i+1]);
    }

    Logger::instance().set_level(Level::INFO);

    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family      = AF_INET;
    peer.sin_port        = htons(static_cast<uint16_t>(port));
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // ---- 声明顺序：loop → bizThread → pool（析构逆序：先收池、再停 biz、最后 loop）----
    EventLoop      loop;
    EventLoopThread bizThread;
    EventLoop*     biz = bizThread.startLoop();
    RpcChannelPool pool(peer, kConns, biz, RpcChannelPool::Policy::RoundRobin);

    auto ctx = std::make_shared<Ctx>();
    ctx->loop = &loop;
    ctx->pool = &pool;

    // ---- Phase A：10k 纯流水线 ----
    ctx->phaseA = [ctx]{
        RpcChannelPool* const pool = ctx->pool;
        pool->setMaxInflight(4096);       // 2500/连接 > 默认 1024，必须放宽才测得到流水线
        ctx->aStartNs.store(nowNs(), std::memory_order_relaxed);

        RpcCallOptions opts;
        opts.deadlineSec = 20.0;          // 10k 在途，给足余量
        for(int i = 0; i < kPipeline; ++i){
            const std::string expect = "p" + std::to_string(i);
            pool->call("echo", expect, opts).then([ctx, expect](const RpcResult& r){
                if(r.status == RpcStatus::Ok && r.payload == expect) ctx->aOk.fetch_add(1);
                if(ctx->aDone.fetch_add(1) + 1 != kPipeline) return;

                ctx->aElapsedUs.store((nowNs() - ctx->aStartNs.load(std::memory_order_relaxed)) / 1000);
                const RpcChannelPool::Stats st = ctx->pool->stats();
                ctx->aPass.store(ctx->aOk.load() == kPipeline && st.exhausted == 0
                                 && st.inflight == 0);
                ctx->phaseB();            // 续体已在 biz loop 上
            });
        }
    };

    // ---- Phase B：背压（用 sleep 制造确定性：300ms 内不会有响应释放槽位）----
    ctx->phaseB = [ctx]{
        RpcChannelPool* const pool = ctx->pool;
        pool->setMaxInflight(kCapB);
        RpcCallOptions opts;              // 默认 5s 足够（服务端 300ms）
        for(int i = 0; i < kBackpressure; ++i){
            pool->call("sleep", std::to_string(kSleepMsB), opts).then([ctx](const RpcResult& r){
                if(r.status == RpcStatus::Ok)                     ctx->bOk.fetch_add(1);
                else if(r.status == RpcStatus::ResourceExhausted) ctx->bExhausted.fetch_add(1);
                if(ctx->bDone.fetch_add(1) + 1 != kBackpressure) return;

                ctx->bPass.store(ctx->bOk.load() == kAcceptedB
                                 && ctx->bExhausted.load() == (kBackpressure - kAcceptedB));
                ctx->phaseC();
            });
        }
    };

    // ---- Phase C：取消后不再回调 ----
    ctx->phaseC = [ctx]{
        RpcChannelPool* const pool = ctx->pool;
        pool->setMaxInflight(1024);
        RpcCallOptions opts;
        opts.deadlineSec = 5.0;

        // 句柄搬进 shared_ptr 容器：biz 线程登记、main loop 线程取消，跨线程共享所有权
        auto futs = std::make_shared<std::vector<RpcFuture>>();
        futs->reserve(kCancel);
        for(int i = 0; i < kCancel; ++i){
            RpcFuture f = pool->call("sleep", std::to_string(kSleepMsC), opts);
            f.then([ctx](const RpcResult& r){
                if(r.status == RpcStatus::Cancelled) ctx->cCancelled.fetch_add(1);
                ctx->cCb.fetch_add(1);    // 每个调用只能回一次（complete 幂等）
            });
            futs->push_back(std::move(f));
        }

        // runAfter 只能在 main loop 线程调用 → 从 biz 线程用 runInLoop 转发进去
        ctx->loop->runInLoop([ctx, futs]{
            EventLoop* const lp = ctx->loop;
            lp->runAfter(kCancelAfterSec, [futs]{
                for(auto& f : *futs) f.cancel();
            });
            lp->runAfter(kLateWaitSec, [ctx]{
                const RpcChannelPool::Stats st = ctx->pool->stats();
                ctx->cCbPass.store(ctx->cCb.load() == kCancel && ctx->cCancelled.load() == kCancel);
                ctx->cLatePass.store(st.lateResponses >= static_cast<uint64_t>(kCancel)
                                     && st.cancelled   == static_cast<uint64_t>(kCancel)
                                     && st.inflight    == 0);
                ctx->loop->quit();
            });
        });
    };

    // 看门狗：任一阶段卡住 → 60s 失败退出（否则进程会一直等）
    loop.runAfter(60.0, [ctx]{
        std::cout << "[rpc_load] FAIL: 60s watchdog (aDone=" << ctx->aDone.load()
                  << " bDone=" << ctx->bDone.load() << " cCb=" << ctx->cCb.load() << ")\n";
        ctx->loop->quit();
    });

    pool.start();
    // 等连接建立：Phase A 要测流水线，不是「未连接时挂起 + 重连全量重发」
    std::this_thread::sleep_for(std::chrono::duration<double>(kWarmupSec));

    ctx->phaseA();
    loop.runloop();

    // ---- 收尾：验证 shutdown 后的调用语义（与 rpc_pool_test 同款）----
    // 注意：shutdown() 会把各连接对象销毁，之后 stats() 全为 0 —— 所以汇总快照必须
    // 在 shutdown 之前取，否则最后打印的 pool stats 和 Phase C 的 late 都显示成 0。
    const RpcChannelPool::Stats st = pool.stats();

    pool.shutdown();
    const RpcResult after = pool.call("echo", "after-shutdown").wait();
    const bool shutdownOk = (after.status == RpcStatus::Unavailable) && pool.inflight() == 0;

    const double elapsedSec = static_cast<double>(ctx->aElapsedUs.load()) / 1e6;
    const double qps        = (elapsedSec > 0.0) ? (kPipeline / elapsedSec) : 0.0;

    const bool ok = ctx->aPass.load() && ctx->bPass.load()
                    && ctx->cCbPass.load() && ctx->cLatePass.load() && shutdownOk;
    std::cout << "[rpc_load] " << (ok ? "PASS" : "FAIL") << "\n"
              << "[rpc_load] A pipeline: ok=" << ctx->aOk.load() << "/" << kPipeline
              << " elapsed=" << elapsedSec << "s qps=" << qps
              << " pass=" << ctx->aPass.load() << "\n"
              << "[rpc_load] B backpressure: ok=" << ctx->bOk.load() << "/" << kAcceptedB
              << " exhausted=" << ctx->bExhausted.load() << "/" << (kBackpressure - kAcceptedB)
              << " pass=" << ctx->bPass.load() << "\n"
              << "[rpc_load] C cancel: cb=" << ctx->cCb.load() << "/" << kCancel
              << " cancelled=" << ctx->cCancelled.load()
              << " late=" << st.lateResponses
              << " cbPass=" << ctx->cCbPass.load() << " latePass=" << ctx->cLatePass.load() << "\n"
              << "[rpc_load] pool stats: calls=" << st.calls << " ok=" << st.ok
              << " timeouts=" << st.timeouts << " exhausted=" << st.exhausted
              << " cancelled=" << st.cancelled << " late=" << st.lateResponses
              << " inflight=" << st.inflight << "\n"
              << "[rpc_load] shutdown -> " << rpcStatusName(after.status) << "\n";

    bizThread.quit();
    std::cout << "[rpc_load] exit\n";
    return ok ? 0 : 1;
}
