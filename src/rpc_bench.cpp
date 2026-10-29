// Day8 压测基准：N 线程 × M 请求 → QPS + p50/p99/p999 + max（配合 ./build/rpc_server）
//
// 两种模式（--mode）：
//   sync     每线程「发一个 → wait → 再发」，in-flight ≈ 线程数，测端到端延迟（单请求往返）。
//   pipeline 每线程先全量发出（多路复用），再逐个 wait，in-flight ≈ M×T，测吞吐。
//
// 对比矩阵（Day8 要求，--conns 参数跑两遍）：
//   --conns 1   单连接 = 单 loop / 单 reactor（基线）
//   --conns 4   连接池 multi-reactor（每连接一个 EventLoopThread）
//
// 输出：total/ok/exhausted/timeouts → wall → QPS → p50/p90/p99/p999/max（µs）。
//
// 用法：
//   ./build/rpc_server --port 8899 &
//   ./build/rpc_bench --conns 4 --threads 4 --reqs 10000 --mode pipeline
//   ./build/rpc_bench --conns 1 --threads 4 --reqs 10000 --mode sync
//
// 捕获纪律（沿用 load test）：工作线程内每线程局部累积，主线程 join 后聚合；
// 跨线程只共享原子计数 + put 进受互斥保护的总延迟向量。
#include <arpa/inet.h>
#include <algorithm>     // std::sort
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>       // std::atoi
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <tuple>
#include <utility>       // std::pair
#include <vector>

#include "client/rpc_pool.hpp"
#include "net/log.hpp"
#include "server/eventloopthread.hpp"

using namespace server;

namespace {

inline long long nowNs(){
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct Ctx {
    std::atomic<int>                  done{0};
    std::atomic<bool>                 go{false};
    std::atomic<long long>            ok{0}, exhausted{0}, timeouts{0}, other{0};
    std::mutex                        m;
    std::vector<long long>            lat;     // μs，全线程聚合
};

// nearest-rank 百分位（函数内排序，不改调用方容器）
long long percentileOf(std::vector<long long> v, double frac){
    if(v.empty()) return 0;
    std::sort(v.begin(), v.end());
    const size_t idx = static_cast<size_t>(frac * static_cast<double>(v.size()));
    return v[idx < v.size() ? idx : v.size() - 1];
}

void printLatency(const std::vector<long long>& lat){
    if(lat.empty()){ std::cout << "  (no latency samples)\n"; return; }
    std::cout << "  p50="   << percentileOf(lat, 0.50)
              << "us  p90=" << percentileOf(lat, 0.90)
              << "us  p99=" << percentileOf(lat, 0.99)
              << "us  p999=" << percentileOf(lat, 0.999)
              << "us  max=" << *std::max_element(lat.begin(), lat.end())
              << "us  (" << lat.size() << " samples)\n";
}

} // namespace

int main(int argc, char** argv){
    bool      sync    = false;
    int       conns   = 4;
    int       threads = 4;
    int       reqs    = 10000;      // 每线程请求数
    int       port    = 8899;
    long long dlSec   = 30;         // 压测给足余量，不误伤超时

    for(int i = 1; i + 1 < argc; ++i){
        const std::string a = argv[i];
        if(a == "--mode")         sync    = (std::string(argv[i+1]) == "sync");
        if(a == "--conns")        conns   = std::atoi(argv[i+1]);
        if(a == "--threads")      threads = std::atoi(argv[i+1]);
        if(a == "--reqs")         reqs    = std::atoi(argv[i+1]);
        if(a == "--port")         port    = std::atoi(argv[i+1]);
        if(a == "--deadline-sec") dlSec   = std::atoll(argv[i+1]);
    }
    if(threads < 1 || reqs < 1 || conns < 1){
        std::cout << "usage: rpc_bench [--mode sync|pipeline] [--conns C] [--threads T]"
                  << " [--reqs M] [--port P] [--deadline-sec S]\n";
        return 1;
    }

    Logger::instance().set_level(Level::WARN);   // 压测只看数字不刷 INFO

    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family      = AF_INET;
    peer.sin_port        = htons(static_cast<uint16_t>(port));
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // biz loop 承载 Future 续体；pool 自管 N 条 IO 线程（每连接一条）
    EventLoopThread bizThread;
    EventLoop*      biz = bizThread.startLoop();
    RpcChannelPool  pool(peer, static_cast<size_t>(conns), biz,
                         RpcChannelPool::Policy::RoundRobin);
    // in-flight 闸放到足够大：pipeline 最坏 per-conn = threads*reqs/conns
    pool.setMaxInflight(static_cast<size_t>(reqs) * static_cast<size_t>(threads) + 256);

    auto ctx = std::make_shared<Ctx>();

    std::vector<std::thread> workers;
    workers.reserve(threads);
    for(int t = 0; t < threads; ++t){
        workers.emplace_back([ctx, t, &pool, sync, reqs, dlSec]{
            long long tOk = 0, tExh = 0, tTo = 0, tOther = 0;
            std::vector<long long> mine;
            mine.reserve(static_cast<size_t>(reqs));

            RpcCallOptions opts;
            opts.deadlineSec = static_cast<double>(dlSec);
            opts.idempotent  = true;   // echo 幂等：安全兜底（本压测不注入断线）

            while(!ctx->go.load(std::memory_order_relaxed)){}   // 齐跑门闩

            if(sync){
                for(int i = 0; i < reqs; ++i){
                    const std::string expect = "s" + std::to_string(t) + "_" + std::to_string(i);
                    const long long   begin = nowNs();
                    RpcFuture f = pool.call("echo", expect, opts);
                    const RpcResult res = f.wait();      // 阻塞：来一回，记一回
                    const long long latUs = (nowNs() - begin) / 1000;
                    if(res.status == RpcStatus::Ok && res.payload == expect){
                        ++tOk; mine.push_back(latUs);
                    } else if(res.status == RpcStatus::ResourceExhausted) ++tExh;
                    else if(res.status == RpcStatus::DeadlineExceeded)   ++tTo;
                    else ++tOther;
                }
            } else {
                // pipeline：先全量发出（记发起点），再逐个 wait
                std::vector<std::tuple<RpcFuture, long long, std::string>> issued;
                issued.reserve(static_cast<size_t>(reqs));
                for(int i = 0; i < reqs; ++i){
                    const std::string expect = "p" + std::to_string(t) + "_" + std::to_string(i);
                    issued.emplace_back(pool.call("echo", expect, opts), nowNs(), expect);
                }
                for(auto& it : issued){
                    RpcFuture&       f     = std::get<0>(it);
                    const long long  begin = std::get<1>(it);
                    const std::string& e   = std::get<2>(it);
                    const RpcResult  res = f.wait();
                    const long long latUs = (nowNs() - begin) / 1000;
                    if(res.status == RpcStatus::Ok && res.payload == e){
                        ++tOk; mine.push_back(latUs);
                    } else if(res.status == RpcStatus::ResourceExhausted) ++tExh;
                    else if(res.status == RpcStatus::DeadlineExceeded)   ++tTo;
                    else ++tOther;
                }
            }

            ctx->ok.fetch_add(tOk);
            ctx->exhausted.fetch_add(tExh);
            ctx->timeouts.fetch_add(tTo);
            ctx->other.fetch_add(tOther);
            {
                std::lock_guard<std::mutex> lk(ctx->m);
                ctx->lat.insert(ctx->lat.end(), mine.begin(), mine.end());
            }
            ctx->done.fetch_add(1);
        });
    }

    pool.start();                              // 发起 TCP 连接
    // 预热：连接建立前的请求会 park 进 pending_、重连时全量重发 —— 那不是本压测要测的语义。
    // 与 load test 同款纪律：先等连接就绪，再开始计时（不预热会把握手+首帧塞进 QPS）。
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    for(int w = 0; w < conns; ++w){
        pool.call("echo", "warmup", RpcCallOptions{}).wait();   // 保证至少一条全链路走通
    }
    const auto tWall0 = std::chrono::steady_clock::now();
    ctx->go.store(true);                       // 齐跑

    for(auto& th : workers) th.join();         // 等全部跑完
    const auto tWall1 = std::chrono::steady_clock::now();

    const double wallSec = std::chrono::duration<double>(tWall1 - tWall0).count();
    const long long total = static_cast<long long>(threads) * reqs;
    const long long okAll = ctx->ok.load();
    const double qps = (wallSec > 0.0) ? (static_cast<double>(okAll) / wallSec) : 0.0;

    const std::vector<long long> lat = [&]{ std::lock_guard<std::mutex> lk(ctx->m);
                                            return ctx->lat; }();

    std::cout << "[rpc_bench] conns=" << conns << " threads=" << threads
              << " reqs/thread=" << reqs << " mode=" << (sync ? "sync" : "pipeline") << "\n"
              << "  total=" << total
              << "  ok=" << okAll
              << "  exhausted=" << ctx->exhausted.load()
              << "  timeouts=" << ctx->timeouts.load()
              << "  other=" << ctx->other.load()
              << "  wall=" << wallSec << "s  qps=" << qps << "\n";
    printLatency(lat);

    const RpcChannelPool::Stats st = pool.stats();    // shutdown 前快照
    pool.shutdown();
    bizThread.quit();

    std::cout << "  pool stats: calls=" << st.calls << " ok=" << st.ok
              << " timeouts=" << st.timeouts << " exhausted=" << st.exhausted
              << " late=" << st.lateResponses << " inflight=" << st.inflight << "\n";

    return (okAll == total && ctx->exhausted.load() == 0 && ctx->timeouts.load() == 0
            && ctx->other.load() == 0) ? 0 : 1;
}