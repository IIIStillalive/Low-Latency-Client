// Day6 RPC 连接池端到端测试（配合 ./build/rpc_server）
//
// Phase1 4 连接 × 100 并发（LeastInflight）：400 请求全部 Ok，每条连接都被使用
// Phase2 切 RoundRobin 再发 40 个：验证策略切换 + 严格轮转（每条连接 +10）
// Phase3 收尾时序：shutdown() 后 call() 立即 Unavailable，in-flight 归零，进程干净退出
//
// 线程：
//   - 每连接一个 IO 线程（池内自建），4 条连接互不共享可变状态
//   - biz loop 由 EventLoopThread 承载，全池共享（Future 续体执行现场）
//   - main 线程只跑一个空转 EventLoop：承载看门狗并阻塞到判定完成
//
// 声明顺序：loop → bizThread → pool
//   → 析构逆序 pool 先收尾、再停 biz loop、最后 loop（续体存活期内 biz 必须还在）。
#include <arpa/inet.h>
#include <atomic>
#include <cstdlib>   // std::atoi
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <vector>

#include "client/rpc_pool.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "server/eventloopthread.hpp"

using namespace server;

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

    // ---- 测试状态（必须先于 biz/pool 声明：回调与收尾期间它们都要活着）----
    std::atomic<int>  manyOk{0}, manyDone{0}, rrOk{0}, rrDone{0};
    std::atomic<bool> phase1Pass{false}, phase2Pass{false};
    std::atomic<bool> verdictDone{false}, allPass{false};

    // ---- 声明顺序：loop → bizThread → pool ----
    EventLoop      loop;
    EventLoopThread bizThread;
    EventLoop*     biz = bizThread.startLoop();
    RpcChannelPool pool(peer, 4, biz, RpcChannelPool::Policy::LeastInflight);

    const int kConn = static_cast<int>(pool.connCount());
    const int kMany = kConn * 100;   // Phase1：每连接 100 并发
    const int kRr   = kConn * 10;    // Phase2：每连接 10 次，严格轮转

    std::vector<uint64_t> before(static_cast<size_t>(kConn), 0);   // Phase2 前的 calls 快照

    auto verdict = [&]{
        if(verdictDone.exchange(true)) return;

        const RpcChannelPool::Stats st = pool.stats();
        std::vector<uint64_t> per(static_cast<size_t>(kConn), 0);
        std::string dist;
        int used = 0;
        bool rrExact = true;
        for(int i = 0; i < kConn; ++i){
            per[i] = pool.statsAt(static_cast<size_t>(i)).calls;
            if(per[i] > 0) ++used;
            if(per[i] - before[i] != static_cast<uint64_t>(kRr / kConn)) rrExact = false;
            dist += " " + std::to_string(per[i]);
        }

        const bool ok = phase1Pass.load() && phase2Pass.load()
                        && used == kConn && rrExact
                        && st.calls == static_cast<uint64_t>(kMany + kRr)
                        && st.ok    == static_cast<uint64_t>(kMany + kRr)
                        && st.inflight == 0;
        allPass = ok;

        std::cout << "[rpc_pool] " << (ok ? "PASS" : "FAIL")
                  << ": phase1=" << phase1Pass.load()
                  << " phase2=" << phase2Pass.load()
                  << " used=" << used << "/" << kConn
                  << " rrExact=" << rrExact
                  << " | stats calls=" << st.calls << " ok=" << st.ok
                  << " timeouts=" << st.timeouts << " inflight=" << st.inflight << "\n"
                  << "[rpc_pool] calls per conn:" << dist << "\n";
        loop.quit();
    };

    // Phase1：400 个并发 echo（LeastInflight 选连接）
    for(int i = 0; i < kMany; ++i){
        const std::string expect = "p" + std::to_string(i);
        pool.call("echo", expect).then([&, expect](const RpcResult& r){
            if(r.status == RpcStatus::Ok && r.payload == expect) manyOk.fetch_add(1);
            if(manyDone.fetch_add(1) + 1 != kMany) return;

            phase1Pass = (manyOk.load() == kMany);

            // ---- Phase2：切 RoundRobin，快照后每连接应恰好 +10 ----
            for(int k = 0; k < kConn; ++k){
                before[static_cast<size_t>(k)] = pool.statsAt(static_cast<size_t>(k)).calls;
            }
            pool.setPolicy(RpcChannelPool::Policy::RoundRobin);

            for(int j = 0; j < kRr; ++j){
                const std::string expect2 = "rr" + std::to_string(j);
                pool.call("echo", expect2).then([&, expect2](const RpcResult& rr){
                    if(rr.status == RpcStatus::Ok && rr.payload == expect2) rrOk.fetch_add(1);
                    if(rrDone.fetch_add(1) + 1 != kRr) return;

                    phase2Pass = (rrOk.load() == kRr);
                    verdict();
                });
            }
        });
    }

    // 看门狗：30s 未判定 → FAIL
    loop.runAfter(30.0, [&]{
        if(verdictDone.exchange(true)) return;
        std::cout << "[rpc_pool] FAIL: 30s watchdog (manyDone=" << manyDone.load()
                  << " rrDone=" << rrDone.load() << ")\n";
        loop.quit();
    });

    pool.start();
    loop.runloop();

    // ---- Phase3：收尾时序（本测试真正要验证的 D2 部分）----
    const RpcChannelPool::Stats stBefore = pool.stats();
    pool.shutdown();

    const RpcResult after = pool.call("echo", "after-shutdown").wait();
    const bool shutdownOk = (after.status == RpcStatus::Unavailable)
                            && stBefore.inflight == 0
                            && pool.inflight() == 0;
    std::cout << "[rpc_pool] " << (shutdownOk ? "PASS" : "FAIL")
              << ": shutdown -> " << rpcStatusName(after.status)
              << " (inflight_before=" << stBefore.inflight << ")\n";

    bizThread.quit();
    std::cout << "[rpc_pool] pool test exit\n";
    return (allPass.load() && shutdownOk) ? 0 : 1;
}
