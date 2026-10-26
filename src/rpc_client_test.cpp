// Day5 RPC 单连接端到端测试（配合 ./build/rpc_server）
//
// 覆盖：
//   Phase1 单次调用      echo("hello")                         → Ok + payload 原样
//   Phase2 乱序响应      sleep(300ms) 先发、echo 后发          → echo 先完成（order=[echo,sleep]）
//   Phase3 本地超时      sleep(1000ms) + deadline=0.2s         → DeadlineExceeded，耗时 ≈ 200ms
//   Phase4 服务端错误    fail("boom")                          → AppError + error="boom"
//   Phase5 并发多路复用  100 个 echo 同时在飞                  → 全部 Ok 且 payload 一一对应
//
// 线程：main 线程跑 IO loop；EventLoopThread 承载 biz loop（Future 续体执行现场）。
// 声明顺序：测试状态 → EventLoop → EventLoopThread → RpcConnection（析构逆序：连接先拆、
// 再 join 业务线程、最后 loop），保证业务回调存活期内它引用的状态都还在。
//
// 收尾：loop.runloop() 退出后 → conn.stop()（完成残余 in-flight）→ bizThread.quit()。
#include <arpa/inet.h>
#include <atomic>
#include <cstdlib>   // std::atoi
#include <cstring>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <vector>

#include "client/rpc_connection.hpp"
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

    // ---- 测试状态（必须先于 biz 线程声明：回调还在跑时它们必须活着）----
    std::mutex               m;
    std::vector<std::string> order;          // Phase2 完成顺序
    std::atomic<bool>        p1{false}, p3{false}, p4{false};
    std::atomic<int>         manyOk{0}, manyDone{0};
    std::atomic<int64_t>     timeoutUs{0};
    std::atomic<bool>        verdictDone{false};

    // ---- 声明顺序：loop → bizThread → conn ----
    EventLoop      loop;
    EventLoopThread bizThread;
    EventLoop*     biz = bizThread.startLoop();
    RpcConnection  conn(&loop, peer, biz);

    // 最终判定（biz 线程；quit 跨线程安全）
    auto verdict = [&]{
        if(verdictDone.exchange(true)) return;
        bool ph2;
        {
            std::lock_guard<std::mutex> lk(m);
            ph2 = (order.size() == 2 && order[0] == "echo" && order[1] == "sleep");
            std::cout << "[rpc] completion order:";
            for(const auto& s : order) std::cout << " " << s;
            std::cout << "\n";
        }
        const bool ok = p1.load() && ph2 && p3.load() && p4.load()
                        && manyDone.load() == 100 && manyOk.load() == 100;
        const RpcCodec::Stats st = conn.stats();
        std::cout << "[rpc] " << (ok ? "PASS" : "FAIL")
                  << ": singleCall=" << p1.load()
                  << " outOfOrder=" << ph2
                  << " timeout=" << p3.load() << "(us=" << timeoutUs.load() << ")"
                  << " appError=" << p4.load()
                  << " concurrent=" << manyOk.load() << "/" << manyDone.load()
                  << " | stats calls=" << st.calls << " ok=" << st.ok
                  << " timeouts=" << st.timeouts << " late=" << st.lateResponses
                  << " inflight=" << st.inflight << "\n";
        loop.quit();
    };

    // 所有条件齐了才判定（Phase2 的 sleep 响应可能晚于 Phase5 完成）
    auto maybeVerdict = [&]{
        if(!p1.load() || !p3.load() || !p4.load()) return;
        if(manyDone.load() < 100) return;
        {
            std::lock_guard<std::mutex> lk(m);
            if(order.size() < 2) return;
        }
        verdict();
    };

    // Phase1：单次调用
    conn.call("echo", "hello").then([&](const RpcResult& r){
        p1 = (r.status == RpcStatus::Ok && r.payload == "hello");
        if(!p1) std::cout << "[rpc] phase1 FAIL: status=" << rpcStatusName(r.status)
                          << " payload=\"" << r.payload << "\" error=\"" << r.error << "\"\n";

        // Phase2：先发慢请求，再发快请求 → 快请求必须先进 order
        conn.call("sleep", "300", RpcCallOptions{2.0, false}).then([&](const RpcResult& rs){
            {
                std::lock_guard<std::mutex> lk(m);
                order.push_back("sleep");
            }
            if(rs.status != RpcStatus::Ok || rs.payload != "slept:300"){
                std::cout << "[rpc] phase2 sleep unexpected: " << rpcStatusName(rs.status)
                          << " payload=\"" << rs.payload << "\"\n";
            }
            maybeVerdict();
        });

        conn.call("echo", "after-sleep").then([&](const RpcResult& re){
            {
                std::lock_guard<std::mutex> lk(m);
                order.push_back("echo");
            }
            if(re.status != RpcStatus::Ok || re.payload != "after-sleep"){
                std::cout << "[rpc] phase2 echo unexpected: " << rpcStatusName(re.status) << "\n";
            }

            // Phase3：超时（此时 Phase2 的 sleep 仍在飞，互不干扰）
            const int64_t t0 = nowMicros();
            conn.call("sleep", "1000", RpcCallOptions{0.2, false}).then([&, t0](const RpcResult& rt){
                timeoutUs = nowMicros() - t0;
                p3 = (rt.status == RpcStatus::DeadlineExceeded
                      && timeoutUs.load() > 120000 && timeoutUs.load() < 900000);
                if(!p3) std::cout << "[rpc] phase3 FAIL: status=" << rpcStatusName(rt.status)
                                  << " us=" << timeoutUs.load() << "\n";

                // Phase4：服务端业务错误
                conn.call("fail", "boom").then([&](const RpcResult& rf){
                    p4 = (rf.status == RpcStatus::AppError && rf.error == "boom");
                    if(!p4) std::cout << "[rpc] phase4 FAIL: status=" << rpcStatusName(rf.status)
                                      << " error=\"" << rf.error << "\"\n";

                    // Phase5：100 个请求同时在飞（单连接多路复用）
                    for(int i = 0; i < 100; ++i){
                        const std::string expect = "p" + std::to_string(i);
                        conn.call("echo", expect).then([&, expect](const RpcResult& rr){
                            if(rr.status == RpcStatus::Ok && rr.payload == expect) manyOk.fetch_add(1);
                            if(manyDone.fetch_add(1) + 1 == 100) maybeVerdict();
                        });
                    }
                });
            });
        });
    });

    // 看门狗：30s 未判定 → FAIL
    loop.runAfter(30.0, [&]{
        if(verdictDone.exchange(true)) return;
        size_t orderN;
        {
            std::lock_guard<std::mutex> lk(m);
            orderN = order.size();
        }
        std::cout << "[rpc] FAIL: 30s watchdog (manyDone=" << manyDone.load()
                  << " order=" << orderN << ")\n";
        loop.quit();
    });

    conn.start();
    loop.runloop();

    // 收尾（顺序不可乱）：先完成残余 in-flight，再停业务 loop
    conn.stop();
    bizThread.quit();
    std::cout << "[rpc] client test exit\n";
    return 0;
}
