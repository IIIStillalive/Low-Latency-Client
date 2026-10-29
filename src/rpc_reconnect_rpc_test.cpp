// Day7 断线重连 + 端到端幂等测试（配合 ./build/rpc_server）
//
// 默认模式跑 T1–T4（对着一个不带开关的普通 rpc_server）；--case T5 单独跑 T5
// （服务端要用 --drop-ack 1 独立起，帧计数是进程级的，不能与其他用例共用一个服务端）。
//
//   T1 执行前断  单连接 cut：Ack 已发、业务未执行时服务端断开；重连重发后**恰好执行一次**
//                → Ok + "cut-once"；随后一次 echo 证明重连链路恢复
//   T2 缓存后断  单连接 cutreply：首次执行并缓存响应("1")后断开、不再重放执行；
//                重连重发命中缓存 → "1"；再调 idem 得 "2"（若重放又执行一次会是 3）
//   T3 池内改道  4 连接池(RR)，slot0 断开；断线窗口内 20 echo 全部改道到 slot1..3 且 Ok，
//                slot0 计数保持只有 cut 那 1 次
//   T4 不丢 in-flight  池 + maxInflight=8；断线窗口内 4 sleep(200) 全 Ok、无 ResourceExhausted；
//                窗口内 pool.inflight()>0，全部完成后 ==0
//   T5 drop-ack  独立服务端；echo，deadline 8s → Ok，耗时 ∈ [1.9, 8)（≈ ReliableCodec 2s 重传）
//
// 线程：
//   - main 跑空转 EventLoop：conn(RpcConnection) 的 IO + 看门狗，阻塞到判定完成
//   - ioDriver 线程执行各用例的同步 wait() 序列（conn 的 IO 在 main loop，等它完成）
//   - biz loop 由 EventLoopThread 承载（Future 续体执行现场）
//   - RpcChannelPool 每连接一个 EventLoopThread（池内自建）
//
// 声明顺序：loop → bizThread → conn → pool，析构逆序（连接先拆，池 shutdown 在 biz 之前）。
// 收尾：loop.runloop() 退出后 → conn.stop()（main 即 conn 的 IO 线程，loop 已停）→ bizThread.quit()。
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdlib>   // std::atoi
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <vector>

#include "client/rpc_connection.hpp"
#include "client/rpc_pool.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "server/eventloopthread.hpp"

using namespace server;

namespace {

constexpr double kDeadlineSec = 8.0;   // 断线用例统一用 8s：重连退避最长 5s，5s 默认 deadline 会撞

struct sockaddr_in makePeer(int port){
    struct sockaddr_in p;
    memset(&p, 0, sizeof(p));
    p.sin_family      = AF_INET;
    p.sin_port        = htons(static_cast<uint16_t>(port));
    p.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return p;
}

bool expectOk(const std::string& expect, const RpcResult& r){
    return r.status == RpcStatus::Ok && (expect.empty() || r.payload == expect);
}

// ----------------------------------------------------------------
// T5：drop-ack（独立函数 + 独立服务端）
// ----------------------------------------------------------------
int runT5(const struct sockaddr_in& peer){
    EventLoop       loop;
    EventLoopThread bizThread;
    EventLoop*      biz = bizThread.startLoop();
    RpcConnection   conn(&loop, peer, biz);

    std::atomic<bool> verdictDone{false};
    std::atomic<bool> ok{false};
    std::atomic<int64_t> elapsedMs{0};
    const auto t0 = std::chrono::steady_clock::now();

    auto verdict = [&]{
        if(verdictDone.exchange(true)) return;
        const int64_t ms = elapsedMs.load();
        // ① 帧 1 号被丢 → ReliableCodec 约 2s 后重传同一帧 → 耗时 ≈ 2s；上限留 8s deadline 余量
        ok = (ms >= 1900) && (ms < 8000);
        std::cout << "[rpc_reconnect] T5 " << (ok.load() ? "PASS" : "FAIL")
                  << ": echo elapsed=" << ms << "ms (in [1900,8000))\n";
        loop.quit();
    };

    loop.runAfter(30.0, [&]{
        if(verdictDone.exchange(true)) return;
        std::cout << "[rpc_reconnect] T5 FAIL: watchdog\n";
        loop.quit();
    });

    RpcCallOptions opts; opts.deadlineSec = kDeadlineSec;
    conn.call("echo", "t5", opts).then([&](const RpcResult& r){
        elapsedMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count());
        if(!expectOk("t5", r)){
            std::cout << "[rpc_reconnect] T5 echo bad: " << rpcStatusName(r.status)
                      << " payload=" << r.payload << "\n";
        } else {
            verdict();
        }
    });

    conn.start();
    // 让 TCP 先建好再发：确保我们的 echo 就是服务端的**第 1 个 Request 帧**
    // （探活不能做——探活帧会先消耗掉被丢的帧 1 号）。
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    loop.runloop();

    conn.stop();
    bizThread.quit();
    std::cout << "[rpc_reconnect] T5 exit code=" << (ok.load() ? 0 : 1) << "\n";
    return ok.load() ? 0 : 1;
}

} // namespace

int main(int argc, char** argv){
    int  port    = 8899;
    bool onlyT5  = false;
    for(int i = 1; i < argc; ++i){
        const std::string a = argv[i];
        if(a == "--port" && i + 1 < argc)        port = std::atoi(argv[i+1]);
        else if(a == "--case" && i + 1 < argc)   onlyT5 = (std::string(argv[i+1]) == "T5");
    }

    Logger::instance().set_level(Level::INFO);
    const auto peer = makePeer(port);

    if(onlyT5) return runT5(peer);

    // ---- 测试状态（先于 biz/pool 声明：回调期间它们都得活着）----
    std::atomic<bool> verdictDone{false};
    std::atomic<bool> t1Pass{false}, t2Pass{false}, t3Pass{false}, t4Pass{false};
    std::atomic<bool> t1ProbeOk{false};

    // ---- 声明顺序：loop → bizThread → conn → pool ----
    EventLoop       loop;
    EventLoopThread bizThread;
    EventLoop*      biz = bizThread.startLoop();
    RpcConnection   conn(&loop, peer, biz);              // T1/T2：单连接，IO 在 main loop
    RpcChannelPool  pool(peer, 4, biz, RpcChannelPool::Policy::RoundRobin);
    pool.setMaxInflight(8);                              // T4 要的背压上限

    const size_t kConn = pool.connCount();

    auto verdict = [&]{
        if(verdictDone.exchange(true)) return;
        const bool pass = t1Pass.load() && t1ProbeOk.load() && t2Pass.load()
                          && t3Pass.load() && t4Pass.load();
        std::cout << "[rpc_reconnect] " << (pass ? "PASS" : "FAIL")
                  << ": T1=" << t1Pass.load()
                  << " T1probe=" << t1ProbeOk.load()
                  << " T2=" << t2Pass.load()
                  << " T3=" << t3Pass.load()
                  << " T4=" << t4Pass.load() << "\n";
        loop.quit();
    };

    loop.runAfter(60.0, [&]{
        if(verdictDone.exchange(true)) return;
        std::cout << "[rpc_reconnect] FAIL: 60s watchdog (T1=" << t1Pass.load()
                  << " T2=" << t2Pass.load() << " T3=" << t3Pass.load()
                  << " T4=" << t4Pass.load() << ")\n";
        loop.quit();
    });

    std::thread ioDriver([&]{
        // 等连接建立（探活交给各用例自己，这里只确保 TCP 已建好再发幂等键前置帧）
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        RpcCallOptions opts; opts.deadlineSec = kDeadlineSec;

        // ===== T1：cut —— 执行前断，重连重发后恰好执行一次 =====
        {
            RpcResult r = conn.call("cut", "", opts).wait();
            t1Pass = expectOk("cut-once", r);
            if(!t1Pass){
                std::cout << "[rpc_reconnect] T1 FAIL: " << rpcStatusName(r.status)
                          << " payload=" << r.payload << "\n";
            }
        }
        {
            // 紧接着一次 echo：证明重连链路（含 RpcCodec 重发）已恢复
            RpcResult r = conn.call("echo", "after-cut", opts).wait();
            t1ProbeOk = expectOk("after-cut", r);
        }

        // ===== T2：cutreply —— 缓存后断，重放命中、业务不再执行 =====
        {
            RpcResult cv = conn.call("cutreply", "", opts).wait();   // 首次执行缓存"1"→关连接
            const bool cached = expectOk("1", cv);                   // 重连重发 → 重放"1"
            RpcResult iv = conn.call("idem", "", opts).wait();       // 普通 idem → 该是"2"
            const bool next  = expectOk("2", iv);
            t2Pass = cached && next;
            if(!t2Pass){
                std::cout << "[rpc_reconnect] T2 FAIL: cutreply=" << rpcStatusName(cv.status)
                          << "/" << cv.payload << " idem=" << rpcStatusName(iv.status)
                          << "/" << iv.payload << "\n";
            }
        }

        // ===== T3：池内改道 —— slot0 断线，窗口内新请求全部改道 =====
        {
            // RR 从 rr_=0 起：4 次探活 echo 把游标走满一圈（0,1,2,3），随后 cut 恰好落 slot0。
            for(int i = 0; i < 4; ++i){
                RpcResult pr = pool.call("echo", "warm", opts).wait();
                if(!expectOk("warm", pr)){
                    std::cout << "[rpc_reconnect] T3 FAIL: warmup echo " << i << ": "
                              << rpcStatusName(pr.status) << " payload=" << pr.payload << "\n";
                    loop.quit();
                    return;
                }
            }
            const auto before = [&]{
                std::vector<uint64_t> v;
                for(size_t i = 0; i < kConn; ++i) v.push_back(pool.statsAt(i).calls);
                return v;
            }();

            RpcFuture cutFut = pool.call("cut", "", opts);         // 不 wait：等重连要 ~0.5s+，
            // 这期间 slot0 才处于断线窗口 —— wait 会等到重连完成，窗口就测不到了
            std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 让 available→false 传播

            int echoOk = 0;
            for(int i = 0; i < 20; ++i){
                RpcResult ev = pool.call("echo", "dt", opts).wait();
                if(expectOk("dt", ev)) ++echoOk;
            }
            RpcResult cut = cutFut.wait();
            const bool cutOk = expectOk("cut-once", cut);

            const auto after = [&]{
                std::vector<uint64_t> v;
                for(size_t i = 0; i < kConn; ++i) v.push_back(pool.statsAt(i).calls);
                return v;
            }();
            uint64_t others = 0;
            for(size_t i = 1; i < kConn; ++i) others += after[i] - before[i];
            const bool slot0Only   = (after[0] - before[0] == 1);   // 窗口内 slot0 一个 echo 都没收到
            const bool othersGot20 = (others == 20);
            t3Pass = cutOk && (echoOk == 20) && slot0Only && othersGot20;
            std::cout << "[rpc_reconnect] T3 slot0=" << (after[0]-before[0])
                      << " others=" << others << " cutOk=" << cutOk << "\n";
        }

        // ===== T4：断线窗口内 in-flight 不被清空 =====
        {
            RpcFuture cutFut = pool.call("cut", "", opts);   // 再造一条断线
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::vector<RpcFuture> sleeps;
            sleeps.reserve(4);
            for(int i = 0; i < 4; ++i){
                sleeps.push_back(pool.call("sleep", "200", opts));
            }
            // 窗口内采样：in-flight 应该 >0（4 个 sleep 尚未完成）
            std::this_thread::sleep_for(std::chrono::milliseconds(120));
            const size_t duringInFlight = pool.inflight();

            int sleepOk = 0;
            for(auto& f : sleeps){ if(expectOk("slept:200", f.wait())) ++sleepOk; }
            const bool cutOk = expectOk("cut-once", cutFut.wait());

            const RpcChannelPool::Stats st = pool.stats();
            t4Pass = cutOk && (sleepOk == 4) && st.timeouts == 0 && st.exhausted == 0
                     && duringInFlight > 0 && pool.inflight() == 0;
            std::cout << "[rpc_reconnect] T4 sleepOk=" << sleepOk
                      << " duringInflight=" << duringInFlight
                      << " timeouts=" << st.timeouts
                      << " exhausted=" << st.exhausted << "\n";
        }

        verdict();     // ioDriver 线程：loop.quit() 跨线程安全
    });

    pool.start();      // ioDriver 休眠期间连接已在建
    conn.start();      // 单连接用例（T1/T2）也要发起连接
    loop.runloop();

    ioDriver.join();
    conn.stop();       // 完成残余 in-flight（main 即 conn 的 IO 线程，loop 已停）

    const bool pass = t1Pass.load() && t1ProbeOk.load() && t2Pass.load()
                      && t3Pass.load() && t4Pass.load();
    bizThread.quit();
    std::cout << "[rpc_reconnect] exit code=" << (pass ? 0 : 1) << "\n";
    return pass ? 0 : 1;
}