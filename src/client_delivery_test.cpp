// Day4-B 端到端：业务侧异步投递 —— 慢业务不卡 IO + 有界背压 + FIFO 保序 + 跨线程 sendData
//
// 连接拓扑（单进程共享一个 IO EventLoop；server 全程存活）：
//   loop ── client1(ReliableCodec async) ──▶ server_ack（普通模式，127.0.0.1:8888）
//   loop ── client2(裸 ProtocolCodec)   ──▶ 同上 server（旁路：测 IO 活性）
//   deliveryLoop（EventLoopThread 承载）── ReliableCodec 业务回调的执行现场
//
// 业务回调（delivery 线程）sleep 1s/帧 —— 模拟慢业务：
//   Phase1 背压/保序：client1 连发 5 帧(seq1..5)；业务队列上限 2 → 连发必触发逐出旧帧+LOST；
//                    保留投递的帧序严格递增（FIFO）；delivered∪lost 恰好覆盖 seq1..6；
//                    5 条 Ack 全在 IO 线程收回（pending=0）——IO 不被业务拖累。
//   Phase2 IO 活性：第一个业务回调内（将睡之际）经 loop->runInLoop 挂 +0.2s 定时器，
//                   让 client2 连发 3 帧并测回显时延 → 断言回显 << sleep(1s) 且测速当刻
//                   业务确实在睡（businessSleeping=true）→ 业务没挡 IO。
//   Phase3 跨线程发送：第一个业务回调内调 rel.sendData（业务线程→runInLoop→IO 线程发出），
//                      seq=6 回显投递收回 → 强制 seq6∈delivered。
//
// 判定：全部断言过 → PASS；20s 超时 → FAIL。
// 收尾次序（硬约束）：loop.quit() → rel.stop() → deliveryThread.quit()；
//              声明顺序：rel 在 deliveryThread 之「前」声明 → dtor 逆序先 join 业务线程、后析构 rel ——
//              慢业务回调（睡 1s）全停，rel 才亡；次序颠倒 = 回调返回碰已析构成员（实测 segfault）。
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <set>
#include <thread>

#include "client/protocol.hpp"
#include "client/reliable_codec.hpp"
#include "client/tcp_client.hpp"
#include "net/event_loop.hpp"
#include "net/length_header_codec.hpp"
#include "net/log.hpp"
#include "server/eventloopthread.hpp"
#include "server/tcp_connnection.hpp"   // TcpConnection 完整类型：conn2Ref=shared_from_this() 需要

using namespace server;

int main(){
    Logger::instance().set_level(Level::INFO);

    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family      = AF_INET;
    peer.sin_port        = htons(8888);
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // ---- 观察量（跨线程护栏）----
    const std::thread::id ioThreadId = std::this_thread::get_id();  // main 线程即 IO loop 线程
    std::atomic<bool>      businessSleeping { false };   // delivery 线程写 / IO 线程读
    std::atomic<uint64_t>  bizThreadHash { 0 };          // 业务线程 id 指纹（证明异步生效）
    std::mutex             m;                            // 护栏：delivered/lost 跨线程追加
    std::vector<uint32_t>  delivered;                    // 业务投递成功的 seq（按投递序，单 worker FIFO）
    std::vector<uint32_t>  lost;                         // 队列满逐出的 seq（IO 线程 evict 写）
    bool  phase2Started = false;  bool phase2Done = false;  // 仅 IO 线程读写
    int   echoCount2 = 0;  int64_t maxEchoDeltaUs = 0;      // 仅 IO 线程
    std::map<uint32_t, int64_t> echoT0;                    // client2 回显计时起点（仅 IO 线程）

    // ---- 声明顺序铁律：ReliableCodec 必须先于 EventLoopThread 声明 ----
    //   dtor 逆序：deliveryThread(quit+join 业务线程) → rel。业务 loop 停完且 join 完，rel 才析构；
    //   否则慢业务回调仍在睡 1s 时 rel 先亡，回调返回后访问 rel 已析构成员（deliveryQueue_）= UAF。
    EventLoop loop;
    LengthHeaderCodec codec1, codec2;
    ProtocolCodec proto1(codec1), proto2(codec2);
    ReliableCodec rel(proto1, &loop);

    // 业务 loop 承载（晚声明 = 早析构，先于 rel 的 join 点）；startLoop 须在 setDeliveryLoop 前
    EventLoopThread deliveryThread;
    EventLoop* deliveryLoop = deliveryThread.startLoop();
    rel.setDeliveryLoop(deliveryLoop, 2);    // 有界背压：上限 2，5 帧连发必触发逐出
    rel.setLostCallback([&](uint32_t seq){
        {
            std::lock_guard<std::mutex> lk(m);
            lost.push_back(seq);
        }
        std::cout << "[client1] LOST(delivery) seq=" << seq << "\n";
    });

    // 慢业务回调：[delivery loop 线程] 记录 + 睡 1s/帧
    bool phase3Sent = false;                 // 仅 delivery 线程写
    std::function<void()> triggerPhase2;     // 前向声明：业务回调运行时调用（届时已赋值）
    rel.setMessageCallback([&](TcpConnection* conn, MsgType t, uint32_t seq, const std::string& body) {
        if(t != MsgType::Data) return;
        const std::thread::id tid = std::this_thread::get_id();
        if(bizThreadHash.load() == 0) bizThreadHash.store(std::hash<std::thread::id>{}(tid));
        {
            std::lock_guard<std::mutex> lk(m);
            delivered.push_back(seq);
        }
        std::cout << "[client1 biz] seq=" << seq << " body=\"" << body
                  << "\" thread=" << tid << (tid == ioThreadId ? " **ON IO THREAD(bad)**" : "") << "\n";
        if(!phase3Sent){                       // Phase3：业务线程跨线程发 seq6（runInLoop 转 IO）
            phase3Sent = true;
            rel.sendData(conn, "biz", 3);
            // Phase2 触发钩：业务线程不能直接 pump 非线程安全 API → 经 IO loop runInLoop 挂定时器
            loop.runInLoop([&]{ loop.runAfter(0.2, triggerPhase2); });
        }
        businessSleeping = true;
        std::this_thread::sleep_for(std::chrono::seconds(1));   // 模拟慢业务
        businessSleeping = false;
    });

    // ---- client1：可靠链路（async）----
    bool firstConnected = true;                // 仅 IO 线程写
    TcpClient client1(&loop, peer);
    client1.setConnectionCallback([&](TcpConnection* c){
        if(c){
            if(firstConnected){
                firstConnected = false;
                rel.onConnected(c);
                for(int i = 0; i < 5; ++i) rel.sendData(c, "m", 1);   // seq 1..5 连发
            }
        } else {
            rel.onDisconnected();
        }
    });
    client1.setMessageCallback([&](TcpConnection* c, const std::string& p){ proto1.onMessage(c, p); });
    proto1.setMessageCallback([&](TcpConnection* c, MsgType t, uint32_t s, const std::string& b){
        rel.onFrame(c, t, s, b);
    });

    // ---- client2：旁路 IO 活性探针（裸 ProtocolCodec，不吃 ReliableCodec）----
    std::shared_ptr<TcpConnection> conn2Ref;   // 仅 IO 线程读写（连接回调 + triggerPhase2 同线程）
    TcpClient client2(&loop, peer);
    client2.setConnectionCallback([&](TcpConnection* c){
        if(c) conn2Ref = c->shared_from_this();
        else  conn2Ref.reset();
    });
    client2.setMessageCallback([&](TcpConnection* c, const std::string& p){ proto2.onMessage(c, p); });

    // Phase2：业务睡期间，client2 发 3 帧测回显时延（全部在 IO 线程）
    int phase2Retries = 0;
    triggerPhase2 = [&]{
        if(phase2Started) return;
        if(!conn2Ref){
            if(++phase2Retries > 15){
                phase2Done = true;
                std::cout << "[client2] FAIL: phase2 could not start (conn2 never connected)\n";
                return;
            }
            loop.runAfter(0.1, [&]{ triggerPhase2(); });
            return;
        }
        phase2Started = true;
        const int64_t t0 = nowMicros();
        for(uint32_t s = 210; s < 213; ++s){
            echoT0[s] = t0;
            proto2.sendFrame(conn2Ref.get(), MsgType::Data, s, "x", 1);
        }
        std::cout << "[client2] phase2: 3 frames sent while business sleeping...\n";
    };
    proto2.setMessageCallback([&](TcpConnection*, MsgType t, uint32_t seq, const std::string& b){
        if(t != MsgType::Data) return;
        auto it = echoT0.find(seq);
        if(it == echoT0.end()) return;                    // 非 phase2 帧：忽略
        const int64_t deltaUs = nowMicros() - it->second;
        if(deltaUs > maxEchoDeltaUs) maxEchoDeltaUs = deltaUs;
        if(++echoCount2 == 3){
            phase2Done = true;
            std::cout << "[client2] phase2 echoes done: maxRttUs=" << maxEchoDeltaUs
                      << ", businessSleepingWhileMeasuring=" << businessSleeping.load() << "\n";
        }
    });

    // ---- 完成判定（IO 线程轮询）----
    auto check = std::make_shared<std::function<void()>>();
    auto pollsLeft = std::make_shared<int>(200);         // 0.1s × 200 = 20s
    *check = [&, check, pollsLeft]() mutable {
        if(--*pollsLeft <= 0){
            std::cout << "[client1] FAIL: timeout pending=" << rel.pendingCount()
                      << " delivered=" << delivered.size() << " lost=" << lost.size() << "\n";
            loop.quit();
            return;
        }
        const bool ph2 = phase2Done && maxEchoDeltaUs < 600000 &&
                         businessSleeping.load();  // 回显 <600ms 且测速当刻业务确实在睡
        const bool pendingOk = rel.pendingCount() == 0;
        {
            std::lock_guard<std::mutex> lk(m);
            const bool done = (delivered.size() + lost.size() >= 6) && pendingOk && ph2;
            if(done){
                std::set<uint32_t> cover;  cover.insert(lost.begin(), lost.end());
                bool fifo = true; uint32_t prev = 0; bool seq6Delivered = false;
                for(uint32_t s : delivered){
                    if(s <= prev) fifo = false;
                    prev = s;
                    cover.insert(s);
                    if(s == 6) seq6Delivered = true;
                }
                const bool coverOk = cover.size() == 6 && cover.count(1)&&cover.count(2)&&cover.count(3)
                                                      && cover.count(4)&&cover.count(5)&&cover.count(6);
                const bool backpressure = !lost.empty();
                const bool asyncOk = (bizThreadHash.load() != std::hash<std::thread::id>{}(ioThreadId));
                const bool ok = fifo && coverOk && backpressure && seq6Delivered && asyncOk && pendingOk;
                std::cout << "[client1] " << (ok ? "PASS" : "FAIL") << ":"
                          << " fifo=" << fifo << " cover=" << coverOk
                          << " backpressure(lost)=" << lost.size()
                          << " seq6Delivered=" << seq6Delivered
                          << " async=" << asyncOk
                          << " delivered=[";
                for(uint32_t s : delivered) std::cout << s << " ";
                std::cout << "] lost=[";
                for(uint32_t s : lost) std::cout << s << " ";
                std::cout << "] pending=" << rel.pendingCount()
                          << " maxBizRttUs=" << maxEchoDeltaUs << "\n";
                loop.quit();
                return;
            }
        }
        loop.runAfter(0.1, *check);
    };
    loop.runAfter(0.1, *check);
    loop.runAfter(20.0, [&]{ std::cout << "[client1] FAIL: 20s watchdog\n"; loop.quit(); });

    client1.connect();
    client2.connect();
    loop.runloop();

    // ---- 收尾：顺序不可乱（见文件头硬约束）----
    rel.stop();
    deliveryThread.quit();
    std::cout << "[client1] delivery test exit\n";
    return 0;
}