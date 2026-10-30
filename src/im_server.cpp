// im_server —— v2 类飞书 IM 服务端入口（替代单房聊天室为可交付 IM 业务）。
//
// 用法：
//   ./build/im_server --port 8891 --threads 2 --kv-path /tmp/im_v2.wal
//        # 默认端口 8891（不与 chat_server 8890 抢）；--kv-path 必给（用户/会话/未读/历史全要盘）
//   ./build/im_server --port 8891 --kv-path /tmp/im_v2.wal --sync everysec
//        # --sync always|everysec|no（默认 everysec ≤1s 丢窗，Redis appendfsync 同款）
//
// 协议：LengthHeaderCodec(4B 长度帧) → [1B type][body]（v2 全新型号 0x10–0x9F，见 message_proto.hpp）。
// 线程：setThreadNum(N) → 连接轮询派发到各 worker loop；会话注册表/在线表互斥保护；
//       广播逐接收者 runInLoop 跨 loop 投递（设计见 plan/task_plan_im_v2.md §3）。
// 停机：SIGINT/SIGTERM → runloop 退出 → im.stop()（kv flush+close）→ 历史/游标确定性落盘。
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <string>

#include "im/im_server.hpp"
#include "kv/kv_store.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "server/tcpserver.hpp"

using namespace server;

namespace {                       // 匿名命名空间：全局可达，供 signal handler 使用
    std::atomic<server::EventLoop*> g_loop{nullptr};

    void handleSig(int){          // async-signal-safe：只调 quit()（atomic store + eventfd write，无锁无日志）
        auto* lp = g_loop.load(std::memory_order_relaxed);
        if(lp) lp->quit();
    }
}

int main(int argc, char** argv){
    int    port    = 8891;
    size_t threads = 0;                                  // 0 = 单 loop（main loop 承载全部连接）
    std::string kvPath;
    std::string syncOpt = "everysec";
    for(int i = 1; i < argc; ++i){
        if(std::string(argv[i]) == "--port" && i + 1 < argc)    port    = std::atoi(argv[i+1]);
        if(std::string(argv[i]) == "--threads" && i + 1 < argc) threads = static_cast<size_t>(std::atoi(argv[i+1]));
        if(std::string(argv[i]) == "--kv-path" && i + 1 < argc) kvPath  = argv[i+1];
        if(std::string(argv[i]) == "--sync" && i + 1 < argc)    syncOpt = argv[i+1];
    }
    Logger::instance().set_level(Level::INFO);
    if(kvPath.empty()){
        Logger::instance().log(Level::ERROR, "im_server: --kv-path required (v2 无历史关闭路径，"
                                             "用户/会话/未读/历史全要盘)");
        return -1;
    }

    struct sockaddr_in srv;
    memset(&srv, 0, sizeof(srv));
    srv.sin_family      = AF_INET;
    srv.sin_port        = htons(static_cast<uint16_t>(port));
    srv.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    kv::KvStore kvs;                             // 局部变量名避开命名空间 `kv`
    kv::Wal::SyncMode mode = kv::Wal::SyncMode::EverySec;
    if(syncOpt == "always") mode = kv::Wal::SyncMode::Always;
    else if(syncOpt == "no") mode = kv::Wal::SyncMode::No;
    if(!kvs.open(kvPath, mode)){
        Logger::instance().log(Level::ERROR, "im_server kv.open failed: ", kvPath);
        return -1;
    }
    Logger::instance().log(Level::INFO, "im_server kv open:", kvPath, " sync=", syncOpt);

    EventLoop loop;
    TcpServer server(&loop, srv);
    server.setThreadNum(threads);

    im::ImServer im(&server);
    im.setStore(&kvs);
    server.setMessageCallback([&](TcpConnection* conn, const std::string& payload){
        im.onMessage(conn, payload);
    });

    if(!server.start()){
        Logger::instance().log(Level::ERROR, "im_server start failed");
        return -1;
    }
    Logger::instance().log(Level::INFO, "im_server listening on 127.0.0.1:", port,
                           " threads=", threads, " kv=", kvPath);

    g_loop.store(&loop);          // 先存指针，再装 handler（避免 handler 在 store 前被触发）
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handleSig;
    sigaction(SIGINT,  &sa, nullptr);   // Ctrl+C
    sigaction(SIGTERM, &sa, nullptr);

    loop.runloop();               // 阻塞事件循环；被 SIGINT/SIGTERM 打断后优雅退出
    im.stop();                    // runloop 返回 = 无更多消息回调 → kv flush+close（次序不可反）
    return 0;
}