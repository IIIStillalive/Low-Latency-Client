// Day10 聊天室 demo 服务端：TcpServer + ChatServer 业务层（真实业务，闭合 README 未勾项）。
//
// 用法：
//   ./build/chat_server --port 8890 --threads 2                  # 2 个 worker（跨 loop 广播）
//   ./build/chat_server --port 8890                              # 单 loop（默认；退化形态仍正确）
//   ./build/chat_server --port 8890 --kv-path /tmp/chat.wal --sync everysec
//        # 历史持久化（kv-engine 回接）：--kv-path 空=历史关闭，Day10 原样；
//        # --sync always|everysec|no（默认 everysec ≤1s 丢窗，Redis appendfsync 同款）
//
// 协议：LengthHeaderCodec(4B 长度帧) → [1B type][body]（见 include/chat/message_proto.hpp）。
// 线程：setThreadNum(N) → 连接轮询派发到各 worker loop；房间 registry/成员表用互斥锁；
// 广播逐成员 runInLoop 跨 loop 投递（设计见 plan/task_plan_chat_room.md §2）。
// 停机：SIGINT/SIGTERM → runloop 退出 → chat.stop()（kv flush+close）→ 历史确定性落盘（G7）。
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <string>

#include "chat/chat_server.hpp"
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
    int    port    = 8890;
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

    struct sockaddr_in srv;
    memset(&srv, 0, sizeof(srv));
    srv.sin_family      = AF_INET;
    srv.sin_port        = htons(static_cast<uint16_t>(port));
    srv.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // 历史持久化（kv-engine 回接）：--kv-path 非空 → open（失败即退出）；空 → 历史关闭。
    kv::KvStore kvs;                             // 局部变量名避开命名空间 `kv`（否则 kv::Wal 被遮蔽）
    kv::Wal::SyncMode mode = kv::Wal::SyncMode::EverySec;
    if(syncOpt == "always") mode = kv::Wal::SyncMode::Always;
    else if(syncOpt == "no") mode = kv::Wal::SyncMode::No;
    if(!kvPath.empty() && !kvs.open(kvPath, mode)){
        Logger::instance().log(Level::ERROR, "chat_server kv.open failed: ", kvPath);
        return -1;
    }

    EventLoop loop;
    TcpServer server(&loop, srv);
    server.setThreadNum(threads);

    chat::ChatServer chat(&server);
    if(kvs.is_open()) chat.setHistory(&kvs);
    server.setMessageCallback([&](TcpConnection* conn, const std::string& payload){
        chat.onMessage(conn, payload);
    });

    if(!server.start()){
        Logger::instance().log(Level::ERROR, "chat_server start failed");
        return -1;
    }
    Logger::instance().log(Level::INFO, "chat_server listening on 127.0.0.1:", port,
                           " threads=", threads, kvs.is_open() ? " kv=" + kvPath : " kv=off",
                           " sync=", syncOpt);

    g_loop.store(&loop);          // 先存指针，再装 handler（避免 handler 在 store 前被触发）
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handleSig;
    sigaction(SIGINT,  &sa, nullptr);   // Ctrl+C
    sigaction(SIGTERM, &sa, nullptr);

    loop.runloop();               // 阻塞事件循环；被 SIGINT/SIGTERM 打断后优雅退出
    chat.stop();                  // runloop 返回 = 无更多消息回调 → kv flush+close（次序不可反）
    return 0;
}