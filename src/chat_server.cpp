// Day10 聊天室 demo 服务端：TcpServer + ChatServer 业务层（真实业务，闭合 README 未勾项）。
//
// 用法：
//   ./build/chat_server --port 8890 --threads 2    # 2 个 worker（跨 loop 广播）
//   ./build/chat_server --port 8890                # 单 loop（默认；退化形态仍正确）
//
// 协议：LengthHeaderCodec(4B 长度帧) → [1B type][body]（见 include/chat/message_proto.hpp）。
// 线程：setThreadNum(N) → 连接轮询派发到各 worker loop；房间 registry/成员表用互斥锁；
// 广播逐成员 runInLoop 跨 loop 投递（设计见 plan/task_plan_chat_room.md §2）。
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <string>

#include "chat/chat_server.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "server/tcpserver.hpp"

using namespace server;

int main(int argc, char** argv){
    int    port    = 8890;
    size_t threads = 0;                                  // 0 = 单 loop（main loop 承载全部连接）
    for(int i = 1; i < argc; ++i){
        if(std::string(argv[i]) == "--port" && i + 1 < argc)    port    = std::atoi(argv[i+1]);
        if(std::string(argv[i]) == "--threads" && i + 1 < argc) threads = static_cast<size_t>(std::atoi(argv[i+1]));
    }
    Logger::instance().set_level(Level::INFO);

    struct sockaddr_in srv;
    memset(&srv, 0, sizeof(srv));
    srv.sin_family      = AF_INET;
    srv.sin_port        = htons(static_cast<uint16_t>(port));
    srv.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    EventLoop loop;
    TcpServer server(&loop, srv);
    server.setThreadNum(threads);

    chat::ChatServer chat(&server);
    server.setMessageCallback([&](TcpConnection* conn, const std::string& payload){
        chat.onMessage(conn, payload);
    });

    if(!server.start()){
        Logger::instance().log(Level::ERROR, "chat_server start failed");
        return -1;
    }
    Logger::instance().log(Level::INFO, "chat_server listening on 127.0.0.1:", port,
                           " threads=", threads);
    loop.runloop();
    return 0;
}