// 端到端回显演示：三层一次通
//   TcpClient(transport) → LengthHeaderCodec(切帧) → ProtocolCodec(5B 私有头) → 业务
// 依赖：server 在 127.0.0.1:8888 监听（./build/server，回显业务）。
// 流程：连接成功 → 发一条 MSG_DATA("hello im") → server 原样回显整个 payload →
//       客户端剥 5B 私有头 → 打印 body → 退出。
#include <arpa/inet.h>   // htons / htonl
#include <cstring>       // memset
#include <iostream>
#include <netinet/in.h>

#include "net/event_loop.hpp"
#include "net/length_header_codec.hpp"
#include "net/log.hpp"
#include "client/protocol.hpp"
#include "client/tcp_client.hpp"

using namespace server;

int main(){
    Logger::instance().set_level(Level::INFO);

    // 服务器地址：与 server 监听一致的本地回环地址
    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family      = AF_INET;
    peer.sin_port        = htons(8888);
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    EventLoop loop;
    TcpClient client(&loop, peer);

    // 业务层组装链路：协议层(ProtocolCodec) 需要一个 LengthHeaderCodec 引用端。
    // 注：TcpClient 内部也持有一把同样的无状态切帧刀（负责收侧传输分帧），
    // 业务层这一把只作为 ProtocolCodec 的编发送链路挂接点 —— 收侧分帧与业务编帧语义一致、互不冲突。
    LengthHeaderCodec codec;
    ProtocolCodec proto(codec);
    client.setMessageCallback([&](TcpConnection* c, const std::string& payload){
        proto.onMessage(c, payload);          // 剥 5B 私有头 → 业务回调
    });
    proto.setMessageCallback([&](TcpConnection*, MsgType type, uint32_t seq, const std::string& body){
        std::cout << "[client] recv type=" << static_cast<int>(type)
                  << " seq=" << seq << " body=\"" << body << "\"\n";
        if(type == MsgType::Data){
            std::cout << "[client] echo received, quit\n";
            loop.quit();
        }
    });

    client.setConnectionCallback([&](TcpConnection* c){
        if(c){
            std::cout << "[client] connected, send hello\n";
            proto.sendFrame(c, MsgType::Data, 1, "hello im", 8);   // 连接成功 → 发一条
        } else {
            std::cout << "[client] connection lost/failed, quit\n";
            loop.quit();
        }
    });

    client.connect();
    loop.runloop();
    return 0;
}