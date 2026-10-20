// Day3 端到端可靠链路演示：TcpClient → LengthHeaderCodec(切帧) → ProtocolCodec(5B头) → ReliableCodec(可靠) → 业务
//
// 依赖：./build/server_ack 在 127.0.0.1:8888 运行（收 Data 回 Ack + 回显 body）。
//
// 正常流程：连接成功 → 可靠层发 3 条 Data(自动 seq=1,2,3) → server 回 Ack×3 + 回显 Data×3
//         → client 去重（重复帧不交业务）+ Ack 自动回收（pending 归零）→ 打印 PASS → 退出。
// 重传注入：server_ack --drop-ack 2 → seq=2 首次不回 Ack，client 超时重传后最终回收（看重传日志）。
#include <arpa/inet.h>
#include <cstring>
#include <functional>
#include <iostream>
#include <netinet/in.h>

#include "client/protocol.hpp"
#include "client/reliable_codec.hpp"
#include "client/tcp_client.hpp"
#include "net/event_loop.hpp"
#include "net/length_header_codec.hpp"
#include "net/log.hpp"

using namespace server;

int main(){
    Logger::instance().set_level(Level::INFO);

    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family      = AF_INET;
    peer.sin_port        = htons(8888);
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    EventLoop loop;
    TcpClient client(&loop, peer);

    // 四层接线：传输 → 切帧 → 5B 私有头 → 可靠层 → 业务
    LengthHeaderCodec codec;
    ProtocolCodec proto(codec);
    ReliableCodec  rel(proto, &loop);

    int recvd = 0;                       // 收到（去重后）的 Data 回显帧数
    client.setMessageCallback([&](TcpConnection* c, const std::string& p){ proto.onMessage(c, p); });
    proto.setMessageCallback([&](TcpConnection* c, MsgType t, uint32_t s, const std::string& b){ rel.onFrame(c, t, s, b); });
    rel.setMessageCallback([&](TcpConnection*, MsgType t, uint32_t s, const std::string& b){
        if(t != MsgType::Data) return;   // Heartbeat/Quit 也透传到这；演示只看 Data 回显
        std::cout << "[client] recv type=" << static_cast<int>(t) << " seq=" << s << " body=\"" << b << "\"\n";
        if(++recvd == 3){
            // 不能「收齐回显就立刻判定」：drop-ack 模式下 seq=2 的 Ack 要等首个重传（~2s）才回来。
            // 改为轮询等待 pending 归零（确认落账）；10s（0.25s×40）还没归零才判 FAIL。
            auto pollAcked = std::make_shared<std::function<void()>>();
            auto pollsLeft = std::make_shared<int>(40);
            *pollAcked = [pollAcked, pollsLeft, &loop, &rel](){
                if(rel.pendingCount() == 0){               // 全部确认落账 → PASS
                    std::cout << "[client] RELIABLE ECHO PASS: 3/3 acked, dedup ok\n";
                    loop.quit();
                    return;
                }
                if(--*pollsLeft <= 0){                     // 10s 超时仍未回收 → FAIL
                    std::cout << "[client] FAIL: not all acked in 10s, pending seqs=";
                    for(uint32_t s : rel.pendingSeqs()) std::cout << s << " ";
                    std::cout << "\n";
                    loop.quit();
                    return;
                }
                loop.runAfter(0.25, *pollAcked);           // 自续轮询：pending 非空就再等一个周期
            };
            loop.runAfter(0.25, *pollAcked);               // 先给首轮 Ack 一个落账周期再查
        }
    });
    rel.setLostCallback([](uint32_t seq){
        std::cout << "[client] LOST seq=" << seq << " (dropped: give-up or disconnect)\n";
    });

    client.setConnectionCallback([&](TcpConnection* c){
        if(c){
            std::cout << "[client] connected, send 3 reliable messages\n";
            rel.onConnected(c);
            rel.sendData(c, "hello", 5);
            rel.sendData(c, "im",    2);
            rel.sendData(c, "day3",  4);
        } else {
            rel.onDisconnected();
            std::cout << "[client] connection lost/failed, quit\n";
            loop.quit();
        }
    });

    client.connect();
    loop.runloop();
    rel.stop();                          // 退出收尾：取消 tick
    return 0;
}