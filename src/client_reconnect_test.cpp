// Day4-A1 端到端：断线挂起缓存 → 自动重连 → 全量重发 → server seen 幂等收账
//
//   TcpClient(transport) → LengthHeaderCodec(切帧) → ProtocolCodec(5B头)
//   → ReliableCodec(可靠) → 业务
//
// 依赖（server 进程全程存活，"断开"由 server 主动发起的连接切断模拟）：
//   ./build/server_ack --drop-ack 2 --cut-after 3 在 127.0.0.1:8888 运行
//
// 场景：首次连接发 3 条(seq=1,2,3) → server 全部首见并回显(echo 只首见)，
//       --drop-ack 2 → seq=2 的 Ack 首次被吞 → pending 剩 {2}；
//       --cut-after 3 → 处理满 3 条首见 Data 后 0.3s server 断开连接（进程不退出，seen 保留）。
//       客户端 onDisconnected → A1 挂起 pending_（不清空、不通知丢失）；
//       自动重连成功 → onConnected 全量重发 seq=2 → server seen 判定 first=N →
//       只 Ack 不重复回显 → 客户端收 ack2 → pending=0 → PASS。
//
// 判定目标：3/3 回显去重收齐 + pending 归零（挂起重发的数据在重连后仍能确认落账）。
// 超时 15s（轮询 0.25s×60）仍未收齐 → FAIL。
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
    client.setAutoReconnect(true, 0.5, 2.0);   // 断线自动重连（A1 的前提）

    // 四层接线：传输 → 切帧 → 5B 私有头 → 可靠层 → 业务
    LengthHeaderCodec codec;
    ProtocolCodec proto(codec);
    ReliableCodec  rel(proto, &loop);

    int recvd = 0;                       // 收到（去重后）的 Data 回显帧数
    bool firstConnect = true;            // 只在首次连接发 3 条；重连由 onConnected 重发 pending
    int reconnects    = 0;               // 重连次数（诊断）

    client.setMessageCallback([&](TcpConnection* c, const std::string& p){ proto.onMessage(c, p); });
    proto.setMessageCallback([&](TcpConnection* c, MsgType t, uint32_t s, const std::string& b){
        rel.onFrame(c, t, s, b);
    });

    rel.setMessageCallback([&](TcpConnection*, MsgType t, uint32_t s, const std::string& b){
        if(t != MsgType::Data) return;   // 只统计回显 Data
        std::cout << "[client] recv type=" << static_cast<int>(t) << " seq=" << s
                  << " body=\"" << b << "\" recvd=" << (recvd + 1) << "\n";
        if(++recvd == 3){
            // 3 条回显都到（去重后）→ 进入收账判定：轮询 pending 归零（A1 收口）
            auto pollAcked = std::make_shared<std::function<void()>>();
            auto pollsLeft = std::make_shared<int>(60);
            *pollAcked = [pollAcked, pollsLeft, &loop, &rel](){
                if(rel.pendingCount() == 0){
                    std::cout << "[client] RECONNECT RESUME PASS: 3/3 echoes dedup ok, pending drained\n";
                    loop.quit();
                    return;
                }
                if(--*pollsLeft <= 0){
                    std::cout << "[client] FAIL: pending not drained in 15s, pending seqs=";
                    for(uint32_t s : rel.pendingSeqs()) std::cout << s << " ";
                    std::cout << "\n";
                    loop.quit();
                    return;
                }
                loop.runAfter(0.25, *pollAcked);
            };
            loop.runAfter(0.25, *pollAcked);
        }
    });
    rel.setLostCallback([](uint32_t seq){
        std::cout << "[client] LOST seq=" << seq << " (unexpected in A1 PASS path)\n";
    });

    client.setConnectionCallback([&](TcpConnection* c){
        if(c){
            // 连接建立（首次 or 重连）：A1 在此恢复挂起缓存
            if(firstConnect){
                firstConnect = false;
                std::cout << "[client] first connected, send 3 reliable messages\n";
                rel.onConnected(c);
                rel.sendData(c, "hello", 5);
                rel.sendData(c, "im",    2);
                rel.sendData(c, "day3",  4);
            } else {
                ++reconnects;
                std::cout << "[client] RECONNECTED #" << reconnects
                          << ", rel resends pending=" << rel.pendingCount() << "\n";
                rel.onConnected(c);      // 内部：剔除超 TTL → 全量重发 → 恢复 tick
            }
        } else {
            std::cout << "[client] disconnected (A1 suspends pending), waiting reconnect...\n";
            rel.onDisconnected();
            // 不退出：autoReconnect 会再次进入 c!=nullptr 分支
        }
    });

    client.connect();
    loop.runloop();
    rel.stop();                          // 退出收尾：取消 tick + 进程退出清空
    std::cout << "[client] reconnect test exit, total reconnects=" << reconnects << "\n";
    return 0;
}