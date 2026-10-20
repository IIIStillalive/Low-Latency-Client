// Day3 可靠链路演示服务器：收到 Data → 回 Ack(seq) + 回显 body 为 Data 帧（同 seq）
//
// 与既有 server（纯 echo，不认识 5B 私有头）并存：本程序演示协议层 Ack 语义，
// server 端也只是"收到 Data 回一条 Ack"，不做完整可靠层 —— 配合 client_reliability_test 验证。
//
// 故障注入（可组合）：
//   --drop-ack <N>        令 seq==N 的 Data「首次」不回 Ack（模拟确认丢失），观察重传路径。
//   --cut-after <N>       首见 Data 处理满 N 条后延迟 0.3s 断开该连接（server 进程不退出，
//                         seen 集合保留）——配合 client 自动重连验证 Day4-A1「断线挂起 →
//                         重连全量重发 → server seen 幂等『只 Ack 不重复回显』」。
#include <arpa/inet.h>
#include <cstdlib>          // std::atoi
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <unordered_set>

#include "client/protocol.hpp"
#include "net/event_loop.hpp"
#include "net/length_header_codec.hpp"
#include "net/log.hpp"
#include "server/tcpserver.hpp"

using namespace server;

int main(int argc, char** argv){
    int dropAck = -1;
    int cutAfter = -1;
    for(int i = 1; i + 1 < argc; ++i){
        if(std::string(argv[i]) == "--drop-ack")   dropAck   = std::atoi(argv[i+1]);
        if(std::string(argv[i]) == "--cut-after")  cutAfter  = std::atoi(argv[i+1]);
    }

    Logger::instance().set_level(Level::INFO);

    struct sockaddr_in srv;
    memset(&srv, 0, sizeof(srv));
    srv.sin_family      = AF_INET;
    srv.sin_port        = htons(8888);
    srv.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    EventLoop loop;
    TcpServer server(&loop, srv);
    server.setThreadNum(0);            // 单线程：seen 集合无需加锁（demo 只用一把）

    LengthHeaderCodec codec;
    ProtocolCodec proto(codec);
    std::unordered_set<uint32_t> seen;   // 已见 seq：判定"首次"；跨连接保留 = A1 幂等基础
    int firstCount = 0;                  // 已处理的首见 Data 条数（--cut-after 触发计数）

    server.setMessageCallback([&](TcpConnection* conn, const std::string& payload){
        MsgType   type;
        uint32_t  seq;
        uint32_t  bodyLen;
        if(!ProtocolCodec::decodeHeader(payload.data(), payload.size(), type, seq, bodyLen)) return;
        if(type != MsgType::Data) return;

        const bool first = seen.insert(seq).second;       // 首见该 seq？
        Logger::instance().log(Level::INFO, "server: recv Data seq=", seq,
                               " first=", first ? "Y" : "N");

        // Day4-A1 注入：首见 Data 满 N 条 → 延迟断开当前连接（进程不退出，seen 保留）
        if(first && cutAfter >= 0 && ++firstCount == cutAfter){
            Logger::instance().log(Level::INFO, "server: CUT connection after ", firstCount,
                                   " first-seen Data frames");
            loop.runAfter(0.3, [conn]{ conn->close(); });   // 延迟断开：让本帧处理完/对端收完
        }

        if(first && dropAck >= 0 && seq == static_cast<uint32_t>(dropAck)){
            Logger::instance().log(Level::INFO, "server: DROP ack of seq=", seq, " (first, simulate loss)");
        } else {
            Logger::instance().log(Level::INFO, "server: sendAck seq=", seq);
            proto.sendAck(conn, seq);                     // 正常确认（含重传重复 → 幂等只 Ack）
        }
        // 只首见时回显：重传重复帧只 Ack 不重复执行业务（server seen 幂等实证）
        if(first && bodyLen > 0){
            Logger::instance().log(Level::INFO, "server: echo Data seq=", seq, " bodyLen=", bodyLen);
            proto.sendFrame(conn, MsgType::Data, seq,
                            payload.data() + ProtocolCodec::kHeadLen, bodyLen);
        }
    });

    if(!server.start()){
        Logger::instance().log(Level::ERROR, "server_ack start failed");
        return -1;
    }
    Logger::instance().log(Level::INFO, "server_ack listening on 127.0.0.1:8888, dropAck=", dropAck,
                           " cutAfter=", cutAfter);
    loop.runloop();
    return 0;
}