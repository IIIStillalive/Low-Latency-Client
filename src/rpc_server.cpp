// Day5 RPC 演示服务端：Request → Ack(传输确认) → 幂等执行 → Response
//
// 线格式：TcpServer(4B 长度切帧) → 5B 私有头 → body = protobuf RpcRequest/RpcResponse
//
// 幂等策略（Day5 起就是端到端语义，Day7 只做断线场景验证）：
//   以 req_id 为键缓存「执行中/已完成 + 序列化响应」。
//   - 重复 req_id（客户端断线重发/重试）：执行中 → 只更新回程连接；已完成 → 直接重放缓存响应。
//     业务方法**不会**被重复执行（idem 方法可验证）。
//   - 不按 seq 去重：seq 是每个客户端连接自己的传输序号，跨连接不唯一；req_id 才是全局关联键。
//
// 方法（demo）：echo / sleep / fail / idem
//   echo  : 原样回 payload
//   sleep : payload = 毫秒字符串，延迟该时长后回 "slept:<ms>" —— 用来制造**乱序响应**
//   fail  : status = AppError，error = payload
//   idem  : 返回服务端累计执行次数（证明重复 req_id 没有重复执行）
//
// 线程：setThreadNum(0) → 所有连接都在 main loop 线程，reqs 表与 runAfter 无需加锁。
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <unordered_map>

#include "client/protocol.hpp"
#include "client/rpc_types.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "rpc.pb.h"
#include "server/tcpserver.hpp"

using namespace server;

namespace {

// 一个 req_id 的服务端状态
struct ReqSlot {
    bool running = false;          // 业务正在执行（sleep 延迟中）
    bool done    = false;          // 响应已生成
    std::string responseBody;      // 序列化后的 RpcResponse（命中幂等时原样重放）
    std::weak_ptr<TcpConnection> conn;   // 最近一次携带该 req_id 的连接（重连后更新）
};

} // namespace

int main(int argc, char** argv){
    int port = 8899;
    for(int i = 1; i + 1 < argc; ++i){
        if(std::string(argv[i]) == "--port") port = std::atoi(argv[i+1]);
    }

    Logger::instance().set_level(Level::INFO);

    struct sockaddr_in srv;
    memset(&srv, 0, sizeof(srv));
    srv.sin_family      = AF_INET;
    srv.sin_port        = htons(static_cast<uint16_t>(port));
    srv.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    EventLoop loop;
    TcpServer server(&loop, srv);
    server.setThreadNum(0);                 // 单线程：reqs/计数器无需加锁，runAfter 合法

    LengthHeaderCodec codec;
    ProtocolCodec     proto(codec);
    std::unordered_map<uint32_t, ReqSlot> reqs;
    uint64_t idemCount = 0;

    // 响应收口：缓存 + 发往「最近一次携带该 req_id 的连接」
    std::function<void(uint32_t, rpc::RpcResponse)> finish;
    finish = [&](uint32_t reqId, rpc::RpcResponse resp){
        auto it = reqs.find(reqId);
        if(it == reqs.end()) return;
        resp.set_req_id(reqId);
        it->second.running      = false;
        it->second.done         = true;
        it->second.responseBody = resp.SerializeAsString();
        if(auto c = it->second.conn.lock()){
            proto.sendFrame(c.get(), MsgType::Response, 0,
                            it->second.responseBody.data(), it->second.responseBody.size());
        }
        // 连接已不在（断线中）：响应留在缓存里，客户端重发同 req_id 时命中重放
    };

    server.setMessageCallback([&](TcpConnection* conn, const std::string& payload){
        MsgType  type;
        uint32_t seq;
        uint32_t bodyLen;
        if(!ProtocolCodec::decodeHeader(payload.data(), payload.size(), type, seq, bodyLen)) return;

        if(type == MsgType::Cancel){       // best-effort：demo 不真正中断执行，只确认掉传输帧
            proto.sendAck(conn, seq);
            return;
        }
        if(type != MsgType::Request) return;

        proto.sendAck(conn, seq);          // 传输确认（无条件；重复帧也要确认，否则对端一直重传）

        rpc::RpcRequest req;
        if(!req.ParseFromArray(payload.data() + ProtocolCodec::kHeadLen,
                               static_cast<int>(bodyLen))){
            Logger::instance().log(Level::WARN, "rpc_server: bad RpcRequest, drop");
            return;
        }
        const uint32_t reqId = req.req_id();
        ReqSlot& slot = reqs[reqId];       // unordered_map 节点地址稳定，可持引用
        slot.conn = conn->shared_from_this();

        if(slot.done){                     // 幂等命中：重放缓存，不重复执行
            Logger::instance().log(Level::INFO, "rpc_server: replay req=", reqId,
                                   " method=", req.method());
            if(!slot.responseBody.empty()){
                proto.sendFrame(conn, MsgType::Response, 0,
                                slot.responseBody.data(), slot.responseBody.size());
            }
            return;
        }
        if(slot.running){                  // 执行中（sleep 未到期）：完成后按最新连接回
            Logger::instance().log(Level::INFO, "rpc_server: dup while running req=", reqId);
            return;
        }
        slot.running = true;

        Logger::instance().log(Level::INFO, "rpc_server: exec req=", reqId,
                               " method=", req.method(), " payloadLen=", req.payload().size());

        rpc::RpcResponse resp;
        resp.set_status(static_cast<int32_t>(RpcStatus::Ok));

        const std::string& method = req.method();
        if(method == "echo"){
            resp.set_payload(req.payload());
        } else if(method == "fail"){
            resp.set_status(static_cast<int32_t>(RpcStatus::AppError));
            resp.set_error(req.payload().empty() ? "app error" : req.payload());
        } else if(method == "idem"){
            resp.set_payload(std::to_string(++idemCount));
        } else if(method == "sleep"){
            int ms = std::atoi(req.payload().c_str());
            if(ms < 0) ms = 0;
            // 异步回：故意让后到的请求先回 → 客户端必须处理乱序响应
            loop.runAfter(ms / 1000.0, [&, reqId, ms]{
                rpc::RpcResponse later;
                later.set_status(static_cast<int32_t>(RpcStatus::Ok));
                later.set_payload("slept:" + std::to_string(ms));
                finish(reqId, std::move(later));
            });
            return;
        } else {
            resp.set_status(static_cast<int32_t>(RpcStatus::AppError));
            resp.set_error("unknown method: " + method);
        }
        finish(reqId, std::move(resp));
    });

    if(!server.start()){
        Logger::instance().log(Level::ERROR, "rpc_server start failed");
        return -1;
    }
    Logger::instance().log(Level::INFO, "rpc_server listening on 127.0.0.1:", port);
    loop.runloop();
    return 0;
}
