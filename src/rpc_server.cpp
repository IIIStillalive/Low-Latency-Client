// Day5 RPC 演示服务端：Request → Ack(传输确认) → 幂等执行 → Response
//
// 线格式：TcpServer(4B 长度切帧) → 5B 私有头 → body = protobuf RpcRequest/RpcResponse
//
// 幂等策略（Day5 起就是端到端语义，Day7 只做断线场景验证）：
//   以 req_id 为键缓存「执行中/已完成 + 序列化响应」。
//   - 重复 req_id（客户端断线重发/重试）：执行中 → 只更新回程连接；已完成 → 直接重放缓存响应。
//     业务方法**不会**被重复执行（idem 方法可验证）。
//   - 不按 seq 去重：seq 是每个客户端连接自己的传输序号，跨连接不唯一；req_id 才是全局关联键。
//   - req_id 是 64 位（proto uint64，别名 server::ReqId）：跨客户端进程、同进程内不同池/连接
//     严格不撞号，缓存才不会被别的实例的请求命中（Day6 用 32 位随机起点只能概率性避免）。
//   - 缓存有两条上限（否则长跑必吃满内存）：TTL 60 秒 + 容量 65536 条。
//     淘汰按插入先后（req_id 从单个客户端看单调递增，FIFO 足够），**执行中的条目不淘汰** ——
//     赶走正在执行的那条，断线重发会变成重复执行。TTL 必须 ≥ ReliableCodec 的帧缓存时长
//     （见 rpc_connection.cpp 的 setCacheLimit），否则重放退化成重复执行。
//     两个上限都可用 CLI 覆盖（--req-cache-cap N / --req-ttl SEC），便于现场验证淘汰路径。
//   - 启动参数：--port N（默认 8899）、--verbose（DEBUG 日志，能看见 reqcache evict 行）。
//   - Day9 指标：--stats-sec S —— 每 S 秒打一行「req 总数 / 速率 / 缓存占用」的 metrics
//     日志（INFO 级，regress.sh/压测时可开）。默认 0 = 关，避免闲时刷屏。
//
// 方法（demo）：echo / sleep / fail / idem / cut / cutreply
//   echo  : 原样回 payload
//   sleep : payload = 毫秒字符串，延迟该时长后回 "slept:<ms>" —— 用来制造**乱序响应**
//   fail  : status = AppError，error = payload
//   idem  : 返回服务端累计执行次数（证明重复 req_id 没有重复执行）
//   cut     : 故障注入——Ack 已发、业务未执行时断开连接（一次性；重连重发后才真正执行）
//   cutreply: 故障注入——正常执行、响应已缓存，但断开连接不发送（重连重发命中重放）
//
// 线程：setThreadNum(0) → 所有连接都在 main loop 线程，reqs 表与 runAfter 无需加锁。
// 启动参数：--port N、--verbose、--req-cache-cap N、--req-ttl SEC、--drop-ack K
//   --drop-ack K：第 K 个 Request 帧既不确认也不执行（客户端约 2s 后重传同一帧）。
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <type_traits>   // static_assert：钉住 proto 的 req_id 类型
#include <unordered_map>
#include <utility>       // std::declval

#include "client/protocol.hpp"
#include "client/rpc_types.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"
#include "net/timer.hpp"     // nowMicros：TTL 判定
#include "rpc.pb.h"
#include "server/tcpserver.hpp"

using namespace server;

// reqs 表以 ReqId 为键。proto 若被改回 uint32，64 位号会在 req.req_id() 处被截断，
// 于是「不同 req_id 映射到同一个缓存槽」——这里编译期拦住。
static_assert(std::is_same_v<decltype(std::declval<const rpc::RpcRequest&>().req_id()), ReqId>,
              "rpc.proto 的 RpcRequest.req_id 必须与 server::ReqId(uint64) 一致");

namespace {

// reqs 缓存的两条上限：TTL 管「旧」，容量管「多」。
constexpr size_t kReqCacheCap      = 65536;   // 最多保留的响应条数
constexpr double kReqTtlSec        = 60.0;    // 从首次见到该 req_id 起算的保留时长
constexpr double kSweepIntervalSec = 1.0;     // 清扫周期（只在 main loop 线程跑）

// 一个 req_id 的服务端状态
struct ReqSlot {
    bool running = false;          // 业务正在执行（sleep 延迟中）
    bool done    = false;          // 响应已生成
    double createdAtSec = 0.0;     // 首次见到该 req_id 的时刻；0 = 还没登记过（nowMicros 恒 > 0）
    std::string responseBody;      // 序列化后的 RpcResponse（命中幂等时原样重放）
    std::weak_ptr<TcpConnection> conn;   // 最近一次携带该 req_id 的连接（重连后更新）

    // ---- D2 故障注入（一次性）----
    bool cutSent   = false;                    // cut：首次到达时断连，重连重发后才真正执行
    bool dropReply = false;                    // cutreply：正常执行但响应不发，直接断连
    std::weak_ptr<TcpConnection> dropConn;     // cutreply 要断的目标：必须是发出请求的那条连接
};

} // namespace

int main(int argc, char** argv){
    int     port        = 8899;
    size_t  reqCacheCap = kReqCacheCap;   // 可调小，用来现场验证淘汰路径（默认 64k）
    double  reqTtlSec   = kReqTtlSec;
    size_t  dropAckN    = 0;              // --drop-ack K：第 K 个 Request 帧不确认不执行（0 = 关）
    double  statsSec    = 0.0;            // --stats-sec S：每 S 秒打一行 metrics（0 = 关）
    bool    verbose     = false;
    for(int i = 1; i < argc; ++i){
        const std::string a = argv[i];
        if(a == "--port" && i + 1 < argc)                port        = std::atoi(argv[i+1]);
        else if(a == "--req-cache-cap" && i + 1 < argc)  reqCacheCap = static_cast<size_t>(std::strtoull(argv[i+1], nullptr, 10));
        else if(a == "--req-ttl" && i + 1 < argc)        reqTtlSec   = std::atof(argv[i+1]);
        else if(a == "--drop-ack" && i + 1 < argc)       dropAckN    = static_cast<size_t>(std::strtoull(argv[i+1], nullptr, 10));
        else if(a == "--stats-sec" && i + 1 < argc)      statsSec    = std::atof(argv[i+1]);
        else if(a == "--verbose")                        verbose     = true;
    }

    Logger::instance().set_level(verbose ? Level::DEBUG : Level::INFO);

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
    std::unordered_map<ReqId, ReqSlot> reqs;
    std::deque<ReqId> lruOrder;      // 插入序（FIFO 淘汰用）；陈旧项由 TTL 清扫同一趟回收
    uint64_t idemCount = 0;
    size_t   requestFrames = 0;     // 进程级 Request 帧总数（1 基；--drop-ack 与 metrics 共用）
    size_t   lastMetricsFrames = 0; // 上一条 metrics 时的 requestFrames 快照（算速率用）

    // 超容量淘汰：从队头取，`running` 的条目扔回队尾（不能赶走执行中的，否则重发会重复执行）。
    // tries 上限防止「整队都在执行」时死循环；若一轮没压到容量以内，下一次插入再试。
    auto enforceReqCap = [&]{
        size_t tries   = 0;
        size_t evicted = 0;
        while(reqs.size() > reqCacheCap && tries < lruOrder.size()){
            const ReqId old = lruOrder.front();
            lruOrder.pop_front();
            ++tries;
            auto it = reqs.find(old);
            if(it == reqs.end()) continue;                                  // 已被 TTL 清掉
            if(it->second.running){ lruOrder.push_back(old); continue; }    // 执行中：不淘汰
            reqs.erase(it);
            ++evicted;
        }
        if(evicted > 0){
            Logger::instance().log(Level::DEBUG, "rpc_server: reqcache cap evict=", evicted,
                                   " size=", reqs.size());
        }
    };

    // 响应收口：缓存 + 发往「最近一次携带该 req_id 的连接」
    std::function<void(ReqId, rpc::RpcResponse)> finish;
    finish = [&](ReqId reqId, rpc::RpcResponse resp){
        auto it = reqs.find(reqId);
        if(it == reqs.end()) return;
        resp.set_req_id(reqId);
        it->second.running      = false;
        it->second.done         = true;
        it->second.responseBody = resp.SerializeAsString();
        if(it->second.dropReply){                       // cutreply：缓存落地，响应不发，断原始连接
            it->second.dropReply = false;
            Logger::instance().log(Level::INFO, "rpc_server: cutreply req=", reqId, " cached, conn closed");
            if(auto c = it->second.dropConn.lock()) c->close();
            return;                                     // 客户端重连重发同 req_id → 命中 handler 里 slot.done 的重放
        }
        if(auto c = it->second.conn.lock()){
            proto.sendFrame(c.get(), MsgType::Response, 0,
                            it->second.responseBody.data(), it->second.responseBody.size());
        }
        // 连接已不在（断线中）：响应留在缓存里，客户端重发同 req_id 时命中重放
    };

    // TTL 清扫：每 1 秒把 lruOrder 旋转一圈，淘汰「已完成且超过 kReqTtlSec」的条目。
    // 与 enforceReqCap 分工：这个管「旧」，那个管「多」；都只在 main loop 线程跑，reqs 无需加锁。
    // 与 ReliableCodec 的帧缓存时长对齐（rpc_connection.cpp 的 setCacheLimit 也是 60s）：
    // 客户端最多把未确认帧挂 60s，服务端至少留 60s，重连重发才命中重放而不是重复执行。
    loop.runEvery(kSweepIntervalSec, [&]{
        const double now = static_cast<double>(nowMicros()) / 1e6;
        const size_t n   = lruOrder.size();          // 固定次数：本轮只看现存条目
        size_t evicted   = 0;
        for(size_t i = 0; i < n; ++i){
            const ReqId id = lruOrder.front();
            lruOrder.pop_front();
            auto it = reqs.find(id);
            if(it != reqs.end() && !it->second.running &&
               now - it->second.createdAtSec > reqTtlSec){
                reqs.erase(it);
                ++evicted;
                continue;
            }
            lruOrder.push_back(id);
        }
        if(evicted > 0){
            Logger::instance().log(Level::DEBUG, "rpc_server: reqcache ttl evict=", evicted,
                                   " size=", reqs.size());
        }
    });

    // Day9 metrics：每 statsSec 秒打一行请求速率 + 幂等缓存占用（仅 main loop 线程，免锁）。
    // 速率 = 相邻两条 metrics 之间的 Request 帧增量 / 间隔（丢注入后的重发也算到达）。
    if(statsSec > 0.0){
        loop.runEvery(statsSec, [&]{
            const double now   = static_cast<double>(nowMicros()) / 1e6;
            const double rate  = static_cast<double>(requestFrames - lastMetricsFrames) / statsSec;
            Logger::instance().log(Level::INFO, "rpc_server metrics t=", now, " req_frames=", requestFrames,
                                   " rate=", rate, "/s reqcache=", reqs.size(), "/", reqCacheCap);
            lastMetricsFrames = requestFrames;
        });
    }

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

        ++requestFrames;                  // 到帧计数（丢失注入/重发都算一次到达；metrics 与 drop-ack 共用）

        if(dropAckN != 0 && requestFrames == dropAckN){
            // 传输层故障注入：第 K 个 Request 帧既不确认也不执行。客户端 ReliableCodec
            // 约 2s 后重传同一帧（同一 seq，因为帧还挂在 pending_），第二次到达才走正常
            // 路径 —— 验证的是「同一连接上的重传 + 重复帧必须再确认」。
            Logger::instance().log(Level::INFO, "rpc_server: drop ack seq=", seq);
            return;
        }
        proto.sendAck(conn, seq);          // 传输确认（无条件；重复帧也要确认，否则对端一直重传）

        rpc::RpcRequest req;
        if(!req.ParseFromArray(payload.data() + ProtocolCodec::kHeadLen,
                               static_cast<int>(bodyLen))){
            Logger::instance().log(Level::WARN, "rpc_server: bad RpcRequest, drop");
            return;
        }
        const ReqId    reqId  = req.req_id();
        const std::string& method = req.method();   // 提前取：cut/cutreply 分支在登记之前就要判断
        ReqSlot& slot = reqs[reqId];       // unordered_map 节点地址稳定，可持引用
        slot.conn = conn->shared_from_this();

        // 故障注入：cut —— 断在「已确认、未执行」（Ack 上面已发）。
        // 一次性：重连后同一个 req_id 再进来时 cutSent=true → 走正常路径执行。
        // 注意不置 running、不缓存响应：这个槽只是个标记，TTL 清扫会把它收走。
        if(method == "cut" && !slot.cutSent){
            slot.cutSent = true;
            Logger::instance().log(Level::INFO, "rpc_server: cut req=", reqId, " closing conn");
            conn->close();
            return;
        }

        if(slot.createdAtSec == 0.0){      // 首次见到该 req_id：登记插入序，TTL 从这一刻起算
            slot.createdAtSec = static_cast<double>(nowMicros()) / 1e6;
            lruOrder.push_back(reqId);
        }

        if(slot.done){                     // 幂等命中：重放缓存，不重复执行
            slot.createdAtSec = static_cast<double>(nowMicros()) / 1e6;   // 重放即「仍在用」：续命
            Logger::instance().log(Level::INFO, "rpc_server: replay req=", reqId,
                                   " method=", method);
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
        enforceReqCap();                   // 本 slot 已是 running，不会被选中；参考 slot 仍有效（节点地址稳定）

        // 故障注入：cutreply —— 首次到达打标记，走正常执行；finish() 里缓存落地后断掉
        // 发请求的那条连接、响应不发。重连重发 → done → 上面重放 → 拿到与首次一致的结果。
        if(method == "cutreply" && !slot.dropReply){
            slot.dropReply = true;
            slot.dropConn  = conn->shared_from_this();
        }

        Logger::instance().log(Level::INFO, "rpc_server: exec req=", reqId,
                               " method=", method, " payloadLen=", req.payload().size());

        rpc::RpcResponse resp;
        resp.set_status(static_cast<int32_t>(RpcStatus::Ok));

        if(method == "echo"){
            resp.set_payload(req.payload());
        } else if(method == "fail"){
            resp.set_status(static_cast<int32_t>(RpcStatus::AppError));
            resp.set_error(req.payload().empty() ? "app error" : req.payload());
        } else if(method == "idem"){
            resp.set_payload(std::to_string(++idemCount));
        } else if(method == "cutreply"){
            // 首次执行走这里：与 idem 同语义（回累计数），只是 finish() 见 dropReply 会把
            // 响应掐掉不发 → 重连重发命中缓存重放时计数不再增加，payload 与首次完全一致。
            resp.set_payload(std::to_string(++idemCount));
        } else if(method == "cut"){
            // 第二次到达（重连重发）才走到这：此时不会再被 cut 分支拦住，真正执行一次。
            resp.set_status(static_cast<int32_t>(RpcStatus::Ok));
            resp.set_payload("cut-once");
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
