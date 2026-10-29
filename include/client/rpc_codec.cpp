#include "client/rpc_codec.hpp"

#include <chrono>
#include <random>
#include <type_traits>   // static_assert：钉住 proto 的 req_id 类型
#include <utility>       // std::declval

#include "rpc.pb.h"                 // protoc 生成（build/generated，CMake 目标 rpc_proto）
#include "client/reliable_codec.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"

namespace server {

// 编译期钉住：proto 的 req_id 必须是 64 位。若有人把 rpc.proto 改回 uint32，这里当场
// 编译失败 —— 而不是运行期把 64 位号静默截成 32 位（截断后撞号 → 命中别人的缓存）。
static_assert(std::is_same_v<decltype(std::declval<const rpc::RpcRequest&>().req_id()), ReqId>,
              "rpc.proto 的 RpcRequest.req_id 必须与 server::ReqId(uint64) 一致");
static_assert(std::is_same_v<decltype(std::declval<const rpc::RpcResponse&>().req_id()), ReqId>,
              "rpc.proto 的 RpcResponse.req_id 必须与 server::ReqId(uint64) 一致");

namespace {
// 进程内每新建一个分配器就跳过 2^32 个号段：同一进程里的连接池 / 独立 RpcCodec 号段
// 严格不相交；按 2^32 对齐后，日志里的 req_id 一眼能看出是哪个分配器发的。
constexpr ReqId kReqIdStride = 1ull << 32;

ReqId randomReqIdBase(){
    // magic static：进程内只做一次随机化；之后每次调用取一个新号段的起点。
    static std::atomic<ReqId> cursor{[]{
        std::random_device rd;
        uint64_t mix = (static_cast<uint64_t>(rd()) << 32) ^ rd();
        // 时间再搅一次：部分平台的 random_device 会退化成固定序列
        mix ^= static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        mix &= (static_cast<uint64_t>(1) << 63) - 1;          // 清最高位：号段不回绕
        mix &= ~(static_cast<uint64_t>(kReqIdStride) - 1);    // 对齐到号段边界
        return static_cast<ReqId>(mix);
    }()};
    return cursor.fetch_add(kReqIdStride, std::memory_order_relaxed);
}
} // namespace

std::shared_ptr<std::atomic<ReqId>> RpcCodec::makeReqIdSequence(){
    return std::make_shared<std::atomic<ReqId>>(randomReqIdBase());
}

RpcCodec::RpcCodec(ReliableCodec& rel, EventLoop* ioLoop, EventLoop* bizLoop)
    : rel_(rel),
      ioLoop_(ioLoop),
      bizLoop_(bizLoop),
      reqIdSeq_(makeReqIdSequence()) {}

RpcFuture RpcCodec::call(const std::string& method, const std::string& payload,
                         const RpcCallOptions& opts){
    RpcFuture fut = RpcFuture::make(bizLoop_);          // 未完成句柄，调用方立即拿到
    if(ioLoop_ == nullptr){
        fut.complete(RpcResult{RpcStatus::Unavailable, "", "no io loop"});
        return fut;
    }

    // req_id 在调用线程分配（原子）：任意线程 call() 都不需要等 IO 线程。
    const ReqId reqId = reqIdSeq_->fetch_add(1, std::memory_order_relaxed);

    rpc::RpcRequest req;
    req.set_req_id(reqId);
    req.set_method(method);
    req.set_payload(payload);
    req.set_idempotent(opts.idempotent);
    std::string body;
    if(!req.SerializeToString(&body)){
        fut.complete(RpcResult{RpcStatus::AppError, "", "RpcRequest serialize failed"});
        return fut;
    }

    calls_.fetch_add(1);
    ioLoop_->runInLoop([this, reqId, body = std::move(body), opts, fut]() mutable {
        startCallInLoop(reqId, std::move(body), opts, std::move(fut));
    });
    return fut;
}

void RpcCodec::startCallInLoop(ReqId reqId, std::string body,
                               const RpcCallOptions& opts, RpcFuture fut){
    if(stopped_){
        exhausted_.fetch_add(1);
        fut.complete(RpcResult{RpcStatus::Unavailable, "", "client stopped"});
        return;
    }
    if(inflight_.size() >= maxInflight_.load(std::memory_order_relaxed)){   // 背压闸：fail-fast，不阻塞调用线程
        exhausted_.fetch_add(1);
        Logger::instance().log(Level::WARN, "[rpc] inflight full (", inflight_.size(),
                               "), reject req=", reqId);
        fut.complete(RpcResult{RpcStatus::ResourceExhausted, "", "max in-flight exceeded"});
        return;
    }

    InflightCall& ic = inflight_[reqId];
    ic.future  = fut;
    ic.timerId = 0;
    ic.requestBody = body;                       // Day7 D4：断线重连时重发 in-flight 用
    if(opts.deadlineSec > 0.0){                          // 超时从 call() 发起时刻起算
        ic.timerId = ioLoop_->runAfter(opts.deadlineSec, [this, reqId]{ onTimeout(reqId); });
    }

    // 取消钩子：Future::cancel() 可能在任意线程 → 切回 IO 线程改 in-flight 表
    fut.setCancelFn([this, reqId]{ ioLoop_->runInLoop([this, reqId]{ cancelInLoop(reqId); }); });

    inflightCount_.store(inflight_.size());
    // 发往当前活动连接（ReliableCodec 内部按 connRef_ 决定直发还是挂起等重连）
    rel_.sendFrame(MsgType::Request, body.data(), body.size());
    Logger::instance().log(Level::DEBUG, "[rpc] send req=", reqId, " bodyLen=", body.size());
}

void RpcCodec::onFrame(TcpConnection*, MsgType type, uint32_t, const std::string& body){
    if(type != MsgType::Response) return;               // Ack 已由 ReliableCodec 吃掉
    rpc::RpcResponse resp;
    if(!resp.ParseFromArray(body.data(), static_cast<int>(body.size()))){
        bad_.fetch_add(1);
        Logger::instance().log(Level::WARN, "[rpc] bad RpcResponse, len=", body.size());
        return;
    }
    const ReqId reqId = resp.req_id();
    if(inflight_.find(reqId) == inflight_.end()){
        // 超时/取消之后迟到的响应，或重复响应：幂等丢弃（不是错误）
        late_.fetch_add(1);
        Logger::instance().log(Level::DEBUG, "[rpc] late/dup response req=", reqId, ", dropped");
        return;
    }
    RpcResult r;
    r.status  = static_cast<RpcStatus>(resp.status());
    r.payload = std::move(*resp.mutable_payload());
    r.error   = std::move(*resp.mutable_error());
    finish(reqId, std::move(r));
}

void RpcCodec::onReconnected(){
    // 首次连接：in-flight 帧还排在 ReliableCodec 的 pending_，可靠层 onConnected 会全量重发，
    // 这里不要重复发。everConnected_ 一旦置位，后续每次连接都是「重连」。
    if(!everConnected_){
        everConnected_ = true;
        return;
    }
    if(inflight_.empty()) return;
    // 重连：把全部 in-flight 请求体用**新 seq** 重发（服务端按 req_id 去重，不重复执行；
    // 尚未被 Ack 的帧会与可靠层 pending_ 重发重叠 —— D4.3 的诚实代价，见头注释）。
    Logger::instance().log(Level::INFO, "[rpc] reconnect resend inflight=", inflight_.size());
    for(const auto& kv : inflight_){
        const InflightCall& ic = kv.second;
        rel_.sendFrame(MsgType::Request, ic.requestBody.data(), ic.requestBody.size());
    }
}

void RpcCodec::onTimeout(ReqId reqId){
    if(inflight_.find(reqId) == inflight_.end()) return;   // 已被响应/取消收口
    timeouts_.fetch_add(1);
    Logger::instance().log(Level::WARN, "[rpc] deadline exceeded req=", reqId);
    finish(reqId, RpcResult{RpcStatus::DeadlineExceeded, "", "deadline exceeded"});
}

void RpcCodec::cancelInLoop(ReqId reqId){
    if(inflight_.find(reqId) == inflight_.end()) return;   // 已完成/已超时：取消无效
    // best-effort：通知服务端早停（可靠发送，服务端只 Ack 不执行——demo 不做真取消）
    rpc::RpcRequest c;
    c.set_req_id(reqId);
    std::string body;
    if(c.SerializeToString(&body)) rel_.sendFrame(MsgType::Cancel, body.data(), body.size());
    cancelled_.fetch_add(1);
    Logger::instance().log(Level::INFO, "[rpc] cancelled req=", reqId);
    finish(reqId, RpcResult{RpcStatus::Cancelled, "", "cancelled by caller"});
}

void RpcCodec::finish(ReqId reqId, RpcResult r){
    auto it = inflight_.find(reqId);
    if(it == inflight_.end()) return;
    if(it->second.timerId != 0 && ioLoop_ != nullptr) ioLoop_->cancel(it->second.timerId);
    RpcFuture fut = std::move(it->second.future);
    inflight_.erase(it);
    inflightCount_.store(inflight_.size());
    if(r.status == RpcStatus::Ok) ok_.fetch_add(1);
    fut.complete(std::move(r));      // 续体由 RpcFuture 投递到 biz loop
}

void RpcCodec::stop(){
    if(stopped_) return;
    stopped_ = true;
    // 约束：loop 已停止、本线程即 IO 线程（与 Day4 ReliableCodec::stop 同款收尾纪律）
    while(!inflight_.empty()){
        finish(inflight_.begin()->first,
               RpcResult{RpcStatus::Unavailable, "", "client stopped"});
    }
}

RpcCodec::Stats RpcCodec::stats() const{
    Stats s;
    s.calls         = calls_.load();
    s.ok            = ok_.load();
    s.timeouts      = timeouts_.load();
    s.cancelled     = cancelled_.load();
    s.exhausted     = exhausted_.load();
    s.badResponses  = bad_.load();
    s.lateResponses = late_.load();
    s.inflight      = inflightCount_.load();
    return s;
}

} // namespace server
