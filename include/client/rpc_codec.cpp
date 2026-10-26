#include "client/rpc_codec.hpp"

#include <utility>

#include "rpc.pb.h"                 // protoc 生成（build/generated，CMake 目标 rpc_proto）
#include "client/reliable_codec.hpp"
#include "net/event_loop.hpp"
#include "net/log.hpp"

namespace server {

RpcCodec::RpcCodec(ReliableCodec& rel, EventLoop* ioLoop, EventLoop* bizLoop)
    : rel_(rel),
      ioLoop_(ioLoop),
      bizLoop_(bizLoop),
      reqIdSeq_(std::make_shared<std::atomic<uint32_t>>(1)) {}

RpcFuture RpcCodec::call(const std::string& method, const std::string& payload,
                         const RpcCallOptions& opts){
    RpcFuture fut = RpcFuture::make(bizLoop_);          // 未完成句柄，调用方立即拿到
    if(ioLoop_ == nullptr){
        fut.complete(RpcResult{RpcStatus::Unavailable, "", "no io loop"});
        return fut;
    }

    // req_id 在调用线程分配（原子）：任意线程 call() 都不需要等 IO 线程。
    const uint32_t reqId = reqIdSeq_->fetch_add(1, std::memory_order_relaxed);

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

void RpcCodec::startCallInLoop(uint32_t reqId, std::string body,
                               const RpcCallOptions& opts, RpcFuture fut){
    if(stopped_){
        exhausted_.fetch_add(1);
        fut.complete(RpcResult{RpcStatus::Unavailable, "", "client stopped"});
        return;
    }
    if(inflight_.size() >= maxInflight_){               // 背压闸：fail-fast，不阻塞调用线程
        exhausted_.fetch_add(1);
        Logger::instance().log(Level::WARN, "[rpc] inflight full (", inflight_.size(),
                               "), reject req=", reqId);
        fut.complete(RpcResult{RpcStatus::ResourceExhausted, "", "max in-flight exceeded"});
        return;
    }

    InflightCall& ic = inflight_[reqId];
    ic.future  = fut;
    ic.timerId = 0;
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
    const uint32_t reqId = resp.req_id();
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

void RpcCodec::onTimeout(uint32_t reqId){
    if(inflight_.find(reqId) == inflight_.end()) return;   // 已被响应/取消收口
    timeouts_.fetch_add(1);
    Logger::instance().log(Level::WARN, "[rpc] deadline exceeded req=", reqId);
    finish(reqId, RpcResult{RpcStatus::DeadlineExceeded, "", "deadline exceeded"});
}

void RpcCodec::cancelInLoop(uint32_t reqId){
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

void RpcCodec::finish(uint32_t reqId, RpcResult r){
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
