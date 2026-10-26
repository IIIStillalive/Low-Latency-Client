#include "client/reliable_codec.hpp"

#include <memory>   // std::make_unique / shared_ptr

#include "net/event_loop.hpp"      // EventLoop 完整定义：runEvery / cancel / runInLoop
#include "net/log.hpp"
#include "server/tcp_connnection.hpp"  // TcpConnection 完整类型：shared_from_this() 需要（头文件保持前向声明）

namespace server {

ReliableCodec::ReliableCodec(ProtocolCodec& proto, EventLoop* loop)
    : proto_(proto), loop_(loop) {}

void ReliableCodec::setRetransmit(double timeoutSec, double tickSec, int maxRetries){
    timeoutSec_ = timeoutSec;
    tickSec_    = tickSec;
    maxRetries_ = maxRetries;
}

void ReliableCodec::setCacheLimit(size_t maxFrames, double ttlSec){
    maxPendingFrames_ = maxFrames;
    pendingTtlSec_    = ttlSec;
}

void ReliableCodec::setDeliveryLoop(EventLoop* deliveryLoop, size_t queueLimit){
    // 配置期调用（连接建立/投递开始前）。nullptr → 回退同步模式（Day3 行为）。
    deliveryLoop_ = deliveryLoop;
    if(deliveryLoop){
        deliveryQueue_ = std::make_unique<BoundedQueue<DeliveryItem>>(queueLimit);
        asyncEnabled_  = true;
    } else {
        deliveryQueue_.reset();
        asyncEnabled_  = false;
    }
}

// 发：任意线程可调。业务线程（异步回调现场）→ runInLoop 转 IO 线程执行。
void ReliableCodec::sendData(TcpConnection* conn, const void* body, size_t n){
    sendFrame(conn, MsgType::Data, body, n);
}

void ReliableCodec::sendFrame(TcpConnection* conn, MsgType type, const void* body, size_t n){
    if(loop_ == nullptr || body == nullptr || n == 0) return;   // 空载拒绝：不烧 seq
    if(loop_->isInLoopThread()){
        if(conn != nullptr) conn_ = conn;   // Day3 直接路径保持《目标 = 入参 conn》语义（self-heal）
        // conn == nullptr = 「发往当前活动连接」：不动 conn_（RPC 层在 IO 线程内发帧用这条）
        sendFrameInLoop(type, std::string(static_cast<const char*>(body), n), conn);
        return;
    }
    // 跨线程（Day4-B）：目标权威 = 当前活动连接 connRef_（持有引用防止任务执行前被析构）
    std::shared_ptr<TcpConnection> target = connRef_;
    loop_->runInLoop(
        [this, type, body = std::string(static_cast<const char*>(body), n),
         target = std::move(target)]() mutable {
            sendFrameInLoop(type, std::move(body), target.get());
        });
}

void ReliableCodec::sendFrame(MsgType type, const void* body, size_t n){
    sendFrame(nullptr, type, body, n);
}

void ReliableCodec::sendFrameInLoop(MsgType type, const std::string& body, TcpConnection* intended){
    uint32_t seq = nextSeq_++;         // 自动序号：调用方不再手传
    PendingFrame pf;
    pf.type   = type;                  // Day5：重传/重连重发按原类型发回
    pf.body   = body;
    pf.sentUs = nowMicros();
    pf.bornUs = pf.sentUs;           // TTL 起点：入队时刻，不随重发刷新
    pf.retries = 0;
    pending_[seq] = std::move(pf);
    Logger::instance().log(Level::DEBUG, "[rel] send type=", static_cast<int>(type),
                           " seq=", seq, " bodyLen=", body.size());
    enforceMaxFrames();                // Day4-A1：超上限先抛最旧 + LOST
    // 发送目标校验：intended 空（发往当前活动连接）或等于当前 conn_ 才上线；
    // 否则（连接已换代/断开期调用）只入 pending_ —— onConnected 全量重发兜底（A1 咬合）。
    const bool targetCurrent = (conn_ != nullptr) && (intended == nullptr || intended == conn_);
    if(targetCurrent){
        proto_.sendFrame(conn_, type, seq, body.data(), body.size());
    } else {
        Logger::instance().log(Level::INFO, "ReliableCodec park seq=", seq,
                               " no current conn (reconnect resend covers)");
    }
    ensureTick();
}

void ReliableCodec::onFrame(TcpConnection* conn, MsgType type, uint32_t seq, const std::string& body){
    // 诊断日志（DEBUG 级）：定位 end-to-end 确认丢失用；Level::DEBUG 才可见
    Logger::instance().log(Level::DEBUG, "[rel] recv type=", static_cast<int>(type),
                           " seq=", seq, " pendBefore=", pending_.size());
    if(type == MsgType::Ack){
        const size_t erased = pending_.erase(seq);   // 对 seq 的确认：从未确认缓冲移除，不再重传
        Logger::instance().log(Level::DEBUG, "[rel] ack seq=", seq,
                               " erased=", erased, " pendAfter=", pending_.size());
        return;
    }
    if(type == MsgType::Data || type == MsgType::Request){
        // 有序请求流：有符号差值判新旧（防 uint32 回绕）。旧帧 = 重传重复，不回业务但仍需回 Ack
        // （对方可能在等我们的确认）；新帧才更新 lastRecvSeq_ 并交业务。
        if(static_cast<int64_t>(seq) - static_cast<int64_t>(lastRecvSeq_) > 0){
            lastRecvSeq_ = seq;
        } else {
            proto_.sendAck(conn, seq);
            return;
        }
        proto_.sendAck(conn, seq);     // 自动回 Ack
    }
    // Response/Cancel：RPC 响应可能乱序（服务端多 worker/延迟返回），单调 seq 判旧会把
    // 后到的早响应误当重复帧丢掉 —— 明确绕过去重与自动 Ack。响应可靠性由「请求断线重发 +
    // 服务端 reqId 缓存重放」覆盖；重复响应由 RpcCodec 的 inflight 表幂等丢弃。
    if(!messageCallback_) return;
    if(asyncEnabled_ && deliveryQueue_){
        // Day4-B 异步：入队业务帧 + 唤醒业务 loop（轻活留在 IO 线程；背压交给有界队列）
        deliverAsync(conn->shared_from_this(), type, seq, std::move(std::string(body)));
    } else {
        messageCallback_(conn, type, seq, body);   // 同步模式（Day3 默认）
    }
}

// [IO 线程] 入队 + 精准唤醒：draining_ 防重复排队，业务 loop 的 pendingFunctors_ 至多一个 drain
void ReliableCodec::deliverAsync(std::shared_ptr<TcpConnection> conn, MsgType type,
                                 uint32_t seq, std::string body){
    if(auto evicted = deliveryQueue_->push(DeliveryItem{std::move(conn), type, seq, std::move(body)})){
        if(lostCallback_) lostCallback_(evicted->seq);   // 队列满：逐出最旧业务帧 + LOST
    }
    if(!deliveryDraining_.load() && !deliveryDraining_.exchange(true)){
        deliveryLoop_->runInLoop([this]{ drainDelivery(); });
    }
}

void ReliableCodec::drainDelivery(){
    // [delivery loop 线程] 弹空一批 → 依次交业务（保序：单 worker + FIFO）
    for(;;){
        std::vector<DeliveryItem> batch;
        while(deliveryQueue_->tryPopAll(batch) > 0){
            for(auto& it : batch){
                if(messageCallback_) messageCallback_(it.conn.get(), it.type, it.seq, it.body);
            }
            batch.clear();
        }
        deliveryDraining_.store(false);
        if(deliveryQueue_->empty()) return;          // 收工：无人再排队
        if(deliveryDraining_.exchange(true)) return; // 并发生产方已另排 drain → 让位
        // 自续：本轮弹空期间又来了新帧 → 内联再来一轮（delivery loop 线程上 runInLoop=直调）
    }
}

void ReliableCodec::onConnected(TcpConnection* conn){
    conn_ = conn;
    connRef_ = conn->shared_from_this();   // Day4-B：权威连接引用（跨线程保活发送目标）
    if(pending_.empty()) return;

    // Day4-A1：重连是"挂起缓存"的恢复点。
    //   先甩掉已超 TTL 作废的旧帧（它们在断开期间已经失去时效），
    //   剩下的全量重发：retries 重置（视为全新机会）、sentUs 刷新（重新计超时）。
    purgeTimeoutFramesNow();
    if(pending_.empty()) return;

    const int64_t now = nowMicros();
    for(auto& kv : pending_){
        PendingFrame& f = kv.second;
        f.retries = 0;
        f.sentUs  = now;
        Logger::instance().log(Level::INFO, "ReliableCodec resend-on-reconnect seq=", kv.first);
        proto_.sendFrame(conn_, f.type, kv.first, f.body.data(), f.body.size());
    }
    ensureTick();
}

void ReliableCodec::onDisconnected(){
    if(tickId_ != 0){
        loop_->cancel(tickId_);        // 断开即停扫描；与析构共存无悬垂
        tickId_ = 0;
    }
    conn_ = nullptr;
    connRef_.reset();                 // Day4-B：连接已逝，权威引用作废（重连恢复点在 onConnected）
    // Day4-A1：挂起而非作废。pending_ 保留、不通知丢失——等重连 onConnected 全量重发。
    //   退出进程走 stop()（仍按 Day3 语义清空 + LOST）。
}

void ReliableCodec::stop(){
    if(tickId_ != 0){
        loop_->cancel(tickId_);        // 退出收尾：取消 tick
        tickId_ = 0;
    }
    if(!pending_.empty() && lostCallback_){        // 进程退出不赖着：全量作废 + 通知
        for(const auto& kv : pending_) lostCallback_(kv.first);
    }
    pending_.clear();
    if(deliveryQueue_){                            // Day4-B：投递队列里没交出去的业务帧同样作废 + LOST
        std::vector<DeliveryItem> leftovers;
        while(deliveryQueue_->tryPopAll(leftovers) > 0){
            for(auto& it : leftovers) if(lostCallback_) lostCallback_(it.seq);
            leftovers.clear();
        }
    }
    conn_ = nullptr;
    connRef_.reset();
}

void ReliableCodec::purgeTimeoutFramesNow(){
    if(pendingTtlSec_ <= 0 || pending_.empty()) return;
    const int64_t ttlUs = static_cast<int64_t>(pendingTtlSec_ * 1000000.0);
    const int64_t now   = nowMicros();
    for(auto it = pending_.begin(); it != pending_.end(); ){
        PendingFrame& f = it->second;
        if(now - f.bornUs >= ttlUs){                 // 总寿命超 TTL → 作废（不管是否还在重传窗口）
            const uint32_t seq = it->first;
            Logger::instance().log(Level::WARN, "ReliableCodec purge seq=", seq, " ttl-exceeded");
            it = pending_.erase(it);
            if(lostCallback_) lostCallback_(seq);
        } else {
            ++it;
        }
    }
}

void ReliableCodec::enforceMaxFrames(){
    if(maxPendingFrames_ == 0 || pending_.size() <= maxPendingFrames_) return;
    // 找出 bornUs 最久远（最旧）的一条抛掉 + LOST —— 缓存推满时退出最旧，给新帧腾位
    auto oldestIt = pending_.begin();
    for(auto it = pending_.begin(); it != pending_.end(); ++it){
        if(it->second.bornUs < oldestIt->second.bornUs) oldestIt = it;
    }
    const uint32_t seq = oldestIt->first;
    Logger::instance().log(Level::WARN, "ReliableCodec cache-full, evict oldest seq=", seq,
                           " pending=", pending_.size());
    pending_.erase(oldestIt);
    if(lostCallback_) lostCallback_(seq);
}

std::vector<uint32_t> ReliableCodec::pendingSeqs() const{
    std::vector<uint32_t> out;
    out.reserve(pending_.size());
    for(const auto& kv : pending_) out.push_back(kv.first);
    return out;
}

void ReliableCodec::tick(){           // [loop 线程] runEvery 驱动
    if(pending_.empty() || conn_ == nullptr) return;
    const int64_t timeoutUs = static_cast<int64_t>(timeoutSec_ * 1000000.0);
    const int64_t now = nowMicros();
    for(auto it = pending_.begin(); it != pending_.end(); ){
        PendingFrame& f = it->second;
        if(now - f.sentUs < timeoutUs){ ++it; continue; }        // 未到超时，跳过
        if(f.retries >= maxRetries_){                            // 已用尽重试 → 放弃本帧
            const uint32_t seq = it->first;
            Logger::instance().log(Level::WARN, "ReliableCodec give up seq=", seq, " after ", f.retries, " retries");
            it = pending_.erase(it);
            if(lostCallback_) lostCallback_(seq);
            continue;
        }
        ++f.retries;
        f.sentUs = now;                                          // 重发：更新时间，重新计超时
        Logger::instance().log(Level::INFO, "ReliableCodec retransmit seq=", it->first, " attempt=", f.retries);
        proto_.sendFrame(conn_, f.type, it->first, f.body.data(), f.body.size());
        ++it;
    }
}

void ReliableCodec::ensureTick(){
    if(tickId_ != 0 || loop_ == nullptr) return;
    tickId_ = loop_->runEvery(tickSec_, [this]{ tick(); });
}

} // namespace server