#pragma once

// Day3 可靠消息层（客户端单连接；每连接一实例）
//
// 产品形态：包在 ProtocolCodec 之上的"有状态"层，补全现有协议骨架（Ack 帧/sendAck 已定义但
// 无人用）缺失的行为：发送自动序号 + 未确认缓冲 + 超时重传 + 收侧去重 + 自动回 Ack。
// Day4-A1 追加：断线挂起 pending_，重连全量重发（server 侧 seen 幂等去重），不再断线作废。
// Day4-B  追加：业务侧异步投递（setDeliveryLoop）——业务回调挪到独立业务 loop 执行，
//              IO 线程只剩轻活（收帧/去重/Ack/发送），慢业务不再卡死整个事件循环。
//
// 分层接线（现有层零改动，ProtocolCodec 保持无状态纯编解码）：
//   发: 业务 → sendData ──自动seq→ ProtocolCodec::sendFrame ──4B头→ LengthHeaderCodec::send
//   收: 业务 ← onFrame ← ProtocolCodec::onMessage（已拆 5B 头）
//
// 语义契约：
//   - 可靠性覆盖「连接存活期 + 存活期内断线重连」：断线挂起（见 onDisconnected），
//     重连成功 onConnected 全量重发；server 用已见 seq 幂等（重复 → 只 Ack 不执行）。
//     注意边界：server 进程重启会让 seen 清零，旧帧会被当新帧重放 —— A1 只保 server
//     存活期内的断开恢复，跨进程幂等不在本层承诺（那是全局单调 seq 方案的范畴）。
//   - 去重：单连接 + TCP 保序下重传只会带来 seq ≤ 已收最大值 的旧帧 → lastRecvSeq_ 单调前进
//     够用（有符号差值判新旧，防 uint32 回绕），不做乱序窗口。
//   - 重传：runEvery 周期扫描 pending_，超 timeout 未确认则重发，超过 maxRetries 次放弃并通知。
//   - 缓存兜底：setCacheLimit 设上限/TTL；超上限丢最旧 + LOST，超 TTL 作废 + LOST（防内存/陈旧堆积）。
//
// 生命周期：本对象必须在 EventLoop 存活期内使用；退出前显式 stop() 取消 tick 定时器，
// 否则析构后 tick 的 [this] 捕获会悬垂（择优：go 前 stop，同 TcpClient 的 loop 线程纪律）。
//
// DNS：单连接语义 —— sendData 的发送目标权威是「当前活动连接」connRef_，
//   跨线程调用时入参与之不同（连接已换代）则只入 pending_，等重连/重传兜底。
//
// ⚠️ 业务回调执行现场（异步模式）：
//   - messageCallback_ 在 delivery loop 线程执行；回调里碰任何 EventLoop API
//     （定时器 runAfter/runEvery、runInLoop 等）都必须经 loop_->runInLoop 转发——
//     EventLoop 的 timerQueue/pendingFunctors_ 非线程安全。
//   - sendData 自身线程安全（内部 runInLoop 转回 IO 线程），回调里可直接调。
//   - 销毁次序（硬约束）：先 client.stop() 清干投递队列，再退业务 loop
//     （EventLoopThread::quit/析构 join 完成），最后本对象析构——残余 drain 任务
//     仍捕获 this，次序颠倒 = UAF。
//
// 异步投递背压：BoundedQueue 容量满 → 逐出最旧业务帧 + LOST（lostCallback_ 通道）——
//   业务帧的 LOST 与重传 LOST 同属"放弃该 seq 的通知"，含义统一为「不再担保交付」。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/protocol.hpp"   // MsgType / ProtocolCodec::MessageCallback
#include "net/bounded_queue.hpp" // 异步投递背压：有界 FIFO，满逐最旧 + LOST
#include "net/timer.hpp"         // TimerId

namespace server {

class TcpConnection;
class EventLoop;

class ReliableCodec {
public:
    using MessageCallback = ProtocolCodec::MessageCallback;  // 交业务的帧
    using LostCallback    = std::function<void(uint32_t seq)>;  // 未确认被放弃的帧（断线/超限）

    ReliableCodec(ProtocolCodec& proto, EventLoop* loop);

    void setMessageCallback(MessageCallback cb){ messageCallback_ = std::move(cb); }
    void setLostCallback(LostCallback cb)      { lostCallback_ = std::move(cb); }

    // 重传参数：timeoutSec 未确认即重发；tickSec 扫描周期；maxRetries 最多重发次数（默认 3）。
    void setRetransmit(double timeoutSec = 2.0, double tickSec = 0.5, int maxRetries = 3);

    // Day4-A1 缓存兜底：maxFrames 上限（超出先抛最旧）+ ttlSec 总寿命（超出作废）。
    // 默认均 0 = 不限制/不作废（保持 Day3 行为）：需要缓存回收才显式调用。
    void setCacheLimit(size_t maxFrames = 1024, double ttlSec = 30.0);

    // Day4-B 异步投递：业务回调挪到 deliveryLoop 所在线程执行（业务 loop，承载
    // 方为 EventLoopThread；调用须在连接建立/投递前配置期完成）。
    //   queueLimit = 业务队列入队上限，满则逐出最旧 + LOST（背压闸）。
    //   deliveryLoop == nullptr → 回退同步模式（Day3 默认：回调在 IO 线程直调）。
    void setDeliveryLoop(EventLoop* deliveryLoop, size_t queueLimit = 1024);

    // 发：自动分配递增 seq → 缓存未确认帧 → 打包发送。body 拷贝进内部缓冲等待确认。
    // Day4-B 线程安全：任意线程可调。业务线程（异步回调）调用 → 内部 runInLoop 转交
    // IO 线程执行；发送目标 = 当前活动连接 connRef_（断开期间只入 pending_，重连重发兜底）。
    void sendData(TcpConnection* conn, const void* body, size_t n);

    // Day5：带帧类型的可靠发送（Request/Cancel 等 RPC 帧复用同一套 seq/Ack/重传/断线重发）。
    // 帧类型随 pending 一起缓存 —— 重传与重连重发必须用原类型发回，否则协议语义就变了。
    //   sendFrame(conn, type, ...)  显式目标（同 sendData 的 conn 语义）
    //   sendFrame(type, ...)        发往「当前活动连接」，连接断开期间只入 pending_
    void sendFrame(TcpConnection* conn, MsgType type, const void* body, size_t n);
    void sendFrame(MsgType type, const void* body, size_t n);

    // 收：由 ProtocolCodec 的消息回调驱动。[IO 线程，同步模式与异步模式的公共轻活]
    //   Ack            → 匹配 pending_ 移除；
    //   Data/Request   → 有序请求流：新 seq 回 Ack + 交业务，旧 seq 只回 Ack（重传重复帧）；
    //   Response/Cancel→ RPC 响应类：可能乱序到达，**不做单调 seq 去重、不回 Ack**，
    //                    直接交业务；重复/迟到响应由 RpcCodec 按 req_id 幂等丢弃；
    //   Heartbeat/Quit → 交业务（不涉可靠）。
    //   交业务两条路径：同步模式 messageCallback_ 直调（Day3 默认）；
    //                 异步模式（setDeliveryLoop）→ enqueue 业务队列 + 唤醒业务 loop
    //                 一次性 drain，队列满逐最旧 + LOST（不给 IO 线程加背压）。
    void onFrame(TcpConnection* conn, MsgType type, uint32_t seq, const std::string& body);

    // 连接回调（由 TcpClient 或业务连接回调同步转发）：
    //   onConnected      记录当前连接 + 挂起缓存全量重发（Day4-A1）+ 恢复 tick；
    //   onDisconnected   取消 tick + 挂起 pending_（不清空、不全量 LOST）；
    //                    断线期间无连接可发，等重连回调触发重发。
    void onConnected(TcpConnection* conn);
    void onDisconnected();

    void stop();                       // 进程退出收尾：取消 tick + 清 pending + 全量 LOST（不挂起）

    size_t pendingCount() const { return pending_.size(); }   // 调试/测试：当前未确认帧数
    std::vector<uint32_t> pendingSeqs() const;                // 诊断：当前未确认帧的 seq 列表

private:
    struct PendingFrame {
        MsgType     type = MsgType::Data;   // Day5：重传/重连重发必须用原帧类型
        std::string body;      // 原始 body（重传时重新打包 5B 头，帧头永远由协议层造）
        int64_t     sentUs;    // 最近一次发送时刻（微秒）
        int64_t     bornUs;    // 入队时刻（微秒）：TTL 兜底的计时起点，不随重发刷新
        int         retries;   // 已重发次数
    };

    void tick();                       // [IO 线程] runEvery 扫描：超时重发 / 超限放弃
    void ensureTick();                 // 惰性启动 tick（首个 pending 才 runEvery）
    void purgeTimeoutFramesNow();      // [IO 线程] 重连时按 TTL 甩掉过期帧 + LOST
    void enforceMaxFrames();           // 发送侧：超上限 → 抛最旧（按 bornUs）+ LOST

    // ---- Day4-B 异步投递（业务帧跨线程）----
    struct DeliveryItem {                          // 一项待投递给业务的已解析帧
        std::shared_ptr<TcpConnection> conn;       // 持有引用：连接关闭后仍能安全投递完残留帧
        MsgType   type;
        uint32_t  seq;
        std::string body;
    };
    void deliverAsync(std::shared_ptr<TcpConnection> conn, MsgType type,
                      uint32_t seq, std::string body);   // [IO 线程] 入队 + 精准唤醒一次
    void drainDelivery();                            // [delivery loop 线程] 弹空一批交业务

    void sendFrameInLoop(MsgType type, const std::string& body,
                         TcpConnection* intended);   // [IO 线程] 发送主体

    ProtocolCodec& proto_;
    EventLoop* loop_;

    MessageCallback messageCallback_;
    LostCallback    lostCallback_;

    // Day4-B：异步投递状态。deliveryQueue_ 非空 = 异步模式开启。
    std::unique_ptr<BoundedQueue<DeliveryItem>> deliveryQueue_;   // 有界：满逐最旧 + LOST
    EventLoop*              deliveryLoop_          = nullptr;     // 业务事件循环（EventLoopThread 承载）
    bool                    asyncEnabled_          = false;
    std::atomic<bool>       deliveryDraining_      { false };    // true = 已有 drain 在跑/在途（防重复排队）
    std::shared_ptr<TcpConnection> connRef_;                      // 当前活动连接权威引用（跨线程保活）

    std::unordered_map<uint32_t, PendingFrame> pending_;
    uint32_t nextSeq_       = 1;     // 发送序号源（调用方不再手传 seq）
    uint32_t lastRecvSeq_   = 0;     // 收侧去重：单调前进

    double timeoutSec_ = 2.0;
    double tickSec_    = 0.5;
    int    maxRetries_ = 3;

    size_t maxPendingFrames_ = 0;   // Day4-A1 缓存上限；0 = 不限制
    double pendingTtlSec_    = 0;   // Day4-A1 缓存总寿命；0 = 不作废

    TimerId         tickId_ = 0;     // 非 0 = tick 在跑；stop/onDisconnected cancel
    TcpConnection*  conn_   = nullptr;   // 当前连接（onConnected 记录；tick 重传用它发帧）
};

} // namespace server