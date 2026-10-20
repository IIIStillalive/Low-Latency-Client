#pragma once

// 客户端私有二进制协议（IM SDK 第一版）
//
// 分层：底层仍用 LengthHeaderCodec 做 [4B 大端长度][payload] 的粘包/半包切帧，
// 本协议只在 payload 内再切一层 —— 保证 server 既有代码零改动、两端语义一致。
//
// payload := [1B type][4B seq 大端][body...]
//             └────────── 5B 帧头，定死 ──────────┘
//
// Day4-P0 瘦身：原 9B 帧头含 [4B bodyLen]，与底层 4B length 完全冗余（length ≡ 5 + bodyLen），
// 已砍。body 现在定到帧尾为止，bodyLen = 帧长 - kHeadLen，由帧长推导、不再线上传输。
//
// ⚠️ 字节序：seq 一律大端写读（Linux 读端可 memcpy 后自行转主机序）。

#include <cstdint>
#include <functional>
#include <string>

namespace server {

class TcpConnection;
class LengthHeaderCodec;

// 消息种类：body 的解释方式由 type 决定
enum class MsgType : uint8_t {
    Heartbeat = 0x01,   // 心跳探测，body 空
    Ack       = 0x02,   // 确认收到某 seq（seq 字段 = 被确认的那条消息号），body 空
    Data      = 0x03,   // 用户数据，body = 透传内容
    Quit      = 0x04,   // 主动下线告别帧，body 空
};

// 私有协议编解码层：夹在 LengthHeaderCodec 与业务回调之间
//   收：LengthHeaderCodec::onMessage（已剥长度头，payload = 一帧完整消息）→ 本层解 5B 头 → 业务回调
//   发：业务 → 本层打包 5B 头 + body → LengthHeaderCodec::send（补 4B 长度头）→ 底层传输
class ProtocolCodec {
public:
    // 业务回调：收到一条「已解析」的私有帧。body 为拷贝，生命周期安全。
    using MessageCallback = std::function<void(TcpConnection*, MsgType,
                                               uint32_t seq, const std::string& body)>;

    explicit ProtocolCodec(LengthHeaderCodec& lc) : lengthCodec_(lc) {}

    void setMessageCallback(const MessageCallback& cb) { messageCallback_ = cb; }

    // ---- 接收侧：由 LengthHeaderCodec 的消息回调驱动，payload = 一帧完整 payload ----
    void onMessage(TcpConnection* conn, const std::string& payload);

    // ---- 发送侧：打包私有帧头，交底层 Codec 补长度头发送 ----
    void sendFrame(TcpConnection* conn, MsgType type, uint32_t seq,
                   const void* body, size_t n);
    void sendHeartbeat(TcpConnection* conn, uint32_t seq);  // body 空
    void sendAck     (TcpConnection* conn, uint32_t seq);   // 确认第 seq 号消息
    void sendQuit    (TcpConnection* conn);                 // 下线告别，seq 可传 0

    // ---- 解包原语（纯函数，便于单测）----
    static constexpr size_t kHeadLen   = 5;      // 1 type + 4 seq（body 到帧尾）
    static constexpr size_t kMaxFrame  = 65536;  // 与 LengthHeaderCodec 对齐的最长 payload
    // 从 payload 起点解帧头：成功返回 true 并填 type/seq；bodyLen = n - kHeadLen（推导，body 到帧尾）
    static bool decodeHeader(const char* p, size_t n,
                             MsgType& type, uint32_t& seq, uint32_t& bodyLen);

private:
    LengthHeaderCodec& lengthCodec_;   // 底层：补/剥 4B 长度头的唯一权威
    MessageCallback    messageCallback_;
};

} // namespace server