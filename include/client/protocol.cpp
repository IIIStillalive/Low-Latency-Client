#include "client/protocol.hpp"

#include <cstring>
#include "net/length_header_codec.hpp"  // LengthHeaderCodec::send / onMessage 需要
#include "net/log.hpp"

namespace server {

namespace {
// 主机序 → 4 字节大端写出到 buf[0..3]
void writeInt32BigEndian(char* buf, uint32_t v){
    buf[0] = static_cast<char>((v >> 24) & 0xFF);
    buf[1] = static_cast<char>((v >> 16) & 0xFF);
    buf[2] = static_cast<char>((v >> 8)  & 0xFF);
    buf[3] = static_cast<char>( v        & 0xFF);
}
// 4 字节大端 → 主机序
uint32_t readInt32BigEndian(const char* p){
    const auto* u = reinterpret_cast<const unsigned char*>(p);
    return (static_cast<uint32_t>(u[0]) << 24) | (static_cast<uint32_t>(u[1]) << 16)
         | (static_cast<uint32_t>(u[2]) << 8)  |  static_cast<uint32_t>(u[3]);
}
} // namespace

bool ProtocolCodec::decodeHeader(const char* p, size_t n,
                                 MsgType& type, uint32_t& seq, uint32_t& bodyLen){
    if(p == nullptr || n < kHeadLen) return false;          // 数据不足一帧头
    type = static_cast<MsgType>(static_cast<uint8_t>(p[0]));
    seq  = readInt32BigEndian(p + 1);
    bodyLen = static_cast<uint32_t>(n - kHeadLen);          // Day4-P0：body 到帧尾，不再读线上字段
    return true;
}

void ProtocolCodec::onMessage(TcpConnection* conn, const std::string& payload){
    MsgType   type;
    uint32_t  seq;
    uint32_t  bodyLen;
    if(!decodeHeader(payload.data(), payload.size(), type, seq, bodyLen)){
        Logger::instance().log(Level::ERROR, "protocol: bad frame header, drop");
        return;                                             // 协议错：丢弃该帧，不断连
    }
    std::string body(static_cast<const char*>(payload.data()) + kHeadLen, bodyLen);
    if(messageCallback_) messageCallback_(conn, type, seq, body);
}

void ProtocolCodec::sendFrame(TcpConnection* conn, MsgType type, uint32_t seq,
                              const void* body, size_t n){
    std::string packet;
    packet.reserve(kHeadLen + n);

    char head[kHeadLen];
    head[0] = static_cast<char>(type);
    writeInt32BigEndian(head + 1, seq);
    packet.append(head, sizeof(head));
    if(n > 0) packet.append(static_cast<const char*>(body), n);

    lengthCodec_.send(conn, packet.data(), packet.size());  // 底层补 4B 长度头
}

void ProtocolCodec::sendHeartbeat(TcpConnection* conn, uint32_t seq){
    sendFrame(conn, MsgType::Heartbeat, seq, nullptr, 0);
}
void ProtocolCodec::sendAck(TcpConnection* conn, uint32_t seq){
    sendFrame(conn, MsgType::Ack, seq, nullptr, 0);         // seq = 被确认的消息号
}
void ProtocolCodec::sendQuit(TcpConnection* conn){
    sendFrame(conn, MsgType::Quit, 0, nullptr, 0);
}

} // namespace server