// ProtocolCodec 独立自测：不依赖真实 server，用 socketpair 模拟对端
//   client(protocol.cpp) ── socketpair ──> 测试读回原始字节，反向喂回 onMessage
// 验证：
//   1) sendFrame 打包 → 底层 Codec 加长度头 → 字节流对端可还原帧头 + body
//   2) onMessage 解包 → 业务回调拿到正确 type/seq/body
//   3) 异常边界：数据不足一帧头（< kHeadLen）→ 正确拦截；bodyLen 由帧长推导
//
// 协议帧（Day4-P0 瘦身，5B 头）：[1B type][4B seq][body...]——body 到帧尾，无 bodyLen 字段
#include <cassert>
#include <cstring>
#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>

#include "net/event_loop.hpp"
#include "net/length_header_codec.hpp"
#include "server/tcp_connnection.hpp"
#include "net/log.hpp"       // Logger / Level：自测日志
#include "client/protocol.hpp"

using namespace server;

// 读满 socketpair fd 上已可读的全部字节（简单版：读到 EAGAIN）
static std::string readAll(int fd){
    std::string out;
    char buf[4096];
    for(;;){
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if(n > 0){ out.append(buf, static_cast<size_t>(n)); }
        else if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        else break;  // 0（对端关）或错误：本轮取到啥算啥
    }
    return out;
}

int main(){
    Logger::instance().set_level(Level::TRACE);

    // socketpair：sock(0) 当对端，sock(1) 交给 TcpConnection 当”client 侧连接”
    int sp[2];
    if(::socketpair(AF_UNIX, SOCK_STREAM, 0, sp) < 0){ std::cerr << "socketpair failed\n"; return 1; }
    // 测试读侧设为非阻塞：readAll 靠 EAGAIN 判断"本轮取完"，阻塞 fd 会在没数据时永远等
    ::fcntl(sp[0], F_SETFL, O_NONBLOCK);

    EventLoop loop;                       // TcpConnection 需要一个 loop 指针（本测试不跑它）
    TcpConnection conn(sp[1], &loop);     // 未上树；send() 直写路径，不碰 epoll

    LengthHeaderCodec lc;                 // 底层切帧（补/剥 4B 长度头）
    ProtocolCodec proto(lc);              // 私有协议层

    // 业务回调：收包断言
    bool gotFrame = false;
    proto.setMessageCallback([&](TcpConnection*, MsgType type, uint32_t seq, const std::string& body){
        gotFrame = true;
        assert(type == MsgType::Data);
        assert(seq == 42);
        assert(body == "hello im");
        std::cout << "[ok] onMessage 解包: type=Data seq=42 body=\"" << body << "\"\n";
    });

    // 1) 打包发送 → 对端读原始字节
    const char* payload = "hello im";
    proto.sendFrame(&conn, MsgType::Data, 42, payload, std::strlen(payload));
    std::string raw = readAll(sp[0]);
    assert(!raw.empty());

    // 2) 按 LengthHeaderCodec 语义剥 4B 长度头，payload 喂回 onMessage
    //    （真实链路里这一步是 server 端 LengthHeaderCodec::onMessage 做的）
    assert(raw.size() >= 4);
    uint32_t len = (static_cast<unsigned char>(raw[0]) << 24) | (static_cast<unsigned char>(raw[1]) << 16)
                 | (static_cast<unsigned char>(raw[2]) << 8)  |  static_cast<unsigned char>(raw[3]);
    std::string payloadFrag = raw.substr(4, len);
    proto.onMessage(&conn, payloadFrag);
    assert(gotFrame);

    // 3) 异常边界 + bodyLen 推导（不再有线上 bodyLen 字段）
    {
        MsgType t; uint32_t s, bl;
        assert(!ProtocolCodec::decodeHeader("", 0, t, s, bl));       // 空
        assert(!ProtocolCodec::decodeHeader("1234", 4, t, s, bl));   // 不足 5B 帧头
        // 5B 帧头（type + seq，无 body）：bodyLen 推导恒等于 0
        std::string hdrHead(ProtocolCodec::kHeadLen, '\0');
        hdrHead[0] = static_cast<char>(MsgType::Ack);
        // seq = 42（大端）：hdrHead[1..4]
        hdrHead[1] = 0; hdrHead[2] = 0; hdrHead[3] = 0; hdrHead[4] = 42;
        assert(ProtocolCodec::decodeHeader(hdrHead.data(), hdrHead.size(), t, s, bl));
        assert(s == 42 && bl == 0);
        // 帧头 + body（body 到帧尾）：bodyLen = n - kHeadLen
        std::string hdrBody = hdrHead + "hello im";
        assert(ProtocolCodec::decodeHeader(hdrBody.data(), hdrBody.size(), t, s, bl));
        assert(s == 42 && bl == 8);
        std::cout << "[ok] 异常边界拦截: 空/短帧头；bodyLen 由帧长推导\n";
    }

    ::close(sp[0]);
    ::close(sp[1]);
    std::cout << "PROTOCOL ROUNDTRIP PASS\n";
    return 0;
}