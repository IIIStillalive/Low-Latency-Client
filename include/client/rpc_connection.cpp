#include "client/rpc_connection.hpp"

#include "server/tcp_connnection.hpp"   // 连接回调里用 TcpConnection 完整类型

namespace server {

RpcConnection::RpcConnection(EventLoop* ioLoop, const struct sockaddr_in& peer, EventLoop* bizLoop)
    : ioLoop_(ioLoop),
      protoCodec_(lengthCodec_),
      relCodec_(protoCodec_, ioLoop),
      rpcCodec_(relCodec_, ioLoop, bizLoop != nullptr ? bizLoop : ioLoop),
      client_(ioLoop, peer) {
    // 接线：TcpClient 已内置 LengthHeaderCodec 切帧，回调拿到的是「一条完整 payload」
    client_.setMessageCallback([this](TcpConnection* conn, const std::string& payload){
        protoCodec_.onMessage(conn, payload);          // 拆 5B 私有头
    });
    protoCodec_.setMessageCallback([this](TcpConnection* conn, MsgType type,
                                          uint32_t seq, const std::string& body){
        relCodec_.onFrame(conn, type, seq, body);      // Ack/去重/重传记账
    });
    relCodec_.setMessageCallback([this](TcpConnection* conn, MsgType type,
                                        uint32_t seq, const std::string& body){
        rpcCodec_.onFrame(conn, type, seq, body);      // Response → req_id 关联
    });
    // RPC 连接状态（Day7 D3/D4）：回调顺序固定，错了会丢帧——
    //   onConnected 先喂 ReliableCodec（重置 seq + 重发 pending_），再让 RpcCodec 重发 in-flight
    //   （用**新 seq**，服务端按 req_id 去重），最后通知池更新 available。
    //   断开时先 relCodec_.onDisconnected()（pending_ 原样保留）再通知池。
    client_.setConnectionCallback([this](TcpConnection* conn){
        if(conn != nullptr){
            relCodec_.onConnected(conn);       // ① ReliableCodec：重置 seq 空间 + 重发 pending_
            rpcCodec_.onReconnected();         // ② RpcCodec：重发 in-flight（仅重连时）
            if(stateCb_) stateCb_(true);       // ③ 池：Slot::available = true
        } else {
            relCodec_.onDisconnected();        // pending_ 原样保留（断线挂起）
            if(stateCb_) stateCb_(false);      // 池：Slot::available = false
        }
    });

    // RPC 的可靠性参数：Ack 超时 2s / 每 0.5s 扫描 / 最多重发 5 次；
    // 缓存上限 + TTL 防断线期间无界堆积。
    // TTL 60s 是和服务端 reqs 缓存 TTL 对齐的（src/rpc_server.cpp 的 kReqTtlSec）：客户端
    // 最多把未确认帧挂 60s，服务端至少留 60s 的响应缓存 —— 重连重发才会命中「重放」而不是
    // 重复执行。改这里必须同步改那边。
    relCodec_.setRetransmit(2.0, 0.5, 5);
    relCodec_.setCacheLimit(4096, 60.0);
    client_.setAutoReconnect(true, 0.5, 5.0);          // RPC 客户端默认断线自动重连
}

void RpcConnection::start(){
    client_.connect();
}

void RpcConnection::setAutoReconnect(bool enable, double initialBackoff, double maxBackoff){
    client_.setAutoReconnect(enable, initialBackoff, maxBackoff);
}

RpcFuture RpcConnection::call(const std::string& method, const std::string& payload,
                              const RpcCallOptions& opts){
    return rpcCodec_.call(method, payload, opts);
}

void RpcConnection::stop(){
    rpcCodec_.stop();          // 先完成 in-flight（Future 续体还会投递到 biz loop）
    client_.disconnect();      // 主动下线：此后 connect() no-op，禁自动重连
}

} // namespace server
