# TcpClient Day1 — 客户端连接建立（方向一）

状态：`实现完成 + 评审修复完成，待用户编译验证`
分工契约（[[llsrv-user-contract]] 继续适用）：接口已由用户确认，实现由我写完；用户在 WSL 编译/运行，报错回传我修。
本文件随代码演进更新。

---

## 1. 目标与边界

**只做一件事**：让客户端「把一条 TCP 连接建立起来，交回给既有的传输/解析链」。
复用（零改动）：`TcpConnection`、`LengthHeaderCodec`、`ProtocolCodec`、`EventLoop`/`Channel`/`Socket`、心跳 runAfter 自续。

**Day1 明确不做**（后续 Day 补，钩子已留）：
- 断线指数退避重连（`active_` 标志已留作互斥开关）
- ACK/去重/重传、网络线程→外部主线程投递
- DNS 解析（只接受 sockaddr_in，同 Acceptor 语义）

## 2. 核心差异：TcpClient 有一节 TcpServer 没有的「connecting 阶段」

Server 侧 `accept()` 拿到的 fd **已连接**，直接包 `TcpConnection` 即可。
Client 侧 `connect()` 在非阻塞 fd 上**立即返回 EINPROGRESS**（握手在途），
必须等 EPOLLOUT（内核只报可写 = 握手有结果），再 `getsockopt(SO_ERROR)` 确认，
**确认成功之后**才构造 `TcpConnection`。这是唯一的全新逻辑，其余是搬运。

```
Idle ──connect()──▶ Connecting ──EPOLLOUT + SO_ERROR==0──▶ Connected(复用TcpConnection)
                       ▲  （握手期：临时 Channel 挂 EPOLLOUT）      │
                       │                                        │ 对端关/超时/断开
                       └────────── SO_ERROR!=0 失败 ──────────────▼
                                                          removeAndReset → Idle
```

## 3. 状态机（单 loop 线程内全串行，无跨线程竞态面）

| 状态 | 持有 | 含义 |
|---|---|---|
| Idle | 无 fd | 未连接 |
| Connecting | `Socket connSock_` + `unique_ptr<Channel> connectingCh_` | 握手在途 |
| Connected | `shared_ptr<TcpConnection> conn_` | 活动连接，接管 fd |

迁移：
- Idle→Connecting：`connect()` → runInLoop → `connectInLoop()`
- Connecting→Connected：`handleConnect()`（EPOLLOUT 回调）SO_ERROR==0，`connSock_.release()` 移交
- Connecting→Idle：SO_ERROR!=0 → `fail()`；或 `disconnect()`/析构挂断
- Connected→Idle：conn 的 close 回调 → `removeAndReset()`

## 4. 接口契约（头文件骨架，用户实现 = 设计所有权在我，代码在你）

```cpp
#pragma once
#include <functional>
#include <memory>
#include <netinet/in.h>   // struct sockaddr_in
#include "net/socket.hpp"
#include "net/channel.hpp"
#include "net/length_header_codec.hpp"
#include "net/noncopyable.hpp"

namespace server {
class EventLoop;
class TcpConnection;

class TcpClient : public Noncopyable {
public:
    TcpClient(EventLoop* loop, const struct sockaddr_in& peer);
    ~TcpClient();

    void connect();              // 任意线程可调（内部 runInLoop）
    void disconnect();           // 主动下线：置 active_=false，此后 connect() no-op
    bool isConnected() const;    // 调试/日志用（loop 线程读）

    void setConnectionCallback(std::function<void(TcpConnection*)> cb); // conn==nullptr→失败/断开
    void setMessageCallback(LengthHeaderCodec::MessageCallback cb);      // 语义同 TcpServer
    void setConnectionTimeout(double seconds);                            // 透传活动连接

private:
    void connectInLoop();    // socket+非阻塞+connect(EINPROGRESS)+挂EPOLLOUT
    void handleConnect();    // EPOLLOUT 或 ERR 回调：SO_ERROR 最终裁判
    void removeAndReset();   // 连接断开回调（loop 线程）：reset conn_ + 通知上层
    void fail();             // 握手失败公共收尾：关 fd + 通知上层 nullptr

    EventLoop* loop_;
    struct sockaddr_in peer_;
    Socket connSock_;                              // 仅 Connecting 持有
    std::unique_ptr<Channel> connectingCh_;        // 仅 Connecting 上树
    std::shared_ptr<TcpConnection> conn_;          // Connected 后持有
    std::function<void(TcpConnection*)> connectionCallback_;
    LengthHeaderCodec::MessageCallback messageCallback_;
    LengthHeaderCodec codec_;                      // 无状态切帧刀，同 TcpServer::codec_
    double connectionTimeout_ = 0;
    bool active_ = true;      // disconnect()/析构 → false；重连互斥开关（Day2 用）
    bool connecting_ = false; // EINPROGRESS 在途标记
};
} // namespace server
```

## 5. 实现要点（.cpp 该写什么，逐条核发）

### 5.1 `connectInLoop()`（全新逻辑核心）
```cpp
int fd = ::socket(AF_INET, SOCK_STREAM, 0);   if(fd < 0){ fail(); return; }
fcntl(O_NONBLOCK);                            // 同 Acceptor 内联写法
int r = ::connect(fd, (sockaddr*)&peer_, sizeof(peer_));
if(r == 0){ /* 本地回环几乎必然直达成功 */ onConnected(fd); return; }
if(errno != EINPROGRESS){ ::close(fd); fail(); return; }   // 真错误
connSock_ = Socket(fd);
connectingCh_ = std::make_unique<Channel>(fd, loop_);
connectingCh_->setwriteback([this]{ handleConnect(); });
connectingCh_->seterrorback([this]{ handleConnect(); });   // 失败时内核报 ERR|OUT|HUP，双挂兜底
connectingCh_->enablewriting();                            // EPOLLOUT|ET + update 上树，一步到位
connecting_ = true;
```

### 5.2 `handleConnect()` — SO_ERROR 是唯一裁判
```cpp
int err = 0; socklen_t len = sizeof(err);
::getsockopt(connSock_.fd(), SOL_SOCKET, SO_ERROR, &err, &len);
connecting_ = false;
if(err != 0){ loop_->removechannel(connectingCh_.get()); connectingCh_.reset(); fail(); return; }
// 成功：先把握手通道下树，再建连接接管 fd
loop_->removechannel(connectingCh_.get());
int fd = connSock_.release();                 // Socket::release()：析构不再 close
connectingCh_.reset();
conn_ = std::make_shared<TcpConnection>(fd, loop_);
conn_->setCloseCallback([this](TcpConnection*){ this->removeAndReset(); });
conn_->setMessageCallback([this](TcpConnection* c, Buffer* b){ codec_.onMessage(c, b); });
if(connectionTimeout_ > 0) conn_->setConnectionTimeout(connectionTimeout_);
if(!conn_->listenconnection()){ conn_.reset(); fail(); return; }   // 上树失败
connectionCallback_(conn_.get());
```

### 5.3 `removeAndReset()`（Connected→Idle）
```cpp
auto connV = std::move(conn_);   // 栈上保活：本回调栈结束后才析构（TcpConnection onRead 的 keep 已兜底）
connectionCallback_(nullptr);    // 通知上层：断开了
```

### 5.4 `fail()`（Connecting→Idle 失败）
```cpp
if(connecting_){ loop_->removechannel(connectingCh_.get()); connectingCh_.reset(); connSock_.close(); connecting_ = false; }
connectionCallback_(nullptr);
```

### 5.5 `disconnect()` / 析构（主动下线）
```cpp
active_ = false;                                        // 重连互斥开关关闭（Day2 生效）
loop_->runInLoop([this]{
    if(connecting_){ /* 同 fail 的清理 */ }
    if(conn_) conn_->close();                           // 走 handleClose 链：下树+取消心跳+close回调
});
```

## 6. 需要动既有代码的两处最小改动（先改，才能实现 5.2/5.5）

| 文件 | 改动 | 理由 |
|---|---|---|
| `include/net/socket.hpp/.cpp` | 加 `int Socket::release();`（`int fd = fd_; fd_ = -1; return fd;`）| 握手期 Socket 把 fd 所有权正式移交给 TcpConnection，防双 close —— 所有权转移原语，通用 |
| `include/server/tcp_connnection.hpp/.cpp` | 加 public `void close(){ handleClose(); }` | 客户端主动下线需要一把「走完整关闭链」的钥匙（下树+cancel心跳+close回调）；server 侧将来踢人也用得上 |

其余零改动——`TcpConnection` 内部 `set_nonblocking`/`send`/心跳/close 链全部原是复用。

## 7. Day1 演示 `src/client_echo.cpp`（三层一次通）

调度链（复用三层架构，正是之前定的）：
```
EventLoop → TcpClient(transport) → LengthHeaderCodec(切帧) → ProtocolCodec(9B私有头) → 业务
```
```cpp
EventLoop loop;
TcpClient client(&loop, addr);          // addr = 与 server 一致的 127.0.0.1:8888
LengthHeaderCodec codec;
ProtocolCodec proto(codec);             // 业务层自持

client.setMessageCallback([&](TcpConnection* c, const std::string& m){ proto.onMessage(c, m); });
proto.setMessageCallback([&](TcpConnection*, MsgType t, uint32_t seq, const std::string& body){
    // 收到 echo：打印 body，quit
});
client.setConnectionCallback([&](TcpConnection* c){
    if(c) proto.sendFrame(c, MsgType::Data, 1, "hello im", 8);   // 连接成功 → 发一条
    else { /* 失败/断开 */ loop.quit(); }
});
client.connect();
loop.runloop();
```

## 8. CMake 新增 target（同 client_protocol_test 依赖集）

```cmake
add_executable(client_echo
    src/client_echo.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/tcp_connnection.cpp
)
target_include_directories(client_echo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
```

## 9. 验证路径（用户跑）

1. `cmake --build build` → 三旧 target（server / timer_demo / client_protocol_test）+ 新 target 全过
2. 回归：`./build/client_protocol_test` → 仍 `PROTOCOL ROUNDTRIP PASS`
3. 端到端：
   - 终端 1：`./build/server`（127.0.0.1:8888 回显）
   - 终端 2：`./build/client_echo` → 应打印回显 body 后干净退出
4. 断开路径：Ctrl+C server → client 应走 `removeAndReset`（连接回调拿 nullptr），不崩、不死锁

## 10. 里程碑清单（Day1 完成标准）

- [x] `Socket::release()`（net）+ 顺带补移动构造/赋值（实现时发现 Socket 非 movable，所有权转移必须）
- [x] `TcpConnection::close()`（server，保活 shared_from_this + handleClose）
- [x] `include/client/tcp_client.hpp/.cpp`（含私有 onConnected；公共接口与确认版一致）
- [x] 代码评审 11 条发现全部核验处置（见 §12；含 1 个库级 pre-existing bug 修复）
- [x] `src/client_echo.cpp` + CMake target `client_echo`
- [ ] client_protocol_test 回归 PASS（用户跑）
- [x] client_echo 对 real server 一发一收成功（用户 2026-10-19 贴日志：echo 原样返回 + 干净退出）

## 12. 代码评审处置记录（2026-10-19，code-reviewer agent）

**评审挖出的库级 pre-existing bug（影响 server 本身，务必一起回归）：**
- `EventLoop::runloop()` 的 `readys` 从不 clear → 被摘除/销毁的 Channel* 残留批中，下一批 `handleback` 变 UAF。client 首次连接和 server 任何连接断开都会触碰。**修复**：`include/net/event_loop.cpp` runloop 顶部加 `readys.clear()`。§9 验证第 3 步的 server 回归顺带覆盖。

**已修复的 7 项：**
| 项 | 发现 | 修复 |
|---|---|---|
| 1 | 握手 Channel 在自身 handleback 帧内被就地销毁（UAF 重入） | `teardownConnecting()` 摘 epoll + close(fd) 后，`runAfter(0.001, [dead]{})` 延迟到下轮循环回收对象 |
| 2 | 无握手兜底超时 + HUP-only 未挂 → 黑洞对端永久泄漏 fd+Channel | `kHandshakeTimeout=5.0` runAfter 兜底；`setcloseback→handleConnect` 由 SO_ERROR 裁决 |
| 3 | `connectionCallback_(conn_.get())` 无保活 → 回调内 disconnect 中途析构 conn、c 悬垂 | `onConnected` 调用前 `auto keep = conn_` |
| 4 | `setConnectionTimeout` 在上树成功前调度 → 失败路径心跳定时器悬垂 | 倒序：`listenconnection()` 成功后才调度（对齐 tcpserver） |
| 5 | `Socket` 非 movable → `connSock_ = Socket(fd)` 编译错 | 补移动构造/赋值（源置 fd_=-1 防双 close）；`close()` 置 fd_=-1 防 fd 号复用误关 |
| 6 | 跨线程析构向将死 loop 排队 `[this]` → 悬垂 | `~TcpClient` 前 `assert(loop_->isInLoopThread())` + 头文件契约文档 |
| 7 | `handleConnect` re-entry / disconnect 竞态 | guard `if(!active_ || !connecting_) return;` 幂等裁决 |

**评审确认安全的点（未改，核验通过）：** Socket 移动语义不双 close；`TcpConnection::close()` 的 self-keep 充分覆盖 close 回调栈；`teardownConnecting` 对 `release()` 后 no-op；demo 析构顺序（loop 先停、client 后析构）。

**未采纳判定：** 涉及 Day2 范围（指数退避重连、ACK/去重/重传）的不在 Day1 改；`connectingCh_` 保 shared_ptr 而非 unique_ptr 是为配合延迟回收持有者（评审建议采纳后正是如此）。

## 11. Day2 钩子（本设计已预留，不在 Day1 实现）
- `active_` 标志：disconnect/析构后层 connect() 变 no-op，重连与退出天然互斥
- `removeAndReset` 就是重连的触发点（现在通知 nullptr，Day2 改为 runAfter 自续退避重连）
- 连接回调签名 `void(TcpConnection*)` 已兼容 Day2 的「重连第 N 次」语义（第 3 参数后续再扩）