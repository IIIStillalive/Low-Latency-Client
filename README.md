# MyServer — 低延迟高性能 TCP 服务器

*Low-Latency High-Performance TCP Server*

> An event-driven, epoll-based TCP server in modern C++ (Muduo-style **multi-Reactor** core), unfolded in clear layers: single-thread echo → threading base → app buffer → transport protocol → safe teardown.
>
> 一个基于 epoll 事件驱动的现代 C++ TCP 服务器（Muduo 风格**多 Reactor** 核心），按清晰分层逐步展开：单线程回显 → 多线程地基 → 应用层缓冲 → 传输协议 → 安全析构。

**Target / 目标环境:** Linux / [WSL2] (Ubuntu) · **Toolchain / 工具链:** g++ 13 · CMake ≥ 3.10

---

## 特性 / Highlights

- **多 Reactor 并行** — 每个线程一个 `EventLoop`；`ThreadPool` + `getNextLoop()` 轮询把新连接派发到 worker loop；`runInLoop`/`eventfd` 跨线程安全派发任务。
- **应用层缓冲 `Buffer`** — `[readerIndex_, writerIndex_)` 未读模型；`readFd` 用 `readv` 双段减拷贝；read/write 与 socket 就绪状态解耦。
- **长度前缀协议 `LengthHeaderCodec`** — `[4 字节大端长度][payload]` 帧；`onMessage` 循环切帧，半包攒、粘包拆。
- **安全析构（UAF 免疫）** — `TcpConnection` 继承 `enable_shared_from_this`，回调栈内局部共享指针保活；连接容器 `conns_` 插入/删除全部串行化在 main loop 线程，消除跨线程 race。
- **优雅停机** — signal handler 仅做 atomic store + eventfd write（async-signal-safe），`quit_` 原子标志，Ctrl+C / SIGTERM 干净退出；main loop 一停即触发对象析构连锁（`~TcpServer → ~ThreadPool → 逐个 ~EventLoopThread quit()+join()`）驱逐并收拢全部 worker 线程。
- **刻意逐层展开** — 每个组件拿到独立职责稳定后再进下一层，骨架始终可审查。

> ✅ **Status / 状态: 传输层已完整闭合 — ThreadPool 多线程接池 + Buffer + 长度前缀 Codec + shared_ptr 安全析构 + 连接容器串行化 + 优雅停机全部落地，`test/codec_client.py`（单条/粘包/半包）与 `test/concurrent_client.py`（50 连接 × 20 消息并发）验证通过。**
>
> ✅ **Status / 状态: 异步 RPC 客户端已完成 — protobuf 线格式 + req_id 关联 + Future/超时/取消 + 连接池多路复用 + 断线重连端到端幂等 + 压测基准与报告（Day5–Day8），工程化收尾（指标日志 + CMake 目标整理 + `scripts/regress.sh` 一键回归）已就绪（Day9）。网络与测试请按交付说明在 WSL 跑 `./scripts/regress.sh --build` 验证。**

---

## 架构 / Architecture

```
┌─────────────────────────────────────────────┐
│                main.cpp                    │  entry point（挂业务回调 + 信号处理）
└─────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────┐
│                TcpServer                    │  服务组装 (owns ThreadPool)
│   Acceptor ──accepts──▶ TcpConnection(×N)  │  监听 + 每连接 I/O
│   LengthHeaderCodec  ◀──剥帧/编帧           │  传输协议
└─────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────┐
│        EventLoopThread / ThreadPool         │  多 Reactor（每个 worker 一个 loop）
│        EventLoop (main)  └─getNextLoop─▶    │  
└───────────────┬─────────────────────────────┘
                │ updatachannel / removechannel / runInLoop(跨线程唤醒)
┌───────────────▼─────────────────────────────┐
│       Poller (抽象/abstract)                │
│       EpollPoller (epoll 实现)              │   epoll_wait → active Channel*
└─────────────────────────────────────────────┘
                    │ fd ↔ Channel
┌─────────────────────────────────────────────┐
│   Channel   (单 fd 事件分发 / 不拥有 fd)    │
│   Socket    (RAII fd 持有者 / 唯一 close 方) │  基础封装 base encap.
│   Buffer    (应用层 I/O 缓冲)               │
└─────────────────────────────────────────────┘
```

设计遵循经典 **Reactor** 模式：就绪状态以 `Channel*` 回传，按事件分发回调。`EventLoop` 用 `eventfd` 实现跨线程唤醒——任务（`runInLoop`）可被派到任意线程的 loop 执行；`ThreadPool` 轮询挑一个 worker loop 承接新连接，每个连接的 buffer 只在其归属 loop 线程内访问。

---

## 异步 RPC 客户端 / Async RPC Client (Day5–Day8)

在服务器的传输层之上，堆出一个**异步、多路复用、断线幂等**的 RPC 客户端。业务侧用法一句话：

```cpp
RpcFuture f = pool.call("echo", "hello", opts);   // 任意线程可调
f.then([](const RpcResult& r){ /* biz loop 上执行 */ });
// 或 f.wait() 获取 RpcResult
```

### 分层图 / Layered Stack

```
业务（任意调用线程，Future 续体跑在 biz loop）
   │ call(method, payload, opts)
   ▼
RpcChannelPool ── N 条连接 × 每连接多路复用（分配 req_id，RoundRobin/LeastInflight 选连接）
   │
   ▼
RpcConnection（一条连接的全部状态 + 背压闸 maxInflight）
   │
   ▼
RpcCodec        (request/response 关联：req_id ↔ Future，超时 Timer / cancel)
   │
   ▼
ReliableCodec   (传输帧：seq/Ack/重传/断线重发，pending_ 队列兜底)
   │
   ▼
ProtocolCodec   (5B 私有帧头 [1B type][4B seq] 编解码)
   │
   ▼
LengthHeaderCodec (4B 大端长度切帧，半包/粘包)
   │
   ▼
TcpClient ── 自动重连 ──▶ TcpConnection（服务端同款连接层）
```

每一层职责单一、上下解耦：`RpcCodec` 不知 seq 为何物，`ReliableCodec` 不知 req_id 为何物。

### 线程模型图 / Threading Model (multi-reactor)

```
调用线程 T1..TN  ──────────────────────────────┐
   │ pool.call()   （req_id 在各调用线程原子自增）  │
   ▼                                             ▼
  RpcChannelPool ──RR──▶ 每条连接一个 EventLoopThread（独立 epoll + TimerQueue）
                                 │
             ┌───────────────────┼───────────────────┐
             ▼                   ▼                   ▼
        conn_0.ioLoop       conn_1.ioLoop       conn_2.ioLoop
        (Reliable/Rpc 状态)  (Reliable/Rpc 状态)   (Reliable/Rpc 状态)
             │                   │                   │
             └────────── Future 续体 ────────────────┘
                              ▼
                      bizLoop（全池共享，业务回调统一在此执行）
```

- **IO 线程（每连接一条）**：跑 epoll + 该连接的全部编解码/重传/关联状态。连接之间无共享可变状态 → 无锁。
- **biz loop（全池共享）**：只跑 `Future::then` 续体与业务回调。慢业务卡住的是 biz，不是任一连接的 IO → 单连接慢调用不拖累其他连接（Day4-B 已验证 async 投递）。
- **调用线程（任意）**：`call()` 只做“分配 req_id + 选连接 + runInLoop 投递”，自身不阻塞。

### 协议图 / Wire Protocol

```
一个 LengthHeader 帧（TCP 流上）：

┌──────────────┬────────────┬──────────────────────────────┐
│ [4B 大端长度] │ 5B 帧头     │     body（protobuf）            │
│   帧总要长    │ type|seq   │  RpcRequest / RpcResponse      │
└──────────────┴────────────┴──────────────────────────────┘
                 type: 0x05 Request / 0x06 Response / 0x07 Cancel
                 seq : 传输层 Ack 序号（ReliableCodec 用，4B 大端）
```

帧头只放【传输层】需要的 `type/seq`；**RPC 关联键 `req_id` 放 body 里**——它是【应用层】身份，生命周期与 seq 不同（重连后 seq 重开、req_id 保持），塞进帧头会造成两层职责耦合。

- `RpcRequest { uint64 req_id; string method; bytes payload; bool idempotent }`
  响应乱序到达：`req_id` 关联，不做 seq 单调去重（Day5 起）。
- `RpcResponse { uint64 req_id; int32 status; bytes payload; string error }`
  `status` 对应 `RpcStatus` 枚举（0=Ok … 6=Unavailable，改号即破坏线上兼容）。

### 关键决策 / Decisions & Tradeoffs

| 决策 | 为什么 | 代价 / 诚实边界 |
|---|---|---|
| **单连接多路复用** + 连接池 | 一个 RTT 内同时 N 个 in-flight，隐藏延迟；连接池缓解单连接 HOL | TCP 层 HOL 仍存在，池是缓解不是消除；响应超大/慢消费者仍会头阻塞 |
| **req_id 不进帧头、放 body** | 区分传输序（seq）与业务键（req_id）；跨连接重发后 req_id 仍唯一 | body 多 8 字节；关联层与传输层解耦换来调试清晰 |
| **服务端按 req_id 缓存响应 + 重放** | 断线重发命中缓存，业务**不重复执行**（端到端幂等，Day7） | 一次调用两次消耗内存（缓存上限 64k + TTL 60s，可调） |
| **背压闸 maxInflight = fail-fast** | 负载过了给 `ResourceExhausted`，不排队叠延迟 | 需要调用方决定重试策略，SDK 不自动重试 |
| **超时用定时器、取消 best-effort** | deadline 到即返回，调用方不用等不可达的响应 | 迟到响应计入 `lateResponses` 被丢弃，不回调 |
| **Future 续体统一跑一个 biz loop** | 业务回调串行化，避免业务侧自己加锁 | 业务里做耗时操作会卡该 loop（demo 的诚实边界） |

> 各层细节见源码注释（`include/client/*`）与 `plan/task_plan_rpc_client.md`；压测方法与判定标准见 `plan/bench_report.md`。

---

## 目录结构 / Directory Layout

```
.
├── CMakeLists.txt
├── README.md
├── scripts/
│   └── regress.sh          # Day1→Day9 一键全量回归（起服务端 → 跑全部用例 → 汇总）
├── include/
│   ├── net/                # Reactor 引擎：socket/event_loop/channel/buffer/codec/timer...
│   ├── server/             # 服务组装：acceptor / tcpserver / thread_pool / tcp_connnection / logger
│   └── client/             # 异步 RPC 客户端：protocol / reliable_codec / tcp_client /
│                           #   rpc_codec / rpc_future / rpc_connection / rpc_pool
├── proto/
│   └── rpc.proto           # RPC 线格式（protobuf；`protoc` 生成物进 build/，不入仓库）
├── src/                    # 各可执行目标入口 + demo + 测试
└── plan/                   # 架构决策 / 任务分解 / 压测报告（task_plan_rpc_client.md, bench_report.md）
```

---

## 构建与运行 / Build & Run

```bash
cmake -S . -B build
cmake --build build
./build/server          # 监听 127.0.0.1:8888
```

### RPC 演示 / RPC demo（WSL：`sudo apt install -y protobuf-compiler libprotobuf-dev`）

```bash
cmake -S . -B build                          # 检测到 protobuf 后生成 RPC 目标
cmake --build build

# 服务端（单线程 demo）+ 客户端演示
./build/rpc_server --port 8899 &
./build/rpc_bench --conns 4 --threads 4 --reqs 10000 --mode pipeline   # 压测（QPS + p50/p99/p999）
./build/rpc_bench --conns 1 --threads 4 --reqs 10000 --mode pipeline   # 对比：单连接基线
# 服务端指标观测（Day9）：每 2 秒一行 req 速率 + 幂等缓存占用
./build/rpc_server --port 8899 --stats-sec 2
```

### 测试 / Tests

```bash
./scripts/regress.sh --build     # 全量回归：重启构建 + 起服务端 + 全部用例 + 汇总
# 或拆开跑（各测试都要先起相应服务端，见 plan/task_plan_rpc_client.md 与我的交付说明）：
./build/rpc_client_test --port 8899              # 单连接：乱序/超时/错误/并发
./build/rpc_pool_test --port 8899                # 连接池：策略/shutdown 收尾
./build/rpc_pool_load_test --port 8899           # 负载：10k 流水线/背压/取消
./build/rpc_reconnect_rpc_test --port 8899       # 故障注入：cut/cutreply/drop-ack 幂等
```

⚠️ **协议是长度前缀帧**：启用 Codec 后，裸 `nc` 发送**不再是原样回显**，需用带长度头的编码客户端收发。最小验证客户端：

```python
import socket, struct

def encode(msg: bytes) -> bytes:
    return struct.pack(">I", len(msg)) + msg     # [4字节大端长度][payload]

s = socket.create_connection(("127.0.0.1", 8888))
s.sendall(encode(b"hello"))                      # 发送帧
ln = struct.unpack(">I", s.recv(4))[0]           # 读回长度头
print(s.recv(ln))                                # 期望 b'hello'
```

---

## 路线图 / Roadmap

- [x] 基础封装 base wrappers: `Socket`, `Logger`, `ErrnoGuard`
- [x] Reactor 核心: `Channel`, `Poller` / `EpollPoller`, `EventLoop`
- [x] `Acceptor` — 监听 + ET accept，把 fd 交给 `TcpConnection`
- [x] `TcpConnection` I/O 回调 — 基于 epoll 的单连接回显（端到端验证通过）
- [x] `TcpServer` — 服务组装（持有连接 + 释放）
- [x] EventLoop 跨线程地基 — `eventfd` 唤醒 + `runInLoop` + `EventLoopThread`
- [x] `ThreadPool` — N 个 worker loop + `getNextLoop()` 轮询（numThreads==0 降级回 main loop）
- [x] TcpServer 接池 — 新连接派发到 worker loop，连接容器按 loop 分区、串行化在 main loop
- [x] `Buffer` — 应用层读缓冲 + 写缓冲，readv 减拷贝
- [x] `LengthHeaderCodec` — 长度前缀编解码，半包/粘包正确切帧
- [x] `TcpConnection` shared_ptr 安全析构 — 回调栈保活，防 UAF / fd 泄漏
- [x] 优雅停机 — signal + atomic quit 标志，async-safe handler
- [x] 连接容器串行化 — `conns_` map 消除跨线程 race
- [ ] 真实业务 — 转发 / 广播 / 聊天室（替换 echo 桩）
- [x] `TimerQueue` — 定时器（心跳 / 超时关闭）
- [x] 空闲超时 / 心跳 — 每连接 `runAfter` 自续检查空闲，超时主动断开回收
- [x] 「正常断开 vs 真错误」日志语义细分（FIN → INFO，仅真错误 → ERROR）
- [x] **Day5 异步 RPC 客户端** — protobuf 线格式 + req_id 关联 + Future/超时/自定义错误（`rpc_client_test`）
- [x] **Day6 连接池 + 多路复用** — N 连接 × 每连接 multi-reactor，RoundRobin/LeastInflight 选路，背压 `ResourceExhausted`，shutdown 收尾（`rpc_pool_test` / `rpc_pool_load_test`，~7 万 QPS）
- [x] **Day7 断线重连 + 端到端幂等** — 服务端按 req_id 缓存重放，业务不重复执行；cut/cutreply/drop-ack 故障注入（`rpc_reconnect_rpc_test` T1–T5）
- [x] **Day8 性能** — 批末写合并 + `TimerQueue::cancel` O(1)；`rpc_bench` 压测基准（1/4 连接 × pipeline/sync，报告 `plan/bench_report.md`）
- [x] **Day9 工程化** — 指标日志（`rpc_server --stats-sec`）+ CMake 目标整理（`netcore`/`rpccore`）+ `scripts/regress.sh` 全量回归

---

## 贡献者说明 / Notes for contributors

- 代码遵循"小文件、清晰分层"原则。声明（`.hpp`）与实现（`.cpp`）已完全分离，规避 ODR 重复定义；新增组件按同样约定落位。
- poller 设计为抽象基类，epoll 后端可替换（例如 `select`、`poll`）而不影响 `EventLoop`。
- **回/转发铁律**：经 Codec 收到的 payload 是已剥头数据，回复时**必须再经同一 Codec 编码长度头**（`TcpServer::sendMessage`），不能裸 `conn->send(payload)`——否则对端把 payload 前 4 字节当长度，解出错误帧。
- **shared_ptr 保活铁律**：`TcpConnection` 的保活 shared_ptr 只能作回调栈内局部变量；绝不可捕获进对象的成员（尤其 close 回调），否则形成自引用循环 → 对象永不析构 → fd 永不关 → 连接泄漏。
- 注释与文档面向对事件驱动服务器尚不熟悉的读者书写。

[WSL2]: https://learn.microsoft.com/en-us/windows/wsl/