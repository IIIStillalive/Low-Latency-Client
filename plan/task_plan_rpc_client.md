# 异步 RPC 高性能客户端 — 设计 & 路线（校招项目展示版）

> 状态：**Day5 已在 WSL 端到端验证通过（2026-10-20）**；设计四项决策见 §6，下一步 Day6（连接池 + 背压/取消）。
> 前置：Day4 已提交并推送（`3478436`，`origin/main` 已同步）——TcpClient / ProtocolCodec /
>       ReliableCodec（Ack+重传+断线重发+异步投递）全绿。

---

## 1. 目标

在现有可靠传输之上做一层 **异步、多路复用、可观测的 RPC 客户端**，并配套一个演示服务端与压测/故障注入程序。

校招展示要看的四件事（缺一不可）：

1. **架构清晰**：分层边界、线程模型、关联模型能讲清楚。
2. **正确性**：超时、取消、乱序响应、断线重连、重复请求都有明确语义和测试。
3. **性能有数**：QPS、p50/p99/p999 延迟、单连接多路复用的收益，用可复现 benchmark 说话。
4. **工程完整**：CMake 目标、测试、README + 架构图 + 压测报告。

---

## 2. 复用现状（不重写已有层）

```
业务/RpcClient  ← 本次新增：关联 + Future + 超时/取消 + 连接池
   │
RpcCodec        ← 本次新增：reqId ↔ pending 关联、乱序响应匹配、背压
   │
ReliableCodec   ← 已有：seq/未确认缓冲/超时重传/收侧去重/Ack/断线重发/异步投递
   │
ProtocolCodec   ← 已有：5B 头 [1B type][4B seq][body...]
   │
LengthHeaderCodec ← 已有：4B 长度切帧
   │
TcpClient       ← 已有：非阻塞握手 + 指数退避重连
```

**不改回已稳定的四层**，只在 `ProtocolCodec` 增加两种帧类型、在 `ReliableCodec` 增加一条「入站帧类别策略」，其余新增均在 RpcCodec/RpcClient。

---

## 3. 核心设计

### 3.1 线格式：protobuf 序列化，reqId 在消息体内

```proto
// proto/rpc.proto
syntax = "proto3";
package rpc;

message RpcRequest  { uint32 req_id = 1; string method = 2; bytes payload = 3; bool idempotent = 4; }
message RpcResponse { uint32 req_id = 1; int32 status = 2; bytes payload = 3; string error = 4; }
```
- 帧头保持 5B 不变：`[1B type][4B seq]`。`seq` 是**可靠层**传输序号，`req_id` 是 **RPC 层**关联键，二者职责/生命周期不同。
- body = protobuf 序列化后的 `RpcRequest` / `RpcResponse`（body 长度由 4B length 头推导，符合 Day4-P0 格式）。
- reqId 不进帧头：帧头是传输层公共字段，塞进去会让心跳/Ack 一起变长。

新增帧类型（`MsgType`）：`Request=0x05`、`Response=0x06`、`Cancel=0x07`。

**protobuf 构建集成**（WSL 前置）：
```bash
sudo apt install -y protobuf-compiler libprotobuf-dev   # protoc + libprotobuf
```
CMake：`find_package(Protobuf REQUIRED)` + 自定义命令 `.proto → .pb.cc/.pb.h`，生成文件放 `build/generated/`，不进仓库。
若 WSL 装包受限，退路：protobuf lite（`option optimize_for = LITE_RUNTIME`）或 vendored 源码（成本更高）。

### 3.2 关联模型：reqId ↔ pending call

- `RpcCodec` 维护 `unordered_map<reqId, PendingCall>`；`call()` 分配**单调递增 reqId**（与可靠层 seq 解耦，跨线程可用）。
- 响应到达 → 按 body 里的 reqId 找 pending → 完成 Future。**不假设响应顺序**（服务端多 worker 并发处理，响应必然乱序）。
- 这是与现有 `ReliableCodec` 的关键咬合点：**入站去重是按 seq 单调判旧的，只适用于请求类帧**；`Response` 必须绕过「旧 seq 丢弃」逻辑。改动很小但必须做，否则乱序响应会被当成重复帧丢掉。

### 3.3 线程模型（沿用 Day4-B 纪律）

| 职责 | 线程 |
|---|---|
| socket/收发/重传/超时计时 | 该连接自己的 IO loop 线程（multi-reactor 时每连接一条） |
| 业务回调 / Future 续体 | biz loop 线程（全池共享一个 EventLoopThread） |
| `call()` 调用方 | 任意线程（内部 runInLoop 转目标连接的 IO loop） |

- `call()` 从任意线程可调：reqId 在调用方分配，帧投递走 `ReliableCodec::sendData`（已线程安全）。
- Future 完成回调统一切到 biz loop 执行（`runInLoop`），保证用户回调不跑在 IO 线程。
- 超时 timer 必须在 IO loop 注册（`runAfter`），到期在 IO 线程完成 Future。

### 3.4 超时与取消

- `deadlineSec`：从 `call()` 发起时刻起算（不是收到响应才起算）。到期 → `RpcStatus::DeadlineExceeded`，从 inflight 移除，取消 timer。
- `Future::cancel()`：IO 线程内移除 inflight + 取消 timer + 完成 `Cancelled`；可选发 `Cancel` 帧（best-effort，帮服务端早停）。
- 幂等重试：`flags.idempotent`。断线重连时，可靠层会自动重发**未确认的 Request**；服务端维护 `reqId → 已回响应` 缓存，重复 reqId 直接回缓存响应（端到端幂等），避免业务重复执行。

### 3.5 多路复用与背压

- 单连接上可同时有 N 个 in-flight 请求（pipelining：不等待前一个响应）。
- `maxInflight`（默认 1024）作为背压闸：超限时 `call()` 立即返回 `ResourceExhausted`（fail-fast，不阻塞调用线程）——与 gRPC 的 flow-control 思路一致。
- 响应投递复用 Day4-B 的有界队列 + LOST 语义。

### 3.6 连接池 + 每连接多路复用（核心，Day6）

```
RpcChannelPool
 ├── RpcConnection[0]  (EventLoopThread #0) ─ TcpClient → LengthHeader → Protocol → Reliable → RpcCodec
 ├── RpcConnection[1]  (EventLoopThread #1) ─ ...
 └── RpcConnection[N-1]
```
- 每个 `RpcConnection` 独立一套四层栈 + RpcCodec 状态；**连接内**多路复用（多个 in-flight），**连接间**由池选择。
- 选连接策略：可配 `RoundRobin` / `LeastInflight`（默认最少 in-flight，带 in-flight 计数）。
- **线程模型**：默认「每连接一个 EventLoopThread」（multi-reactor，展示价值最高）；`RpcChannelPool` 也支持共享单一 IO loop（`connLoop == 单 loop`）的低复杂度模式，便于分阶段落地。
- 断线只影响该连接上的 in-flight；该连接重连后由可靠层重发，其他连接不受影响。
- reqId 用**全局原子递增**分配（跨连接唯一），服务端缓存键用 reqId —— 重发/换连接重放都能命中幂等缓存。
- **析构硬约束（继承 Day4 教训）**：每个 RpcConnection 的 RpcCodec/ReliableCodec 必须先于其 EventLoopThread 声明 → dtor 逆序先 join 业务/IO 线程，再析构 codec。

### 3.7 性能策略（P1/P2）

1. **写合并**：一轮 IO 循环内收集多帧，循环末尾一次 `writev`/flush，减少 syscall（现有 outputBuffer 已有补发能力，只需加「批末 flush」）。
2. **减少拷贝**：响应 body 从 `std::string` 拷贝改为 Buffer 切片/移动（先量测再优化，避免过早优化）。
3. **µs 级计时**：压测用 `CLOCK_MONOTONIC`，输出 p50/p90/p99/p999/max。

---

## 4. 公开 API（已按拍板更新）

```cpp
// include/client/rpc_client.hpp
enum class RpcStatus : uint8_t {
    Ok = 0, AppError, UnknownReq, DeadlineExceeded,
    Cancelled, ResourceExhausted, Unavailable
};
struct RpcResult { RpcStatus status; std::string payload; std::string error; };  // payload = protobuf bytes

class RpcFuture {
public:
    using Callback = std::function<void(const RpcResult&)>;
    void      then(Callback cb);   // 已完成→投递到 biz loop；未完成→登记
    RpcResult wait();              // 阻塞取结果（测试/同步场景）
    void      cancel();
};

struct RpcCallOptions {
    double deadlineSec = 5.0;   // 0 = 不超时
    bool   idempotent  = false; // 允许断线重发端到端去重
};

class RpcConnection {  // 单连接：四层栈 + RpcCodec
public:
    RpcConnection(EventLoop* loop, const sockaddr_in& peer, EventLoop* bizLoop);
    RpcFuture call(const std::string& method, const std::string& payload, RpcCallOptions = {});
    Stats     stats() const;
    size_t    inflight() const;
};

class RpcChannelPool {  // N 连接 × 每连接多路复用
public:
    enum class Policy { RoundRobin, LeastInflight };
    RpcChannelPool(const sockaddr_in& peer, size_t connCount /*=4*/,
                   EventLoop* bizLoop, Policy policy = Policy::LeastInflight);
    RpcFuture call(const std::string& method, const std::string& payload, RpcCallOptions = {});
    struct Stats { uint64_t sent, retransmits, timeouts, inflight, dropped; };
    Stats stats() const;   // 池级聚合
};
```

### 演示服务端 `rpc_server`
- 收 `Request` → 按 `method` 执行（echo / sleep / 累加计算），回 `Ack` + `Response`。
- 维护 `seen(seq)`（传输幂等）+ `reqId → 缓存响应`（RPC 幂等）。
- 可注入：延迟、错误码、断连。

---

## 5. 里程碑（P0/P1/P2）

### Day5 — protobuf + RPC 关联核心（P0，单连接）——代码已写完，待验证
- [x] `proto/rpc.proto` + CMake 生成集成（`protoc` → `build/generated/rpc.pb.{cc,h}`）
      —— `find_package(Protobuf QUIET)`，找不到只跳过 RPC 目标、不阻断既有构建
- [x] `MsgType::Request/Response/Cancel`（0x05/0x06/0x07）
- [x] `ReliableCodec` 入站策略：Response/Cancel 不做单调 seq 去重、不自动 Ack；
      另加类型化发送 `sendFrame(type, ...)` + `PendingFrame.type`（重传必须用原类型发回）
- [x] `RpcCodec`：原子 req_id 分配、inflight 表（仅 IO 线程）、乱序响应按 req_id 匹配、
      背压 maxInflight、超时 runAfter、cancel → Cancel 帧
- [x] `RpcFuture`（mutex 保护的共享状态 + 幂等 complete + 续体投递 biz loop + wait/cancel）
- [x] 超时：IO loop `runAfter` → DeadlineExceeded，迟到响应按未知 req_id 丢弃（计 late）
- [x] `RpcConnection`（单连接门面，组装四层栈 + RpcCodec，默认开自动重连）
- [x] `rpc_server`：protobuf 解析 + echo/sleep/fail/idem + **按 req_id 幂等缓存重放**
- [x] 测试 `rpc_client_test`：单次调用 / 乱序响应 / 超时 / 服务端错误 / 100 并发
- [x] **WSL 编译 + 跑通（2026-10-20 用户验证）**

#### Day5 验证证据（2026-10-20）
```text
[rpc] completion order: echo sleep
[rpc] PASS: singleCall=1 outOfOrder=1 timeout=1(us=200308) appError=1
          concurrent=100/100 | stats calls=105 ok=103 timeouts=1 late=0 inflight=0
```
- `outOfOrder=1`：服务端延迟返回的 300ms 响应没有挤掉先到的 echo 响应 —— 入站策略改动实证。
- `timeout(us=200308)`：deadline 定时器精度 ~308µs 超出目标值。
- `late=0`：测试在超时后立即跑完 100 并发并退出，那个 1s 才到的迟到响应还没到进程就结束了；
  不是丢弃逻辑没生效（`lateResponses` 计数保留，Day6 的长驻测试里可观察）。
- 编译期修正：`rpc.proto` 误写 C++ 类型 `uint32_t/int32_t` → protobuf 类型 `uint32/int32`。

#### Day5 实现期拍板/修正（与原设计的两处偏差）
1. **服务端幂等键用 req_id，不用 seq**：seq 是每个客户端连接自己的传输序号，跨连接不唯一；
   req_id 才是全局关联键。重复 req_id → 执行中只更新回程连接，已完成直接重放缓存。
2. **RpcCodec 不走 ReliableCodec 的异步投递**：响应处理（protobuf 解析 + inflight 表）
   必须留在 IO 线程（定时器/inflight 无锁）；用户续体由 RpcFuture 投递到 biz loop。
   这样慢业务回调同样不卡 IO，但不引入 Day4-B 的 DeliveryItem 队列开销。
3. `ReliableCodec::sendFrame(type, ...)` 的「当前活动连接」语义：不动 `conn_`，
   断开期间只入 pending_，重连全量重发兜底。

### Day6 — 连接池 + 多路复用 + 背压/取消（P0）
- [ ] `RpcChannelPool`（N 连接，RoundRobin / LeastInflight）
- [ ] 每连接 EventLoopThread（multi-reactor；亦可共享单 loop）
- [ ] `maxInflight` 背压 + `ResourceExhausted`
- [ ] `Future::cancel()` + `Cancel` 帧
- [ ] 测试：10k 请求流水线、取消后不再回调、池级统计

### Day7 — 断线重连 + 端到端幂等（P0）
- [ ] 单连接断线：in-flight 保留 + 可靠层重发
- [ ] 服务端 `reqId → 缓存响应`，重放命中缓存不重复执行
- [ ] 池内某连接断开时，新请求改道 + 该连接 in-flight 恢复
- [ ] 测试：断线中 in-flight 恢复、重复 reqId 不重复执行

### Day8 — 性能与故障注入（P1）
- [ ] 写合并 / 批末 flush
- [ ] `rpc_bench`：N 线程 × M 请求 → QPS + p50/p99/p999
- [ ] 对比：1 连接 vs 4 连接、单 loop vs multi-reactor
- [ ] 故障注入：丢 Ack、服务端延迟、随机断连
- [ ] 压测报告（表格 + 结论）

### Day9 — 工程化与展示（P2）
- [ ] README：分层图、线程模型图、协议图、决策与取舍
- [ ] 指标日志 / metrics 输出
- [ ] CMake 目标整理；全量回归脚本

---

## 6. 已拍板决策（2026-10-20）

1. **并发模型**：连接池 + 每连接多路复用（multi-reactor，Day6 落地）。单连接模式保留为 `connCount=1` 的特例，用于对比压测。
2. **API 形态**：Future + `then`（续体统一在 biz loop 执行）。
3. **重试幂等**：reqId 端到端幂等——服务端缓存 `reqId → 响应`，重放直接回缓存。
4. **序列化**：protobuf（`proto/rpc.proto`，CMake 生成；WSL 需 `protobuf-compiler` + `libprotobuf-dev`）。

### 落地顺序调整
- 原 Day6「多路复用/背压」与原 Day7「连接池」合并为 Day6；断线幂等单独成 Day7。
- Day5 先做**单连接**闭环（可运行、可测试），Day6 再扩成池——每一步都有可验证产物。

---

## 7. 边界与风险

- 现有 `ReliableCodec` 的收侧去重模型是为「有序请求流」设计的，RPC 响应乱序是新增语义——必须显式扩展（3.2），这是本方向最大的结构性改动点。
- 单连接多路复用在超大响应/慢消费者下会头阻塞（TCP 层 HOL），连接池是缓解手段，不是消除手段——报告里要诚实写明。
- 超时与取消存在竞态（响应与 cancel 同时到达）：统一在 IO 线程串行化 inflight 状态，Future 状态用 mutex 保护。
