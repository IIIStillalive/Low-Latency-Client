# TcpClient Day4 — 协议头瘦身(P0) + 断线缓存重发(A) + 外部业务线程投递(B)

状态：`P0 协议瘦身已回归（2026-10-20 用户 WSL 验证 ✅）；Part A 断线重发端到端已验证 ✅；Part B 异步投递端到端已验证 ✅（含退出 UAF 修复）`
前置：Day3 验证通过（2026-10-20）——正常 PASS + `--drop-ack 2` 重传路径实证。
执行顺序：**P0 协议瘦身 → Part A 断线缓存重发 → Part B 业务线程投递**（P0 是 A/B 的公共前置，先行落地并回归）。

---

## P0：砍 bodyLen，协议头 13B → 9B（拍板项）

### 为什么要砍
两个长度头的根源是**层间边界信息被丢弃**：
`LengthHeaderCodec` 剥掉 4B length 后才回调上层（[length_header_codec.cpp:33-34](include/net/length_header_codec.cpp#L33-L34)），
`ProtocolCodec` 拿不到 length，只能自备一个 `bodyLen` 才能切 body → 冗余叠加。
发送侧恒等式：`length ≡ 9 + bodyLen` → 线上多 4B/帧；心跳/Ack 空 body 帧 13B→9B 省 31%。

### 新帧格式
```
改前: [4B length][1B type][4B seq][4B bodyLen][body]   = 13B/帧
改后: [4B length][1B type][4B seq][body...]            =  9B/帧
```
- `body` 到帧尾为止；越界防护由 LengthHeaderCodec 的 `kMaxMsgLen` + 帧内解析兜底，安全性不降。
- `kHeadLen` 9 → 5。

### 改动面（精确清单）
- [x] `ProtocolCodec::kHeadLen` 9 → 5（[protocol.hpp:54](include/client/protocol.hpp#L54)）
- [x] `ProtocolCodec::sendFrame`：去掉 `writeInt32BigEndian(head+5, n)`（[protocol.cpp:47-60](include/client/protocol.cpp#L47-L60)）
- [x] `ProtocolCodec::onMessage` / `decodeHeader`：不再读 bodyLen，body 到帧尾
- [x] `ProtocolCodec` 头注释同步（9B 帧头描述）
- [x] `client_echo.cpp` 注释同步（不影响逻辑，sendFrame 签名不变）
- [x] 回归：client_protocol_test（需同步改 assert：无 bodyLen 字段的打包/解包）、client_echo、client_reliability_test、server_ack

### 为什么保留 LengthHeaderCodec 不合并
砍完后线上仍是 9B——两方案收益相同；合并单层 = 把纯编解码状态化（自己管半包/粘包），
结构性倒退。所以**只砍 bodyLen，保 LengthHeaderCodec 切帧层**。

---

## Part A：断线消息缓存跨重连恢复（A1 缓存-重发）

### 目标
Day3 `onDisconnected()` 清空 pending_ + 全量 LOST；向前推：断开缓存，重连后自动重发。

### A1 行为定义（拍板项）
```
断开时：pending_ 保留（不再 clear + 不再全量 LOST）
  - conn_ = nullptr；tick cancel（无连接可发，停下扫描）
重连成功（onConnected，conn != nullptr）：
  - conn_ = 新连接；pending_ 全量重发；尽快恢复 tick 扫描
  - retries 计数重置（视为全新机会）
退出 stop()：同 Day3 清空（进程退出不赖着）
```
- 去重责任在 server：`seen` 判定「重复 → 只 Ack 不执行」。
- 缓存兜底：`setCacheLimit(maxFrames=1024, ttlSec=30)` → 超限先抛最旧 + LOST。

### 改动面（精确清单，全部落在 ReliableCodec）
- [x] `onDisconnected()`：语义「清空」→「挂起 pending + cancel tick + conn_=nullptr」（重写 [reliable_codec.cpp:59-69](include/client/reliable_codec.cpp#L59-L69)）
- [x] `onConnected()`：全量重发 + retries 重置 + 恢复 tick（改 [reliable_codec.cpp:54-57](include/client/reliable_codec.cpp#L54-L57)）
- [x] 沿连接挂起期间的 TTL 检查（重连时先剔除超 TTL 帧 → LOST）
- [x] `setCacheLimit(size_t maxFrames, double ttlSec)` + 上限超限丢最旧
- [x] `stop()` 保持「进程退出清空」
- [x] `rel.onDisconnected/onConnected` 在业务连接回调里的调用次序不变（client_reliability_test / client_reconnect_test 均按此）
- [ ] 验证：断开-重连-续传端到端（见 §验证路径 3）

### A1 的边界（诚实声明）
server 侧 `seen` 是**内存态**——server 进程重启 → seen 清零 → 重连后旧帧当新帧执行。
所以 A1 只补「server 存活期内的断开恢复」，不承诺跨 server 重启的幂等（那是 A2 全局 seq 的范围）。A1 是真实缺口的最小补齐。

---

## Part B：外部业务线程投递（修订：不强行复用 EventLoop 队列）

### 背景
业务回调在 IO 线程同步执行；业务阻塞 → 整个 EventLoop 卡死 → 全部连接停摆。

### 目标形态
```
收: IO 线程（收帧/Ack/去重，轻）──enqueue──▶ 业务队列(有界) ──worker线程──▶ 业务回调
发: 业务线程 ──sendData（线程安全）→ runInLoop 转交 IO 线程 → socket
```

### 决策修正（针对「为了简单强行复用」）
原设计「复用 EventLoopThread + runInLoop 做收侧投递」**被否**：
`EventLoop::pendingFunctors_` 是无界 `vector`（[event_loop.hpp:54](include/net/event_loop.hpp#L54)），
背压「有界队列、满时丢最旧」**实现不了**。落地下改为：

1. **自建有界业务队列**（mutex + condvar + 环形队列，上限如 1024）
   - 满时丢最旧 + LOST 通知（防 IO 线程被慢业务拖垮）
   - 单一 worker 线程消费（复用 EventLoopThread 的线程封装，不碰其 loop 队列）
   - 保序：单 worker + FIFO
2. **发侧跨线程复用 `runInLoop`**：业务线程 `sendData` → `loop_->runInLoop` 转交 IO 线程执行
   - pending_ / conn_ 只在 IO 线程读写 → 零加锁，回调时序不变
3. **作用范围最小侵入**：`ReliableCodec::setDeliveryLoop` 开异步模式，默认同步（Day3 行为不变）

### 关键设计约束
- 业务回调**不一定在 loop 线程**执行 → 回调里不能再碰 loop 非线程安全 API（sendData 除外，走 runInLoop）。
- 业务 loop 用 `EventLoopThread` 承载（子线程内 new EventLoop + threadFunc 内 runloop，遵守 [llsrv-eventloop-multithread-base] 的地基纪律）。
- 队列满丢弃的 LOST 与重传 LOST 用同一 LostCallback 通道。

### 改动面（精确清单）
- [x] 新增 `BoundedQueue<T>`（mutex + condvar + deque，上限/丢最旧）——新容器，不碰 EventLoop 向量
- [x] `ReliableCodec::sendData` 线程安全（业务线程可调；内部 runInLoop 转交）
- [x] `ReliableCodec::setDeliveryLoop(EventLoop* deliveryLoop, size_t queueLimit=1024)`
- [x] 收侧：非 Ack/去重帧 → `runInLoop` 投递到业务 loop 再触发 `messageCallback_`
- [x] 验证：业务回调 sleep 观察 IO 不卡 + 帧序保持测试 + 队列满丢弃 LOST（见 §验证路径 4）

---

## 验证路径（用户在 WSL 跑）

1. P0 回归：`cmake --build build` 全过；`./build/client_protocol_test` → `PROTOCOL ROUNDTRIP PASS`
2. P0 端到端回归：正常模式 `client_reliability_test` 3/3 PASS + `--drop-ack 2` PASS（协议瘦身后链路仍通）
3. Part A 端到端（server 进程全程存活，「断开」由 server 主动切断模拟，见 [src/client_reconnect_test.cpp](src/client_reconnect_test.cpp)）：
   - 终端1 `./build/server_ack --drop-ack 2 --cut-after 3`（首见满 3 条后 0.3s 切断连接，seen 保留）
   - 终端2 `./build/client_reconnect_test`
   - 判定线索：client 3 条全首见回显收齐 → CUT 后 disconnect（pending 挂起剩 {2}）→ RECONNECTED #1 → 重发 seq=2 → server first=N 只 Ack 不回显 → pending 归零 → `RECONNECT RESUME PASS`
4. Part B：新增验证程序 [src/client_delivery_test.cpp](src/client_delivery_test.cpp)（单进程三线程：IO loop + client1 可靠链路 + client2 旁路探针 + delivery loop 业务承载）——
   - 终端1 `./build/server_ack`（普通模式，不 drop）
   - 终端2 `./build/client_delivery_test`
   - 判定线索：业务回调 sleep 1s/帧（delivery 线程）同时 client2 回显 maxRttUs << 1s 且测速当刻 businessSleeping=true（IO 不被业务阻塞）；
     client1 连发 5 帧 + 跨线程回发 1 帧（seq6），队列上限 2 → lost 非空（背压逐出）+ delivered∪lost 恰好覆盖 seq1..6 + delivered 严格递增（FIFO）+ pending=0 → `[client1] PASS`
5. 回归：Day3 三场景、既有 server/client_echo 不受影响（reliable/reconnect 复跑一遍）

## 里程碑（P0 → A → B，各自独立）

### P0 协议头瘦身
- [x] kHeadLen 9→5 + sendFrame/onMessage/decodeHeader 去 bodyLen
- [x] 注释/测试同步
- [x] client_protocol_test + 端到端三家回归 PASS（2026-10-20 用户 WSL 验证）

### Part A
- [x] onDisconnected 挂起 pending（不再清空）
- [x] onConnected 全量重发 + retries 重置 + tick 恢复
- [x] TTL/上限 + 超限抛最旧 LOST
- [x] 断开-重连-续传端到端 PASS（server_ack seen 幂等，2026-10-20 用户 WSL 验证 `RECONNECT RESUME PASS`）

### Part B
- [x] BoundedQueue 容器
- [x] sendData 跨线程安全（runInLoop 转交）
- [x] setDeliveryLoop 收侧异步投递
- [x] 背压：队列满丢最旧 + LOST
- [x] IO 不被业务阻塞验证 + 帧序保持测试（client_delivery_test 端到端 PASS，2026-10-20 用户 WSL 验证；
      同时修复退出 UAF：rel 须先于 deliveryThread 声明，EventLoopThread dtor 改带锁读 loop_）

### Day4 总回归
- [x] Day3 三场景 + client_protocol_test 全过（2026-10-20 用户 WSL 验证：protocol 单测、reliability 正常/`--drop-ack 2`、
      reconnect `--cut-after 3`、server+client_echo 旁路、delivery_test 全部 PASS）