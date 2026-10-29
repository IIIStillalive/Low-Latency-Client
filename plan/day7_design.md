# Day7 设计：req_id 升 64 位 + 断线端到端幂等 + 断线时序验证

> 状态：**待批准**。批准后按 D0 → D4 顺序实现，每步都可独立编译、独立验证。
> 日期：2026-10-27

## 0. 结论先说

1. **D0 必须先做**：`req_id` 从 32 位升到 64 位。改 proto 后线上格式不兼容，两端必须同时重建，所以它排在最前面，一次重建解决。
2. **Day7 有一个原计划没写清的结构缺口**：传输层 Ack 是"我收到了"，不是"我会给你响应"。服务端在 `sendAck` 之后、发响应之前被切断时，客户端 ReliableCodec 已把帧从 `pending_` 删掉，**可靠层不会再发**，调用只能等 deadline。所以 Day7 真正的重发点必须在 RpcCodec 层：**保留 in-flight 请求体，重连后由 RpcCodec 重发**，靠服务端 `req_id` 缓存去重。这条不补，Day7 的"断线中 in-flight 恢复"做不到。
3. **故障注入从"时间型 CLI 开关"改成"两个业务方法 + 一个传输层开关"**。时间型开关不可确定（见 D2 的理由），方法型可以让测试精确指定"我要这条连接断"，且能在连接池测试里定位到具体是哪条连接。
4. **`reqs` 缓存现在无上限**（`src/rpc_server.cpp:68`，只增不减），长跑必爆内存。加 TTL + 容量双上限，TTL 与 `ReliableCodec` 的 60 秒帧缓存对齐。
5. 明确不做：真取消、按 `idempotent` 字段选择性缓存、多线程服务端（`reqs` 无锁的前提是单 loop）。

---

## D0 前置：`req_id` 32 → 64 位

### D0.1 改动清单（精确到行）

| 文件 | 位置 | 改动 |
|---|---|---|
| `proto/rpc.proto` | 14、22 | `uint32 req_id = 1;` → `uint64 req_id = 1;`（字段号不变） |
| `include/client/rpc_types.hpp` | 命名空间顶部 | 新增 `using ReqId = std::uint64_t;`，注释写明"必须与 proto 的 req_id 类型一致" |
| `include/client/rpc_codec.hpp` | 66、75、96–100、106、108 | `uint32_t` → `ReqId`（含 `shared_ptr<atomic<ReqId>>`、`unordered_map<ReqId, InflightCall>`） |
| `include/client/rpc_codec.cpp` | 16、18、20、26、28、32、33、34、51、71、90、94、110、124、131、143 | 同上；`kReqIdStride = 1ull << 32` |
| `include/client/rpc_connection.hpp` | 49–50 | `setReqIdSequence(std::shared_ptr<std::atomic<ReqId>>)` |
| `include/client/rpc_pool.hpp` | 25 附近成员 | `reqIdSeq_` 类型随动 |
| `src/rpc_server.cpp` | 68、72、73、107、144 | `unordered_map<ReqId, ReqSlot>`、`finish(ReqId, ...)`、`const ReqId reqId = req.req_id();` |
| `plan/task_plan_rpc_client.md` | 51–52、300 | 同步文档里的 proto 引用与 32 位说明 |

`rpc_pool.cpp:22/33`、`rpc_connection.cpp` 的 `setReqIdSequence` 转发只跟着改类型，逻辑不变。

### D0.2 实现要点

- **别名 + 静态断言**，防止以后有人把 proto 改回 `uint32` 而代码静默截断：
  ```cpp
  // rpc_codec.cpp 与 src/rpc_server.cpp 各加一次（#include <type_traits>）
  static_assert(std::is_same_v<
                    decltype(std::declval<const rpc::RpcRequest&>().req_id()), ReqId>,
                "rpc.proto 的 req_id 类型必须与 ReqId 一致");
  ```
- **分配器**：`kReqIdStride = 1ull << 32`，随机起点用 63 位混合值（`random_device` ⊕ `steady_clock`）。
  为什么 stride 是 2^32：号段之间隔 40 亿，两个分配器各跑一整天也不会相交；按 2^32 对齐后，日志里一眼能看出是哪个进程/哪个池的号段。
- **传输层不动**：5B 头的 `seq` 仍是 `uint32`。`seq` 是每连接的传输序号（连接重建即重置），`req_id` 才是端到端身份。两者不能混。
- `MsgType::Cancel` 帧的 body 也是 `RpcRequest`（`rpc_codec.cpp:135`），自动跟着变 64 位，无需单独处理。

### D0.3 取舍（写进报告的实话）

- **protobuf varint 变长**：随机 64 位 id 编码成 10 字节（32 位小 id 只要 1–5 字节）。按随机起点，每个请求多约 5 字节。单条请求本来就有 5B 传输头 + method + payload，可忽略；但压测报告算带宽时要提一句。
- **不向后兼容**：32 位解析器读到 10 字节 varint 会截断或丢字段。本项目无线上部署，两端一起重编即可；`static_assert` 让这种错误在编译期就炸。
- **不用 `oneof`/`reserved`**：字段号保持 1，语义未变。

### D0.4 验证

- `grep -rn 'atomic<uint32_t>\|uint32_t reqId' include src` 应为空。
- 服务端日志 `exec req=` 的数值应远大于 2^32（随机起点）。

---

## D1 服务端 `reqs` 缓存：TTL + 容量双上限

现状：`std::unordered_map<ReqId, ReqSlot> reqs` 只增不减，每个 slot 持有完整响应体 —— 长期运行必然吃满内存。

### D1.1 策略

- 常量：`kReqCacheCap = 65536`、`kReqTtlSec = 60.0`、`kSweepIntervalSec = 1.0`。
  两个上限都可由 CLI 覆盖（`--req-cache-cap N` / `--req-ttl SEC`），配合 `--verbose`
  把日志降到 DEBUG，才能看见 `reqcache cap/ttl evict=` 行 —— 默认 64k/60s 在测试里永远
  触发不到，等于没实现。
- `ReqSlot` 增加 `double createdAtSec`（首次见到该 req_id 的时刻）。
- 插入序 `std::deque<ReqId> lruOrder`。**不是真 LRU**：req_id 从单个客户端看是单调递增的，先进先出足够；真 LRU 要 hash+list 双向索引，收益不值。
- 新请求首次出现：记 `createdAtSec` → `lruOrder.push_back` → `enforceCap()`。
- `enforceCap()`：
  ```
  size_t tries = 0;
  while(reqs.size() > kReqCacheCap && tries < lruOrder.size()){
      ReqId old = lruOrder.front(); lruOrder.pop_front(); ++tries;
      auto it = reqs.find(old);
      if(it == reqs.end()) continue;                 // 已被 TTL 清掉
      if(it->second.running){ lruOrder.push_back(old); continue; }  // 执行中的不能赶：赶走会让重发变成重复执行
      reqs.erase(it);
  }
  ```
  `tries` 上限防止"全部都在执行"时死循环。
- TTL 清扫：`loop.runEvery(kSweepIntervalSec, sweep)` 扫 `lruOrder` 一趟，删掉 `done && now - createdAtSec > kReqTtlSec` 的 slot 并同步摘出队列。同一趟顺带回收被 `enforceCap` 跳过的陈旧项，队列长度 ≤ 活跃数 + cap。
- 可观测：有驱逐时打一行 `reqcache evict=<n> size=<m>`（DEBUG）。

### D1.2 与可靠层的对齐（必须交叉引用注释）

`kReqTtlSec` 必须 **≥** `ReliableCodec` 的帧缓存时长（`rpc_connection.cpp` 里 `setCacheLimit(4096, 60.0)`）。
语义：客户端最多把未确认帧挂 60 秒，服务端缓存至少留 60 秒 —— 这样重连重发一定命中重放，不会变成重复执行。**改一个必须改另一个**，两处注释互相指。

### D1.3 明确不做的

- 不按 `req.idempotent()` 选择性缓存。当前"无条件缓存"是保守方向：缓存只会**避免**重复执行，代价是内存，而内存已被 D1.1 限住。按字段选择性缓存留作后续（需要客户端在每次调用时正确声明，是接口契约变更）。

### D1.4 已落地的现场验证（2026-10-27）

| 场景 | 命令要点 | 结果 |
|---|---|---|
| 默认参数回归 | `rpc_server` + `rpc_client_test` / `rpc_pool_test` / `rpc_pool_load_test` | 三者全 PASS；压测 `A 10000/10000`、`B 256/256 + 40/40`、`C 8/8`、`calls=10304 ok=10256` 与 Day6 一致 |
| 容量淘汰 | `--req-cache-cap 100 --verbose`，连跑 3 次 `rpc_client_test`（315 个 req_id） | `reqcache cap evict=1 size=100` 共 215 条，表大小稳定压在 100 |
| TTL 淘汰 | `--req-ttl 2 --verbose`，跑一次 `rpc_client_test`（105 条）后等 3 秒 | `reqcache ttl evict=105 size=0`，一条不剩 |
| 容量上限不影响正确性 | `--req-cache-cap 100` 下跑 10k 压测 | PASS，数值与默认参数完全相同 |

注意：10k 流水线首个阶段的 10000 条全是 `running`，容量淘汰一条都不会发生 —— 淘汰只在"插入时表已超限且队头已完成"时触发，所以验证要跑**多轮**客户端（新 req_id 进来，旧条目已完成）。

---

## D2 故障注入：为什么不用时间型开关

原计划的 `--cut-after-ms` / `--cut-seq` 有两个硬伤：

- `--cut-after-ms` 要断在"响应已缓存、尚未到达客户端"这一瞬，时间窗是微秒级，测试变成碰运气；
- `--cut-seq` 用传输 `seq` 定位，但 `seq` 是**每连接**的，池里 4 条连接并发，第 N 个帧落在哪条连接不可控。

改成：**两个业务方法（由测试精确触发）+ 一个传输层开关（业务方法表达不了的那一层）**。关闭连接直接用 `TcpConnection::close()`（`include/server/tcp_connnection.hpp:32`，走完整关闭链），不新造 API。

### D2.1 `cut` —— 断在"已确认、未执行"

```cpp
// ReqSlot 增 bool cutSent = false;
if(method == "cut" && !slot.cutSent){
    slot.cutSent = true;      // 一次性：重连后同一个 req_id 再进来才真正执行
    conn->close();            // 同步关闭：handleClose → 下树 → owner 释放 → fd 关闭
    return;                   // 不执行、不缓存、不回复
}
// 第二次（重连重发）落到正常路径 → 执行 → 回 "cut-once"
```

### D2.2 `cutreply` —— 断在"已缓存、未发送"

```cpp
// 首次：slot.dropReply = true，走正常执行
// finish() 里：
//   写入 responseBody 之后
if(it->second.dropReply){
    it->second.dropReply = false;
    if(auto c = it->second.conn.lock()) c->close();   // 缓存已落地，响应不发
    return;
}
```
重连重发 → 命中 `slot.done` → 重放缓存 → 客户端拿到与首次**完全一致**的结果，业务方法不再执行。

### D2.3 `--drop-ack K` —— 唯一的 CLI 开关

- `K` 是 **1 基帧序号**（不是 req_id）：第 K 个 `MsgType::Request` 帧到达时既不 Ack 也不执行，只打日志 `drop ack seq=<seq>`。
- 约 2.0 秒后客户端 ReliableCodec 重传同一帧（`setRetransmit(2.0, 0.5, 5)`），第二次到达走正常路径 → Ack + 执行 + 响应。
- 为什么必须留在 CLI：Ack 在方法执行**之前**发出（`src/rpc_server.cpp:99`），业务方法无法表达这个层次的故障。
- 注意：帧计数是进程级的，多个用例共用一个服务端会串号 → 该用例单独起一个服务端。

---

## D3 连接池改道：Slot 可用性状态机

现状：`pickSlot` 完全不知道连接死活。断线期间 RoundRobin 照样把请求派给死连接 —— 不会失败（帧进 `pending_` 等重连），但全压在一条连接上，重连后形成突发。

### D3.1 设计

- `Slot` 增 `std::atomic<bool> available{false}`。
- `RpcConnection` 增 `void setStateCallback(std::function<void(bool connected)>)`，在已有的 `client_.setConnectionCallback`（`include/client/rpc_connection.cpp:25`）里调用：
  - `conn != nullptr` → `relCodec_.onConnected(conn)` 之后回调 `true`
  - `conn == nullptr` → `relCodec_.onDisconnected()` 之后回调 `false`
  - **始终在 IO 线程**，这是纪律，写进头文件。
- 池构造里：`s.conn->setStateCallback([&a = s.available](bool ok){ a.store(ok, std::memory_order_relaxed); });`
  - 地址稳定性：`slots_.reserve(connCount)` 之后才 push，Slot 地址不变（现有注释已保证）；池对象生存期覆盖所有连接（`shutdown()` 先 join 再释放）。
- `pickSlot`：
  - RoundRobin：从 `start` 起扫一圈，取第一个 `available`；**全部不可用 → 仍返回 `start`**（与今天行为一致：请求挂起等重连，不 fail）。
  - LeastInflight：只在 `available` 里挑最小负载；全不可用 → 退回全量扫描。

### D3.2 为什么不用 `isConnected()`

`TcpClient::isConnected()` 只能在 loop 线程读（头文件已注明）。池线程要读，只能靠 `available` 这个原子快照。

### D3.3 纪律（写进头文件注释）

`available` 是**启发式快照**，不是同步原语：真实可用性仍由 ReliableCodec 的 `pending_` 队列兜底（帧不会丢）。和 `LeastInflight` 的"只是避开最忙连接的启发式"是同一套说法。

### D3.4 行为不变性检查

连接还没建立时 4 个 slot 全是 `false` → RR 返回 `start`，与现状完全一致。Day6 压测的分布（每连接 64 接受 / 10 拒绝）不受影响。

---

## D4 RpcCodec 层的重连重发（Day7 的结构核心）

### D4.1 问题

`ReliableCodec` 的 Ack 语义是"这个传输帧我收到了"，收到 Ack 即从 `pending_` 删除。
服务端在 `sendAck`（`src/rpc_server.cpp:99`）之后、响应发出之前被切断时：
客户端帧已被 Ack → `pending_` 里没有 → 重连后可靠层无从重发 → 调用一直挂到 deadline，返回 `DeadlineExceeded`。

Day7 要实现"断线中 in-flight 恢复"，只能在这一层补：

### D4.2 设计

- `InflightCall` 增 `std::string requestBody;`（序列化后的 `RpcRequest`，几十字节）。
  代价：内存 = in-flight 数 × body。默认 `maxInflight=1024`；Day6 压测把上限开到 10000 时也就几十 MB 量级，可接受。
- `RpcCodec` 增 `void onReconnected()`：遍历 `inflight_`，对每一项 `rel_.sendFrame(MsgType::Request, body)`。
- `RpcConnection` 的连接回调里顺序固定（**顺序错了会丢帧**）：
  ```
  conn != nullptr:
      relCodec_.onConnected(conn);     // ① 重置 seq 空间（新连接新序号）
      rpcCodec_.onReconnected();       // ② 用新 seq 重发 in-flight（仅重连时）
      stateCb(true);
  ```
- "仅重连"：`RpcCodec` 记 `everConnected_`；首次连接不重发（此时 in-flight 的帧还在 ReliableCodec 的 `pending_` 里，可靠层自己会发）。

### D4.3 重复帧怎么办（诚实说明）

某个 in-flight 调用如果还没被 Ack，重连时会出现两份：
① 可靠层 `pending_` 里的重发，② RpcCodec 的重发 —— 两条不同 `seq` 的帧携带同一个 `req_id`。

服务端按 `req_id` 去重（`running` → 只更新回程连接；`done` → 重放缓存），**不会重复执行**；客户端会对第二份响应计一次 `lateResponses`（重复响应，幂等丢弃）。
代价是每次重连多一条帧/一个 duplicate 计数，换来"断线窗口不丢调用"。报告里写明这个取舍。

### D4.4 失效窗口（必须写进报告）

- in-flight 帧超过 `ReliableCodec` cacheLimit（4096 条 / 60 秒）被丢弃 → 该调用只能等 deadline。
- 服务端缓存被 TTL 清掉后重发 → 会重复执行一次。两处 60 秒对齐（D1.2）就是把窗口关掉的手段。
- 重连退避 0.5→5.0 秒：`deadline` 必须显著大于最大退避（默认 5 秒偏紧，测试里对断线用例统一用 8 秒）。

---

## D5 新测试 `src/rpc_reconnect_rpc_test.cpp`

全部对着**一个不带任何开关的普通 `rpc_server`** 跑（故障由业务方法触发，除 T5 外）。

| 用例 | 场景 | 断言 |
|---|---|---|
| T1 执行前断 | 单连接，`cut` | `Ok` + payload `cut-once`；服务端 `exec req=<id>` 恰好 1 次；`stats.timeouts==0` |
| T2 缓存后断 | 单连接，`cutreply` on `idem` | payload `1`；服务端日志出现 `replay req=`；随后再调 `idem` 得 `2`（若重放又执行了一次会是 3） |
| T3 池内改道 | 4 连接池；首次调用（RR 落在 slot 0）就是 `cut`；断线窗口内连发 20 个 echo | 20/20 `Ok`；窗口内 `statsAt(0).calls` 不增（改道生效），`statsAt(1..3)` 合计 +20；恢复后 `statsAt(0).ok` +1；再发 4 个请求应命中全部 4 个 slot |
| T4 in-flight 不被清空 | 池 + `maxInflight=8`；断线窗口内发 4 个 `sleep(200)` | 4/4 `Ok`，无 `ResourceExhausted`；窗口内 `pool.inflight() > 0`，全部完成后 `==0` |
| T5 drop-ack | 独立服务端 `--drop-ack 1`，`echo`，deadline 8s | `Ok`；耗时 ∈ [1.9s, 8s)；服务端 `drop ack` 恰好 1 条 |

实现纪律：

- 开跑前先等连接建立（睡 300 ms + 探活一次 `echo` 确认 `Ok`），否则 RR 起点判断不可靠。
- 所有 `stats()` / `statsAt()` 快照**必须在 `shutdown()` 之前取**（Day6 踩过：shutdown 后连接对象已销毁，全为 0）。
- T3 里的 slot 归属依赖 RoundRobin 从 `rr_=0` 开始且 4 个连接都已 available —— 用探活保证。
- 输出格式与既有测试一致：`[rpc_reconnect] T1 ... PASS/FAIL`，末尾 `RESULT: PASS` / `FAIL`，exit code 0/1。
- `CMakeLists.txt` 加 `rpc_reconnect_rpc_test` 目标（与 `rpc_pool_test` 同一组源文件 + 新测试源）。

---

## D6 断线时序（实现要依赖的事实链）

1. 服务端 `conn->close()` → `handleClose()`（`tcp_connnection.cpp:144`）：cancel 心跳 → `removechannel`（EPOLL_CTL_DEL，**不关 fd**）→ `closeCallback_(this)` → `TcpServer` 从 `conns_` 摘除 → 最后一个 shared_ptr 在 `close()` 的 `keep` 析构时释放 → `Socket` 析构关 fd → 客户端收到 RST（接收队列有未读数据时是 RST 而不是 FIN）。
2. 客户端 IO 线程：epoll 事件 → `TcpClient` teardown → `connectionCallback(nullptr)` → `relCodec_.onDisconnected()`（`pending_` 原样保留）→ 池置 `available=false`。
3. `TcpClient` 按 0.5→5.0 秒指数退避重连（`tcp_client.cpp:225`）。
4. 连上 → `relCodec_.onConnected(conn)`：seq 重置 → 重发 `pending_`；随后 `rpcCodec_.onReconnected()` 重发全部 in-flight（D4）。
5. 服务端在新连接上收到同 `req_id` 的帧：`running` → 只更新回程连接；`done` → 重放 `responseBody`；不存在 → 首次执行。
6. 响应回到客户端 IO 线程 → `RpcCodec::onFrame` 按 `req_id` 命中 in-flight → Future 完成 → 续体投递到 biz loop。

---

## 7. 验证计划（命令 + 用法）

```bash
# ① 重建（D0 改了 proto，必须全量重建，不要增量）
cmake -S . -B build && cmake --build build -j4
# 用法：两条命令都要跑；只跑第二条时 protoc 生成物可能还是旧的。

# ② 出问题先验二进制（/mnt/f 上出过 0 字节 / 全零 ELF）
file build/rpc_server build/rpc_reconnect_rpc_test
# 用法：两行都必须报 "ELF 64-bit LSB pie executable"；报 "data" 就 rm -rf build 重建。
```

```bash
# ③ 普通用例（T1–T4）：一个 bash 调用内同时起停服务端
cd /mnt/f/C++项目/低延迟高性能服务器2
stdbuf -oL -eL ./build/rpc_server > /tmp/srv.log 2>&1 & SRV=$!
sleep 1
stdbuf -oL -eL ./build/rpc_reconnect_rpc_test 127.0.0.1 8899; echo "exit=$?"
grep -c 'replay req=' /tmp/srv.log
kill $SRV
# 用法：stdbuf 是为了行缓冲（重定向时默认全缓冲，日志会是空的）；
#       $SRV 记 PID 后 kill —— 不要用 pkill -f 'build/rpc_server'，那个模式会匹配到执行它的 shell 自身。
#       exit=0 且 replay 计数 >= 1 才算通过。
```

```bash
# ④ T5（drop-ack）单独起服务端，帧计数是进程级的
stdbuf -oL -eL ./build/rpc_server --drop-ack 1 > /tmp/srv_ack.log 2>&1 & SRV=$!
sleep 1
stdbuf -oL -eL ./build/rpc_reconnect_rpc_test 127.0.0.1 8899 --case T5; echo "exit=$?"
grep -c 'drop ack' /tmp/srv_ack.log      # 期望 1
kill $SRV
```

```bash
# ⑤ 回归（D0 改了线上格式，所有既有测试都要重跑）
stdbuf -oL -eL ./build/rpc_client_test 127.0.0.1 8899
stdbuf -oL -eL ./build/rpc_pool_test   127.0.0.1 8899
for i in 1 2 3; do stdbuf -oL -eL ./build/rpc_pool_load_test 127.0.0.1 8899 | tail -3; done
./build/timer_demo
# 用法：load_test 三连跑是为了确认 req_id 随机起点在 64 位下仍然生效（Day6 就是被跨进程撞号坑过）。
```

---

## 8. 风险

| 风险 | 影响 | 处理 |
|---|---|---|
| 64 位 proto 不兼容 | 新旧二进制混跑会静默错关联 | 两端一起重建；`static_assert` 编译期拦截 |
| `cut` / `cutreply` 依赖 `close()` 的"立即关 fd"语义 | 若将来改成"优雅关闭先 flush"，`cutreply` 的"不发"就失效 | 在 `rpc_server.cpp` 注释里钉住这条依赖 |
| RpcCodec 重发与可靠层重发重复 | 每次重连多约 1 条帧/调用（未确认的那些） | 服务端 req_id 去重兜底，`lateResponses` 可观测 |
| 重连退避最长 5 秒 vs 默认 deadline 5 秒 | 断线用例可能刚好撞 deadline | 断线用例统一 `deadlineSec = 8.0` |
| `--drop-ack` 的帧计数是进程级 | 多用例共用一个服务端会串号 | 该用例单独起服务端 |
| `reqs` TTL 与帧缓存时长不一致 | 重放变成重复执行 | 两处常量交叉注释，D1.2 |

## 9. 工作量与改动面

| 阶段 | 新增/修改 | 预估 |
|---|---|---|
| D0 64 位 | 8 个文件 + proto + 计划文档 | 0.5 h |
| D1 缓存上限 | `src/rpc_server.cpp` | 0.7 h |
| D2 故障注入 | `src/rpc_server.cpp` + `tcp_connnection.hpp`（只加注释，不改 API） | 0.7 h |
| D3 Slot 可用性 | `rpc_pool.{hpp,cpp}`、`rpc_connection.{hpp,cpp}` | 0.5 h |
| D4 RpcCodec 重发 | `rpc_codec.{hpp,cpp}`、`rpc_connection.cpp` | 0.5 h |
| D5 新测试 | 新文件 + `CMakeLists.txt` | 1.0 h |
| 合计 | 约 13 个文件 | 4 h 左右 |
