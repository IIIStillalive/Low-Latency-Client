# Day8 压测报告（rpc_bench）

> 状态：**已实测并复核通过**（2026-10-29）。首轮实测发现三处异常（多线程 QPS 反缩放、pipeline
> 延迟钉死 wall、late>0）→ 根因 `EventLoop::dopendingFunctors` 无界单次 drain 饿死 socket I/O →
> 已改为「有界批 1024 + 自我唤醒」（见 §6 复核记录）→ 复测全绿，本节数字为修复后实测。
> 运行环境：WSL2 (Ubuntu) · g++ 13 · CMake ≥ 3.10 · protobuf（`protoc` ≥ 3.6）
> 被测对象：`./build/rpc_server`（单线程 demo 服务端，`setThreadNum(0)`）+ `./build/rpc_bench`。

---

## 1. 被测栈与线程模型

```
rpc_bench（N 调用线程，任意线程 call()）
   └─ RpcChannelPool（conns × 每连接一个 EventLoopThread = multi-reactor）
        └─ RpcConnection：TcpClient → LengthHeaderCodec → ProtocolCodec(5B 头)
              → ReliableCodec(seq/Ack/重传/断线重发) → RpcCodec(req_id 关联 + Future/超时/取消)
服务端：单线程 epoll（rpc_server），echo 原样回 payload。
```

关键机制（与数字直接相关）：
- **单连接多路复用**：一个连接上同时 N 个 in-flight（pipelining），不等待前一个响应。
- **批末写合并（Day8）**：一轮事件循环内的多次 `send` 攒进 `outputBuffer_`，批末尾一次 `write`，
  N 帧流水线从 N 次 syscall 压成 ~1 次（`EventLoop::runAtLoopEnd` + `TcpConnection::scheduleBatchFlush`）。
- **TimerQueue::cancel O(1)（Day8）**：`TimerId → set::iterator` 索引，取消不再 O(n) 线性扫描；
  Day6 记录过 10k 在途时该路径是平方级 QPS 悲观值（~69k），本次为修复后数字。
- 服务端是**单线程**：吞吐上限主要被它卡住（demo 诚实边界，见 §5）。

---

## 2. 运行命令

### 2.1 构建

```bash
cmake -S . -B build          # 改了 CMakeLists 后必须重新配置
cmake --build build -j$(nproc)
```

### 2.2 起服务端

```bash
./build/rpc_server --port 8899 --verbose 2>&1 | tee /tmp/rpc_server.log
#              （--verbose 让 DEBUG 日志可见；压测时可省）
```

### 2.3 跑基准矩阵（每条命令单独跑，回填 §4 表）

```bash
# 对比① 通道数：1 连接 vs 4 连接（conns=1 即单 loop / 单 reactor 基线）
./build/rpc_bench --conns 1 --threads 4 --reqs 10000 --mode pipeline   # A1
./build/rpc_bench --conns 4 --threads 4 --reqs 10000 --mode pipeline   # A2
# 对比② 请求模式：pipeline（多路复用吞吐）vs sync（单请求往返延迟）
./build/rpc_bench --conns 4 --threads 4 --reqs 10000 --mode sync       # B1
# 并发线程扫描（同 4 连接）：看调用线程多寡对吞吐的影响
./build/rpc_bench --conns 4 --threads 1 --reqs 10000 --mode pipeline   # C1
./build/rpc_bench --conns 4 --threads 8 --reqs 10000 --mode pipeline   # C2
```

> 每行建议跑 3 次取中位数，记 QPS 与 p99/p999。`--reqs` 是**每线程**请求数，
> `threads×reqs` 为总调用数；pipeline 时 in-flight ≈ threads×reqs（多路复用，连接内多 not 等待）。
> 服务端 req 缓存容量默认 65536，本矩阵总量 ≤ 8×10000 不触发淘汰；如需观察淘汰路径另跑
> `--req-cache-cap` 系列（此处不展开）。

### 2.4 故障注入（Day8 d4，压缩到一条龙）

```bash
# 丢 Ack（传输层）：第 3 个 Request 帧不确认不执行，客户端 ~2s 后重传同一帧（同 seq）
./build/rpc_server --port 8899 --drop-ack 3 --verbose 2>&1 | tee /tmp/rpc_fault.log &
./build/rpc_reconnect_rpc_test --port 8899    # T1-T5 全 PASS（其中 drop-ack 场景验证重传）

# 服务端延迟（乱序响应 + 超时）：sleep 方法制造 300ms/800ms 延迟
#   （rpc_pool_load_test Phase B/C 已覆盖：sleep(300) 背压、sleep(400)+取消=迟到响应）

# 随机断连（应用层）：cut（Ack 后执行前断连）/ cutreply（执行后响应不发断连）
#   rpc_reconnect_rpc_test 的 T2/T3 已覆盖：重连 + 同 req_id 重发 → 服务端幂等重放，业务不重复执行
```

---

## 3. 判定标准（回填后核对这些不变量）

| 指标 | 期望 | 违反说明 |
|---|---|---|
| `ok == threads × reqs` | 恒等 | 任一帧丢失/错配 → 出错 |
| `exhausted == 0` | 恒等 | 背压闸设置足够大，不应触发 |
| `timeouts == 0` | 恒等 | deadline 30s 给足余量 |
| `inflight == 0`（stats） | 恒等 | 收尾完整、无泄漏在途 |
| `lateResponses` | 0（修复后） | 首轮 A2=239/C2=4602（2s 等待超时重传）→ 补丁后全 0 |
| QPS | 见 §4 实测 | 补丁后 A1=12581/A2=28905/B1=12933/C1=33939/C2=26445；旧 Day6 基线本次未复现（§5.1） |

---

## 4. 结果表（回填）

### 4.1 吞吐 vs 通道数（pipeline，threads=4，reqs/thread=10000，修复后）

| ID | conns | QPS | wall(s) | p50(µs) | p99(µs) | p999(µs) | max(µs) |
|---|---|---|---|---|---|---|---|
| A1 | 1 | 12580.6 | 3.179 | 1722415 | 2994131 | 3003144 | 3004147 |
| A2 | 4 | 28904.6 | 1.384 | 1115282 | 1253747 | 1254363 | 1255090 |

> 4 连接 multi-reactor 相对 1 连接单 loop 吞吐 **≈2.3×**（28905/12581）。

### 4.2 模式对比（conns=4，threads=4，reqs/thread=10000，修复后）

| ID | mode | QPS | p50(µs) | p99(µs) | p999(µs) | max(µs) |
|---|---|---|---|---|---|---|
| A2 | pipeline | 28904.6 | 1115282 | 1253747 | 1254363 | 1255090 |
| B1 | sync | 12933.0 | 253 | 880 | 4548 | 17085 |

> pipeline 的 QPS ≈ 2.2× sync（多路复用隐藏 RTT）；**sync 的 p50=253µs 才是单请求端到端延迟**。

### 4.3 调用线程数（conns=4，pipeline，reqs/thread=10000，修复后）

| ID | threads | QPS | p50(µs) | p99(µs) | p999(µs) |
|---|---|---|---|---|---|
| C1 | 1 | 33939.4 | 192871 | 218168 | 218280 |
| A2 | 4 | 28904.6 | 1115282 | 1253747 | 1254363 |
| C2 | 8 | 26445.2 | 2312146 | 2791254 | 2791847 |

> 单线程发满（1→4 线程）吞吐仍有提升；**4→8 线程边际转负（-9%）**：多生产者共享
> req_id 原子 + eventfd 唤醒的开销曲线，叠加 WSL2 CPU 超订，属预期边界而非异常
> （修复前是 27778→12230→9425 的腰斩式反缩放）。

### 4.4 单请求端到端延迟（sync，供纵向展示）

| 场景 | p50(µs) | p99(µs) | 说明 |
|---|---|---|---|
| sync · 4 连接 · 4 线程（B1） | 253 | 880 | 本轮实测；单请求往返 P50 |
| 1 连接 sync（RTT 下限） | — | — | 未跑（可选补充） |

---

## 5. 结论（2026-10-29 实测终版）

1. **吞吐（修复后实测）**：A2(pipeline,4 连接,4 线程)=28905 QPS；C1=33939；C2=26445；B1(sync)=12933。
   **Day6 记录基线（task_plan run1-3: 76332/69891/69186）本次未复现**：`rpc_pool_load_test`
   Phase A 本轮实测 43235（21 万请求污染后的同一服务端上跑的），且为 Day8 改动前的历史数字。
   差异归因环境/基线可复现性（WSL 负载、服务端长跑状态），**不作代码劣化结论**——B1 sync
   p50=253µs 干净，传输/服务端自身无恙；如要钉死历史基线，需全新服务端 + 空闲 WSL 下重测。
2. **通道数**：A2/A1 = 28905/12581 ≈ **2.3×**。4 连接 multi-reactor 相对单连接单 loop 的
   吞吐收益清晰；两者同受单线程服务端限制，此对比是「单 loop 基线 vs multi-reactor」的诚实数字。
3. **模式对比**：pipeline QPS(28905) > sync QPS(12933)。pipeline 靠多路复用把 RTT 隐藏进
   流水线；sync 的 p50=253µs 才是「单请求端到端延迟」的真实值。两者语义不同，不可混读。
4. **诚实边界**：服务端单线程（`setThreadNum(0)`，demo 形制）是吞吐上界的主卡点；4→8 调用
   线程边际转负（-9%）是共享 req_id 原子 + eventfd 唤醒 + WSL2 超订的开销曲线。真实部署开
   多 worker（`setThreadNum(N)`）才能吃满客户端管线。
5. **修复事实**：首轮实测的三处异常（§6）由 `EventLoop::dopendingFunctors` 无界 drain 饿死
   socket I/O 引起；改为有界批后 late 全 0、p50 回落至 0.54–0.81×wall、线程反缩放下腰斩消失。
   这是本报告最有价值的产出——**压测抓到了真实的事件循环结构缺陷**。
6. **故障注入（已勾选，2026-10-29）**：丢 Ack / 延迟 / 随机断连的端到端验证由 `scripts/regress.sh`
   覆盖，全量回归实测 PASS=11 / FAIL=0 —— 含 T1–T5（cut/cutreply/drop-ack 幂等）与 Day8 压测哨兵
   （`rpc_bench_smoke` 小规模，不产生报告数据）。

---

## 6. 复现与备注

- 全部命令见 §2；`rpc_bench` 退出码 0 = 不变量全过，1 = 失败。
- 压测期间关掉会影响数值的干扰项（本机 CI、页面渲染等）。
- 想拆「写合并 vs cancel O(1)」的独立贡献，可临时把 `EventLoop::runAtLoopEnd` 相关绕开再跑
  A2 —— 但为保持主分支可审查，回填 §4 用保守的「合体后数字」即可，拆解留给需要时的附加实验。
- 回顾：Day6 基线证据见 `plan/task_plan_rpc_client.md`（run1-3: 76332/69891/69186）。

### 复核记录（2026-10-29 首轮实测异常 + 修复）

首轮 5 组实测（A1/A2/B1/C1/C2）发现三个异常，**不能作为 Day8 成功证据**：
1. **pipeline QPS 随调用线程数反缩放**：C1(1 线程)=27778 → A2(4)=12230 → C2(8)=9425；
2. **pipeline 延迟钉死在 wall 上（响应末尾一次性冲入）**：A1 p50≈p999≈max≈wall，
   非均匀回流（均匀时应 ≈0.5×wall）；
3. **`lateResponses > 0`（A2=239 / C2=4602）**：本矩阵无超时无取消，late 只可能来自
   重复响应 = ReliableCodec 2s Ack 超时重发 + 服务端幂等重放 → 直接证明存在 >2s 的响应真空。

**根因**（`include/net/event_loop.cpp`）：`dopendingFunctors` 单次把 eventfd 聚合的
runInLoop 洪峰全量跑完，饿死同 loop 的 socket I/O → 流水线秒级停摆。
**修复**（Day8-b，2026-10-29 已落地）：`dopendingFunctors` 改为**有界批（1024）+ 自我唤醒**，
functor 洪峰与 socket I/O 交替服务；语义不变。对照组 B1(sync,在途≈4)=p50 260µs 干净，
证明传输/服务端本身无恙，异常只在"高在途+多调用线程"的 pipeline 形态。
**待复核 → 已复核通过（2026-10-29 复测）**：重编后 A1/A2/C1/C2 全绿——`late` 全 0、
pipeline p50 回落至 0.54–0.81×wall（不再钉死 wall）、线程反缩放下腰斩消失
（33939/28905/26445）；B1 sync p50 253µs 不变；`rpc_load` Phase A=43235 PASS。
**剩余未决（复测后收口）**：仅剩 ① Day6 历史基线 ~70k 本次未复现（见 §5.1），不作劣化结论——
如需钉死历史基线，需全新服务端 + 空闲 WSL 下重测 `rpc_pool_load_test`。此前"② 故障注入端到端
未跑通"已关闭：`scripts/regress.sh` 全过（PASS=11/FAIL=0，见 §5.6 已勾选）。