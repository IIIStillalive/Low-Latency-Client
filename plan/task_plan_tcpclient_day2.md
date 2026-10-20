# TcpClient Day2 — 指数退避自动重连

状态：`验证通过（用户 WSL 编译 + client_protocol_test 回归 + server/client_echo 一发一收）`
依赖：Day1 已交付（连接建立 + 握手 5s 兜底 + teardown 延迟回收）；契约不变
（我先展示接口 → 用户指令 → 我实现；用户 WSL 编译/运行验证）。

---

## 0. 范围（Day2 只做一件事）

断线 / 握手失败后，**指数退避自动重连**，且与用户主动下线（`disconnect()` 后
`connect()` no-op）天然互斥，生命周期安全（**所有** pending timer 可取消）。

Day2 明确不做（后续 Day，钩子已留）：
- ACK / 去重 / 重传（协议可靠性，Day3+）
- 网络线程 → 外部业务线程消息投递（Day4+）
- DNS 解析（继续只接受 sockaddr_in）

## 1. 状态机扩充（在 Day1 的 Idle→Connecting→Connected 上增一段）

```
 Idle ──connect()──▶ Connecting ──SO_ERROR==0──▶ Connected ──对端关/超时──▶┐
   ▲                    │  (失败/超时)                                       │
   │                    └── teardownConnecting ──▶ scheduleReconnect ───────┤
   │                                                                         ▼
   │                                    ┌────────────────────────────── 失败哨兵
   │                                    │  （conn_ 已空 / connecting_ 已 false）
   └── disconnect()：cancel 全部 pending timer（newTimer→oldTimer→0，无悬挂）◀─┘
```

新增段（loop 线程内全串行）：任何「非主动下线」的失败（握手失败、握手超时、活动连接断开）
统一汇入 `scheduleReconnect()` → 按退避调 `runAfter` 单次 timer → 到期
`onReconnectTick()` → 复用 `connectInLoop()` 完整握手链。成功即 `retryCount_=0`。

「主动下线」= `disconnect()`（active_→false）：不调度重连，且 cancel 所有 pending timer
（既有重连 timer / 握手兜底 timer），保证 `TcpClient` 可安全析构。

## 2. 新增接口（全部向后兼容，不破坏 Day1 已确认签名）

```cpp
// 自动重连开关 + 退避参数（秒）。loop 线程外调用会被 runInLoop 转交；
// 已连接时调用：随后断线/失败按新参数调度。
void setAutoReconnect(bool enable, double initialBackoff = 1.0, double maxBackoff = 30.0);
```
退避序列：`initial * 2^retry`，≥max 封顶（默认 1s, 2s, 4s, ... 30s）。
成功（`connectionCallback_(conn)`）清零计数。

仅当用户确认「要暴露重连次数给上层」才追加（不破坏现有 `setConnectionCallback`）：

```cpp
// 可选：每次「将要发起」第 n 次尝试前回调（n 从 1 起），业务可据此打日志/决定放弃。
void setAttemptCallback(std::function<void(int n)> cb);
```

## 3. 私有实现（.cpp 增补）

```cpp
void scheduleReconnect();          // [loop] 按退避调度一次重连；autoReconnect_=false 直接返回
void onReconnectTick();            // [loop] retry timer 到期 → guard → connectInLoop()
TimerId reconnectTimerId_ = 0;     // 0 = 无 pending 重连
TimerId handshakeTimerId_ = 0;     // （补 Day1 洞）握手 5s 兜底 timer 句柄，可 cancel
int     retryCount_ = 0;           // 连续失败次数，成功（onConnected）清零
double  initialBackoff_ = 1.0;
double  maxBackoff_ = 30.0;
bool    autoReconnect_ = false;    // 默认值 = 决策点 2，用户拍板
```

关键时序约束：
- `fail()` / `removeAndReset()`：**先 `scheduleReconnect()` 再通知上层 nullptr**
  —— 上层在回调里 `disconnect()` 时，cancel 已把 pending timer 摘掉，不会「通知后又被调度」。
- `scheduleReconnect()` 内 guard `if(!active_) return;` 双保险。
- `disconnect()`（runInLoop 内）：cancel `reconnectTimerId_` + cancel `handshakeTimerId_`
  再走既有 teardown/close 链。析构前无任何 pending `[this]` timer → 可安全析构。
- `onConnected()`：`retryCount_ = 0;`
- `teardownConnecting()`：增加 cancel `handshakeTimerId_`（覆盖成功/失败/主动拆三条路径）。

## 4. Day1 洞补修（本计划顺带，属 Day2 生命周期完整性）

`connectInLoop()` 的握手兜底 timer 返回值 Day1 被丢弃、从未 cancel。
触发条件：客户在 Connecting 期间被析构（loop 线程合法）→ 5s 后 timer 到期触发 `[this]` = UAF。
修：成员 `handshakeTimerId_` 持有句柄，`teardownConnecting()` 内 cancel。

## 5. 决策点（AskUserQuestion 收敛，随后写实现）

1. **范围**：仅重连（用户拍板 ✅）
2. **自动重连默认开关**：默认关闭、显式 `setAutoReconnect(true)`（用户拍板 ✅）
3. **是否暴露重连次数给上层**：加可选 `setAttemptCallback()`（用户拍板 ✅）

## 5b. 实现记录（2026-10-19）

- 按决策实现；`attemptCallback(n)` 在「调度时」触发（n 从 1 起），成功清零。
- 关键时序：`fail()/removeAndReset()` 一律**先 scheduleReconnect 后通知上层**——
  上层在回调里 `disconnect()` 时，runInLoop 内联执行的 cancel 已抢先摘掉刚调度的 timer。
- `disconnect()` 全量 cancel（重连 timer + 握手兜底 timer）→ `TcpClient` 可安全析构。
- `onConnected` 防御性撤掉幽灵重连 timer（业务在失败回调里手动 connect() 抢跑场景）。
- `setAutoReconnect(false)` 不清已在途 timer：到期由 onReconnectTick 的 `autoReconnect_` guard 作废，
  一次空转无害，避免跨线程 cancel 竞态。
- Day1 洞补修：`handshakeTimerId_` 成员持有握手兜底 timer，`teardownConnecting` 内 cancel
  （成功/失败/主动拆三路径全覆盖）。

## 6. 验证路径（用户 WSL 跑）

1. `cmake --build build` 全过。
2. `./build/client_protocol_test` → 仍 `PROTOCOL ROUNDTRIP PASS`（回归）。
3. 端到端：终端1 `./build/server`；终端2 `./build/client_echo` → 一发一收后干净退出（回归）。
4. 重连观察（默认开启时）：起 server → 起 client_echo → `Ctrl+C` server → client 应打印
   `connect failed/timeout` + 退避（间隔递增）重试，不再使 `loop` 退出；再起 server →
   client 应自动连上并继续（可发、可收）。默认关闭时本步验证 `setAutoReconnect(true)` 等效行为。
5. 主动下线互斥：连接后调 `disconnect()` → 永不重连；`TcpClient` 析构干净。

## 7. 里程碑

- [x] `setAutoReconnect()` 开关 + 退避参数
- [x] `scheduleReconnect / onReconnectTick` + `retryCount_` 成功清零
- [x] `fail / removeAndReset` 先调度后通知 + guard
- [x] `disconnect` 全量 cancel（重连 + 握手兜底 timer）+ `onConnected` 防幽灵 timer
- [x] Day1 洞：`handshakeTimerId_` 补 cancel
- [x] client_echo 回归 PASS（用户 2026-10-19 验证）
- [x] 断线重连端到端观察（用户 2026-10-19 验证）