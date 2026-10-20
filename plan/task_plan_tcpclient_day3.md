# TcpClient Day3 — ACK / 去重 / 重传（可靠消息层）

状态：`验证通过（2026-10-20，用户 WSL：正常模式连跑 PASS + --drop-ack 2 重传路径实证）`
决策（用户拍板）：独立 ReliableCodec 层 ✅ / 断线清空+通知 ✅ / 有限次重传后放弃 ✅
依赖：Day1/2 已验证通过（连接建立 + 自动重连）；本设计不依赖 Day2 编译结果，可并行推进。

---

## 0. 现状盘点（已核实，设计硬约束）

- `ProtocolCodec` 无状态，一实例服务所有连接；seq 由调用者显式传（client_echo 硬编码 seq=1）。
- `MsgType::Ack` + `sendAck(conn, seq)` 已定义但**零调用**——ACK 骨架在、行为不在。
- 无自动 seq、无未确认缓冲、无重传、无收侧去重。

## 1. 目标与边界

**只做一件事**：客户端新增**有状态可靠层** `ReliableCodec`——发送自动序号 + 未确认缓冲 +
超时重传 + 收侧去重 + 自动回 Ack，**仅保证连接存活期内的可靠投递**。

Day3 明确不做：
- server 侧完整可靠层（server_ack 只做「收到 Data 回一条 Ack」配合验证）。
- 乱序接收窗口（单连接 + TCP 保序，重传只可能带来 seq≤已收最大值的旧帧，lastRecvSeq 单调够）。
- 协议/分帧变更（9B 私有头 + 4B 长度头全不动）。

## 2. 架构（决策点 1：独立层）

```
发: 业务 → ReliableCodec::sendData ──自动seq→ ProtocolCodec::sendFrame ──4B头→ LengthHeaderCodec::send ──▶ socket
收: 业务 ← ReliableCodec::onFrame  译 Ack/去重/回Ack  ← ProtocolCodec::onMessage（拆 9B 头）
```

独立层而非并入 ProtocolCodec：后者保持无状态纯编解码（client_protocol_test 零回归），
可靠状态（pending_ 缓冲、lastRecvSeq_、tick TimerId）放每连接一实例的 ReliableCodec。

## 3. 接口（已按确认版实现）

```cpp
class ReliableCodec {
public:
    ReliableCodec(ProtocolCodec& proto, EventLoop* loop);   // loop 仅 runEvery tick

    void sendData(TcpConnection* conn, const void* body, size_t n);  // 自动 seq + 缓存 + 发
    void onFrame(TcpConnection* conn, MsgType type, uint32_t seq, const std::string& body);
    //   Ack → 匹配 pending_ 移除；Data(新) → 回 Ack + 交业务；Data(旧) → 丢但回 Ack；
    //   Heartbeat/Quit → 透传业务
    void setMessageCallback(MessageCallback cb);   // 真正交业务的帧
    void setLostCallback(LostCallback cb);         // 未确认被放弃（断线/超限）
    void setRetransmit(double timeoutSec = 2.0, double tickSec = 0.5, int maxRetries = 3);
    void onConnected(TcpConnection* conn);         // 记录当前连接（tick 重传挂 conn）
    void onDisconnected();                         // cancel tick + 清 pending + 通知丢失
    void stop();                                   // 退出收尾，同 onDisconnected
    size_t pendingCount() const;                   // 调试/测试
};
```

## 4. 实现记要（2026-10-19 已实现 + 诊断零报错）

- 新交付 4 文件 + CMake 2 target：`reliable_codec.hpp/.cpp`、`server_ack.cpp`（`--drop-ack N` 故障注入）、
  `client_reliability_test.cpp`。现有层零改动（ProtocolCodec/LengthHeaderCodec/TcpClient 均未动）。
- 接线四层：TcpClient → LengthHeaderCodec → ProtocolCodec → ReliableCodec → 业务。
- 关键实现：
  - 自动 seq（`nextSeq_` 从 1 起步）；pending 存**原始 body**，重传用 `sendFrame` 重打包 9B 头
    （帧头永远由协议层造，单一路径）。
  - tick 惰性启动（首个 sendData 才 `runEvery`）；`onDisconnected/stop` cancel，无悬挂 `[this]` timer。
  - 去重新旧判定：`int64(seq) - int64(lastRecvSeq_) > 0`（防 uint32 回绕）；旧帧不回业务但回 Ack。
  - 重传上限：初始 1 次 + 最多 maxRetries(=3) 次重发，超限 WARN + lostCallback 通知。
  - `server_ack`：单线程（`setThreadNum(0)`），`seen` 判定「首见 seq」实现 `--drop-ack` 语义。

## 5. 决策点记录（AskUserQuestion，用户拍板）

1. **架构**：独立 ReliableCodec 层 ✅（并入 ProtocolCodec 破坏无状态，弃）
2. **断线时未确认帧**：清空 + lostCallback 通知 ✅（保留跨重连重发需 server 去重配合，弃）
3. **重传上限**：有限次（默认 3 次重发）后放弃 ✅（无限重发防积压，弃）

## 6. 验证路径（用户 WSL 跑）

1. `cmake --build build` 全过（新增 `server_ack` + `client_reliability_test` 两个 target）。
2. `./build/client_protocol_test` → 回归 `PROTOCOL ROUNDTRIP PASS`（验证独立层零侵入）。
3. 可靠端到端：
   - 终端1 `./build/server_ack`；终端2 `./build/client_reliability_test`：
     发 3 条 → 期待 3 条 echo 回显 + `RELIABLE ECHO PASS: 3/3 acked, dedup ok` + pending=0。
   - 故障注入：终端1改为 `./build/server_ack --drop-ack 2` → seq=2 首次不回 Ack，
     client 应在约 2s 后打 `retransmit seq=2 attempt=1`，随后最终 PASS。
4. 回归：既有 server 与 client_echo 一发一收不受影响（现有层零改动）。

## 7. 里程碑

- [x] `ReliableCodec` 类（sendData/onFrame/onConnected/onDisconnected/stop）
- [x] 自动 seq + pending 缓冲 + tick 扫描重传（timeout/次数封顶）
- [x] 收侧去重（lastRecvSeq 有符号判新）+ 自动回 Ack
- [x] 连接回调接入（业务层显式 onConnected/onDisconnected，TcpClient 零侵入）
- [x] `server_ack` + `client_reliability_test` target + `--drop-ack` 注入
- [x] 编译 + 端到端验证 PASS（用户跑，2026-10-20）

## 8. 验证实录（2026-10-20）

两次 WSL 实测，证据链完整：

1. **正常模式（dropAck=-1）**：3 条 Ack 全部 `erased=1` 落账，`pending=0`，`RELIABLE ECHO PASS`。
2. **故障注入（--drop-ack 2）**：seq=2 首次被吞（`server: DROP ack of seq=2`），
   client 约 2s 后 `ReliableCodec retransmit seq=2 attempt=1` → server `recv Data seq=2 first=N` +
   `sendAck seq=2` → client `ack seq=2 erased=1 pendAfter=0` → PASS。
   重传后的重复 echo 未再次交业务（去重实证）。

期间修复一处**测试程序自身缺陷**（非可靠层逻辑）：原 `client_reliability_test` 在收齐 3 条回显后
用固定 0.6s 判定，短于重传超时 2s → drop-ack 场景必然误判 FAIL/LOST；
改为 0.25s 轮询 pending（10s 上限），给重传路径留足时间。诊断日志（`[rel] send/recv/ack`、
`server: sendAck/echo`、`pendingSeqs()`）保留供后续排障。