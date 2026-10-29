# task_plan_chat_room.md —— Day10 聊天室广播（真实业务，闭合 README 唯一未勾项）

> 状态：**验证完成**（2026-10-29，WSL）：`./scripts/regress.sh --build` 全过，**PASS=12/FAIL=0**
> —— 旧 Day1–9 共 11 项不回归 + 新增 `chat_room_test` 全 G 过（`--threads 2` 跨 loop 广播）。
> 方向 A + 设计已批（「继续」）。实施/验证中修正 3 处：CMake 目标补接线（初稿未落盘）、
> `server::Logger` 命名空间前缀、G4 测试预期对齐 codec kMaxMsgLen 拦截。
> 契约：实现编写由我完成 → 编译/运行验证由用户执行，报错回传我修。

---

## 0. 目标与范围

- **目标**：在现有 server 栈（`TcpServer + LengthHeaderCodec + 多 Reactor`）上加业务层「聊天室」——
  多个客户端 join 同一房间，任一成员发消息 → 房间内广播。闭合 README 唯一未勾项 `真实业务`.
- **范围内**：`JOIN / MSG / LEAVE / LIST`（C2S）+ `JOINED / MSG / LEFT / MEMBERS / ERROR`（S2C）；跨 worker loop 广播正确性。
- **明确不做**（防止范围蔓延）：消息历史（kv-engine 回接是下一步）、离线补发、认证、RPC push 扩展、
  带外重传（聊天可丢帧，非可靠流）。

---

## 1. 帧协议（独立 1B type 层，位于 LengthHeaderCodec payload 之上，无 protobuf）

长度前缀由 `LengthHeaderCodec` 负责剥头；业务 payload = `[1B type][body]`。
字符串一律 `[u8 len][utf8 bytes]`（len ≤ 255）。

| type | 方向 | body | 语义 |
|---|---|---|---|
| 0x01 | C2S | `[room_len][room][name_len][name]` | join 房间（昵称入房） |
| 0x02 | C2S | `[room_len][room][<msg 余下全部>]` | 向房间发消息 |
| 0x03 | C2S | `[room_len][room]` | 离开房间 |
| 0x04 | C2S | `[room_len][room]` | 请求成员列表 |
| 0x81 | S2C | `[room_len][room][name_len][name]` | join 成功回执 |
| 0x82 | S2C | `[room_len][room][from_len][from][msg]` | 广播（**含发送者**，客户端逻辑统一） |
| 0x83 | S2C | `[room_len][room][name_len][name]` | 成员离开通知 |
| 0x84 | S2C | `[room_len][room][cnt][name_len][name]*` | 成员列表 |
| 0x8F | S2C | `[code][reason_len][reason]` | 错误（未知房间/非法帧） |

约束：room/name 非空且 ≤255；收到未知 type / 非 JOIN 时无房间上下文 → 发 ERROR 或静默忽略（demo 选静默 + 日志）。

---

## 2. 线程模型与广播路径

```
client A(B)   ──帧──▶  A 所在 ioLoop 线程：TcpServer::setMessageCallback(payload)
                          │ ChatServer::onMessage(conn, payload)
                          │   解析 1B type → 查/建房间（registry mutex）
                          ▼
                     广播：lock 房间 → 逐个成员 weak_ptr->lock() 收集 shared_ptr
                            → unlock
                            → 对每个成员:
                              member->getLoop()->runInLoop([keep, frame]{
                                   server.sendMessage(member, frame);   // 本 loop 线程内编码+send
                              })
```

三条纪律（全部来自代码事实，见 §4）：
1. **绝不跨线程直接 send**：`TcpConnection::send` 非本 loop 线程时落入直写 fallback，
   与批末 flush 并发操作 `outputBuffer_` → 数据竞争。广播一律 `runInLoop` 到成员 loop 再发。
2. **保活捕获 shared_ptr**：functor 闭包捕获 `keep`（`stop`= conn 的 shared_ptr），
   确保成员在投递队列里排队期间不被析构。
3. **关闭清理走弱引用**：`handleClose` 的 closeCallback 槽已被 TcpServer 占用（移除 conns_），
   不可再加第二个槽；成员表用 `weak_ptr`，广播前 `lock()` 失败即修剪（惰性），
   显式 LEAVE 走同一修剪路径。连接释放后残余 weak 自动过期，不阻塞关闭、不悬垂。

---

## 3. 并发 / UAF 论证（广播正确性核心）

- **房间表**：`ChatServer` 持 `mutex + unordered_map<room_id, shared_ptr<ChatRoom>>`；
  `ChatRoom` 持 `mutex + vector<weak_ptr<TcpConnection>>`。成员列表所有读写都在房间锁内。
- **广播快照**：锁内 `weak->lock()` 收集活连接副本，锁外逐个投递 → 投递期不持锁（成员数无上限，不把锁当水桶）。
- **关闭竞态**：成员 B 关闭与 A 的广播并发。A 广播已在锁内把 B lock 到 shared_ptr
  → B 活到该 send functor 执行完；即使 B 已 `closed_`，`send()` 首行 `if(closed_||isremove) return` → 空操作。
  B 的 ioLoop 上 send 与该 loop 的 writeNow/flush 同线程串行 → 无跨线程触碰 outputBuffer_。
- **leave 与 join 竞态**：全部房间锁内串行化；同一连接的帧按连接 FIFO 到达（单连接单 loop），无乱序。
- **跨连接全局次序不保证**：两个成员同时发消息，全局 no-total-order 是诚实边界（单连接内次序严格 FIFO）。

---

## 4. 文件与 CMake

| 文件 | 内容 | 归属 |
|---|---|---|
| `include/chat/message_proto.hpp/.cpp` | type 常量 + join/msg/leave/list/broadcast 等编解码（小函数） | 新 `chatcore` 静态库（或并入 server，视 CMake 结构） |
| `include/chat/chat_server.hpp/.cpp` | `ChatServer`：房间 registry + `onMessage(TcpConnection*, std::string)` 分发 + `broadcast()` | 同上 |
| `src/chat_server.cpp` | demo 入口：`--port` `--threads`（默认 0=单 loop 可跑）；`setThreadNum(N)` + 挂 ChatServer | 可执行 |
| `src/chat_room_test.cpp` | 收敛测试：N 客户端（含跨 ≥2 worker）并发互发，逐连接核对计数 | 可执行 |

CMake：新增 `chatcore` 目标 + `chat_server` / `chat_room_test`；`regress.sh` 追加 chat_server 服务端 + 用例（滚动进现有框架）。

**不改核心**：`TcpServer` / `TcpConnection` / `EventLoop` / Codec 均不动（sendMessage 与 setMessageCallback 两个公开口已够）。

---

## 5. 测试矩阵与验收

| # | 场景 | 判定 |
|---|---|---|
| G1 | 2 客户 join 同房，A 发 3 条 | 双方各收 3 条 0x82，内容/顺序一致（含发送者 echo） |
| G2 | **跨 loop**：`--threads 2`（或 4），6 客户分配多个 worker，各发爆发 200 | 每连接收量 = 其余成员发送总数之和；无丢、无重 |
| G3 | 成员断线（close，无 LEAVE）后再广播 | 无崩溃、无 send-to-closed；其余成员收量正确 |
| G4 | 非法帧 / 未知 type / 空 room | 静默忽略或收 ERROR，服务端不崩 |
| G5 | LEAVE 后重建（同房号同昵称重进） | 成员表正确；广播只到当前成员 |
| R1 | 全量回归 `scripts/regress.sh --build` | 既有 Day1–9 用例全 PASS（新增 chat 用例） |

---

## 6. 实施记录（2026-10-29 已完成本轮全部代码）

1. ✅ `message_proto.{hpp,cpp}`：type 常量 + 9 种帧编解码 + 上限守卫（纯函数）
2. ✅ `chat_server.{hpp,cpp}`：`ChatRoom`（weak_ptr 成员 + 惰性修剪）+ `ChatServer`（registry +
   分发 + cross-loop 广播）
3. ✅ `src/chat_server.cpp`：demo 入口（--port/--threads）
4. ✅ `src/chat_room_test.cpp`：G1–G5 收敛断言（裸 socket harness + 读者线程）
5. ✅ CMake `chatcore` + 两个可执行目标；`regress.sh` 新增第 (6) 段（--threads 2 跨 loop）

**自查已修（6 处）**：① `ChatRoom::nameOf` 的 erase 迭代器回退 bug（begin() 处 UB）→ 改
erase-and-continue 模式；② `chat_room_test` 的 sendPayload 两次构造临时 `framed()` → 单局部；
③ 缺 `<utility>` 编译头；④ **G5 时序竞态**：`e.sendLeave()` 与后续广播跨连接/跨 loop 无全局次序，
LEAVE 可能晚于 MSG 导致 e 误收 → 加 **LIST 栅栏**（同连接 LEAVE→LIST 紧排，等 S2C_MEMBERS 回执
即证明 LEAVE 已处理），同名重建前 resetJoined() 确保等本次 JOINED；⑤ 编译期：`Logger`/`Level`
定义在 `namespace server`，`namespace chat` 内的 4 处调用需 `server::` 前缀（首编 4 ERROR）；
⑥ **G4 测试预期纠偏**：长度头 >65536 的帧被 LengthHeaderCodec `kMaxMsgLen` 拦截丢弃、不会进
chat 回调，无 ERROR 回执——改为断言「空 payload → S2C_ERROR + 服务端存活 + 超大帧无回显」。
全部理由见实现排查记录。
**待验证**：WSL 编译 + `chat_room_test` 全 G 过 + 旧用例不回归（regress.sh 第 1–5 段）。

---

## 7. 边界 / 风险

- 广播是"尽最大努力、单点有序"：聊天丢帧可接受（无可靠层）；每秒广播条数=成员数×消息率，无背压 → 长的 INPUT 帧由 LengthHeaderCodec 半包/粘包自理，超长 frame 由 codec 层上限截断（kMaxMsgLen=65536：`invalid message length` 丢弃、不回 ERROR，防非法长度撑爆 buffer）；只有**空 payload** 才会走到 chat 层并回 S2C_ERROR 回执（G4 实测契约）。
- 房间数无上限（内存随活跃房间增长）；demo 不做房间存活回收（空房保留，诚实边界）。
- 未引入 protobuf：client 端测试直接用 LengthHeaderCodec + 裸帧解析（复用 server 侧 `message_proto`）。