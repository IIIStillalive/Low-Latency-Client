# IM 客户端 SDK — 私有二进制协议设计（第一版）

状态：`设计定稿，待用户按此实现 client/protocol.cpp/.hpp`
存放：本仓库 `client/`（同仓库复用 server 资产）。本文件随代码演进更新。

---

## 1. 原则（先立规矩）

- **协议 = 分层不改现有 Codec。** 底层仍用 `LengthHeaderCodec`：`[4B 大端长度][payload]`。
  私有协议只在 **payload 内**再切一层。好处：server 既有代码零改动，客户端只需「解析 payload 结构」。
- **二进制、定死布局、不用文本/JSON**。利于解析快、无歧义、面试可讲字节序对齐。
- **发送方负责把结构体 packed 进 payload；接收方按同布局解包。** 校验不靠猜测，靠显式字段。

## 2. 消息帧总览（一个「消息」= 一帧 payload）

```
[4B 大端 length]   ← 现有 LengthHeaderCodec 已处理，不算作 payload 部分
payload := [1B type][4B seq(大端)][body...]

type           : 1 字节，消息种类（见 §3 枚举）
seq            : 4 字节大端无符号，消息号（去重/ACK 用；同帧内单调递增）
body           : 其余字节，type 相关负载（定到帧尾为止）
```

定死：head 固定 **5 字节**（1 type + 4 seq）。解析「帧头」只需前 5 字节，
body 即帧尾剩余部分。半包依旧靠底层 Codec 攒齐整帧再回调——客户端复用逻辑不变。

> Day4-P0 瘦身：原 head 为 9B（1 type + 4 seq + 4 bodyLen），其中 bodyLen 与底层 4B length
> 完全冗余（length ≡ 5 + bodyLen）而砍除；bodyLen 改为接收端由帧长推导。

## 3. type 枚举（第一版）

```
constexpr uint8_t MSG_HEARTBEAT = 0x01;   // body: 空。心跳探测（网络空闲保活）
constexpr uint8_t MSG_ACK      = 0x02;   // body: 空。收到某 seq 的确认（seq 字段指明确认的是哪条）
constexpr uint8_t MSG_DATA     = 0x03;   // body: 用户数据（IM 里的普通消息，先透传）
constexpr uint8_t MSG_QUIT     = 0x04;   // body: 空。客户端主动下线的告别帧（重连互斥逻辑用它）
```

预留：`MSG_CHAT_PRIVATE/CHAT_GROUP` 之类业务帧可后续加（第 4 层业务，不在本文件设计）。

## 4. 关键机制映射

| 机制 | 如何从协议上实现 |
|---|---|
| **粘包/半包** | 底层 Codec 已解决；payload 解析需保证「不足 5B 首帧」兜底（半包在 payload 内也攒够再判）|
| **心跳** | `MSG_HEARTBEAT`：客户端定期发，server 收到回 `MSG_ACK(seq=来的seq)`；或 server 主动发心跳证明自己活着。第一版选「客户端发请求、server 回 ACK」双向心跳 |
| **去重/重传** | 每条待确认消息分配 `seq`（本客户端单调递增）；发方缓存在「未 ACK 表」（用 kv-engine 的跳表/有序表实现，见回接计划）；收到 `MSG_ACK(seq)` 就从表删除；重传超时(如 1s)未 ACK → 重发同 seq |
| **连接安全退出** | 发 `MSG_QUIT` → 停重连 → 关闭。重连 or 退出必须互斥（否则断线重连与用户退出打架）|

## 5. seq / ACK 语义（面试必讲）

- seq 是「**发送方视角**的消息号」，不是全局唯一 id：每端各自计数，端到端只需「发方知道哪些没被对方确认」。
- ACK 的 seq 字段 = **被确认的那条消息的 seq**（不是本条 ACK 自己的号——ACK 自身无需再确认）。
- 与本仓库 kv-engine 的结合点：**未 ACK 滑动窗口**可以用跳表 `[lo, hi]` 范围查询直接实现——
  「重传所有 seq 在 窗口 内未确认的消息」就是一次跳表 range——两段故事天然咬合。

## 6. 边界 / 不做（防 scope 蔓延）

- 第一版不做加密/鉴权、消息分片（>1 帧拆传）、流式传输。全部标注「未来可扩展」。
- body 内容此时**透传不解析**：IM 业务层（聊天/转发）在 server 端另起业务帧，本协议只管「可靠传输骨架」。
- 不做 type 扩展表驱动解析（enum 硬编码够用，避免过早泛化）。

## 7. 交付清单（用户手写，我审）

- [ ] `client/protocol.hpp` + `client/protocol.cpp`：FrameHeader 打包/解包、type 枚举、msgBody 封装
- [ ] 单测/自测：打包→解包 roundtrip；非法 type 兜底；半包边界
- [ ] 接上 `LengthHeaderCodec` 回调，验证「一发一收」连 server 回显