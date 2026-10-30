# QQ 风格 IM 客户端 × 低延迟 IM 服务端

*Qt6 IM Client × Muduo-Style Low-Latency IM Server*

> 一个即时通讯系统：**Qt6 图形 IM 客户端**（Windows，无边框三栏、会话队列、气泡、未读角标、在线状态）与 **多 Reactor** 网络内核上的 IM 服务端（Linux / [WSL2]，kv 持久化）同仓实现，协议到界面走同一套 v2 协议。

**客户端 Target:** Windows · Qt6.11 (MinGW) · **服务端 Target:** Linux / [WSL2] (Ubuntu) · **Toolchain:** g++ 13 / MinGW g++ · CMake ≥ 3.16

---

## 客户端：Qt6 类 QQ IM

登录 → 三栏会话 IM：左功能条（56px）｜会话面板（280px：搜索 + 会话队列）｜聊天区 + 群成员栏（群聊专属）。

### 功能特性

- **无边框标题栏** — `nativeEvent` 处理 `WM_NCHITTEST`：四边/四角 6px 返回系统缩放、标题栏区 `HTCAPTION`（系统级拖动 + 双击最大化）、右上按钮区 `HTCLIENT` 保持可点；最小化/最大化/关闭在自绘栏内。
- **会话队列** — 每行自绘 `ConvRow`：头像 + 昵称 + 最后消息预览 + 时间 + 未读角标（超过 99 显示 `99+`）。服务端 `0x93` 携带 `lastTsMs` 作为排序锚，会话按时间**降序**排列；`0x96` 新消息到达实时把对应会话**顶到队首**。
- **添加好友 / 用户** — ✚ 菜单 → 输入 uid → `0x1B` 查找用户、`0x9C` 回执 → 结果卡（头像 / 昵称 / uid）+「发起聊天」打开 DM；也可不查找直接按 uid 发起。**添加/发起自己被拦截**：对话框输入登录用户自己时红字拒绝，服务端 `0x12` 同样权威拒绝并回 `0x8F` 错误。
- **气泡历史** — 自己/对方左右分栏、头像 + 昵称稳定取色（FNV-1a 哈希调色板，同一 uid 恒同色）；相邻消息间隔 >5 分钟自动插入居中时间线；滚动到顶触发按 seq 游标上翻补历史（200 条/批）。
- **群聊** — 新建群聊（成员列表写进登录用户自己会被拒绝）/ 按群 id 入群（`grp:N`，幂等）均从 ✚ 菜单发起；右键群会话退群（DM 无退群）。群打开时右侧成员栏显示「成员 N · 在线 M」，`0x99` 快照 + `0x9A` 推送实时切换在线圆点。
- **断线不丢** — 断线保留凭据并在状态栏明示 + [重连] 按钮；重连后 `0x93` 全量恢复会话列、自动续开原活动会话，断线期间消息按 **seq 并集** 补入历史（零重复零漏）。
- **未读管理** — 服务端维护 unread 计数 → 会话列角标；打开会话经 `0x17` 上报已读游标清零。

### 客户端 构建 & 运行（Windows）

前置：Qt6 ≥ 6.2（演示机 `D:\QT\6.11.1\mingw_64`，必须配 Qt 捆绑的 MinGW，勿混用其他 MinGW）。先按下方「服务端」一节把 `im_server` 起着（客户端连 `127.0.0.1:8891`，WSL2 自动转发 localhost），再执行：

```powershell
# 一次性配置 PATH（Qt 捆绑 cmake / mingw / Qt DLL）
$env:PATH = "D:\QT\Tools\CMake_64\bin;D:\QT\Tools\mingw1310_64\bin;D:\QT\6.11.1\mingw_64\bin;$env:PATH"
cmake -S qt -B build-qt -G Ninja -DCMAKE_BUILD_TYPE=Release `
  -DCMAKE_MAKE_PROGRAM=D:\QT\Tools\Ninja\ninja.exe -DCMAKE_PREFIX_PATH=D:/QT/6.11.1/mingw_64 `
  -DCMAKE_C_COMPILER=D:/QT/Tools/mingw1310_64/bin/gcc.exe `
  -DCMAKE_CXX_COMPILER=D:/QT/Tools/mingw1310_64/bin/g++.exe
cmake --build build-qt
D:\QT\6.11.1\mingw_64\bin\windeployqt.exe build-qt\qt_chat.exe   # 铺 Qt6 DLL，否则启动报缺 Qt6Core.dll
start build-qt\qt_chat.exe
```

> 编译产出 `qt_chat.exe`（客户端）与 `qt_smoke.exe`（无 GUI 协议冒烟）。完整手动验收清单（步骤 0–5：冒烟、A↔B 建 DM、多人群聊、上翻历史灌消息、退群/断线续拉）见 [qt/README.md](qt/README.md)。

### 客户端 源码布局

```
qt/
├── CMakeLists.txt            # 独立构建（Windows 首发；也可 -DBUILD_QT_CLIENT=ON 并入顶层）
├── README.md                 # 构建 + 手动验证清单（步骤 0-5）
└── src/
    ├── login_dialog.*        # 注册 / 登录（SHA-256，demo 级无 TLS）
    ├── im_session.*          # QTcpSocket 切帧 + v2 编解码 + 断线重连 / seq 去重状态机
    ├── main_window.*         # 无边框标题栏 + 三栏布局 + 会话队列 + 气泡 + 成员面板
    ├── conv_row.*            # 会话行自绘（头像/预览/时间/未读角标）
    ├── message_bubble.*      # 消息气泡自绘（左右分栏 + 时间线）
    ├── add_friend_dialog.*   # 添加好友 / 用户（0x1B → 0x9C → 发单聊）
    ├── avatar.*  theme.*     # 稳定取色 / v3 调色板 QSS
    └── smoke/smoke.cpp       # 无 GUI 协议冒烟（qt_smoke）
```

---

## 服务端：低延迟 IM 服务端（放在最后介绍）

客户端连的是 **多 Reactor** 网络内核之上的 IM 服务，实现见 [include/im/im_server.cpp](include/im/im_server.cpp)：会话/未读/在线/历史语义走 kv 持久化，网络层为同仓内的多 Reactor 库。

### 一键起服务端（WSL）

```bash
cd /mnt/f/lowlat                                  # WSL；或你的挂载路径
cmake -S . -B build && cmake --build build -j
./build/im_server --port 8891 --threads 2 --kv-path /tmp/im_qt.wal
```

- `--kv-path` **必给**：会话/成员/历史/未读/在线全链路依赖 kv 持久化，重启后数据仍在；不给则相关接口回 `0x8F Unavailable`。
- 端口占用显式报错（残留守卫）；清理用 `pkill -x im_server`（勿用 `-f` 误伤）。

### v2 协议（应用层）

长度前缀帧 `[4B 大端长度][1B type][body]`，字符串 `[1B 长] ≤ 255`。

| C2S | 意义 | S2C | 意义 |
|---|---|---|---|
| `0x10` 注册 / `0x11` 登录 | SHA-256 凭据，uid 唯一；`0x10` 幂等 | `0x90` 认证成功 | uid+nick |
| `0x12` 建会话 | dm 幂等 / grp 单调号；**dm peer 与群邀请均拒绝=自己** | `0x92` 已建 / `0x9B` 变更推送 | 收者自做 0x93 刷新 |
| `0x13` 会话列 / `0x93` | 携带 lastMsg+lastTsMs；grp 预览服务端拼 `昵称: 消息` | `0x94`/`0x95` | 打开回执 / 历史批（seq 游标翻页） |
| `0x16` 发消息 / `0x96` | 广播 + seq 单调 + ts | `0x17` 已读游标 | 未读角标清零 |
| `0x18` 退群 / `0x19` 成员表 / `0x1A` 入群 | dm 退群/入群均拒 | `0x99` / `0x9A` | 成员表（含在线）/ 在线推送 |
| `0x1B` 查找用户 / `0x9C` | 添加好友前校验存在 | `0x8F` 错误 | code+reason |

### 多 Reactor 内核（网络层）

- **分层架构**：`EventLoop`（eventfd 跨线程唤醒 + runInLoop）→ `ThreadPool`（每线程一个 loop，`getNextLoop()` 轮询接池）→ `Acceptor` / `TcpConnection` → `EpollPoller` / `Channel` / `Socket` / `Buffer`（`readv` 减拷贝）→ `LengthHeaderCodec`（半包/粘包切帧）→ `TimerQueue`（心跳/超时）。
- **连接安全**：`TcpConnection` 继承 `enable_shared_from_this`，回调栈内局部共享指针保活防 UAF；连接容器 `conns_` 全部串行化在 main loop 线程消除 race；信号处理 async-signal-safe → 优雅停机连锁析构。
- **性能**：批末写合并 + `TimerQueue::cancel` O(1)；RPC 客户端压测峰值约 7 万 QPS。
- **RPC 客户端（历史归档）**：protobuf 线格式 + req_id 关联 + Future/超时/取消 + 连接池多路复用 + 断线重连端到端幂等（Day5–Day9）。

### 测试

```bash
./scripts/regress.sh --build        # WSL 全量回归（重建 + 起服务端 + 全部用例 + 汇总）
./build/im_test                     # IM 协议层自动用例：注册/登录、建 DM/群、会话列未读、
                                    #   历史翻页、已读上报、成员在线、变更推送、断线重连（F1–F14）
# 客户端无 GUI 冒烟（Windows，见「客户端」节产出）：
build-qt\qt_smoke.exe --host 127.0.0.1 --port 8891
```

---

## 路线图 / Roadmap

- [x] 多 Reactor 内核：Buffer / LengthHeaderCodec / shared_ptr 安全析构 / 优雅停机 / TimerQueue / 空闲心跳
- [x] RPC 客户端 + 连接池 + 断线重连端到端幂等 + 压测（Day5–9，归档）
- [x] v1 聊天室（Day10，`chat_server` + kv 历史）
- [x] **v2 IM** — 用户层多会话 / 未读 / 在线 / 已读游标；Qt 客户端断线重连续拉
- [x] **v3 类 QQ 体验** — 无边框标题栏、添加好友（禁自加）、会话队列实时置顶、气泡与成员面板
- [ ] 图片/文件传输、已读明细、多设备同步（候选，不在当前交付内）

---

## 目录结构 / Directory Layout

```
.
├── CMakeLists.txt
├── README.md
├── qt/                    # Qt6 图形客户端（见上）
├── include/
│   ├── net/  server/      # 多 Reactor 内核（EventLoop/ThreadPool/TcpConnection/…）
│   ├── chat/              # v1 聊天室 + 跨端协议 message_proto.{hpp,cpp} + sha256（纯头）
│   └── im/                # IM 服务端实现（im_server.{hpp,cpp}）
├── src/                   # 各可执行入口 + im_test / chat_room_test
├── scripts/
│   ├── regress.sh         # WSL 全量回归
│   └── im_fill.py         # 灌历史消息（触顶翻页测试前置）
└── proto/                 # RPC 线格式（protobuf，历史归档）
```

[WSL2]: https://learn.microsoft.com/en-us/windows/wsl/