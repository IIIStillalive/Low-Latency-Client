# qt/ —— Qt6 v2 类飞书 IM 客户端

`qt_chat`：跨平台 Qt6 Widgets 图形客户端（登录 → 三栏会话 IM）+ 无 GUI 协议冒烟 `qt_smoke`。
作为本仓库「网络库 server + Qt 客户端 + kv 历史」v2 IM 全链路的交付端，连 WSL 里的 `im_server`（`../include/chat/message_proto.{hpp,cpp}` v2 部分 + `sha256.hpp` 同协议）。

- 状态机 / 三栏布局 / 断线重连续拉（seq 并集）/ 触顶上翻的设计要点见源码注释。
- 协议层复用 `../include/chat/message_proto.cpp`（纯 C++，无 Qt 依赖）+ `sha256.hpp`（纯头）。
- 只依赖 Qt6 **Widgets + Network**（无 protobuf、无 QML）。开发环境 Windows Qt 6.11.1 MinGW / Qt Creator。

```
qt_chat（Windows 首发）── 127.0.0.1:8891 ──▶ im_server（WSL，--kv-path）
```

---

## 构建

### Windows（Qt Creator / 命令行）

先确认 Qt6 安装目录（本机 `D:\QT\6.11.1\mingw_64`）与配套 MinGW：
本机 `cmake` / Ninja 不在系统 PATH，用 Qt 捆绑版（`D:\QT\Tools\CMake_64`、`D:\QT\Tools\Ninja`），
编译器必须用 Qt 捆绑的 `mingw1310_64`（与 Qt DLL 严格配套，勿混用其他 MinGW）。

PowerShell（一次性，全路径，不依赖当前目录）：

```powershell
$env:PATH = "D:\QT\Tools\CMake_64\bin;D:\QT\Tools\mingw1310_64\bin;D:\QT\6.11.1\mingw_64\bin;$env:PATH"
cmake -S F:\lowlat\qt -B F:\lowlat\build-qt -G Ninja -DCMAKE_BUILD_TYPE=Debug `
  -DCMAKE_MAKE_PROGRAM=D:\QT\Tools\Ninja\ninja.exe -DCMAKE_PREFIX_PATH=D:/QT/6.11.1/mingw_64 `
  -DCMAKE_C_COMPILER=D:/QT/Tools/mingw1310_64/bin/gcc.exe `
  -DCMAKE_CXX_COMPILER=D:/QT/Tools/mingw1310_64/bin/g++.exe
cmake --build F:\lowlat\build-qt
# → F:\lowlat\build-qt\qt_chat.exe、qt_smoke.exe
# 运行也必须带 PATH（上面的 $env:PATH 已含 Qt DLL + MinGW 运行时）：
F:\lowlat\build-qt\qt_smoke.exe --host 127.0.0.1 --port 8891
```

- 换用 `mingw32-make`（`-G "MinGW Makefiles"`）也行，二者任选；Ninja 更快。
- 全新的 build 目录：同目录换 generator 会报错，删掉重配即可。
- Qt Creator 打开 `F:\lowlat\qt\CMakeLists.txt` 后，选一个已配置的 Qt6 kit 直接构建即可（代码不依赖具体编译器：MSVC / MinGW 均可）。中文路径坑记录：项目在 `F:\lowlat`（无中文），Qt 自动处理 UTF-8 源文件。

### Linux（WSL，可选，同代码）

```bash
sudo apt install -y qt6-base-dev
cmake -S qt -B build-qt -DCMAKE_BUILD_TYPE=Release
cmake --build build-qt -j
# → ./build-qt/qt_chat、./build-qt/qt_smoke
```

### 顶层 CMake 集成（默认 OFF，不影响 WSL 回归）

顶层 `CMakeLists.txt` 提供 `option(BUILD_QT_CLIENT ...)`，需 `-DBUILD_QT_CLIENT=ON` 才把 qt/ 编进顶层构建；不开则 WSL `scripts/regress.sh` 完全不受影响。

---

## 前置：起服务端

v2 客户端必须连本仓库的 `im_server`，且**必须 `--kv-path`**（开会话/成员/历史/未读/在线全链路都依赖 kv）。WSL：

```bash
cd /mnt/f/lowlat                            # 或你的挂载路径
cmake -S . -B build && cmake --build build -j
./build/im_server --port 8891 --threads 2 --kv-path /tmp/im_qt.wal
```

- Windows 客户端连 WSL：`127.0.0.1:8891`（WSL2 自动转发 localhost）。
- 端口被占会显式报错退出（residual server 守卫），用 `pkill -f 'build/im_server'` 清理或换个端口。

---

## 手动验证清单（guideline，无自动断言）

> `qt_smoke`（步骤 0）是唯一可自动判定的；GUI 步骤按描述人工核对。

### 步骤 0：协议冒烟（可选，无 GUI）

```bat
set PATH=D:\QT\6.11.1\mingw_64\bin;%PATH%
build-qt\qt_smoke.exe --host 127.0.0.1 --port 8891
```

期望链式 PASS：`auth` → 建 DM → 发 3 条 → `convList unread=3` → 拉最新页 3 条 seq0..2 → 上翻 before=1 仅 seq0 → `convList unread=0`（0x17 已读落地）。终点 `ALL PASS`、退出码 0。
失败即退出码非 0：断线 / 超时（watchdog 15s）/ 服务端没开 `--kv-path`（收 S2C_ERROR）。

### 步骤 1：A ↔ B 对上话（登录 → 建 DM → 角标）

1. 起 WSL `im_server`（见上），`--kv-path` 必须给。
2. Windows 开一个 `qt_chat.exe`：登录框填 uid `alice`、口令（>0 位即可）、昵称 `艾丽斯` → [注册]（新账号）。
3. 再开第二个 `qt_chat.exe`：uid `bob`、昵称 `鲍勃` → [注册]。
4. A 点 [新建单聊] → 填 `bob` → 自动打开该会话；**B 的会话列实时出现该 DM**（服务端 0x9B 变更推送 → B 客户端自动 0x93 刷新，无需断连重连；B 此刻未打开该会话）。
5. A 发消息。判定：B 会话列 `bob`...（A 侧会话名 = DM 对方昵称 `鲍勃`）行出现 `(1)` 角标且加粗，打开后角标清零；气泡带 `[时:分] 昵称: 正文`，双方即时。

### 步骤 2：多人群聊 + 成员在线态

1. A 点 [新建群聊] → 群名 `三人帮`、成员 `bob,carol`（carol 也需先注册）。
2. 判定：三人都能在会话列看到群，打开后右栏成员全列出；carol 上下线时 A 的右栏成员点 绿/灰 实时切换（0x99 快照 + 0x9A 推送）。
3. 群内发消息 → 全员即时可见，未打开者角标 +1。
4. 固定群 id 入群：把当前群会话列里的 `grp:N`（如 `grp:3`）告诉未入群的第三人 `dave` → dave 点 [加入群聊] → 输入该 id（只填数字会自动补 `grp:`）→ 自动进入并打开会话。
   判定：dave 会话列出现该群且右栏可见；A/B 的会话列**不动任何操作**成员变化即刷新（入群者收 0x92 自动打开；已有成员收 0x9B → 自动 0x93），右栏成员即时多出 dave。dave 重复 [加入群聊] 同 id → 幂等进入不报错；乱填不存在的 `grp:999999` → 红字错误。

### 步骤 3：上翻历史（>200 条触顶加载）

前置：
- WSL `im_server` 起着且带 `--kv-path`（见「前置」小节），端口 8891。
- 目标会话存在且你（灌消息账号）是成员——群聊 id 在 GUI 会话列看 `群名 (grp:N)`；单聊给 `--peer` 让脚本自动建 DM。

灌消息（用仓库里现成脚本 `scripts/im_fill.py`，仅需 Python 3 标准库，Windows/WSL 均可）：

```bash
# A 给 bob 发 300 条：自动 注册 alice → 登录 → 建 DM → 逐条 0x16
python scripts/im_fill.py --uid alice --pwd x --peer bob --count 300

# 指定会话（群聊；须已是成员）：
python scripts/im_fill.py --uid alice --pwd x --conv grp:3 --count 300

# 非默认地址/端口：
python scripts/im_fill.py --host 127.0.0.1 --port 8891 --uid alice --peer bob --count 300
```

脚本行为：建连 → 注册（uid 已存在会忽略 user-exists 错误）→ 登录（等 0x90）→ 拿 cid → 逐条发 `fill-N`（默认每条间隔 10ms 防刷爆）→ 打印结果。会话不存在/非成员会收 S2C_ERROR 并退出。
完成后按下面验：新开客户端登录 → 点开会话 → 首屏应 200 条（服务端 clamp）→ 滚到顶触发上翻（一次 200）。判定：顶部 prepend 出更旧一页，**首屏第 1 条与翻出页尾部不重复、不丢号**（seq 游标精确）；反复触顶可翻到最早（首条 seq0 / 不足一页 / 空批 → 置顶灰字「—— 没有更早的消息 ——」）。重复/跳号 = 失败。

### 步骤 4：退群 / 断线重连 + 续拉

1. 群成员 B 右键群会话 → [退群]。判定：B 会话列该群消失（0x18 后刷新 0x93）；若 B 正开着该群则退回列表。A 打开该群成员列不再有 B（服务端卸载成员）。
2. A 正在会话中，Ctrl-C 停 WSL `im_server`。判定：状态栏红字「已断开：… —— [重连]恢复」，输入框禁用，消息区保留（honest 不自动清）。
3. 起新 server（同 `/tmp/im_qt.wal`，历史仍在）→ A 点 [重连]。判定：登录成功 → 会话列整列恢复 → 自动续开原会话，断线期间 B/C 发的消息**按 seq 并集补入**（零重复零漏）；继续收发正常。

### 步骤 5：退出登录 / 错误提示

- [退出登录] → 断连，按钮变 [重连]；再次 [重连] 复用原账号凭据登录（0x11）。
- 登录不存在账号（点 [登录] 而非 [注册]）→ 红字「认证失败…」；重复注册已存在账号 → 幂等注册放行，[注册] 后仍以该账号正常进。

---

## 交付边界 / 已知取舍

- **续拉范围 = 最新页**：断线期间消息量大时按 count=200 封顶，页外增量通过继续触顶上翻补齐。
- **打开即整批已读**：0x17 报「视图最大 seq」，不做逐条已读明细（协议支持也够，v2 不做）。
- **DM 无退群**：服务端 0x18 对 dm 拒；UI 隐藏退群入口（右键菜单仅群显示）。
- **认证 demo 级**：SHA-256 无盐无迭代、无 TLS；登录框与 README 诚实标注。
- **多设备同 uid**：服务端旧连接被新认证替换；客户端不做多端同步。
- **会话变更 0x9B = 刷新信号**：服务端只推 conv_id（成员集变化：建 DM/入群/退群），客户端收后自做 0x93 全量权威拉取，不推会话行内容；收者只认「再拉一次」。
- **无离线队列**：断线期间客户端不积压发送。
- **qt_smoke 不进 regress**（需 GUI 环境 / 单独服务端；服务端侧语义已由 `im_test` F1–F13 覆盖）。