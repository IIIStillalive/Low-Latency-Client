#!/usr/bin/env bash
# ============================================================================
# regress.sh —— Day1→Day9 全量回归（自 WSL 仓库根运行）
#
#   ./scripts/regress.sh [--build]
#
# 顺序：(1) 可选重编译 → (2) 单测（不需服务端） → (3) 传输/可靠链路
#        (4) RPC 客户端全链路 → (5) Day8 压测哨兵（小规模，不产生报告数据）。
#
# 判定：所有用例 exit 0 记 PASS，全过脚本 exit 0；任一失败 exit 1。
# 失败时保留服务端日志与用例输出于 build/regress_tmp/ 便于复盘；成功则清理。
#
# 服务端需求（脚本按需启动并自动收尸）：
#   server_ack —— 绑定 127.0.0.1:8888（hardcode）
#   rpc_server —— 8899（普通）+ 8898（--drop-ack 1，专供 T5）
#   chat_server —— 8890，--threads 2（多 worker：聊天广播跨 loop 扇出路径的端到端验证）
# ============================================================================
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1      # 仓库根

BIN=build
TMP=build/regress_tmp
mkdir -p "$TMP"

PASS=0
FAIL=0
declare -a FAILED=()

# run_test <名称> <超时秒> <命令...>
run_test() {
    local name=$1 timeout_s=$2; shift 2
    echo "==> [$name] starting (timeout ${timeout_s}s)"
    if timeout "$timeout_s" "$@" >"$TMP/$name.out" 2>&1; then
        PASS=$((PASS+1))
        echo "    [$name] PASS"
    else
        local rc=$?
        FAIL=$((FAIL+1))
        FAILED+=("$name")
        echo "    [$name] FAIL (rc=$rc) —— tail:"
        tail -n 15 "$TMP/$name.out" | sed 's/^/      | /'
    fi
}

# port_busy <port> —— TCP connect 探测该端口是否已有进程在监听
port_busy() {
    if (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; then
        return 0
    fi
    return 1
}

# start_srv <名称> <监听端口> <pidfile> <日志> <命令...>
#   后台拉起服务端，用 TCP connect 探测端口就绪（最多 3s）。
#   守卫（2026-10-29 残留进程教训）：启动前预检端口必须空闲——若被残留/孤儿服务端占着，
#   connect 探测会命中旧进程，新服务端 bind 失败静默退出，测试就"静默连错服务端"、结论全废。
#   预检不过 / 进程启动失败 / 超时未监听，一律显式报错 exit 1（trap 收尸已启服务端），不静默继续。
start_srv() {
    local name=$1 port=$2 pidf=$3 logf=$4; shift 4
    if port_busy "$port"; then
        echo "ERROR: 端口 $port 已被占用（[$name] 无法启动）—— 疑似残留/孤儿服务端。详情："
        ss -ltnp "sport = :$port" 2>/dev/null | tail -n +2 | sed 's/^/    /'
        echo "    清理后重跑，例如：pkill -f '$(basename "$1")'（或按上面 pid 逐个 kill）"
        exit 1
    fi
    "$@" >"$logf" 2>&1 &
    echo $! > "$pidf"
    local okay=0
    for _ in $(seq 1 30); do
        if port_busy "$port"; then okay=1; break; fi
        if ! kill -0 "$(cat "$pidf")" 2>/dev/null; then
            echo "ERROR: server [$name] 启动失败（进程已退出）—— 日志尾部："
            tail -n 20 "$logf" | sed 's/^/      | /'
            exit 1
        fi
        sleep 0.1
    done
    if [ "$okay" -ne 1 ]; then
        echo "ERROR: server [$name] 超过 3s 未监听 $port —— 日志尾部："
        tail -n 20 "$logf" | sed 's/^/      | /'
        exit 1
    fi
    sleep 0.3
    echo "    server [$name] port=$port pid=$(cat "$pidf")"
}

stop_srv() {
    local pidf=$1
    if [ -f "$pidf" ]; then
        kill "$(cat "$pidf")" 2>/dev/null
        wait "$(cat "$pidf")" 2>/dev/null || true
        rm -f "$pidf"
    fi
}

cleanup() {
    stop_srv "$TMP/srv_ack.pid"
    stop_srv "$TMP/srv_ack_cut.pid"
    stop_srv "$TMP/srv_rpc.pid"
    stop_srv "$TMP/srv_rpc_t5.pid"
    stop_srv "$TMP/srv_chat.pid"
}
trap cleanup EXIT

echo "=== regress: 仓库根 = $(pwd) ==="
echo

# ---- (1) 可选构建 ----
if [ "${1:-}" = "--build" ]; then
    echo "=== 重新配置 + 构建 ==="
    cmake -S . -B build || { echo "BUILD FAIL: cmake configure"; exit 1; }
    cmake --build build -j"$(nproc)" || { echo "BUILD FAIL: compile"; exit 1; }
fi

# 必需二进制（protobuf 缺失时 RPC 目标缺失，会在这里提前报错而不是跑到一半才 FAIL）
NEEDED="$BIN/client_protocol_test $BIN/server_ack $BIN/client_reliability_test \
        $BIN/client_delivery_test $BIN/client_reconnect_test \
        $BIN/rpc_server $BIN/rpc_client_test $BIN/rpc_pool_test \
        $BIN/rpc_pool_load_test $BIN/rpc_reconnect_rpc_test $BIN/rpc_bench \
        $BIN/chat_server $BIN/chat_room_test"
for b in $NEEDED; do
    if [ ! -x "$b" ]; then
        echo "ERROR: 缺少 $b —— 先跑 cmake -S . -B build && cmake --build build（RPC 目标需 protobuf）"
        exit 1
    fi
done

echo
echo "=== (2) 无服务端自测 ==="
run_test client_protocol_test 30 "$BIN/client_protocol_test"
[ -x "$BIN/timer_demo" ] && run_test timer_demo 30 "$BIN/timer_demo"

echo
echo "=== (3) 传输/可靠链路（server_ack @8888）==="
start_srv server_ack 8888 "$TMP/srv_ack.pid" "$TMP/srv_ack.log" "$BIN/server_ack"
run_test client_reliability_test 60 "$BIN/client_reliability_test"
run_test client_delivery_test    60 "$BIN/client_delivery_test"
stop_srv "$TMP/srv_ack.pid"

echo
echo "--- (3b) 断线重发（server_ack --drop-ack 2 --cut-after 3）---"
start_srv server_ack_cut 8888 "$TMP/srv_ack_cut.pid" "$TMP/srv_ack_cut.log" \
         "$BIN/server_ack" --drop-ack 2 --cut-after 3
run_test client_reconnect_test 60 "$BIN/client_reconnect_test"
stop_srv "$TMP/srv_ack_cut.pid"

echo
echo "=== (4) RPC 客户端全链路（rpc_server @8899）==="
start_srv rpc_server 8899 "$TMP/srv_rpc.pid" "$TMP/srv_rpc.log" "$BIN/rpc_server"
run_test rpc_client_test         120 "$BIN/rpc_client_test" --port 8899
run_test rpc_pool_test           120 "$BIN/rpc_pool_test" --port 8899
run_test rpc_pool_load_test      180 "$BIN/rpc_pool_load_test" --port 8899
run_test rpc_reconnect_rpc_test  180 "$BIN/rpc_reconnect_rpc_test" --port 8899

echo "--- (5) Day8 压测哨兵（小规模，不写报告）---"
run_test rpc_bench_smoke         120 "$BIN/rpc_bench" --port 8899 --conns 4 --threads 4 --reqs 1000 --mode pipeline
stop_srv "$TMP/srv_rpc.pid"

echo
echo "--- (4b) T5 drop-ack（独立 rpc_server @8898 + --drop-ack 1）---"
start_srv rpc_server_t5 8898 "$TMP/srv_rpc_t5.pid" "$TMP/srv_rpc_t5.log" \
         "$BIN/rpc_server" --port 8898 --drop-ack 1
run_test rpc_reconnect_t5        180 "$BIN/rpc_reconnect_rpc_test" --port 8898 --case T5
stop_srv "$TMP/srv_rpc_t5.pid"

echo
echo "=== (6) Day10 聊天室真实业务（chat_server @8890，--threads 2 跨 loop 广播）==="
start_srv chat_server 8890 "$TMP/srv_chat.pid" "$TMP/srv_chat.log" \
         "$BIN/chat_server" --port 8890 --threads 2
run_test chat_room_test 120 "$BIN/chat_room_test" --port 8890
stop_srv "$TMP/srv_chat.pid"

echo
echo "=== 汇总 ==="
echo "  PASS=$PASS FAIL=$FAIL"
if [ "$FAIL" -gt 0 ]; then
    printf '  失败: %s\n' "${FAILED[@]}"
    echo "  服务端/用例日志保留在 $TMP/"
    exit 1
fi
rm -rf "$TMP"
echo "  regress 全过"
exit 0