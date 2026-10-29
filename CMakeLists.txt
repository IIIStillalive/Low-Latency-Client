cmake_minimum_required(VERSION 3.10)
project(lowlatency_server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ============================================================================
# netcore：net 层 + 连接层静态库（Day9 目标整理）
#
#   server / server_ack / rpc_server 与全部 client_*/rpc_* 目标此前各自把这 10 个
#   .cpp 抄一遍（改一处、漏八处的坑）。抽成静态库后，目标只需 link netcore +
#   各自私有的业务源。链接语义不变：静态库照旧打进每个可执行文件。
# ============================================================================
add_library(netcore STATIC
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/tcp_connnection.cpp
    include/server/eventloopthread.cpp
)
target_include_directories(netcore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(server
    src/main.cpp
    include/server/acceptor.cpp
    include/server/tcpserver.cpp
    include/server/thread_pool.cpp
)
target_link_libraries(server PRIVATE netcore)

# 独立定时器演示（不污染 server 的 echo 业务；只用 net 层）
add_executable(timer_demo
    src/timer_demo.cpp
)
target_link_libraries(timer_demo PRIVATE netcore)

# 客户端私有协议自测：协议层(include/client) + 传输链(Buffer/TcpConnection/Codec)
add_executable(client_protocol_test
    src/client_protocol_test.cpp
    include/client/protocol.cpp
)
target_link_libraries(client_protocol_test PRIVATE netcore)

# 端到端回显演示：TcpClient(transport) → LengthHeaderCodec → ProtocolCodec → 业务
add_executable(client_echo
    src/client_echo.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
)
target_link_libraries(client_echo PRIVATE netcore)

# Day3 可靠链路演示服务器：收 Data 回 Ack + 回显 body（--drop-ack N 故障注入）
add_executable(server_ack
    src/server_ack.cpp
    include/client/protocol.cpp
    include/server/acceptor.cpp
    include/server/tcpserver.cpp
    include/server/thread_pool.cpp
)
target_link_libraries(server_ack PRIVATE netcore)

# Day3 可靠层端到端：sendData(自动seq) → Ack 回收 → 去重回显 → PASS
add_executable(client_reliability_test
    src/client_reliability_test.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/client/reliable_codec.cpp
)
target_link_libraries(client_reliability_test PRIVATE netcore)

# Day4-A1 断线挂起重发端到端：配合 ./build/server_ack --drop-ack N --cut-after M
#   断线(server 存活) → pending 挂起 → 自动重连 → 全量重发 → seen 幂等只 Ack → pending 归零 PASS
add_executable(client_reconnect_test
    src/client_reconnect_test.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/client/reliable_codec.cpp
)
target_link_libraries(client_reconnect_test PRIVATE netcore)

# Day4-B 异步投递端到端：慢业务(sleep)不卡 IO + 背压 LOST + FIFO 保序 + 跨线程 sendData
#   配合 ./build/server_ack 普通模式；业务 loop 由 netcore 里的 eventloopthread.cpp 承载
add_executable(client_delivery_test
    src/client_delivery_test.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/client/reliable_codec.cpp
)
target_link_libraries(client_delivery_test PRIVATE netcore)

# ============================================================================
# Day10 聊天室业务（真实业务，闭合 README 唯一未勾项）
#
#   播放层 chatcore：帧编解码 + ChatServer（房间 registry + 跨 loop 广播）。
#   不依赖 protobuf → 放 Protobuf 块外，无 protobuf 也照常生成。
#   广播路径只走 TcpServer::setMessageCallback / sendMessage 两个公开口，核心零改动。
# ============================================================================
add_library(chatcore STATIC
    include/chat/message_proto.cpp
    include/chat/chat_server.cpp
)
target_include_directories(chatcore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(chatcore PUBLIC netcore)

add_executable(chat_server
    src/chat_server.cpp
    include/server/acceptor.cpp
    include/server/tcpserver.cpp
    include/server/thread_pool.cpp
)
target_link_libraries(chat_server PRIVATE chatcore)

add_executable(chat_room_test src/chat_room_test.cpp)
target_link_libraries(chat_room_test PRIVATE chatcore)

# ============================================================================
# Day5：异步 RPC 客户端（protobuf 序列化 + req_id 关联 + Future）
#
#   proto/rpc.proto ──protoc──▶ build/generated/rpc.pb.{cc,h}（生成物不进仓库）
#
# 找不到 protobuf 时不阻断既有目标：跳过全部 RPC 目标并给出警告。
# WSL 前置：sudo apt install -y protobuf-compiler libprotobuf-dev
# ============================================================================
find_package(Protobuf QUIET)
if(Protobuf_FOUND)
    set(RPC_PROTOC ${Protobuf_PROTOC_EXECUTABLE})
    if(NOT RPC_PROTOC)
        set(RPC_PROTOC ${PROTOBUF_PROTOC_EXECUTABLE})
    endif()

    set(RPC_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
    set(RPC_GEN_DIR   ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${RPC_GEN_DIR})

    add_custom_command(
        OUTPUT  ${RPC_GEN_DIR}/rpc.pb.cc ${RPC_GEN_DIR}/rpc.pb.h
        COMMAND ${RPC_PROTOC} --cpp_out=${RPC_GEN_DIR} -I${RPC_PROTO_DIR}
                ${RPC_PROTO_DIR}/rpc.proto
        DEPENDS ${RPC_PROTO_DIR}/rpc.proto
        COMMENT "protoc: rpc.proto -> rpc.pb.cc/.h"
        VERBATIM
    )
    add_library(rpc_proto STATIC ${RPC_GEN_DIR}/rpc.pb.cc)
    target_include_directories(rpc_proto PUBLIC ${RPC_GEN_DIR})
    target_link_libraries(rpc_proto PUBLIC protobuf::libprotobuf)

    # ---- Day9 目标整理 rpccore：客户端 RPC 传输链（协议/可靠/TCP/Future/关联/池）----
    #   ProtocolCodec → ReliableCodec → TcpClient → LengthHeaderCodec → TcpConnection 之上
    #   叠 RpcCodec(关联) + RpcFuture + RpcConnection + RpcChannelPool。5 个 RPC 客户端
    #   目标此前各自把这 7 个源抄一遍；抽成静态库后它们只剩「自己的测试源 + link rpccore」。
    add_library(rpccore STATIC
        include/client/rpc_connection.cpp
        include/client/rpc_codec.cpp
        include/client/rpc_future.cpp
        include/client/rpc_pool.cpp
        include/client/tcp_client.cpp
        include/client/protocol.cpp
        include/client/reliable_codec.cpp
    )
    target_include_directories(rpccore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(rpccore PUBLIC netcore rpc_proto)

    # 演示服务端：Request → Ack → 按 req_id 幂等执行 → Response（echo/sleep/fail/idem）
    #   服务端只剥 5B 头 + protobuf 解析，复用协议层 ProtocolCodec 但不需要客户端传输链。
    add_executable(rpc_server
        src/rpc_server.cpp
        include/client/protocol.cpp
        include/server/acceptor.cpp
        include/server/tcpserver.cpp
        include/server/thread_pool.cpp
    )
    target_link_libraries(rpc_server PRIVATE netcore rpc_proto)

    # Day5 端到端：单次调用 / 乱序响应 / 超时 / 业务错误 / 100 并发多路复用
    add_executable(rpc_client_test src/rpc_client_test.cpp)
    target_link_libraries(rpc_client_test PRIVATE rpccore)

    # Day6 连接池端到端：4 连接 × 每连接多路复用 / 策略切换 / shutdown 收尾时序
    add_executable(rpc_pool_test src/rpc_pool_test.cpp)
    target_link_libraries(rpc_pool_test PRIVATE rpccore)

    # Day6 负载测试：10k 流水线 / 背压 ResourceExhausted / cancel 后不再回调
    add_executable(rpc_pool_load_test src/rpc_pool_load_test.cpp)
    target_link_libraries(rpc_pool_load_test PRIVATE rpccore)

    # Day7 断线端到端幂等：cut/cutreply/drop-ack 故障注入 + 池内改道 + in-flight 恢复
    add_executable(rpc_reconnect_rpc_test src/rpc_reconnect_rpc_test.cpp)
    target_link_libraries(rpc_reconnect_rpc_test PRIVATE rpccore)

    # Day8 压测基准：N 线程 × M 请求 → QPS + p50/p99/p999；--conns 1|4 对比单loop vs multi-reactor
    add_executable(rpc_bench src/rpc_bench.cpp)
    target_link_libraries(rpc_bench PRIVATE rpccore)
else()
    message(WARNING "protobuf not found -> rpc_server / rpc_client_test / rpc_pool_test /"
                    " rpc_pool_load_test / rpc_reconnect_rpc_test / rpc_bench 未生成"
                    "（sudo apt install -y protobuf-compiler libprotobuf-dev）")
endif()