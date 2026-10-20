cmake_minimum_required(VERSION 3.10)
project(lowlatency_server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(server
    src/main.cpp
    include/net/event_loop.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/length_header_codec.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/acceptor.cpp
    include/server/tcpserver.cpp
    include/server/eventloopthread.cpp
    include/server/thread_pool.cpp
    include/server/tcp_connnection.cpp
)

target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 独立定时器演示（不污染 server 的 echo 业务）；只用 net 层
add_executable(timer_demo
    src/timer_demo.cpp
    include/net/event_loop.cpp
    include/net/channel.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
)
target_include_directories(timer_demo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 客户端私有协议自测：协议层(include/client) + 传输链(Buffer/TcpConnection/Codec)
add_executable(client_protocol_test
    src/client_protocol_test.cpp
    include/client/protocol.cpp
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/tcp_connnection.cpp
)
target_include_directories(client_protocol_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 端到端回显演示：TcpClient(transport) → LengthHeaderCodec → ProtocolCodec → 业务
add_executable(client_echo
    src/client_echo.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/tcp_connnection.cpp
)
target_include_directories(client_echo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Day3 可靠链路演示服务器：收 Data 回 Ack + 回显 body（--drop-ack N 故障注入）
add_executable(server_ack
    src/server_ack.cpp
    include/client/protocol.cpp
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/acceptor.cpp
    include/server/tcpserver.cpp
    include/server/eventloopthread.cpp
    include/server/thread_pool.cpp
    include/server/tcp_connnection.cpp
)
target_include_directories(server_ack PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Day3 可靠层端到端：sendData(自动seq) → Ack 回收 → 去重回显 → PASS
add_executable(client_reliability_test
    src/client_reliability_test.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/client/reliable_codec.cpp
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/tcp_connnection.cpp
)
target_include_directories(client_reliability_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Day4-A1 断线挂起重发端到端：配合 ./build/server_ack --drop-ack N --cut-after M
#   断线(server 存活) → pending 挂起 → 自动重连 → 全量重发 → seen 幂等只 Ack → pending 归零 PASS
add_executable(client_reconnect_test
    src/client_reconnect_test.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/client/reliable_codec.cpp
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/tcp_connnection.cpp
)
target_include_directories(client_reconnect_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Day4-B 异步投递端到端：慢业务(sleep)不卡 IO + 背压 LOST + FIFO 保序 + 跨线程 sendData
#   配合 ./build/server_ack 普通模式；唯一需要 eventloopthread.cpp 的 client 目标（业务 loop 承载）
add_executable(client_delivery_test
    src/client_delivery_test.cpp
    include/client/tcp_client.cpp
    include/client/protocol.cpp
    include/client/reliable_codec.cpp
    include/net/length_header_codec.cpp
    include/net/channel.cpp
    include/net/buffer.cpp
    include/net/event_loop.cpp
    include/net/timer_queue.cpp
    include/net/socket.cpp
    include/net/log.cpp
    include/net/errno.cpp
    include/server/eventloopthread.cpp
    include/server/tcp_connnection.cpp
)
target_include_directories(client_delivery_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)