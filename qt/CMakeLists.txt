# qt/CMakeLists.txt —— Qt6 Widgets v2 类飞书 IM 客户端（独立构建，不侵入顶层）。
#
# 独立构建（Windows 首发，用户环境 D:/QT/6.11.1/mingw_64）：
#   cmake -S qt -B build-qt -DCMAKE_PREFIX_PATH=D:/QT/6.11.1/mingw_64
#   cmake --build build-qt -j  -> build-qt\qt_chat.exe
# 或顶层构建：顶层 CMakeLists 开 option(BUILD_QT_CLIENT=ON) 后 add_subdirectory(qt)。
# 唯一必需：本机装有 Qt6（>=6.2），协议源复用 ../include/chat/message_proto.cpp +
# im_server 客户端共享头（sha256 纯头）。v1 三件（chat_session/chat_window）已删。
cmake_minimum_required(VERSION 3.16)
project(qt_chat LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)                     # Q_OBJECT 类（im_session/login_dialog/main_window）自动 moc

find_package(Qt6 REQUIRED COMPONENTS Widgets Network)

set(PROTO_CPP ${CMAKE_CURRENT_SOURCE_DIR}/../include/chat/message_proto.cpp)
set(CHAT_INC  ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# 图形客户端：LoginDialog → ImSession(QTcpSocket 切帧+v2编解码) → MainWindow(QQ 三栏)
add_executable(qt_chat
    src/main.cpp
    src/login_dialog.cpp
    src/im_session.cpp
    src/main_window.cpp
    src/theme.cpp
    src/avatar.cpp
    src/conv_row.cpp
    src/message_bubble.cpp
    src/add_friend_dialog.cpp
    ${PROTO_CPP}
)
target_include_directories(qt_chat PRIVATE src ${CHAT_INC})
target_link_libraries(qt_chat PRIVATE Qt6::Widgets Qt6::Network)

# 无 GUI 冒烟（QTcpSocket v2 链路，不进 regress；手动跑，见 README §手动清单 步骤 0）
add_executable(qt_smoke
    src/im_session.cpp
    smoke/smoke.cpp
    ${PROTO_CPP}
)
target_include_directories(qt_smoke PRIVATE src ${CHAT_INC})
target_link_libraries(qt_smoke PRIVATE Qt6::Network)