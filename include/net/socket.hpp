#pragma once
#include <unistd.h>                  // ::close（仅用于声明需要；实现已移 socket.cpp）


namespace server{

class Socket{

public:
    explicit Socket(int fd = -1) noexcept;
    ~Socket() noexcept;

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept;              // 移动：窃取 fd，源置无效（所有权转移必需）
    Socket& operator=(Socket&& other) noexcept;

    int fd() const noexcept;
    bool vaild() const noexcept;
    void close() noexcept;
    int release() noexcept;                       // 移交 fd 所有权：返回 fd 并置自身无效（析构不再 close）

private:

    int fd_;
};

}