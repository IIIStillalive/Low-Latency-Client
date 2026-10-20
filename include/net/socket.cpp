#include "socket.hpp"

#include <unistd.h>   // ::close

namespace server{

Socket::Socket(int fd) noexcept: fd_(fd) {}
Socket::~Socket() noexcept{
    close();
}
int Socket::fd() const noexcept{
    return fd_;
}
bool Socket::vaild() const noexcept{
    return fd_ >= 0;
}
void Socket::close() noexcept{
    if(vaild()){
        ::close(fd_);
        fd_ = -1;   // 置无效：重复 close()/析构不会二次 close 同号（可能已被新 fd 复用）
    }
}
Socket::Socket(Socket&& other) noexcept: fd_(other.fd_){
    other.fd_ = -1;   // 源置无效：析构/close 不再触碰该 fd
}
Socket& Socket::operator=(Socket&& other) noexcept{
    if(this != &other){
        close();      // 先释放自己当前持有的 fd
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}
int Socket::release() noexcept{
    int fd = fd_;
    fd_ = -1;
    return fd;
}

}