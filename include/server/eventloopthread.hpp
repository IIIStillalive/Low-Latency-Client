#pragma once
#include <mutex>
#include <condition_variable>
#include <thread>
#include <string>
#include "net/event_loop.hpp"

namespace server {

class EventLoopThread {
public:
    EventLoopThread();
    ~EventLoopThread();

    // 启动线程，并在该线程中创建 EventLoop，最后返回该 EventLoop 的指针
    EventLoop* startLoop();
    void quit();

    // 仅等待线程自然退出，**不**主动置 quit。
    // 用途：让 loop 线程自己以 runAfter(...→ quit) 引导退出时，调用方只负责 join；
    //   直接 reset 本对象会先 quit() 提前置位，把尚未到期的定时器任务截断
    //   （RpcChannelPool 的连接析构就排在这样一条定时器里）。
    // 幂等：已 join 过则立即返回；建议只在创建本对象的线程调用。
    void join();

private:
    void threadFunc(); // 线程的入口函数

    EventLoop* loop_;                  // 指向子线程中创建的 EventLoop 对象
    bool exiting_;                     // 是否正在退出
    std::thread thread_;               // 封装的底层线程
    std::mutex mutex_;                 // 保护 loop_ 指针的互斥锁
    std::condition_variable cond_;     // 用于同步等待 loop 创建完成的条件变量
};

}