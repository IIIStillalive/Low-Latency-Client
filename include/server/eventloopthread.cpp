#include "eventloopthread.hpp"


// --- 实现部分 ---
namespace server{
EventLoopThread::EventLoopThread()
    : loop_(nullptr),
      exiting_(false) {}

EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    // 带锁读 loop_ 再 quit：子线程退出前会在锁内置 loop_=nullptr（threadFunc 收尾），
    // 因此「读到非空」= 子线程尚未走到销毁 loop 那一步 → quit() 一定打在仍存活的 loop 上；
    // 直接无锁读可能拿到 stale 指针，指向子线程栈上已析构的 EventLoop → 写已释放内存 = UAF。
    EventLoop* loop = nullptr;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        loop = loop_;
    }
    if (loop != nullptr) {
        loop->quit(); // 唤醒子线程停机（若已在退出则无害）；join 保证销毁完成
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}
void EventLoopThread::quit() {
    EventLoop* loop = nullptr;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        loop = loop_; // 获取子线程中创建的 EventLoop 指针
    }
    
    if (loop != nullptr) {
        loop->quit(); // 核心：调用 EventLoop 的 quit，将其退出标志置为 true，并 wakeup
    }
}
void EventLoopThread::join() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

EventLoop* EventLoopThread::startLoop() {
    // 启动底层线程，绑定成员函数 threadFunc
    thread_ = std::thread(&EventLoopThread::threadFunc, this);

    EventLoop* loop = nullptr;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        // 等待子线程把 EventLoop 创建出来并赋值给 loop_
        while (loop_ == nullptr) {
            cond_.wait(lock);
        }
        loop = loop_;
    }
    return loop;
}

void EventLoopThread::threadFunc() {
    EventLoop loop; // 【核心】在子线程自己的栈空间上创建 EventLoop！

    {
        std::unique_lock<std::mutex> lock(mutex_);
        loop_ = &loop;
        cond_.notify_one(); // 唤醒正在 startLoop 中等待的主线程
    }

    loop.runloop(); // 开启子线程的事件循环

    std::unique_lock<std::mutex> lock(mutex_);
    loop_ = nullptr;
}

}