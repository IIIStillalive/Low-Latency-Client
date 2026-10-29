
#include <algorithm>   // std::min
#include <cstddef>     // std::ptrdiff_t

#include "event_loop.hpp"
#include "epollpoller.hpp"
#include "channel.hpp"
#include "timer_queue.hpp"
#include "log.hpp"
#include "errno.hpp"

namespace server{

EventLoop::EventLoop():poller_(std::make_unique<Epollpoller>()), timerQueue_(new TimerQueue(this)), thread_id_(std::this_thread::get_id()), eventsoc_(createEventfd()), eventch_(std::make_unique<Channel>(eventsoc_.fd(), this)){
    eventch_->setreadback([this]{ handlewake(); });
    eventch_->enablereading();
}

// 析构放到 .cpp：这里 Epollpoller/Poller 已是完整类型，unique_ptr 能正确走虚析构释放
EventLoop::~EventLoop(){
    quit();
}
void EventLoop::quit(){
    quit_ = true;
    if(!isInLoopThread()) wakeup();
}

TimerId EventLoop::runAfter(double delay, TimerCallback cb){
    int64_t expiry = nowMicros() + static_cast<int64_t>(delay * 1000000.0);
    return timerQueue_->addTimer(std::move(cb), expiry, 0);
}
TimerId EventLoop::runEvery(double interval, TimerCallback cb){
    int64_t iv = static_cast<int64_t>(interval * 1000000.0);
    return timerQueue_->addTimer(std::move(cb), nowMicros() + iv, iv);
}
void EventLoop::cancel(TimerId id){
    timerQueue_->cancel(id);
}
bool EventLoop::updatachannel(Channel* ch){
    return poller_->updatechannel(ch);
}
void EventLoop::removechannel(Channel* ch){
    poller_->removechannel(ch);
}
void EventLoop::runloop(){
    while(!quit_){
        readys.clear();          // 清空上一批：否则被摘除/销毁的 Channel* 残留批中，下一批 handleback 会成为 UAF
        poller_->poll(-1, readys);
        inDispatch_ = true;      // Day8 写合并：批内以「本轮末尾统一 flush」替代逐帧 write
        for(auto& it : readys){
            it->handleback();  //为就绪fd调用回调
        }
        inDispatch_ = false;
        //dopendingFunctors();  //额外执行一次,多余
        // Day8 批末写合并：readys 全部处理完、回到 epoll_wait 前，执行本批攒下的统一 flush。
        // TcpConnection::send 借此把一轮内的多次 send 合并成一次写（减少 syscall）。
        if(!loopEndFunctors_.empty()){
            std::vector<Callback> batch;
            batch.swap(loopEndFunctors_);
            for(auto& cb : batch) cb();
        }
    }
}

void EventLoop::runAtLoopEnd(Callback cb){
    if(isInLoopThread()){
        loopEndFunctors_.emplace_back(std::move(cb));
    } else {
        runInLoop([this, cb = std::move(cb)]() mutable { loopEndFunctors_.emplace_back(std::move(cb)); });
    }
}
int EventLoop::createEventfd(){
    int eventfd = ::eventfd(0,EFD_NONBLOCK | EFD_CLOEXEC);
    if(eventfd < 0){
        ErrnoGuard eg;
        Logger::instance().log(Level::ERROR, "createEventfd() failed ", eg.message());
        return -1;
    }
    return eventfd;
}
bool EventLoop::isInLoopThread() const{
    return (std::this_thread::get_id() == this->thread_id_);
}

bool EventLoop::wakeup(){
    uint64_t one = 1;
    size_t n = ::write(eventsoc_.fd(), &one, sizeof(one));
    if(n != sizeof(one)){
        ErrnoGuard eg;
        Logger::instance().log(Level::ERROR, "wakeup() failed", eg.message());
        return false;
    }
    return true;
}

void EventLoop::runInLoop(Callback cb){
    if(isInLoopThread()){
        cb();  //在当前线程就执行
        return;
    }
    {
        std::unique_lock<std::mutex> lock(mutex_);  //跨线程加锁
        pendingFunctors_.emplace_back(cb);  //加入任务队列,一般这些任务是调用poller进行注册
    }
    wakeup();  //唤醒目标线程
    
}

bool EventLoop::handlewake(){  
    //处理唤醒就和main中处理lfd的处理一致，它主要就是呼应write并对工作队列进行处理
    //就是这个子线程lfd的回调函数
    uint64_t one = 1;
    int n = ::read(eventsoc_.fd(), &one, sizeof(one));
    if(n != sizeof(one) && errno != EAGAIN){
        ErrnoGuard eg;
        Logger::instance().log(Level::ERROR, "handlewake() failed", eg.message());
        return false;
    }
    dopendingFunctors();  //处理任务队列，主要就是循环清空就可以了
    return true;
}

// 有界批 + 自我唤醒（Day8-b 修复）：eventfd 会把多线程聚合的 runInLoop 洪峰一次性交过来，
// 若这里全量 swap 跑完，本 loop 在单次 dispatch 内长时间只跑 functor、不读 socket ——
// 高在途多调用线程的流水线会秒级停摆（实测 p50≈wall、late>0=2s 超时重传）。改为每轮至多
// 取 kBatch 个；跑完若队列仍有积压，再写一次 eventfd 自我唤醒，回到 epoll_wait 与 socket
// I/O 交替服务。语义不变（任务最终全部执行），只约束"单次执行多少"。
void EventLoop::dopendingFunctors(){
    constexpr size_t kBatch = 1024;
    std::vector<Callback> functors;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        const size_t take = std::min(pendingFunctors_.size(), kBatch);
        if(take == 0) return;                              // 空队列：无活
        functors.assign(pendingFunctors_.begin(),
                        pendingFunctors_.begin() + static_cast<std::ptrdiff_t>(take));
        pendingFunctors_.erase(pendingFunctors_.begin(),
                               pendingFunctors_.begin() + static_cast<std::ptrdiff_t>(take));
    }
    for(auto& cb : functors){
        cb();  //进行任务队列的处理
    }
    {
        // 本批没拿完 / 执行期间又有新任务 → 自我唤醒，下一轮 epoll_wait 继续（与 socket I/O 交替）
        std::unique_lock<std::mutex> lock(mutex_);
        if(!pendingFunctors_.empty()) wakeup();
    }
}
}