#include "timer_queue.hpp"

#include <sys/timerfd.h>
#include <unistd.h>
#include <cerrno>
#include <sys/errno.h>
#include <algorithm>

#include "net/event_loop.hpp"   // EventLoop 完整定义：handleRead 里调 loop_->updatachannel 等
#include "net/log.hpp"
#include "net/errno.hpp"        // ErrnoGuard：错误描述

namespace server {

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timerfd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)),
      channel_(timerfd_.fd(), loop)
{
    if(!timerfd_.vaild()){
        Logger::instance().log(Level::ERROR, "timerfd_create() failed");
        return;
    }
    channel_.setreadback([this]{ handleRead(); });
    channel_.enablereading();   // 上树到 loop 的 poller；与 eventfd 唤醒环平行共存
}

TimerQueue::~TimerQueue(){
    for(auto* t : timers_) delete t;
}

TimerId TimerQueue::addTimer(TimerCallback cb, int64_t expiry, int64_t interval){
    auto* t = new Timer(std::move(cb), expiry, interval, ++nextSequence_);   // 所有权归 TimerQueue
    timers_.insert(t);
    // 只有比 timerfd 当前更早才需重设（earliestExpiry_==0 表示还没设定过）。
    // 再加一条：记录的到期时刻若已过去（到期但尚未被 handleRead 取走，timerfd 此时
    // 可能已被内核自动解除定时），也强制重设一次 —— 否则 earliestExpiry_ 会停在旧值，
    // 后续到期更晚的定时器全被这个判断跳过，该 loop 的定时功能就此永久失联。
    if(expiry < earliestExpiry_ || earliestExpiry_ <= nowMicros())
        resetTimerfd();
    return t->sequence();
}

void TimerQueue::cancel(TimerId id){
    // 遍历删除尚未到期的；回调内 cancel 正在执行的重复 timer 属边界，第一版不支持
    for(auto it = timers_.begin(); it != timers_.end(); ++it){
        if((*it)->sequence() == id){
            delete *it;
            timers_.erase(it);
            resetTimerfd();   // 若删的是最早，重算 timerfd 到期
            return;
        }
    }
}

void TimerQueue::handleRead(){
    uint64_t exp = 0;
    ssize_t n = ::read(timerfd_.fd(), &exp, sizeof(exp));   // 读掉计数，清可读标记
    // TFD_NONBLOCK + ET 下，边沿残留/重装竞态会让 read 返回 EAGAIN（计数已被消费，或
    // 本就没到新到期）。这是正常态，静默放行回调自然由下一次触发接手；真正读不出才报错。
    if(n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)){
        return;
    }
    if(n != static_cast<ssize_t>(sizeof(exp))){
        ErrnoGuard eg;
        Logger::instance().log(Level::ERROR, "timerfd read failed", eg.message());
        return;
    }
    int64_t now = nowMicros();
    std::vector<Timer*> expired;
    getExpired(expired, now);

    for(auto* t : expired){
        t->run();                       // 回调在 loop 线程；此处不碰 timers_
        if(t->repeat()){
            t->restart(now);            // 重算下次到期，插回有序集
            timers_.insert(t);
        } else {
            delete t;                   // 单次：释放
        }
    }
    resetTimerfd();                     // 设成下一个最早到期
}

void TimerQueue::getExpired(std::vector<Timer*>& expired, int64_t now){
    // 直白顺序取：从最早开始，把所有 expiry<=now 的移出到 expired（到期立刻处理）
    while(!timers_.empty() && (*timers_.begin())->expiry() <= now){
        auto it = timers_.begin();
        Timer* t = *it;
        timers_.erase(it);
        expired.push_back(t);
    }
}

void TimerQueue::resetTimerfd(){
    struct itimerspec its{};
    if(!timers_.empty()){
        int64_t earliest = (*timers_.begin())->expiry();
        int64_t now = nowMicros();
        int64_t diff = earliest - now;
        // timerfd 语义：it_value==0 表示**解除定时**，不是「立即到期」。
        // 到期时刻已过（diff<=0）时必须兜底成 1µs：一旦写入 0，这个 timerfd 就再也不会
        // 触发，而 earliestExpiry_ 仍留着旧值，后续定时器会被 addTimer 的判断永久跳过。
        if(diff <= 0) diff = 1;
        its.it_value.tv_sec  = diff / 1000000;
        its.it_value.tv_nsec = (diff % 1000000) * 1000;
        earliestExpiry_ = earliest;
    } else {
        its.it_value.tv_sec  = 0;   // 空：timerfd 设 0 → 永不过期
        its.it_value.tv_nsec = 0;
        earliestExpiry_ = 0;
    }
    if(::timerfd_settime(timerfd_.fd(), 0, &its, nullptr) < 0){
        Logger::instance().log(Level::ERROR, "timerfd_settime() failed");
    }
}

} // namespace server