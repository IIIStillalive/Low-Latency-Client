#pragma once

// 有界线程安全 FIFO 队列（Day4-Part B 背压原语）
//
// 用途：IO 线程（生产者）↔ 业务线程（消费者）之间传递已解析帧。
//   容量上限 = 背压闸：满则逐出队头（最旧）→ 由 caller 决定 LOST 通知。
//   **绝不阻塞生产者**（IO 线程的硬底线）——满即丢旧，新元素必落队。
//
// 为什么不需要 condition_variable：
//   消费者是 EventLoopThread 承载的业务 loop（runloop 阻塞在 epoll），唤醒走其
//   runInLoop/eventfd，不 sleep 在 condvar 上 → 无消费方等待，故只留互斥量
//   （保证 push/empty/clear 的内存序与互斥）。若未来换「裸线程 sleep 等待」，
//   在此补 wait/notify 即得经典 condvar 版。刻意不做多余——按需再补。

#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace server {

template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t cap) : cap_(cap == 0 ? 1 : cap) {}

    // 入队：满则逐出队头（最旧）并返回之；未满返回 nullopt。新元素永远入队。
    BoundedQueue(const BoundedQueue&)            = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    std::optional<T> push(T item){
        std::optional<T> evicted;
        {
            std::lock_guard<std::mutex> lock(m_);
            if(q_.size() >= cap_){
                evicted = std::move(q_.front());
                q_.pop_front();
            }
            q_.push_back(std::move(item));
        }
        return evicted;
    }

    // 非阻塞取走全部现存元素（FIFO 序填 out）；返回取走条数。空返回 0。
    size_t tryPopAll(std::vector<T>& out){
        size_t n = 0;
        {
            std::lock_guard<std::mutex> lock(m_);
            n = q_.size();
            if(n > 0){
                out.reserve(out.size() + n);
                while(!q_.empty()){
                    out.push_back(std::move(q_.front()));
                    q_.pop_front();
                }
            }
        }
        return n;
    }

    bool     empty() const { std::lock_guard<std::mutex> lock(m_); return q_.empty(); }
    size_t   size()  const { std::lock_guard<std::mutex> lock(m_); return q_.size();  }

    // 清空（stop() 收尾 / 判断重新开放）。不通知——由 caller 决定是否逐个 LOST。
    void clear(){
        std::lock_guard<std::mutex> lock(m_);
        q_.clear();
    }

private:
    mutable std::mutex m_;
    std::deque<T>      q_;
    size_t             cap_;
};

} // namespace server