#pragma once

// RpcFuture：一次 RPC 调用的结果句柄（Future + then）
//
// 线程模型：
//   - 结果状态由 shared State 内的 mutex 保护：响应(IO 线程)、超时(IO 线程)、
//     cancel(任意线程)、wait()(任意线程) 可以安全竞争。
//   - complete() 幂等：谁先到谁生效（响应 / 超时 / 取消三方竞态只认第一个），
//     后到的完成请求直接丢弃 —— 这是「超时与取消竞态」的统一收敛点。
//   - 续体 then() 一律投递到 biz loop 执行（cross-thread runInLoop），保证用户回调
//     不跑在 IO 线程；bizLoop 为空时退化为就地调用。
//   - then() 只支持登记一个续体（本项目的用法足够；需要多订阅可后续扩成 vector）。
//
// 生命周期约束：Future 可以比 RpcCodec 活得久，但**不得在 RpcCodec 析构后调用 cancel()**
// （cancelFn 捕获 codec 的 this）。正常收尾顺序：先 codec.stop()（完成所有 in-flight，
// 顺带清空 cancelFn），再析构 codec。

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

#include "client/rpc_types.hpp"

namespace server {

class EventLoop;
class RpcCodec;

class RpcFuture {
public:
    using Callback = std::function<void(const RpcResult&)>;

    RpcFuture() = default;

    bool valid() const { return static_cast<bool>(state_); }
    bool ready() const;

    // 登记续体：已完成 → 立即（经 biz loop）投递；未完成 → 登记，完成时投递。
    void then(Callback cb);

    // 阻塞取结果（测试/同步场景）；未完成则一直等。
    RpcResult wait();

    // 请求取消：未完成时切到 IO 线程移除 in-flight 并以 Cancelled 完成。
    // 已完成 / 已取消 → no-op。
    void cancel();

private:
    friend class RpcCodec;

    struct State {
        mutable std::mutex      m;
        std::condition_variable cv;
        bool                    ready    = false;
        RpcResult               result;
        Callback                cb;        // 至多一个续体
        EventLoop*              bizLoop  = nullptr;
        std::function<void()>   cancelFn;  // RpcCodec 注入；完成时清空
    };

    static RpcFuture make(EventLoop* bizLoop);          // [RpcCodec] 造一个未完成句柄
    void complete(RpcResult r);                         // 线程安全；幂等
    void setCancelFn(std::function<void()> fn);

    std::shared_ptr<State> state_;
};

} // namespace server
