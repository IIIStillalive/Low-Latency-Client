#include "client/rpc_future.hpp"

#include "net/event_loop.hpp"   // runInLoop：续体投递到 biz loop

namespace server {

RpcFuture RpcFuture::make(EventLoop* bizLoop){
    RpcFuture f;
    f.state_ = std::make_shared<State>();
    f.state_->bizLoop = bizLoop;
    return f;
}

RpcFuture RpcFuture::makeFailed(RpcStatus st, std::string error){
    RpcFuture f;
    f.state_ = std::make_shared<State>();
    f.state_->ready  = true;                       // 直接以终态入世：complete()/cancel() 均为 no-op
    f.state_->result = RpcResult{st, "", std::move(error)};
    return f;
}

bool RpcFuture::ready() const{
    if(!state_) return false;
    std::lock_guard<std::mutex> lk(state_->m);
    return state_->ready;
}

void RpcFuture::complete(RpcResult r){
    if(!state_) return;
    Callback   cb;
    EventLoop* loop = nullptr;
    RpcResult  res;
    {
        std::lock_guard<std::mutex> lk(state_->m);
        if(state_->ready) return;                 // 幂等：响应/超时/取消三方竞态只认第一个
        state_->ready    = true;
        state_->result   = std::move(r);
        cb               = std::move(state_->cb);
        loop             = state_->bizLoop;
        res              = state_->result;
        state_->cancelFn = nullptr;               // 完成后禁止再触发取消路径
    }
    state_->cv.notify_all();                      // 唤醒 wait()
    if(!cb) return;
    if(loop != nullptr){
        // 续体一律回到 biz loop：用户回调不跑在 IO 线程（与 Day4-B 的业务线程纪律一致）
        loop->runInLoop([cb = std::move(cb), res = std::move(res)]{ cb(res); });
    } else {
        cb(res);                                  // 未配置 biz loop：退化为就地调用
    }
}

void RpcFuture::then(Callback cb){
    if(!state_ || !cb) return;
    EventLoop* loop = nullptr;
    RpcResult  res;
    {
        std::lock_guard<std::mutex> lk(state_->m);
        if(!state_->ready){                       // 未完成：登记，完成时投递
            state_->cb = std::move(cb);
            return;
        }
        loop = state_->bizLoop;
        res  = state_->result;
    }
    if(loop != nullptr){
        loop->runInLoop([cb = std::move(cb), res = std::move(res)]{ cb(res); });
    } else {
        cb(res);
    }
}

RpcResult RpcFuture::wait(){
    if(!state_) return RpcResult{RpcStatus::Unavailable, "", "invalid future"};
    std::unique_lock<std::mutex> lk(state_->m);
    state_->cv.wait(lk, [this]{ return state_->ready; });
    return state_->result;
}

void RpcFuture::cancel(){
    if(!state_) return;
    std::function<void()> fn;
    {
        std::lock_guard<std::mutex> lk(state_->m);
        if(state_->ready) return;                 // 已完成：取消无效（结果已定）
        fn = state_->cancelFn;
    }
    if(fn) fn();                                  // cancelFn 内部切到 IO 线程执行
}

void RpcFuture::setCancelFn(std::function<void()> fn){
    if(!state_) return;
    std::lock_guard<std::mutex> lk(state_->m);
    if(state_->ready) return;                     // 已完成的调用不再需要取消路径
    state_->cancelFn = std::move(fn);
}

} // namespace server
