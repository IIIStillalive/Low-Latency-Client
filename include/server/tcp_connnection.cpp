#include "tcp_connnection.hpp"

#include <fcntl.h>
#include <unistd.h>
#include "net/log.hpp"
#include <thread>
#include "net/event_loop.hpp"  // EventLoop 完整定义：listenconnection() 里调 loop_->updatachannel

namespace server{

TcpConnection::TcpConnection(int fd, EventLoop* loop): sock_(fd), ch_(fd, loop), loop_(loop){}
int TcpConnection::set_nonblocking(){
    int fd = sock_.fd();
    int flags = ::fcntl(fd, F_GETFL, 0);
    if(flags == -1) return -1;
    if(::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) return -1;
    return 0;
}
int TcpConnection::fd(){
    return sock_.fd();
}
EventLoop* TcpConnection::getLoop() const{
    return loop_;
}

// onRead：用 inputBuffer_ 读，回显走 send（不再裸 write）
void TcpConnection::onRead(){
    if(closed_) return;  // 已关闭（可能同帧 HUP|IN 已走 handleClose）→ 不再 shared_from_this
    std::shared_ptr<TcpConnection> keep = shared_from_this();  // 保活：回调栈结束前不许被析构(UAF 免疫)
    int saveErrno = 0;
    // ET 边缘触发铁律：必须循环读到 EAGAIN 才停。
    // 否则一次 readv 未读尽的数据滞留内核缓冲，而 ET 下 epoll 不再报 EPOLLIN，
    // 对端 close() 时本端 close 一个仍有未读数据的 socket → 内核发 RST → 对端 ConnectionResetError。
    for(int i = 0; i < 64; ++i){  // 循环上限：防单次 EPOLLIN 疯狂读饿死事件循环
        ssize_t n = inputBuffer_.readFd(sock_.fd(), &saveErrno);
        if(n > 0){
            lastActive_ = nowMicros();                     // 刷新活跃时刻：读进数据 = 连接还活着
            if(messageCallback_) messageCallback_(this, &inputBuffer_);  // 交给解析层(Codec)剥帧
            // 继续读：可能还有剩余数据
        } else if(n == 0){
            handleClose();          // 对端 FIN
            return;                 // 已关闭，不再读
        } else {
            if(saveErrno == EAGAIN || saveErrno == EWOULDBLOCK) break;  // 读空，本轮正常结束
            handleError();          // 真错误（非 EAGAIN）
            return;
        }
    }
}

// send：批末写合并（Day8）。仅在「readys 分发中」攒批——IO 线程把本轮所有 send 攒进
// outputBuffer_，循环末尾统一 writeNow() 一次（每帧一次 write → 每轮一次，多帧流水线收益）。
// 批外（如 client_protocol_test：loop 从不 runloop，conn 为栈对象）保持旧「能直写就直写」
// 语义：既不引入 shared_from_this 的 bad_weak_ptr，也保证字节立刻落线。
void TcpConnection::send(const void* data, size_t n){
    if(ch_.isremove() || closed_ || n == 0) return;
    if(loop_->isInLoopThread() && loop_->inDispatch()){
        outputBuffer_.append(static_cast<const char*>(data), n);   // 攒批（不直写）
        scheduleBatchFlush();
        return;
    }
    // 批外 / 跨线程 fallback：保留旧「能直写就直写」语义（outputBuffer_ 非线程安全，
    // 此分支既服务 loop 不运行的测试，也防御非 loop 线程误用，不在此攒批）。
    const char* raw = static_cast<const char*>(data);
    size_t remaining = n;
    if(!ch_.isWriting() && outputBuffer_.readableBytes() == 0){  // 无积压才直写
        ssize_t nwrote = ::write(sock_.fd(), raw, n);
        if(nwrote >= 0){
            remaining = n - static_cast<size_t>(nwrote);
            if(remaining == 0) return;        // 一次发完，不需要缓冲
        } else if(errno == EPIPE || errno == ECONNRESET){
            return handleClose();             // 对端已关
        }
        // EAGAIN → remaining 保持 n，全进缓冲
    }
    if(remaining > 0){
        outputBuffer_.append(raw + (n - remaining), remaining);
        if(!ch_.isWriting()) ch_.enablewriting();  // 积压 → 挂 EPOLLOUT 补发
    }
}

void TcpConnection::scheduleBatchFlush(){
    if(batchFlushScheduled_) return;              // 本轮已注册：后续 send 只是再攒一段
    batchFlushScheduled_ = true;
    // 保活：flush 回调可能晚于 handleClose 到达，捕获 shared_ptr 让本对象活到 flush 执行完。
    // （TcpConnection 恒由 shared_ptr 管理：server 的 conns_ / client 的 conn_；
    //   栈对象只在批外直写路径出现，进不到这里。）
    std::shared_ptr<TcpConnection> keep = shared_from_this();
    loop_->runAtLoopEnd([this, keep]() mutable {
        batchFlushScheduled_ = false;
        if(closed_ || ch_.isremove()) return;     // 已关闭/已下树：残余输出作废
        writeNow();                               // 尽力把 outputBuffer_ 一次写空
        if(closed_ || ch_.isremove()) return;     // writeNow 内部出错路径会 handleClose，别再碰 ch_
        if(outputBuffer_.readableBytes() > 0 && !ch_.isWriting())
            ch_.enablewriting();                  // 没写完（内核缓冲满）→ 挂 EPOLLOUT 续补
        // keep 在 lambda 尾部析构：若期间别处已释放所有权，这里送走最后一份引用
    });
}

void TcpConnection::onError(){
    std::shared_ptr<TcpConnection> keep = shared_from_this();  // 保活
    Logger::instance().log(Level::ERROR, "write or read failed\n");
    handleClose();                              // 统一走关闭
}
void TcpConnection::writeNow(){  //把输出缓冲尽力发到内核缓冲满为止
    while(outputBuffer_.readableBytes() > 0){
        ssize_t n = ::write(sock_.fd(), outputBuffer_.peek(), outputBuffer_.readableBytes());
        if(n > 0){
            outputBuffer_.retrieve(static_cast<size_t>(n));
        }
        else if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)){
            break;                              // 内核缓冲满 → 保持可写，下轮补发
        }
        else{
            handleError();
            return;
        }
    }
    if(outputBuffer_.readableBytes() == 0)
        ch_.disablewriting();                   // 发空 → 关可写（同步到 epoll 才真停）
}
void TcpConnection::onWrite(){  // EPOLLOUT 可写事件回调
    std::shared_ptr<TcpConnection> keep = shared_from_this();  // 保活
    if(ch_.isremove()) return;
    writeNow();
}

bool TcpConnection::listenconnection(){
    if(set_nonblocking() < 0) return false;  //设置为非堵塞
    ch_.setreadback([this](){ this->onRead();});
    ch_.setcloseback([this](){ this->handleClose();});  // 对端挂断 → 关闭（INFO，不按错误）
    ch_.setwriteback([this](){ this->onWrite();});
    ch_.seterrorback([this](){ this->onError();});
    ch_.enablereading();   // 内部已调用 update() → updatachannel 上树，单点发生
    return true;
}

bool TcpConnection::connection(){

    return true;

}
void TcpConnection::setCloseCallback(std::function<void(TcpConnection*)> cb){
    closeCallback_ = cb;
}
void TcpConnection::setMessageCallback(MessageCallback cb){
    messageCallback_ = cb;
}
void TcpConnection::setConnectionTimeout(double seconds){
    std::shared_ptr<TcpConnection> keep = shared_from_this();  // 保活：启动定时器瞬间不允许被析构
    if(closed_ || ch_.isremove()) return;                      // 已关/已下树：不启动
    timeoutUs_ = static_cast<int64_t>(seconds * 1000000.0);
    lastActive_ = nowMicros();                                 // 从此刻起算空闲
    // 用「单次 runAfter + 自续」而非 runEvery：闭环停在当前 tick（单次执行完即删），
    // 正在执行的 tick 不在 set 里，handleClose 的 cancel 只需取消「尚未触发的下一次」，
    // 天然规避 repeat-timer 在回调内 cancel 失效导致的悬垂 this。（见 task_plan 边界）
    timeoutTimerId_ = loop_->runAfter(1.0, [this]{ checkIdle(); });
}
void TcpConnection::checkIdle(){
    if(closed_ || ch_.isremove()) return;   // 已关闭/已下树（cancel 前的残留触发）→ 忽略且不再续
    if(nowMicros() - lastActive_ >= timeoutUs_){
        timeoutTimerId_ = 0;
        handleClose();                      // 超时无活跃 → 走既有关闭链（所有权交还 main 释放）
    } else {
        timeoutTimerId_ = loop_->runAfter(1.0, [this]{ checkIdle(); });  // 未超时 → 再等一秒
    }
}
void TcpConnection::close(){
    std::shared_ptr<TcpConnection> keep = shared_from_this();  // 保活：close 链可能触发 owner 释放最后一个引用
    handleClose();
}
void TcpConnection::handleClose(){
    if(closed_) return;                  // 防重复关闭（HUP 可能会触达多次）
    closed_ = true;
    if(timeoutTimerId_ != 0){            // 停掉尚未触发的下一次心跳（当前 tick 为单次，跑完即删）
        loop_->cancel(timeoutTimerId_);
        timeoutTimerId_ = 0;
    }
    Logger::instance().log(Level::INFO, "threadID: ", std::this_thread::get_id());
    Logger::instance().log(Level::INFO, "fd: ", sock_.fd(), " closed, cleaning");
    loop_->removechannel(&ch_);          // ① 下树：poller 不再引用 ch_，防悬空
    if(closeCallback_) closeCallback_(this);  // ② 通知所有者 main 把我释放；此后别再用 this
}
void TcpConnection::handleError(){
    if(closed_) return;                  // 防重复：事件可能多次触达，只处理一次（与 handleClose 一致）
    Logger::instance().log(Level::ERROR, "conn error, closing");
    handleClose();
}


}