#include "client/tcp_client.hpp"

#include <cassert>
#include <cerrno>
#include <cstring>          // std::strerror
#include <fcntl.h>          // fcntl / O_NONBLOCK
#include <sys/socket.h>     // socket / connect / getsockopt
#include <unistd.h>         // ::close

#include "net/channel.hpp"        // Channel 完整定义：make_shared + 挂回调 + enablewriting
#include "net/errno.hpp"
#include "net/event_loop.hpp"     // EventLoop 完整定义：runInLoop / runAfter / removechannel
#include "net/log.hpp"
#include "server/tcp_connnection.hpp"  // TcpConnection 完整定义

namespace server {

namespace {   // 连接握手兜底：SYN 被黑洞 / 对端死机时不能无限挂起（否则 fd + Channel 永久泄漏）
constexpr double kHandshakeTimeout = 5.0;   // 秒
}

TcpClient::TcpClient(EventLoop* loop, const struct sockaddr_in& peer)
    : loop_(loop), peer_(peer) {}

TcpClient::~TcpClient(){
    // 契约：必须在 loop 线程析构。析构要拆连接，依赖同线程 runInLoop 内联执行；
    // 跨线程析构会向已死的 loop 排队 [this] 任务 → 悬垂 UAF。
    assert(loop_->isInLoopThread());
    disconnect();
}

void TcpClient::connect(){
    loop_->runInLoop([this]{ connectInLoop(); });   // 任意线程可调：跨线程则队列 + eventfd 唤醒转交
}

void TcpClient::disconnect(){
    if(!active_.load()) return;                     // 幂等：已下线
    active_ = false;                                // 唯一闸门：此后 connect()/自动重连全部变 no-op
    loop_->runInLoop([this]{
        // 取消 pending 重连 timer：紧急降温 —— 否则析构后到期触发 [this] = 悬垂
        if(reconnectTimerId_ != 0){
            loop_->cancel(reconnectTimerId_);
            reconnectTimerId_ = 0;
        }
        if(connecting_){
            teardownConnecting();                   // 内部 cancel handshakeTimerId_
            fail();                                 // 通知上层：握手被中止（active_=false → 不调度重连）
        }
        if(conn_){
            conn_->close();                         // 完整关闭链 → close 回调 → removeAndReset 再通知
        }
    });
}

bool TcpClient::isConnected() const{
    return conn_ != nullptr;
}

void TcpClient::setConnectionCallback(std::function<void(TcpConnection*)> cb){
    connectionCallback_ = std::move(cb);
}

void TcpClient::setMessageCallback(const MessageCallback& cb){
    codec_.setMessageCallback(cb);
}

void TcpClient::setConnectionTimeout(double seconds){
    connectionTimeout_ = seconds;
    if(conn_) conn_->setConnectionTimeout(seconds);  // 已连上：立即透传
}

void TcpClient::setAutoReconnect(bool enable, double initialBackoff, double maxBackoff){
    // 配置期同步赋值（loop 线程随用随读，同 setConnectionTimeout 语义）。
    // 关闭时已在途的重连 timer 不强行 cancel：到期后 onReconnectTick 见 autoReconnect_=false 作废，
    // 一次空转无害 —— 避免跨线程 cancel 竞态。
    autoReconnect_  = enable;
    initialBackoff_ = initialBackoff;
    maxBackoff_     = maxBackoff;
}

void TcpClient::setAttemptCallback(std::function<void(int)> cb){
    attemptCallback_ = std::move(cb);
}

// ==================== 以下全部在 loop 线程内串行执行 ====================

void TcpClient::connectInLoop(){
    if(!active_) return;              // disconnect 抢跑后的残留调用
    if(connecting_ || conn_) return;  // 幂等：已在握手 / 已连接
    // 注：上一握手通道的销毁已由 teardownConnecting 交给下一轮循环，这里无需（也不应）reset 成员

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0){
        ErrnoGuard eg;
        Logger::instance().log(Level::ERROR, "TcpClient socket() failed:", eg.message());
        return fail();
    }
    // 非阻塞（ET 收发前提）；同 Acceptor 的内联写法
    int flags = ::fcntl(fd, F_GETFL, 0);
    if(flags == -1 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1){
        ErrnoGuard eg;
        Logger::instance().log(Level::ERROR, "TcpClient fcntl(O_NONBLOCK) failed:", eg.message());
        ::close(fd);
        return fail();
    }

    int r = ::connect(fd, reinterpret_cast<const struct sockaddr*>(&peer_), sizeof(peer_));
    if(r == 0){
        return onConnected(fd);       // 本地回环几乎必然直达成功：直接建连接
    }
    if(errno != EINPROGRESS){
        ErrnoGuard eg;                 // connect 真错误：errno 即失败原因
        Logger::instance().log(Level::ERROR, "TcpClient connect() failed:", eg.message());
        ::close(fd);
        return fail();
    }

    // EINPROGRESS：等 EPOLLOUT（内核只报可写 = 握手有结果）；失败时内核同时报 ERR | HUP
    connSock_ = Socket(fd);
    connectingCh_ = std::make_shared<Channel>(fd, loop_);   // 上一握手通道已在 teardown 时交给下一轮循环回收
    connectingCh_->setwriteback([this]{ handleConnect(); });
    connectingCh_->seterrorback([this]{ handleConnect(); });
    connectingCh_->setcloseback([this]{ handleConnect(); });   // HUP-only（无 OUT）兜底：由 SO_ERROR 裁决
    connectingCh_->enablewriting();                            // EPOLLOUT|ET + update 上树，一步到位
    connecting_ = true;

    // 握手兜底超时：对端黑洞 / 死机时不再无限挂起，5s 后按失败收尾。
    // 句柄入成员：teardownConnecting/disconnect 必 cancel —— 否则客户在 Connecting 期间析构后
    // 此 timer 仍持有 [this]，到期触发 → 悬垂 UAF（Day1 遗留洞补修）。
    handshakeTimerId_ = loop_->runAfter(kHandshakeTimeout, [this]{
        if(!connecting_) return;      // 已成功 / 已失败 / 已主动拆 → 本次超时作废
        Logger::instance().log(Level::INFO, "TcpClient connect timeout");
        teardownConnecting();         // cancel 自身句柄（本 timer 正在执行，对当前轮无害）
        fail();                       // active_ 若已 false（disconnect 抢跑）→ fail 内不调度重连
    });
}

void TcpClient::handleConnect(){      // EPOLLOUT / EPOLLERR / EPOLLHUP 回调；loop 线程
    if(!active_ || !connecting_) return;   // 已拆 / 已裁决：重入（同批二次分发）一律忽略
    int err = 0;
    socklen_t len = sizeof(err);
    ::getsockopt(connSock_.fd(), SOL_SOCKET, SO_ERROR, &err, &len);

    if(err != 0){
        Logger::instance().log(Level::INFO, "TcpClient connect failed:", std::strerror(err));
        teardownConnecting();
        return fail();
    }

    // 成功：fd 所有权移交要在摘握手通道之前（release 后 connSock_ 失效，teardown 的 close 是 no-op）
    int fd = connSock_.release();
    teardownConnecting();                     // 摘 epoll + connecting_=false
    onConnected(fd);
}

void TcpClient::onConnected(int fd){
    conn_ = std::make_shared<TcpConnection>(fd, loop_);
    conn_->setCloseCallback([this](TcpConnection*){ this->removeAndReset(); });
    conn_->setMessageCallback([this](TcpConnection* c, Buffer* b){ codec_.onMessage(c, b); });

    if(!conn_->listenconnection()){
        Logger::instance().log(Level::ERROR, "TcpClient connection register failed");
        conn_.reset();                          // fd 已托管在 conn_ 的 Socket：析构自动 close
        return fail();
    }
    // 上树成功后才启动空闲超时：心跳定时器不能挂在未注册的连接上（失败路径会悬垂）
    if(connectionTimeout_ > 0) conn_->setConnectionTimeout(connectionTimeout_);

    retryCount_ = 0;   // 重连成功：退避计数清零（下一轮从 initial 重新开始退避）
    // 防御：撤掉可能的幽灵重连 timer（业务在失败回调里手动 connect() 抢跑时仍有一条在途）
    // —— 连接已建立，无需稍后再空转一次 onReconnectTick，也避免其在窗口期压制下次失败的重连调度。
    if(reconnectTimerId_ != 0){
        loop_->cancel(reconnectTimerId_);
        reconnectTimerId_ = 0;
    }

    Logger::instance().log(Level::INFO, "TcpClient connected fd=", fd);
    std::shared_ptr<TcpConnection> keep = conn_;   // keep 跨用户回调：回调内 disconnect() 也不让 conn 中途析构（c 不悬垂）
    if(connectionCallback_) connectionCallback_(conn_.get());
    (void)keep;
}

void TcpClient::teardownConnecting(){     // 拆握手通道；对象延迟到下一轮事件循环回收
    if(handshakeTimerId_ != 0){           // 摘掉握手兜底 timer：覆盖成功/失败/主动拆三条路径
        loop_->cancel(handshakeTimerId_);
        handshakeTimerId_ = 0;
    }
    if(connectingCh_){
        loop_->removechannel(connectingCh_.get());   // 幂等：已摘除则返回 false（ENOENT 忽略）
        // 不能就地 reset() 销毁：本对象可能仍在本批 readys 中待分发（含自己 handleback 帧尚未走完的
        // ERR/HUP 分支，以及跨线程 disconnect 同批竞态），批内销毁 → 后续 handleback 用已释放内存 = UAF。
        // 延迟回收：runAfter 单次定时器持活一个 tick，下一轮循环才真正销毁 —— 远离一切在途分发帧。
        auto dead = connectingCh_;
        connectingCh_.reset();                        // 摘走成员引用：状态即可判定「已拆」
        loop_->runAfter(0.001, [dead](){});           // 下轮循环触点释放（fd 已 close，纯对象回收）
    }
    connSock_.close();                    // fd 已 release 时是 no-op
    connecting_ = false;
}

void TcpClient::removeAndReset(){     // 活动连接 close 回调；loop 线程
    std::shared_ptr<TcpConnection> keep = std::move(conn_);   // 移出：本回调栈结束才析构
    (void)keep;
    Logger::instance().log(Level::INFO, "TcpClient disconnected");
    scheduleReconnect();              // 先调度再通知：上层在回调里 disconnect() 时 cancel 已生效
    if(connectionCallback_) connectionCallback_(nullptr);
    // conn_ 保持 nullptr；自动重连走 scheduleReconnect → onReconnectTick → connectInLoop
}

void TcpClient::fail(){               // 连接失败 / 中止（断连回调拿 nullptr）
    scheduleReconnect();              // 先调度再通知：上层在回调里 disconnect() 时 cancel 已生效
    if(connectionCallback_) connectionCallback_(nullptr);
}

void TcpClient::scheduleReconnect(){     // [loop 线程] 任一失败收尾：按退避调度一次自动重连
    if(!autoReconnect_ || !active_.load() || reconnectTimerId_ != 0){
        return;                          // 未开启重连 / 已主动下线 / 已有一条在途
    }
    double delay = initialBackoff_;      // 退避：initial * 2^retryCount，达到 max 即封顶
    for(int i = 0; i < retryCount_ && delay < maxBackoff_; ++i) delay *= 2.0;
    if(delay > maxBackoff_) delay = maxBackoff_;
    ++retryCount_;
    if(attemptCallback_) attemptCallback_(retryCount_);        // 第 n 次尝试（n 从 1 起）
    Logger::instance().log(Level::INFO, "TcpClient reconnect #", retryCount_, " in ", delay, "s");
    reconnectTimerId_ = loop_->runAfter(delay, [this]{ onReconnectTick(); });
}

void TcpClient::onReconnectTick(){     // [loop 线程] 重连 timer 到期
    reconnectTimerId_ = 0;             // 已触发：句柄失效，下次失败可再调度
    if(!active_.load() || !autoReconnect_) return;   // disconnect 抢跑 / 重连被中途关闭 → 作废
    Logger::instance().log(Level::INFO, "TcpClient reconnect tick");
    connectInLoop();                   // 复用完整握手链（幂等 guard + 5s 兜底超时）
}

} // namespace server