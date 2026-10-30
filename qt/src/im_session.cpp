// im_session.cpp —— v2 IM 客户端网络层：QTcpSocket 切帧 + imEncode/imDecode → 纯信号。
// 全部 GUI 线程事件驱动；inBuf_ 增量切帧逻辑与 v1 ChatSession 同款（含坏帧踢出）。
#include "im_session.hpp"

#include <QByteArray>

#include "chat/sha256.hpp"

namespace chat {
namespace {

// 4B 大端帧长解码（与 LengthHeaderCodec 一致）
uint32_t frameLenFrom(const std::string& b){
    return (static_cast<uint32_t>(static_cast<uint8_t>(b[0])) << 24)
         | (static_cast<uint32_t>(static_cast<uint8_t>(b[1])) << 16)
         | (static_cast<uint32_t>(static_cast<uint8_t>(b[2])) <<  8)
         |  static_cast<uint32_t>(static_cast<uint8_t>(b[3]));
}
void appendFrameLen(std::string& b, uint32_t len){          // 4B 大端写
    b.push_back(static_cast<char>((len >> 24) & 0xff));
    b.push_back(static_cast<char>((len >> 16) & 0xff));
    b.push_back(static_cast<char>((len >>  8) & 0xff));
    b.push_back(static_cast<char>(len & 0xff));
}
QString qs(const std::string& s){ return QString::fromUtf8(s.c_str(), int(s.size())); }

} // namespace

ImSession::ImSession(QObject* parent) : QObject(parent){
    socket_ = new QTcpSocket(this);
    connect(socket_, &QTcpSocket::connected,    this, &ImSession::onConnected);
    connect(socket_, &QTcpSocket::readyRead,    this, &ImSession::onReadyRead);
    connect(socket_, &QTcpSocket::disconnected, this, [this]{
        // 主动 abort() 关旧连接也会触发本信号：用 intentionalClose_ 屏蔽。
        if(intentionalClose_){ intentionalClose_ = false; return; }
        authed_ = false;
        emit disconnected(QStringLiteral("连接已断开"));
    });
    connect(socket_, &QTcpSocket::errorOccurred, this, &ImSession::onSocketError);
}

void ImSession::start(const QString& host, quint16 port, bool registerMode,
                      const QString& uid, const QString& nick, const QString& pwd){
    host_ = host; port_ = port;
    uid_ = uid; nick_ = nick;
    doRegister_ = registerMode;
    pwd32_ = sha256(pwd.toStdString());        // 只留 32B 摘要，不存明文
    doConnect();
}

void ImSession::reconnect(){
    if(port_ == 0 || pwd32_.empty()) return;   // 从未 start 过：无凭据可重连
    doRegister_ = false;                       // 重连仅登录（0x11）
    doConnect();
}

void ImSession::doConnect(){
    authed_ = false;
    inBuf_.clear();
    // 断线重连：同一对象复用 —— abort() 旧连接（同步触发 AbortError/disconnected，全部屏蔽），
    // 再连新主机。abort 信号同步发 → connectToHost 前先置意向、abort 后一律清除。
    intentionalClose_ = true;
    socket_->abort();
    intentionalClose_ = false;
    socket_->connectToHost(host_, port_);
}

void ImSession::disconnectNow(){
    intentionalClose_ = true;
    socket_->abort();
    intentionalClose_ = false;
    authed_ = false;
}

void ImSession::onConnected(){
    intentionalClose_ = false;
    if(pwd32_.empty()) return;                 // 无凭据：只建立 TCP（smoke 可探 connect）
    if(doRegister_){
        const std::string reg = imEncodeRegister(uid_.toStdString(), nick_.toStdString(), pwd32_);
        if(!reg.empty()) writeFrame(reg);      // 注册（无回执），随即 0x11
    }
    sendAuth();
    emit connected();
}

void ImSession::sendAuth(){
    const std::string a = imEncodeAuth(uid_.toStdString(), pwd32_);
    if(a.empty()) return;
    writeFrame(a);
}

void ImSession::writeFrame(const std::string& payload){
    if(payload.empty()) return;                // 编码失败（字段超长）：丢弃，不写 0 长帧
    std::string frame;
    appendFrameLen(frame, static_cast<uint32_t>(payload.size()));
    frame.append(payload);
    socket_->write(frame.data(), static_cast<qint64>(frame.size()));
}

// ---- 业务发送（未认证静默丢弃，防重连中途乱发） ----
void ImSession::requestConvList(){
    if(!authed_) return;
    const std::string f = imEncodeConvList();
    if(!f.empty()) writeFrame(f);
}
void ImSession::createConv(uint8_t type, const QString& name, const QStringList& members){
    if(!authed_) return;
    std::vector<std::string> ms;
    ms.reserve(size_t(members.size()));
    for(const auto& m : members) ms.push_back(m.toStdString());
    const std::string f = imEncodeConvCreate(type, name.toStdString(), ms);
    if(!f.empty()) writeFrame(f);
}
void ImSession::openConv(const QString& convId){
    if(!authed_) return;
    const std::string f = imEncodeConvOpen(convId.toStdString());
    if(!f.empty()) writeFrame(f);
}
void ImSession::requestHistory(const QString& convId, uint32_t count, uint32_t before){
    if(!authed_) return;
    const std::string f = imEncodeHistory(convId.toStdString(), count, before);
    if(!f.empty()) writeFrame(f);
}
void ImSession::sendMessage(const QString& convId, const QString& msg){
    if(!authed_) return;
    const std::string f = imEncodeMsg(convId.toStdString(), msg.toStdString());
    if(!f.empty()) writeFrame(f);
}
void ImSession::sendRead(const QString& convId, uint64_t seq){
    if(!authed_) return;
    const std::string f = imEncodeRead(convId.toStdString(), seq);
    if(!f.empty()) writeFrame(f);
}
void ImSession::leaveConv(const QString& convId){
    if(!authed_) return;
    const std::string f = imEncodeConvLeave(convId.toStdString());
    if(!f.empty()){
        writeFrame(f);
        socket_->flush();      // 退群后可能立即关窗：flush 尽力发出
    }
}
void ImSession::requestMembers(const QString& convId){
    if(!authed_) return;
    const std::string f = imEncodeMembers(convId.toStdString());
    if(!f.empty()) writeFrame(f);
}
void ImSession::joinConv(const QString& convId){
    if(!authed_) return;
    const std::string f = imEncodeConvJoin(convId.toStdString());
    if(!f.empty()) writeFrame(f);      // 服务端回 0x92 → convCreated：客户端自动刷新+打开
}
void ImSession::lookupUser(const QString& uid){
    if(!authed_) return;
    const std::string f = imEncodeUserLookup(uid.toStdString());
    if(!f.empty()) writeFrame(f);      // 回执走 0x9C userLookupResult
}

// ---- 收帧 ----
void ImSession::onReadyRead(){
    QByteArray data = socket_->readAll();
    inBuf_.append(data.constData(), int(data.size()));

    // 循环切帧：够 4B 读大端长 L；L 非法（0 或 >65536）→ 断帧踢出（防恶意头撑爆内存）；
    // 够 L 字节则抽 payload 解码；未知 type / 非法 → 日志忽略（与 server 对称）。
    for(;;){
        if(static_cast<size_t>(inBuf_.size()) < 4) return;
        const uint32_t len = frameLenFrom(inBuf_);
        if(len == 0 || len > kMaxFrameLen){
            inBuf_.clear();
            intentionalClose_ = true;
            socket_->abort();
            intentionalClose_ = false;
            authed_ = false;
            emit disconnected(QStringLiteral("bad frame length, connection dropped"));
            return;
        }
        if(static_cast<size_t>(inBuf_.size()) < 4u + len) return;   // 半包，等下一批
        std::string payload(inBuf_, 4, len);
        inBuf_.erase(0, 4 + len);
        handlePayload(payload);
    }
}

void ImSession::handlePayload(const std::string& payload){
    const ImDecoded d = imDecode(payload);
    if(!d.ok()) return;                          // 未知/非法 type：忽略（与 server 对称）

    switch(d.type){
    case IM_AUTH_OK:
        authed_ = true;
        uid_ = qs(d.uid); nick_ = qs(d.nick);
        emit authOk(uid_, nick_);
        break;
    case IM_CONV_CREATED:
        emit convCreated(qs(d.conv));
        break;
    case IM_CONV_LIST_R: {
        std::vector<ImConvQt> out;
        out.reserve(d.convs.size());
        for(const auto& c : d.convs)
            out.push_back(ImConvQt{qs(c.convId), c.type, qs(c.name), c.unread,
                                   qs(c.lastMsg), c.lastTsMs});
        emit convList(out);
        break;
    }
    case IM_CONV_OPENED:
        emit convOpened(qs(d.conv), d.seq);
        break;
    case IM_HISTORY_R: {
        std::vector<ImHistoryQt> out;
        out.reserve(d.history.size());
        for(const auto& e : d.history)
            out.push_back(ImHistoryQt{e.seq, e.tsMs, qs(e.from), qs(e.msg)});  // from=发送者 uid
        emit historyBatch(qs(d.conv), out);
        break;
    }
    case IM_MSG_R:
        emit message(qs(d.conv), qs(d.fromUid), qs(d.fromNick), qs(d.msg), d.seq, d.tsMs);
        break;
    case IM_MEMBERS_R: {
        std::vector<ImMemberQt> out;
        out.reserve(d.membersResp.size());
        for(const auto& m : d.membersResp)
            out.push_back(ImMemberQt{qs(m.uid), qs(m.nick), m.online});
        emit members(qs(d.conv), out);
        break;
    }
    case IM_PRESENCE:
        emit presence(qs(d.uid), d.online);
        break;
    case IM_USER_LOOKUP_R:
        emit userLookupResult(d.lookFound, qs(d.nick));   // 0x9C（d.uid=目标，nick=找到的昵称）
        break;
    case IM_CONV_CHANGED:
        emit convChanged(qs(d.conv));          // 会话成员集变更：收者应刷新会话列（0x93）
        break;
    case S2C_ERROR:
        emit sessionError(qs(d.reason));
        break;
    default:
        break;
    }
}

void ImSession::onSocketError(QAbstractSocket::SocketError err){
    // Qt6 移除 AbortError → 以 intentionalClose_ 为准：只静默自己 abort() 触发的错误。
    Q_UNUSED(err);
    if(intentionalClose_){ intentionalClose_ = false; return; }
    authed_ = false;
    emit disconnected(socket_->errorString());
}

} // namespace chat