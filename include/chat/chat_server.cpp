#include "chat/chat_server.hpp"

#include <utility>   // std::move

#include "chat/message_proto.hpp"
#include "net/event_loop.hpp"      // server::EventLoop：isInLoopThread / runInLoop（跨 loop 投递）
#include "net/log.hpp"
#include "server/tcp_connnection.hpp"
#include "server/tcpserver.hpp"

namespace chat {

// ---------- ChatRoom ----------
bool ChatRoom::join(const std::shared_ptr<server::TcpConnection>& conn, const std::string& name){
    std::lock_guard<std::mutex> lk(mtx_);
    for(auto& m : members_){
        if(auto c = m.first.lock()){
            if(c.get() == conn.get()){
                m.second = name;              // 同连接重复 join：原地更新昵称
                return false;
            }
        }
    }
    members_.emplace_back(conn, name);
    return true;
}
std::string ChatRoom::leave(server::TcpConnection* conn){
    std::lock_guard<std::mutex> lk(mtx_);
    for(auto it = members_.begin(); it != members_.end(); ++it){
        if(auto c = it->first.lock()){
            if(c.get() == conn){
                std::string gone = std::move(it->second);
                members_.erase(it);
                return gone;
            }
        }
    }
    return {};
}
std::string ChatRoom::nameOf(server::TcpConnection* conn){
    std::lock_guard<std::mutex> lk(mtx_);
    for(auto it = members_.begin(); it != members_.end();){
        auto aliveC = it->first.lock();
        if(!aliveC){ it = members_.erase(it); continue; }   // 已析构成员：顺手修剪
        if(aliveC.get() == conn) return it->second;
        ++it;
    }
    return {};
}
std::vector<std::shared_ptr<server::TcpConnection>> ChatRoom::alive(){
    std::vector<std::shared_ptr<server::TcpConnection>> out;
    std::lock_guard<std::mutex> lk(mtx_);
    out.reserve(members_.size());
    for(auto it = members_.begin(); it != members_.end();){
        if(auto c = it->first.lock()){
            out.push_back(std::move(c));
            ++it;
        } else {
            it = members_.erase(it);          // 惰性修剪：连接已析构（断线/关闭）→ 出房
        }
    }
    return out;
}
std::vector<std::string> ChatRoom::names(){
    std::vector<std::string> out;
    std::lock_guard<std::mutex> lk(mtx_);
    for(auto it = members_.begin(); it != members_.end();){
        if(auto c = it->first.lock()){
            (void)c;
            out.push_back(it->second);
            ++it;
        } else {
            it = members_.erase(it);
        }
    }
    return out;
}

// ---------- ChatServer ----------
ChatServer::ChatServer(server::TcpServer* srv): srv_(srv){}

std::shared_ptr<ChatRoom> ChatServer::findRoom(const std::string& room){
    std::lock_guard<std::mutex> lk(roomsMtx_);
    auto it = rooms_.find(room);
    return it == rooms_.end() ? nullptr : it->second;
}
std::shared_ptr<ChatRoom> ChatServer::getOrCreate(const std::string& room){
    std::lock_guard<std::mutex> lk(roomsMtx_);
    auto it = rooms_.find(room);
    if(it != rooms_.end()) return it->second;
    auto r = std::make_shared<ChatRoom>();
    rooms_[room] = r;
    return r;
}
void ChatServer::sendError(server::TcpConnection* conn, const std::string& reason){
    std::string frame = encodeError(1, reason);
    if(!frame.empty()) sendTo(conn, frame);
}
void ChatServer::sendTo(server::TcpConnection* conn, const std::string& frame){
    srv_->sendMessage(conn, frame.data(), frame.size());   // 本线程 = conn 的 ioLoop 线程（批末合并）
}

void ChatServer::onMessage(server::TcpConnection* conn, const std::string& payload){
    if(payload.empty()){ sendError(conn, "bad frame: empty"); return; }
    if(payload.size() > kMaxFrameLen){                   // codec kMaxMsgLen 已限 64KB，此为纵深防御
        sendError(conn, "bad frame: too large");
        return;
    }
    const DecodedMsg d = decode(payload);
    if(!d.ok()){
        server::Logger::instance().log(server::Level::WARN, "chat: drop invalid frame type=",
                                       static_cast<int>(payload[0]) & 0xff);
        return;                                                     // 非法/未知 type：静默忽略
    }
    switch(d.type){
    case C2S_JOIN:  handleJoin(conn, d.room, d.name); break;
    case C2S_MSG:   handleMsg(conn, d.room, d.msg);   break;
    case C2S_LEAVE: handleLeave(conn, d.room);        break;
    case C2S_LIST:  handleList(conn, d.room);         break;
    default:        server::Logger::instance().log(server::Level::WARN, "chat: drop S2C-type frame from client");
    }
}

void ChatServer::handleJoin(server::TcpConnection* conn, const std::string& room, const std::string& name){
    if(room.empty() || name.empty()){
        sendError(conn, "join: room and name required");
        return;
    }
    auto r = getOrCreate(room);
    r->join(conn->shared_from_this(), name);        // 消息回调栈内 conn 必被 conns_/keep 持有，安全
    sendTo(conn, encodeJoined(room, name));
    server::Logger::instance().log(server::Level::INFO, "chat: join room=", room, " name=", name);
}
void ChatServer::handleMsg(server::TcpConnection* conn, const std::string& room, const std::string& msg){
    auto r = findRoom(room);
    if(!r){ sendError(conn, "msg: no such room"); return; }
    const std::string from = r->nameOf(conn);       // 顺带修剪；空 = 非成员
    if(from.empty()){ sendError(conn, "msg: not joined"); return; }
    broadcast(r, encodeBroadcast(room, from, msg)); // 含发送者 echo
}
void ChatServer::handleLeave(server::TcpConnection* conn, const std::string& room){
    auto r = findRoom(room);
    if(!r) return;
    const std::string gone = r->leave(conn);
    if(gone.empty()) return;
    const std::string frame = encodeLeft(room, gone);
    for(auto& m : r->alive()){
        if(m.get() != conn) deliver(m, frame);      // 只通知留下的人（离开者自己知道）
    }
    server::Logger::instance().log(server::Level::INFO, "chat: leave room=", room, " name=", gone);
}
void ChatServer::handleList(server::TcpConnection* conn, const std::string& room){
    auto r = findRoom(room);
    if(!r){ sendError(conn, "list: no such room"); return; }
    sendTo(conn, encodeMembers(room, r->names()));
}

void ChatServer::deliver(const std::shared_ptr<server::TcpConnection>& conn, const std::string& frame){
    server::EventLoop* loop = conn->getLoop();
    if(loop->isInLoopThread()){
        // 与源连接同 loop（含发送者 echo / 单线程退化）：直发，批末写合并
        srv_->sendMessage(conn.get(), frame.data(), frame.size());
        return;
    }
    // 跨 loop：runInLoop 投递到成员「自己」的 loop 再 send —— 绝不跨线程碰 outputBuffer_
    loop->runInLoop([this, conn, frame]{
        srv_->sendMessage(conn.get(), frame.data(), frame.size());
    });
}
void ChatServer::broadcast(const std::shared_ptr<ChatRoom>& room, const std::string& frame){
    for(auto& m : room->alive()) deliver(m, frame);
}

} // namespace chat