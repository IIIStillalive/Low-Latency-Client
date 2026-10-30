#include "chat/chat_server.hpp"

#include <algorithm>  // std::reverse（历史批回转为用户序）
#include <cstdint>
#include <limits>
#include <utility>   // std::move

#include "chat/message_proto.hpp"
#include "kv/kv_store.hpp"
#include "net/event_loop.hpp"      // server::EventLoop：isInLoopThread / runInLoop（跨 loop 投递）
#include "net/log.hpp"
#include "server/tcp_connnection.hpp"
#include "server/tcpserver.hpp"

namespace chat {
namespace {

// ---- 历史键值编解码（设计见 plan/task_plan_kv_im.md §1）----
// 键: "h"[1B room_len][room][8B BE seq] —— BE seq 使字典序 = 数值序，range 区间直接按 seq 算。
constexpr uint64_t kMaxSeq = std::numeric_limits<uint64_t>::max();   // 8B 哨兵：range 的「全房间」hi

std::string histKeyEnc(const std::string& room, uint64_t seq){
    std::string k;
    k.reserve(2 + room.size() + 8);
    k.push_back('h');
    k.push_back(static_cast<char>(room.size()));     // room ≤255（putStr 已限）
    k.append(room);
    for(int sh = 56; sh >= 0; sh -= 8)
        k.push_back(static_cast<char>((seq >> sh) & 0xff));
    return k;
}
bool histSeqDecode(const std::string& key, uint64_t& seq){
    if(key.size() < 10 || key[0] != 'h') return false;
    const size_t rlen = static_cast<size_t>(static_cast<uint8_t>(key[1]));
    if(key.size() != 2 + rlen + 8) return false;
    seq = 0;
    for(size_t i = 2 + rlen; i < key.size(); ++i)
        seq = (seq << 8) | static_cast<uint8_t>(key[i]);
    return true;
}
// 值: [1B from_len][from][msg 余下全部]（msg 0..64KB，与帧上限一致）
std::string histValEnc(const std::string& from, const std::string& msg){
    std::string v;
    v.reserve(1 + from.size() + msg.size());
    v.push_back(static_cast<char>(from.size()));
    v.append(from);
    v.append(msg);
    return v;
}
bool histValDecode(const std::string& v, std::string& from, std::string& msg){
    if(v.empty()) return false;
    const size_t flen = static_cast<size_t>(static_cast<uint8_t>(v[0]));
    if(1 + flen > v.size()) return false;
    from.assign(v, 1, flen);
    msg.assign(v, 1 + flen, v.size() - 1 - flen);
    return true;
}

} // namespace

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

// ---- 历史 seq（kv-engine 回接，见 plan/task_plan_kv_im.md §3）----
// 锁内取下一 seq 并自增：并发成员写不同键，无丢无重、无覆盖。
uint64_t ChatRoom::takeSeq(){
    std::lock_guard<std::mutex> lk(mtx_);
    return next_seq_++;
}
bool ChatRoom::seqSeeded() const{
    std::lock_guard<std::mutex> lk(mtx_);
    return seeded_;
}
uint64_t ChatRoom::highestSeq() const{
    std::lock_guard<std::mutex> lk(mtx_);
    return next_seq_ == 0 ? 0 : next_seq_ - 1;   // 未 seed(=0) 恒 0；seeded 后 = 当前最大 seq
}
void ChatRoom::seedSeq(uint64_t low){
    std::lock_guard<std::mutex> lk(mtx_);
    if(next_seq_ < low) next_seq_ = low;         // 只升不降：两 loop 竞态重复 seed 也不回拨
    seeded_ = true;
}

// ---------- ChatServer ----------
ChatServer::ChatServer(server::TcpServer* srv): srv_(srv){}

void ChatServer::setHistory(kv::KvStore* kv){ kv_ = kv; }

void ChatServer::stop(){
    if(!kv_) return;
    kv_->flush();       // 先刷 C/OS 缓冲（EverySec/No 下保证 SIGTERM 时落盘，G7）
    kv_->close();       // 再关：写 EOF 收尾标记，恢复时跳过
}

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
    case C2S_JOIN:  handleJoin(conn, d.room, d.name);                       break;
    case C2S_MSG:   handleMsg(conn, d.room, d.msg);                         break;
    case C2S_LEAVE: handleLeave(conn, d.room);                              break;
    case C2S_LIST:  handleList(conn, d.room);                               break;
    case C2S_HISTORY: handleHistory(conn, d.room, d.hCount, d.hBefore);     break;
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
    // 成员表变更广播：新人 join 后，先把更新后的成员表推给已在线成员。
    // 排除加入者本人 —— 其成员表由 C2S_LIST 回执填充（服务端同一快照），
    // 且不给自己连接增帧 → 不扰动 chat_room_test G5 的 membersEpoch LIST 栅栏。
    const std::string membersFrame = encodeMembers(room, r->names());
    if(!membersFrame.empty()){
        for(auto& m : r->alive()){
            if(m.get() != conn) deliver(m, membersFrame);   // 跨 loop runInLoop 投递，见 deliver 头注
        }
    }
    server::Logger::instance().log(server::Level::INFO, "chat: join room=", room, " name=", name);
}
void ChatServer::handleMsg(server::TcpConnection* conn, const std::string& room, const std::string& msg){
    auto r = findRoom(room);
    if(!r){ sendError(conn, "msg: no such room"); return; }
    const std::string from = r->nameOf(conn);       // 顺带修剪；空 = 非成员
    if(from.empty()){ sendError(conn, "msg: not joined"); return; }
    // 先落库再广播（顺序不变量）：接收者看到消息 ⇒ 已持久化 → G6/G7 可确定性断言。
    if(kv_ && kv_->is_open()){
        seedRoomSeq(r, room);                       // 首次写：全扫推导 next_seq_（防重启 seq 回 0）
        const uint64_t seq = r->takeSeq();
        kv_->put(histKeyEnc(room, seq), histValEnc(from, msg));
    }
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

void ChatServer::seedRoomSeq(const std::shared_ptr<ChatRoom>& room, const std::string& roomName){
    if(room->seqSeeded()) return;
    std::vector<std::pair<std::string,std::string>> all;
    kv_->range(histKeyEnc(roomName, 0), histKeyEnc(roomName, kMaxSeq), all);   // 全扫一次，O(M)
    uint64_t maxS = 0;
    bool found = false;
    for(const auto& e : all){
        uint64_t s = 0;
        if(histSeqDecode(e.first, s)){ if(!found || s > maxS){ maxS = s; found = true; } }
    }
    // 空历史 → next=0（首条 seq0，与计划 §6 G6b 的 0 基断言一致）；
    // 有历史 → next=maxS+1（续接不重号）。只升不降防两 loop 竞态回拨。
    room->seedSeq(found ? maxS + 1 : 0);
}

void ChatServer::handleHistory(server::TcpConnection* conn, const std::string& room,
                               uint32_t count, uint32_t before){
    if(!kv_ || !kv_->is_open()){ sendError(conn, "history: unavailable"); return; }
    if(count == 0 || count > 200) count = 200;     // clamp 1..200

    auto r = findRoom(room);
    uint64_t maxS = 0;
    if(r){
        if(!r->seqSeeded()) seedRoomSeq(r, room);  // 首次访问此房：全扫推导 + seed，此后 O(count) 读
        maxS = r->highestSeq();                    // in-memory 当前最大 seq
    } else {
        // 历史在 kv 但 registry 无此房（重启后房间表空）：不建房，直接全扫推导 max。
        // range 升序 → 最后一个合法键即 max。
        std::vector<std::pair<std::string,std::string>> all;
        kv_->range(histKeyEnc(room, 0), histKeyEnc(room, kMaxSeq), all);
        for(const auto& e : all){
            uint64_t s = 0;
            if(histSeqDecode(e.first, s)) maxS = s;
        }
    }

    // 翻页：before>0 → 严格早于 before 的最多 count 条（滚动上翻）；before=0 → 最新 count 条。
    // ⚠️ kv range 实际为闭区间 [lo,hi]（skiplist.hpp range: 扫到 key <= hi 才停，非计划 §1 所记
    //    半开）→ hisHi 传「最后一条要包含的 seq」，不是排他界。lo 下界 0；range 升序 → 天然
    //    「最旧→最新」。全扫/seed 用 kMaxSeq 哨兵作 hi 不受影响（哨兵键永不落库）。
    const uint64_t hisHi = before > 0 ? static_cast<uint64_t>(before) - 1 : maxS;
    const uint64_t hisLo = before > 0
                         ? (static_cast<uint64_t>(before) > count ? before - count : 0)
                         : (maxS + 1 > count ? maxS + 1 - count : 0);

    std::vector<std::pair<std::string,std::string>> all;
    kv_->range(histKeyEnc(room, hisLo), histKeyEnc(room, hisHi), all);

    // 解码 + 预算 60KB（codec 64KB 上限的纵深防御）：从最新端装不下更旧的条目则丢弃。
    // HistoryEntry 携带 seq：客户端把它当上翻游标（before_seq），S2C_HISTORY 帧内逐条带 8B seq。
    std::vector<HistoryEntry> msgs;
    constexpr uint64_t kHistoryBudget = 60000;
    uint64_t frameBytes = 0;
    for(auto it = all.rbegin(); it != all.rend(); ++it){                 // 最新 → 最旧
        std::string from, m;
        uint64_t s = 0;
        if(!histSeqDecode(it->first, s) || !histValDecode(it->second, from, m)) continue;
        const uint64_t entry = from.size() + m.size() + 3 + 8;           // 帧内该条 ≈ 1+from+2+msg+8seq
        if(!msgs.empty() && frameBytes + entry > kHistoryBudget) continue;
        msgs.push_back(HistoryEntry{s, std::move(from), std::move(m)});
        frameBytes += entry;
    }
    std::reverse(msgs.begin(), msgs.end());                              // 最旧 → 最新

    const std::string frame = encodeHistoryResp(room, msgs);
    if(!frame.empty()) sendTo(conn, frame);
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