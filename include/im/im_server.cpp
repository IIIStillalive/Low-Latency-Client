#include "im/im_server.hpp"

#include <algorithm>   // std::reverse（历史批回转为用户序）
#include <chrono>      // 消息时间戳 ts_ms
#include <cstdint>
#include <cstdlib>     // strtoull（群号推导）
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "chat/message_proto.hpp"
#include "kv/kv_store.hpp"
#include "net/event_loop.hpp"        // server::EventLoop：isInLoopThread / runInLoop（跨 loop 投递）
#include "net/log.hpp"
#include "server/tcp_connnection.hpp"
#include "server/tcpserver.hpp"

using namespace chat;    // 消息协议全家（imEncode*/imDecode*/kImConv*/kMax*）都在 chat 命名空间

namespace im {
namespace {

// ============================================================================
// 键 / 值编解码（schema 见 plan/task_plan_im_v2.md §1；length-prefix 防碰撞）
// ============================================================================
// 长度前缀字符串：≤255（frame 层与键层同约束；调用方先拒超长）。
void putStr(std::string& out, const std::string& s){
    out.push_back(static_cast<char>(s.size()));
    out.append(s);
}
// [8B 大端]：BE → 字节序即数值序，range 区间直接按 seq 定位。
std::string be64(uint64_t v){
    std::string out(8, '\0');
    for(int i = 0; i < 8; ++i)
        out[i] = static_cast<char>((v >> (56 - 8 * i)) & 0xff);
    return out;
}
uint64_t readBe64(const std::string& s, size_t off){
    uint64_t v = 0;
    for(size_t i = 0; i < 8 && off + i < s.size(); ++i)
        v = (v << 8) | static_cast<uint8_t>(s[off + i]);
    return v;
}

// —— 键 ——
std::string userKey(const std::string& uid){   std::string k = "user|";  putStr(k, uid); return k; }
std::string convKey(const std::string& cid){   std::string k = "conv|";  putStr(k, cid); return k; }
std::string memberKey(const std::string& cid, const std::string& uid){
    std::string k = "member|"; putStr(k, cid); putStr(k, uid); return k;
}
std::string memberPrefix(const std::string& cid){
    std::string k = "member|"; putStr(k, cid); return k;
}
std::string cursorKey(const std::string& uid, const std::string& cid){
    std::string k = "cursor|"; putStr(k, uid); putStr(k, cid); return k;
}
std::string histKey(const std::string& cid, uint64_t seq){
    std::string k = "h|"; putStr(k, cid); k += be64(seq); return k;
}
std::string histPrefix(const std::string& cid){
    std::string k = "h|"; putStr(k, cid); return k;
}
// 最近消息键：`convl|<cid>`，值=histVal(uid,msg,ts)。handleMsg 每次落历史顺带覆盖写，
// convList 读它 O(1) 出「最后消息预览 + 时间」（会话列排序锚）。旧 wal 无该键 → 空预览。
std::string lastKey(const std::string& cid){
    std::string k = "convl|"; putStr(k, cid); return k;
}
// 前缀扫描哨兵：prefix + "\xff" 兜住该前缀下剩余全部字节（键带 8B seq 或 length-prefix uid，
// 最小语义 = "\xff"，覆盖所有合法后缀；哨兵键永不落库）。
std::string prefixSentinel(const std::string& prefix){ return prefix + std::string(1, '\xff'); }
// conv 键解码：`conv|` + [1B len][cid] → cid；失败返回空。
std::string convKeyDecode(const std::string& k){
    if(k.size() < 5 + 1 || k.compare(0, 5, "conv|") != 0) return {};
    const size_t cl = static_cast<size_t>(static_cast<uint8_t>(k[5]));
    if(k.size() != 5 + 1 + cl) return {};
    return k.substr(6, cl);
}

// —— 值 ——
std::string userVal(const std::string& nick, const std::string& pwd32){
    std::string v; v.reserve(1 + nick.size() + pwd32.size());
    putStr(v, nick); v.append(pwd32); return v;
}
bool userDecode(const std::string& v, std::string& nick, std::string& pwd32){
    if(v.size() < 1 + 1 + 32) return false;                  // [1B nick_len][nick][32B pwd]
    const size_t nl = static_cast<size_t>(static_cast<uint8_t>(v[0]));
    if(v.size() != 1 + nl + 32) return false;
    nick.assign(v, 1, nl);
    pwd32.assign(v, 1 + nl, 32);
    return true;
}
std::string convVal(uint8_t type, const std::string& name){
    std::string v; v.reserve(2 + name.size());
    v.push_back(static_cast<char>(type)); putStr(v, name); return v;
}
bool convDecode(const std::string& v, uint8_t& type, std::string& name){
    if(v.size() < 2) return false;                           // [1B type][1B name_len][name]
    type = static_cast<uint8_t>(v[0]);
    const size_t nl = static_cast<size_t>(static_cast<uint8_t>(v[1]));
    if(v.size() != 2 + nl) return false;
    name.assign(v, 2, nl);
    return true;
}
// member 值：空串（存在即成员；range(prefix, sentinel) 输出条数 = 成员数）
// cursor 值：[8B BE seq]（已读游标；未读数 = range(seq+1..哨兵).size()，免 seed）
// 历史值：[1B from_len][from][2B msg_len][msg][8B ts_ms]
std::string histVal(const std::string& from, const std::string& msg, uint64_t tsMs){
    std::string v;
    v.reserve(1 + from.size() + 2 + msg.size() + 8);
    putStr(v, from);
    v.push_back(static_cast<char>((msg.size() >> 8) & 0xff));
    v.push_back(static_cast<char>(msg.size() & 0xff));
    v.append(msg);
    v += be64(tsMs);
    return v;
}
bool histDecode(const std::string& v, std::string& from, std::string& msg, uint64_t& tsMs){
    if(v.size() < 1 + 1 + 2 + 8) return false;                // [1B fl][from][2B ml][msg][8B ts]
    const size_t fl = static_cast<size_t>(static_cast<uint8_t>(v[0]));
    if(1 + fl + 2 > v.size()) return false;
    const size_t ml = (static_cast<size_t>(static_cast<uint8_t>(v[1 + fl])) << 8)
                    | static_cast<size_t>(static_cast<uint8_t>(v[2 + fl]));
    if(v.size() != 1 + fl + 2 + ml + 8) return false;
    from.assign(v, 1, fl);
    msg.assign(v, 1 + fl + 2, ml);
    tsMs = readBe64(v, 1 + fl + 2 + ml);
    return true;
}
// member 键解码：`member|` + [1B conv_len][cid] + [1B uid_len][uid] → (cid, uid)
bool memberKeyDecode(const std::string& k, std::string& cid, std::string& uid){
    if(k.size() < 7 + 1 + 1 || k.compare(0, 7, "member|") != 0) return false;
    const size_t cl = static_cast<size_t>(static_cast<uint8_t>(k[7]));
    if(k.size() < 7 + 1 + cl + 1) return false;
    const size_t ul = static_cast<size_t>(static_cast<uint8_t>(k[8 + cl]));
    if(k.size() != 7 + 1 + cl + 1 + ul) return false;
    cid.assign(k, 8, cl);
    uid.assign(k, 8 + cl + 1, ul);
    return true;
}
// 历史键中的 seq 提取：键 = `h|` + [len][cid] + [8B seq]，取末 8B。
uint64_t histSeqOf(const std::string& k){ return readBe64(k, k.size() - 8); }

} // namespace

// 会话注册表条目：成员集 / 序号缓存（映射层镜像 kv）。线程约定：
//   - 全部读写都在 ImServer::convsMtx_ 临界区内（create/list/msg 可能跨 loop 线程）。
//   - 首次触碰才从 kv 加载（getConv）；加载后互斥保护，不额外加成员锁。
struct ImConv {
    uint8_t type = 0;
    std::string name;
    std::map<std::string, bool> members;   // uid → 在组；O(1) 判成员 + 广播遍历基底
    void add(const std::string& uid){ members[uid] = true; }
    void remove(const std::string& uid){ members.erase(uid); }
    bool has(const std::string& uid) const{ return members.count(uid) != 0; }

    uint64_t next_seq_ = 0;                 // 下一条消息 seq（重启后由 seed 全扫回推）
    bool seeded_ = false;                   // 是否已从此会话推导过 next_seq_
};

// ---------------------------------------------------------------------------
// ImServer
// ---------------------------------------------------------------------------
ImServer::ImServer(server::TcpServer* srv) : srv_(srv){
    // presence 挂点：TcpServer 在连接归属 ioLoop 线程回调（见 tcpserver.cpp）。
    // up=true 无动作（认证后才绑定 uid）；up=false → 移除在线 + 广播离线（见 onDisconnect）。
    srv_->setConnectionCallback([this](server::TcpConnection* c, bool up){
        if(!up) onDisconnect(c);
    });
}

void ImServer::setStore(kv::KvStore* kv){ kv_ = kv; }

void ImServer::stop(){
    if(!kv_) return;
    kv_->flush();       // 先刷 C/OS 缓冲（EverySec/No 下保证 SIGTERM 时落盘）
    kv_->close();       // 再关：写 EOF 收尾标记，恢复时跳过
}

// 本连接绑定的 uid；空 = 未认证。
std::string ImServer::uidOf(server::TcpConnection* conn){
    std::lock_guard<std::mutex> lk(onlineMtx_);
    auto it = connUid_.find(conn);
    return it == connUid_.end() ? std::string() : it->second;
}

// 查找会话：注册表缺失 → 从 kv 加载（元数据 + 成员集）；kv 无此会话 → nullptr。
// 全程持 convsMtx_：首访加载动作原子化（两 loop 并发首访不重复建房）。
std::shared_ptr<ImConv> ImServer::getConv(const std::string& cid){
    std::lock_guard<std::mutex> lk(convsMtx_);
    auto it = convs_.find(cid);
    if(it != convs_.end()) return it->second;
    if(!kv_ || !kv_->is_open()) return nullptr;
    auto v = kv_->get(convKey(cid));
    if(!v.has_value()) return nullptr;
    uint8_t type = 0; std::string name;
    if(!convDecode(*v, type, name)) return nullptr;
    auto c = std::make_shared<ImConv>();
    c->type = type; c->name = name;
    std::vector<std::pair<std::string, std::string>> rows;
    kv_->range(memberPrefix(cid), prefixSentinel(memberPrefix(cid)), rows);
    for(auto& r : rows){
        std::string kcid, uid;
        if(memberKeyDecode(r.first, kcid, uid) && kcid == cid) c->add(uid);
    }
    convs_[cid] = c;
    return c;
}

// conn 是会话成员？（空 c / 未认证 → false）
bool ImServer::isMember(server::TcpConnection* conn, const std::string& cid,
                        const std::shared_ptr<ImConv>& c){
    (void)cid;
    if(!c) return false;
    const std::string uid = uidOf(conn);
    if(uid.empty()) return false;
    std::lock_guard<std::mutex> lk(convsMtx_);
    return c->has(uid);
}

// 会话显示名：群 = 存的名；单聊 = 对方昵称（自己定不了、取另一方；对方不存在 → 空）。
std::string ImServer::displayName(const std::string& cid, const std::shared_ptr<ImConv>& c,
                                  const std::string& meUid){
    if(c->type == kImConvGrp) return c->name;
    std::string peer;
    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        for(auto& kv : c->members){
            if(kv.first != meUid){ peer = kv.first; break; }
        }
    }
    return peer.empty() ? std::string() : userNick(peer);
}

// kv 读昵称（0 = 未知用户）。KvStore 读自带共享锁 → 无需本层锁。
std::string ImServer::userNick(const std::string& uid){
    if(!kv_ || !kv_->is_open()) return {};
    auto v = kv_->get(userKey(uid));
    if(!v.has_value()) return {};
    std::string nick, pwd;
    return userDecode(*v, nick, pwd) ? nick : std::string();
}

// 反查「uid 参与的会话集」：member: 前缀全扫 + 尾部 uid 过滤（规模小，见 plan §3 反查一次可接受）。
std::vector<std::string> ImServer::convsOfUser(const std::string& uid){
    std::vector<std::string> out;
    if(!kv_ || !kv_->is_open()) return out;
    std::vector<std::pair<std::string, std::string>> rows;
    kv_->range(std::string("member|"), prefixSentinel(std::string("member|")), rows);
    for(auto& r : rows){
        std::string cid, m;
        if(memberKeyDecode(r.first, cid, m) && m == uid) out.push_back(cid);
    }
    return out;
}

uint64_t ImServer::readCursor(const std::string& uid, const std::string& cid){
    if(!kv_ || !kv_->is_open()) return 0;
    auto v = kv_->get(cursorKey(uid, cid));
    if(!v.has_value() || v->size() < 8) return 0;
    return readBe64(*v, 0);
}

// 未读数：
//   - 无 cursor 键（从未读）= 全部历史都未读 → range(key(0), 哨兵).size()
//     （区别于「已读 seq0」：后者有键、值 0，只漏 seq1 以后 → range(key(1), 哨兵)）
//   - 有 cursor=S     = 游标之后未读   → range(key(S+1), 哨兵).size()（免 seed，见 plan §1）
// 两种情形必须按「读键还是无键」区分，否则新用户 3 条消息会算成 2。
uint32_t ImServer::unreadFor(const std::string& uid, const std::string& cid){
    if(!kv_ || !kv_->is_open()) return 0;
    auto curV = kv_->get(cursorKey(uid, cid));
    std::vector<std::pair<std::string, std::string>> rows;
    if(!curV.has_value()){
        kv_->range(histKey(cid, 0), prefixSentinel(histPrefix(cid)), rows);
        return static_cast<uint32_t>(rows.size());
    }
    const uint64_t cur = readBe64(*curV, 0);
    if(cur == std::numeric_limits<uint64_t>::max()) return 0;     // 防 cur+1 回绕
    kv_->range(histKey(cid, cur + 1), prefixSentinel(histPrefix(cid)), rows);
    return static_cast<uint32_t>(rows.size());
}

bool ImServer::onlineOf(const std::string& uid){
    std::lock_guard<std::mutex> lk(onlineMtx_);
    return online_.count(uid) != 0;
}

void ImServer::sendTo(server::TcpConnection* conn, const std::string& frame){
    if(!frame.empty()) srv_->sendMessage(conn, frame.data(), frame.size());  // 本线程=conn 的 ioLoop
}
void ImServer::deliver(const std::shared_ptr<server::TcpConnection>& conn, const std::string& frame){
    server::EventLoop* loop = conn->getLoop();
    if(loop->isInLoopThread()){
        srv_->sendMessage(conn.get(), frame.data(), frame.size());
        return;
    }
    loop->runInLoop([this, conn, frame]{   // 跨 loop：投到接收者「自己」的 loop 再 send（不跨线程碰缓冲）
        srv_->sendMessage(conn.get(), frame.data(), frame.size());
    });
}
void ImServer::sendError(server::TcpConnection* conn, uint8_t code, const std::string& reason){
    sendTo(conn, imEncodeError(code, reason));
}

// 覆盖性广播：会话全部【在线】成员（含发送者 echo，exclude 连接跳过）。
// 三步、锁不嵌套：①拷贝成员集（convsMtx_）②查在线集（onlineMtx_）③逐连接投递。
void ImServer::broadcastToConv(const std::shared_ptr<ImConv>& c, const std::string& cid,
                               const std::string& frame, server::TcpConnection* exclude){
    (void)cid;
    std::vector<std::string> members;
    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        for(auto& kv : c->members) members.push_back(kv.first);
    }
    std::vector<std::shared_ptr<server::TcpConnection>> targets;
    {
        std::lock_guard<std::mutex> lk(onlineMtx_);
        for(auto& uid : members){
            auto it = online_.find(uid);
            if(it != online_.end() && it->second.get() != exclude)
                targets.push_back(it->second);
        }
    }
    for(auto& t : targets) deliver(t, frame);
}

// presence 广播：uid 上线/下线 → 通知其全部会话的在线成员（exclude = 自己 conn → 无自我噪音）。
void ImServer::broadcastPresence(server::TcpConnection* conn, const std::string& uid, bool online){
    const std::string frame = imEncodePresence(uid, online);
    if(frame.empty()) return;
    for(auto& cid : convsOfUser(uid)){
        auto c = getConv(cid);
        if(c) broadcastToConv(c, cid, frame, online ? conn : nullptr);
    }
}

// 群号分配：`grp:<u64 单调>`；重启时由既有 grp: 键扫描推导 max+1（同 v1 seq seed 思路）。
std::string ImServer::takeGroupId(){
    std::lock_guard<std::mutex> lk(convsMtx_);
    if(!grpSeeded_ && kv_ && kv_->is_open()){
        std::vector<std::pair<std::string, std::string>> all;
        kv_->range(std::string("conv|"), prefixSentinel(std::string("conv|")), all);
        uint64_t maxN = 0;
        static const std::string kG = "grp:";
        for(auto& e : all){
            const std::string id = convKeyDecode(e.first);
            if(id.compare(0, kG.size(), kG) != 0) continue;
            char* endp = nullptr;
            const uint64_t n = strtoull(id.c_str() + kG.size(), &endp, 10);
            if(endp && *endp == '\0' && n > maxN) maxN = n;
        }
        grpCounter_ = maxN + 1;
        grpSeeded_ = true;
    }
    return std::string("grp:") + std::to_string(grpCounter_++);
}

// ---------------------------------------------------------------------------
// 连接下线（up=false，连接归属 ioLoop 线程）：移除在线表 + 广播离线 presence。
// connUid_ 记 conn→uid；仅当「该 uid 当前在线连接就是本 conn」才推动离线广播——
// 同 uid 换端重登时旧 conn 断线不误报离线（用户仍在线）。
void ImServer::onDisconnect(server::TcpConnection* conn){
    std::string uid;
    bool current = false;
    {
        std::lock_guard<std::mutex> lk(onlineMtx_);
        auto it = connUid_.find(conn);
        if(it != connUid_.end()){
            uid = it->second;
            connUid_.erase(it);
            auto oit = online_.find(uid);
            if(oit != online_.end() && oit->second.get() == conn){
                online_.erase(oit);
                current = true;
            }
        }
    }
    if(current && !uid.empty()) broadcastPresence(conn, uid, false);
}

// ---------------------------------------------------------------------------
// 消息入口（callback = conn 的 ioLoop 线程；payload = [1B type][body]，长度头已剥）
void ImServer::onMessage(server::TcpConnection* conn, const std::string& payload){
    if(payload.empty()){ sendError(conn, kErrBad, "empty frame"); return; }
    if(payload.size() > kMaxFrameLen){ sendError(conn, kErrBad, "frame too large"); return; }
    const ImDecoded d = imDecode(payload);
    if(!d.ok()){
        server::Logger::instance().log(server::Level::WARN, "im: drop invalid frame type=",
                                       static_cast<int>(static_cast<uint8_t>(payload[0])));
        return;
    }
    switch(d.type){
    case IM_REGISTER:    handleRegister(conn, d.uid, d.nick, d.pwd32);          return;
    case IM_AUTH:        handleAuth(conn, d.uid, d.pwd32);                      return;
    case IM_CONV_CREATE: handleConvCreate(conn, d.convType, d.name, d.members); return;
    case IM_CONV_LIST:   handleConvList(conn);                                   return;
    case IM_CONV_OPEN:   handleConvOpen(conn, d.conv);                          return;
    case IM_HISTORY:     handleHistory(conn, d.conv, d.hCount, d.hBefore);      return;
    case IM_MSG:         handleMsg(conn, d.conv, d.msg);                        return;
    case IM_READ:        handleRead(conn, d.conv, d.seq);                       return;
    case IM_CONV_LEAVE:  handleConvLeave(conn, d.conv);                         return;
    case IM_MEMBERS:     handleMembers(conn, d.conv);                           return;
    case IM_CONV_JOIN:   handleConvJoin(conn, d.conv);                          return;
    case IM_USER_LOOKUP: handleUserLookup(conn, d.uid);                         return;
    default:
        server::Logger::instance().log(server::Level::WARN, "im: drop S2C-type frame from client");
    }
}

// ---- 用户层：注册 / 认证 ----
void ImServer::handleRegister(server::TcpConnection* conn, const std::string& uid,
                              const std::string& nick, const std::string& pwd32){
    if(uid.empty() || nick.empty() || pwd32.size() != 32){
        sendError(conn, kErrBad, "register: uid+nick+pwd32 required"); return;
    }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    if(kv_->get(userKey(uid)).has_value()){ sendError(conn, kErrUserExists, "user exists"); return; }
    kv_->put(userKey(uid), userVal(nick, pwd32));    // 无显式回执：客户端随后 AUTH 收 IM_AUTH_OK
    server::Logger::instance().log(server::Level::INFO, "im: register uid=", uid);
}

void ImServer::handleAuth(server::TcpConnection* conn, const std::string& uid, const std::string& pwd32){
    if(uid.empty() || pwd32.size() != 32){ sendError(conn, kErrBad, "auth: uid+pwd32 required"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    auto v = kv_->get(userKey(uid));
    if(!v.has_value()){ sendError(conn, kErrAuthFail, "unknown user"); return; }
    std::string nick, stored;
    if(!userDecode(*v, nick, stored)){ sendError(conn, kErrAuthFail, "bad stored user"); return; }
    if(stored != pwd32){ sendError(conn, kErrAuthFail, "wrong password"); return; }

    // 认证成功 → 绑定 conn→uid + 压入在线表（同 uid 再登入：新连接覆盖旧连接，旧 conn 仅不再被视为在线）
    {
        std::lock_guard<std::mutex> lk(onlineMtx_);
        connUid_[conn] = uid;
        online_[uid] = conn->shared_from_this();    // 消息回调栈内 conn 必被 conns_ 保活，安全
    }
    sendTo(conn, imEncodeAuthOk(uid, nick));
    broadcastPresence(conn, uid, true);             // 上线通知给其全部会话成员（不含自己）
    server::Logger::instance().log(server::Level::INFO, "im: auth ok uid=", uid);
}

void ImServer::handleUserLookup(server::TcpConnection* conn, const std::string& uid){
    const std::string me = uidOf(conn);
    if(me.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    if(uid.empty() || uid.size() > kMaxUidLen){ sendError(conn, kErrBad, "bad uid"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    auto v = kv_->get(userKey(uid));
    if(!v.has_value()){ sendTo(conn, imEncodeUserLookupR(false, "")); return; }
    std::string nick, stored;
    if(!userDecode(*v, nick, stored)){ sendTo(conn, imEncodeUserLookupR(false, "")); return; }
    sendTo(conn, imEncodeUserLookupR(true, nick));
    server::Logger::instance().log(server::Level::DEBUG, "im: lookup uid=", uid, " found=1");
}

// ---- 会话：建 / 列 / 开 / 退 ----
void ImServer::handleConvCreate(server::TcpConnection* conn, uint8_t type,
                                const std::string& name, const std::vector<std::string>& members){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }

    std::string cid;
    std::vector<std::string> toAdd = members;        // 出参：全部成员集（含自己）
    bool isNew = false;
    if(type == kImConvDm){
        if(members.size() != 1){ sendError(conn, kErrBad, "dm needs exactly one peer"); return; }
        if(members[0] == uid){ sendError(conn, kErrBad, "dm peer cannot be self"); return; }  // 禁止自建自环会话
        cid = imDmConvId(uid, members[0]);           // 幂等：A↔B 得同一 id
        toAdd.push_back(uid);
    } else if(type == kImConvGrp){
        if(members.empty()){ sendError(conn, kErrBad, "group needs at least one peer"); return; }
        for(const auto& u : members){
            if(u == uid){ sendError(conn, kErrBad, "cannot invite self"); return; }  // 禁止邀请自己入群
        }
        cid = takeGroupId();                          // 单调新号 → isNew 恒真
        toAdd.push_back(uid);
        isNew = true;
    } else { sendError(conn, kErrBad, "bad conv type"); return; }

    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        auto reg = convs_.find(cid);
        if(!isNew && reg != convs_.end()){
            // DM 已存在：幂等回归 → 只补成员落库（自身 + 对方），回执同 conv_id
            for(auto& u : toAdd){
                kv_->put(memberKey(cid, u), std::string());
                reg->second->add(u);
            }
        } else {
            kv_->put(convKey(cid), convVal(type, name));
            for(auto& u : toAdd) kv_->put(memberKey(cid, u), std::string());
        }
    }
    auto conv = getConv(cid);                             // 确保注册表已加载（包括上面幂等路径的成员）
    sendTo(conn, imEncodeConvCreated(cid));
    // 会话变更推送 0x9B：除创建者外的成员（在线）实时刷新会话列 —— 修「A 建 DM/群后 B 不出现」
    {
        const std::string chg = imEncodeConvChanged(cid);
        if(!chg.empty() && conv) broadcastToConv(conv, cid, chg, conn);
    }
    server::Logger::instance().log(server::Level::INFO, "im: conv create uid=", uid, " cid=", cid);
}

void ImServer::handleConvList(server::TcpConnection* conn){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    std::vector<chat::ImConvSummary> out;
    for(auto& cid : convsOfUser(uid)){
        auto c = getConv(cid);
        if(!c) continue;
        chat::ImConvSummary s;
        s.convId = cid;
        s.type   = c->type;
        s.name   = displayName(cid, c, uid);
        s.unread = unreadFor(uid, cid);
        // 最后消息预览：lastKey O(1) 读；grp 拼 `发送者昵称: 消息`（客户端免拉成员表凑昵称）。
        {
            auto lv = kv_->get(lastKey(cid));
            if(lv.has_value()){
                std::string f, m; uint64_t ts = 0;
                if(histDecode(*lv, f, m, ts)){
                    if(c->type == kImConvGrp){
                        const std::string nn = userNick(f);
                        s.lastMsg = nn.empty() ? m : nn + ": " + m;
                    } else s.lastMsg = m;
                    if(s.lastMsg.size() > 250) s.lastMsg.resize(250);  // 预览截断（协议 1B 长 ≤255）
                    s.lastTsMs = ts;
                }
            }
        }
        out.push_back(std::move(s));
    }
    sendTo(conn, imEncodeConvListResp(out));         // 按 conv_id 稳定序（扫描升序）
}

void ImServer::handleConvOpen(server::TcpConnection* conn, const std::string& cid){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    auto c = getConv(cid);
    if(!c){ sendError(conn, kErrConvNone, "conv not found"); return; }
    if(!isMember(conn, cid, c)){ sendError(conn, kErrNotMember, "not a member"); return; }
    sendTo(conn, imEncodeConvOpened(cid, readCursor(uid, cid)));   // 回执 last-read 游标 → 首拉起点
}

// ---- 历史 / 消息 / 已读 ----
void ImServer::handleHistory(server::TcpConnection* conn, const std::string& cid,
                             uint32_t count, uint32_t before){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    auto c = getConv(cid);
    if(!c){ sendError(conn, kErrConvNone, "conv not found"); return; }
    if(!isMember(conn, cid, c)){ sendError(conn, kErrNotMember, "not a member"); return; }
    if(count == 0 || count > 200) count = 200;       // clamp 1..200（设计 §2）

    uint64_t maxS = 0;
    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        if(!c->seeded_){                             // 首次访问会话：全扫推导 next_seq_（防重启 seq 回 0）
            std::vector<std::pair<std::string, std::string>> all;
            kv_->range(histPrefix(cid), prefixSentinel(histPrefix(cid)), all);
            uint64_t m = 0; bool found = false;
            for(auto& e : all){
                const uint64_t s = histSeqOf(e.first);
                if(!found || s > m){ m = s; found = true; }
            }
            c->next_seq_ = found ? m + 1 : 0;
            c->seeded_ = true;
        }
        maxS = c->next_seq_ == 0 ? 0 : c->next_seq_ - 1;  // 当前最大 seq（无历史 → 0，range 自然空）
    }

    // 翻页：before>0 → 严格早于 before 的最多 count 条（滚动上翻）；before=0 → 最新 count 条。
    // kv range 闭区间 [lo,hi] → hi 传「最后一条要包含的 seq」（v1 已踩过坑的精确 hi）。
    const uint64_t hisHi = before > 0 ? static_cast<uint64_t>(before) - 1 : maxS;
    const uint64_t hisLo = before > 0
                         ? (static_cast<uint64_t>(before) > count ? before - count : 0)
                         : (maxS + 1 > count ? maxS + 1 - count : 0);

    std::vector<std::pair<std::string, std::string>> all;
    kv_->range(histKey(cid, hisLo), histKey(cid, hisHi), all);

    // 解码（逐条 seq+ts；最旧→最新供 UI 顺序渲染）+ 帧预算（codec 64KB 上限的纵深防御）
    std::vector<chat::ImHistoryEntry> msgs;
    constexpr uint64_t kBudget = 60000;
    uint64_t frameBytes = 0;
    for(auto it = all.rbegin(); it != all.rend() && msgs.size() < count; ++it){  // 最新 → 最旧
        std::string from, m; uint64_t ts = 0;
        if(!histDecode(it->second, from, m, ts)) continue;
        const uint64_t entry = from.size() + m.size() + 3 + 16;   // ≈ 帧内 1+from+2+msg+8seq+8ts
        if(!msgs.empty() && frameBytes + entry > kBudget) continue;
        chat::ImHistoryEntry e;
        e.seq = histSeqOf(it->first); e.tsMs = ts;
        e.from = std::move(from); e.msg = std::move(m);
        msgs.push_back(std::move(e));
        frameBytes += entry;
    }
    std::reverse(msgs.begin(), msgs.end());          // 最旧 → 最新
    sendTo(conn, imEncodeHistoryResp(cid, msgs));
}

void ImServer::handleMsg(server::TcpConnection* conn, const std::string& cid, const std::string& msg){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    if(msg.size() > kMaxMsgLen){ sendError(conn, kErrBad, "msg too long"); return; }
    auto c = getConv(cid);
    if(!c){ sendError(conn, kErrConvNone, "conv not found"); return; }
    if(!isMember(conn, cid, c)){ sendError(conn, kErrNotMember, "not a member"); return; }

    const uint64_t ts = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    uint64_t seq = 0;
    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        if(!c->seeded_){                             // 首次写会话：全扫推导 next_seq_（重启续接不重号）
            std::vector<std::pair<std::string, std::string>> all;
            kv_->range(histPrefix(cid), prefixSentinel(histPrefix(cid)), all);
            uint64_t m = 0; bool found = false;
            for(auto& e : all){
                const uint64_t s = histSeqOf(e.first);
                if(!found || s > m){ m = s; found = true; }
            }
            c->next_seq_ = found ? m + 1 : 0;
            c->seeded_ = true;
        }
        seq = c->next_seq_++;
        kv_->put(histKey(cid, seq), histVal(uid, msg, ts));   // 先落库再广播（顺序不变量，同 v1）
        kv_->put(lastKey(cid), histVal(uid, msg, ts));        // v3：最近消息预览键（覆盖写，O(1) 读）
    }
    const std::string frame = imEncodeMsgBroadcast(cid, uid, userNick(uid), msg, ts, seq);
    if(!frame.empty()) broadcastToConv(c, cid, frame, nullptr);   // 含发送者 echo
}

void ImServer::handleRead(server::TcpConnection* conn, const std::string& cid, uint64_t seq){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    auto c = getConv(cid);
    if(!c){ sendError(conn, kErrConvNone, "conv not found"); return; }
    if(!isMember(conn, cid, c)){ sendError(conn, kErrNotMember, "not a member"); return; }
    kv_->put(cursorKey(uid, cid), be64(seq));      // 收到已读游标 → 未读 pivot 更新（无回执）
}

void ImServer::handleConvLeave(server::TcpConnection* conn, const std::string& cid){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    auto c = getConv(cid);
    if(!c){ sendError(conn, kErrConvNone, "conv not found"); return; }
    if(c->type == kImConvDm){ sendError(conn, kErrDmNoLeave, "dm has no leave"); return; }  // type 建时固化
    if(!isMember(conn, cid, c)){ sendError(conn, kErrNotMember, "not a member"); return; }
    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        kv_->del(memberKey(cid, uid));             // durable 成员删；退群者不再收该会话广播
        kv_->del(cursorKey(uid, cid));             // 顺带清游标（再入不残留未读）
        c->remove(uid);
    }
    // 成员集变更 → 剩余成员收 0x9B 刷新（退群者已不在成员集，broadcastToConv 天然跳过自己）
    {
        const std::string chg = imEncodeConvChanged(cid);
        if(!chg.empty()) broadcastToConv(c, cid, chg, conn);
    }
}

void ImServer::handleConvJoin(server::TcpConnection* conn, const std::string& cid){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    if(!kv_ || !kv_->is_open()){ sendError(conn, kErrUnavail, "store unavailable"); return; }
    auto c = getConv(cid);
    if(!c){ sendError(conn, kErrConvNone, "conv not found"); return; }
    if(c->type == kImConvDm){ sendError(conn, kErrBad, "dm has no join"); return; }
    bool added = false;
    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        if(!c->has(uid)){                          // 已是成员 → 幂等成功（不重写不推送）
            kv_->put(memberKey(cid, uid), std::string());
            c->add(uid);
            added = true;
        }
    }
    sendTo(conn, imEncodeConvCreated(cid));        // 0x92：入群者走 onConvCreated 刷新+自动打开
    if(added){                                     // 成员集真变 → 其余在线成员收 0x9B 刷新
        const std::string chg = imEncodeConvChanged(cid);
        if(!chg.empty()) broadcastToConv(c, cid, chg, conn);
    }
    server::Logger::instance().log(server::Level::INFO, "im: conv join uid=", uid, " cid=", cid);
}

void ImServer::handleMembers(server::TcpConnection* conn, const std::string& cid){
    const std::string uid = uidOf(conn);
    if(uid.empty()){ sendError(conn, kErrNotAuthed, "auth first"); return; }
    auto c = getConv(cid);
    if(!c){ sendError(conn, kErrConvNone, "conv not found"); return; }
    if(!isMember(conn, cid, c)){ sendError(conn, kErrNotMember, "not a member"); return; }
    std::vector<std::string> uids;
    {
        std::lock_guard<std::mutex> lk(convsMtx_);
        for(auto& kv : c->members) uids.push_back(kv.first);
    }
    std::vector<chat::ImMember> out;
    for(auto& m : uids){
        chat::ImMember mem;
        mem.uid = m;
        mem.nick = userNick(m);
        mem.online = onlineOf(m);
        out.push_back(std::move(mem));
    }
    sendTo(conn, imEncodeMembersResp(cid, out));
}

} // namespace im