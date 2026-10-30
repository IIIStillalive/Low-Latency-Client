// im_test —— v2 类飞书 IM 端到端：F1–F13（配合 im_server 运行）。
//
//   ./build/im_server --port 8891 --threads 2 --kv-path /tmp/im_v2.wal &   # regress 用 2 worker
//   ./build/im_test --port 8891            # exit 0 = 全过（F1–F8 + F10–F13）
//   ./build/im_test --port 8891 --seed9 N  # F9 持久化第一阶段：注册+建群+发 N 条+READ 游标
//   ./build/im_test --port 8891 --verify9 N # F9 第二阶段：同 kv 重启后 AUTH → 会话/成员/未读全在
//
// 测试客户端用裸阻塞 socket + 每客户端一个读者线程（纯测试 harness，不走事件循环）：
//   帧 = [4B 大端长度][payload]，payload = v2 业务帧（1B type，见 message_proto.hpp imEncode*）。
// 判定矩阵（plan/task_plan_im_v2.md §6）：
//   F1 注册：新 uid 成功（随后 AUTH 收 OK）；重复注册 → S2C_ERROR
//   F2 认证：对的口令成功；错口令 / 未注册 → S2C_ERROR
//   F3 建 DM：A 建 → CREATED 回执；B 侧 CONV_LIST 见同 id；A 再建同 DM → 同 conv_id（幂等）
//   F4 DM 收发：A→B 发 3 条，双方各收 3 条（含 sender echo），ts 严格递增
//   F5 建群 3 人：全员 CONV_LIST 可见；广播 3/3；B 退群后不再收
//   F6 未读：B 离线，A 发 3 条 → B 重连 AUTH → CONV_LIST unread=3 → B 打开+READ → 再拉 unread=0
//   F7 在线态：B 断连 → A 收 0x9A(bob,0)；B 重建同 uid → A 收 0x9A(bob,1)
//   F8 历史分页：250 条 → HISTORY(200,0) + HISTORY(50, before) 无重叠无漏 → seq 拼接 = 0..249
//   F9 重启持久化（seed9/verify9 两进程段）：注册+建群+发消息+READ → 重启 → 全恢复
//   F10 非法：未认证发 MSG → ERROR；超大帧不崩；退 dm → ERROR
//   F11 变更推送：B 在线时 A 建 DM/群 → B（及 C）收 0x9B，随后 0x93 含新会话（无需断连重连）
//   F12 入群：非成员 B join → 收 0x92；A 收 0x9B 且成员含 B；重复 join 幂等（不重复推 0x9B）；
//       不存在的群 / 加 dm → ERROR
//   F13 退群推送：B 退群 → 剩余成员 A 收 0x9B；A 成员列表不再含 B；B 自己不收 0x9B
//   F14 用户查找 + 会话列表预览（v3 UI 前置）：0x1B 查到/查不到/空 uid；0x93 lastMsg+lastTsMs
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "chat/message_proto.hpp"
#include "chat/sha256.hpp"

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// 裸 socket 帧读写小工具（同 chat_room_test harness）
// ---------------------------------------------------------------------------
static int connectTo(int port){
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0) return -1;
    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family      = AF_INET;
    peer.sin_port        = htons(static_cast<uint16_t>(port));
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if(::connect(fd, reinterpret_cast<struct sockaddr*>(&peer), sizeof(peer)) != 0){
        ::close(fd);
        return -1;
    }
    struct timeval tv { 1, 0 };            // 1s 接收超时：读者线程空闲时周期醒来查 closed_
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return fd;
}
static bool writeAll(int fd, const void* data, size_t n){
    const char* p = static_cast<const char*>(data);
    size_t sent = 0;
    while(sent < n){
        ssize_t w = ::send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if(w > 0){ sent += static_cast<size_t>(w); continue; }
        if(w < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}
static bool recvN(int fd, void* buf, size_t n, bool& timeout){
    char* p = static_cast<char*>(buf);
    size_t got = 0;
    while(got < n){
        ssize_t r = ::recv(fd, p + got, n - got, 0);
        if(r > 0){ got += static_cast<size_t>(r); continue; }
        if(r == 0) return false;
        if(errno == EINTR) continue;
        if(errno == EAGAIN || errno == EWOULDBLOCK){ timeout = true; return false; }
        return false;
    }
    return true;
}
static void putBe32(std::string& out, uint32_t v){
    out.push_back(static_cast<char>((v >> 24) & 0xff));
    out.push_back(static_cast<char>((v >> 16) & 0xff));
    out.push_back(static_cast<char>((v >>  8) & 0xff));
    out.push_back(static_cast<char>( v        & 0xff));
}
static std::string framed(const std::string& payload){
    std::string f;
    putBe32(f, static_cast<uint32_t>(payload.size()));
    f.append(payload);
    return f;
}
static int waitFor(const std::function<bool()>& pred, int timeoutMs){
    for(int i = 0; i < timeoutMs / 10; ++i){
        if(pred()) return 1;
        std::this_thread::sleep_for(10ms);
    }
    return pred() ? 1 : 0;
}
static std::string pass32(const std::string& pwd){ return chat::sha256(pwd); }   // 32B 客户端摘要

// ---------------------------------------------------------------------------
// v2 测试客户端：连接 + 读者线程 + 计数（imDecode 分流 C2S/S2C）
// ---------------------------------------------------------------------------
class ImClient{
public:
    ImClient() = default;
    ~ImClient(){ close(); }

    bool connect(int port){ fd_ = connectTo(port); return fd_ >= 0; }
    void listen(){ rd_ = std::thread([this]{ reader(); }); }
    void close(){
        if(closed_.exchange(true)) return;
        if(fd_ >= 0){ ::shutdown(fd_, SHUT_RDWR); ::close(fd_); fd_ = -1; }
        if(rd_.joinable()) rd_.join();
    }
    bool sendPayload(const std::string& payload){
        return fd_ >= 0 && writeAll(fd_, framed(payload).data(), framed(payload).size());
    }
    // ---- v2 会话辅助（均先建临时 frame） ----
    bool sendRegister(const std::string& uid, const std::string& nick, const std::string& pwd){
        return sendPayload(chat::imEncodeRegister(uid, nick, pass32(pwd)));
    }
    bool sendAuth(const std::string& uid, const std::string& pwd){
        return sendPayload(chat::imEncodeAuth(uid, pass32(pwd)));
    }
    bool sendAuthRaw(const std::string& uid, const std::string& badPwd20){   // 错口令：任意 32B 摘要
        return sendPayload(chat::imEncodeAuth(uid, badPwd20));
    }
    bool sendConvCreate(uint8_t type, const std::string& name,
                        const std::vector<std::string>& members){
        return sendPayload(chat::imEncodeConvCreate(type, name, members));
    }
    bool sendConvList(){ return sendPayload(chat::imEncodeConvList()); }
    bool sendConvOpen(const std::string& cid){ return sendPayload(chat::imEncodeConvOpen(cid)); }
    bool sendHistory(const std::string& cid, uint32_t count, uint32_t before){
        return sendPayload(chat::imEncodeHistory(cid, count, before));
    }
    bool sendMsg(const std::string& cid, const std::string& msg){
        return sendPayload(chat::imEncodeMsg(cid, msg));
    }
    bool sendRead(const std::string& cid, uint64_t seq){ return sendPayload(chat::imEncodeRead(cid, seq)); }
    bool sendConvLeave(const std::string& cid){ return sendPayload(chat::imEncodeConvLeave(cid)); }
    bool sendConvJoin(const std::string& cid){ return sendPayload(chat::imEncodeConvJoin(cid)); }
    bool sendMembers(const std::string& cid){ return sendPayload(chat::imEncodeMembers(cid)); }
    bool sendUserLookup(const std::string& uid){ return sendPayload(chat::imEncodeUserLookup(uid)); }

    // ---- 同步等单事件 ----
    bool waitAuthOk(int ms = 5000){ return waitFor([&]{ return authOk_.load(); }, ms) != 0 && authOk_.load(); }
    bool waitErr(int ms = 5000){ return waitFor([&]{ return errCount_.load() > 0; }, ms) != 0; }
    bool waitCreated(int ms = 5000){
        return waitFor([&]{ return !createdId().empty(); }, ms) != 0;
    }
    // createdId 是单槽（最近一次 CONV_CREATED）：同一客户端二次建会话时，
    // 首次回执已使 created_ 非空，waitCreated 会立即返回 → 用「id 不再是 prev」区分两批回执。
    bool waitCreatedNew(const std::string& prev, int ms = 5000){
        return waitFor([&]{ return createdId() != prev; }, ms) != 0;
    }
    bool waitConvListEpoch(int e0, int ms = 5000){ return waitFor([&]{ return convListEpoch_.load() > e0; }, ms) != 0; }
    bool waitOpened(int ms = 5000){
        return waitFor([&]{ return openedEpoch_.load() > 0; }, ms) != 0;
    }
    bool waitHistory(int e0, int ms = 5000){
        return waitFor([&]{ return historyEpoch_.load() > e0; }, ms) != 0;
    }
    bool waitPresence(const std::string& uid, bool online, int ms = 5000){
        return waitFor([&]{
            std::lock_guard<std::mutex> lk(m_);
            for(auto& p : presence_) if(p.first == uid && p.second == online) return true;
            return false;
        }, ms) != 0;
    }
    // 收到过该会话的 0x9B 变更推送
    bool waitConvChanged(const std::string& cid, int ms = 5000){
        return waitFor([&]{
            std::lock_guard<std::mutex> lk(m_);
            for(auto& id : convChangedIds_) if(id == cid) return true;
            return false;
        }, ms) != 0;
    }
    // 用户查找回执（epoch 超过 baseline 才算新）
    bool waitLookup(int e0, int ms = 5000){
        return waitFor([&]{ return lookupEpoch_.load() > e0; }, ms) != 0;
    }
    int  lookupEpoch() const { return lookupEpoch_.load(); }
    bool lookFound()   const { return lookFound_.load(); }
    // 消息数到达 n（broadcast 含 sender echo）
    bool waitMsgCount(int n, int ms = 8000){ return waitFor([&]{ return msgCount_.load() >= n; }, ms) != 0; }

    // ---- 读取 ----
    std::string createdId(){
        std::lock_guard<std::mutex> lk(m_);
        return created_;
    }
    long long   errCount() const { return errCount_.load(); }
    long long   msgCount() const { return msgCount_.load(); }       // 收到的广播数（含 echo）
    int         convListEpoch() const { return convListEpoch_.load(); }
    // 会话列表快照（v2 conv summaries）
    std::vector<chat::ImConvSummary> convs(){
        std::lock_guard<std::mutex> lk(m_); return convs_;
    }
    chat::ImConvSummary findConv(const std::string& cid){
        std::lock_guard<std::mutex> lk(m_);
        for(auto& c : convs_) if(c.convId == cid) return c;
        return {};
    }
    unsigned unreadOf(const std::string& cid){
        return findConv(cid).unread;
    }
    // 收到的消息（(conv, fromUid, fromNick, msg, tsMs) 按接收序）
    std::vector<std::tuple<std::string,std::string,std::string,std::string,uint64_t>> msgs(){
        std::lock_guard<std::mutex> lk(m_); return msgs_;
    }
    // 历史中所有 seq（页面内最旧→最新；跨页按请求序拼接由测试负责切片）
    std::vector<uint64_t> histSeqAll(){
        std::lock_guard<std::mutex> lk(m_);
        std::vector<uint64_t> out;
        for(auto& e : historyMsgs_) out.push_back(e.seq);
        return out;
    }
    int historyEpoch(){ return historyEpoch_.load(); }
    size_t historySize(){ std::lock_guard<std::mutex> lk(m_); return historyMsgs_.size(); }
    std::vector<chat::ImHistoryEntry> historySlice(size_t from){
        std::lock_guard<std::mutex> lk(m_);
        if(from >= historyMsgs_.size()) return {};
        return std::vector<chat::ImHistoryEntry>(historyMsgs_.begin() + static_cast<std::ptrdiff_t>(from),
                                                 historyMsgs_.end());
    }
    std::vector<chat::ImMember> members(){
        std::lock_guard<std::mutex> lk(m_); return members_;
    }
    uint64_t openedCursor(){ return openedCursor_.load(); }
    bool authOk(){ return authOk_.load(); }
    int convChangedEpoch(){ return convChangedEpoch_.load(); }
    std::vector<uint64_t> bcastSeqs(){ std::lock_guard<std::mutex> lk(m_); return bcastSeqs_; }

private:
    void reader(){
        const int fd = fd_;                            // 线程启动后 fd 不再变
        while(!closed_.load()){
            uint8_t hdr[4];
            bool to = false;
            if(!recvN(fd, hdr, 4, to)){
                if(to && !closed_.load()) continue;
                break;
            }
            const uint32_t len = (static_cast<uint32_t>(hdr[0]) << 24) | (static_cast<uint32_t>(hdr[1]) << 16)
                               | (static_cast<uint32_t>(hdr[2]) << 8)  |  static_cast<uint32_t>(hdr[3]);
            if(len > 1u << 20) break;
            std::string payload(len, '\0');
            if(!recvN(fd, &payload[0], len, to)){
                if(to && !closed_.load()) continue;
                break;
            }
            const chat::ImDecoded d = chat::imDecode(payload);
            switch(d.type){
            case chat::IM_AUTH_OK: authOk_.store(true); break;
            case chat::IM_CONV_CREATED:{
                std::lock_guard<std::mutex> lk(m_);
                created_ = d.conv;
                break;
            }
            case chat::IM_CONV_LIST_R:{
                std::lock_guard<std::mutex> lk(m_);
                convs_ = d.convs;
                convListEpoch_.fetch_add(1);
                break;
            }
            case chat::IM_CONV_OPENED:
                openedCursor_.store(d.seq);
                openedEpoch_.fetch_add(1);
                break;
            case chat::IM_HISTORY_R:{
                std::lock_guard<std::mutex> lk(m_);
                for(auto& e : d.history) historyMsgs_.push_back(e);
                historyEpoch_.fetch_add(1);
                break;
            }
            case chat::IM_MSG_R:{
                std::lock_guard<std::mutex> lk(m_);
                msgCount_++;
                msgs_.emplace_back(d.conv, d.fromUid, d.fromNick, d.msg, d.tsMs);
                bcastSeqs_.push_back(d.seq);         // 0x96 广播 seq（F4 断言单调一致）
                break;
            }
            case chat::IM_MEMBERS_R:{
                std::lock_guard<std::mutex> lk(m_);
                members_ = d.membersResp;
                break;
            }
            case chat::IM_PRESENCE:{
                std::lock_guard<std::mutex> lk(m_);
                presence_.emplace_back(d.uid, d.online);
                break;
            }
            case chat::IM_CONV_CHANGED:{
                std::lock_guard<std::mutex> lk(m_);
                convChangedIds_.push_back(d.conv);
                convChangedEpoch_.fetch_add(1);
                break;
            }
            case chat::IM_USER_LOOKUP_R:
                lookFound_.store(d.lookFound);
                lookupEpoch_.fetch_add(1);
                break;
            case chat::S2C_ERROR:
                errCount_.fetch_add(1);
                break;
            default: break;                            // 未知/测试不关心的帧：忽略
            }
        }
    }

    int fd_ = -1;
    std::atomic<bool> closed_{false}, authOk_{false};
    std::atomic<long long> errCount_{0}, msgCount_{0};
    std::atomic<int> convListEpoch_{0}, openedEpoch_{0}, historyEpoch_{0};
    std::atomic<int> convChangedEpoch_{0};
    std::atomic<int> lookupEpoch_{0};                // 0x9C 用户查找回执（epoch 判定新回执）
    std::atomic<bool> lookFound_{false};             // 最近一次 0x9C 的 found
    std::atomic<uint64_t> openedCursor_{0};
    std::thread rd_;
    std::mutex m_;
    std::string created_;                             // 最近一次 CONV_CREATED 的 conv_id
    std::vector<chat::ImConvSummary> convs_;
    std::vector<std::tuple<std::string,std::string,std::string,std::string,uint64_t>> msgs_;
    std::vector<std::pair<std::string,bool>> presence_;
    std::vector<std::string> convChangedIds_;         // 0x9B 推送过的会话
    std::vector<uint64_t> bcastSeqs_;                 // 收到的 0x96 广播 seq（按接收序）
    std::vector<chat::ImHistoryEntry> historyMsgs_;
    std::vector<chat::ImMember> members_;
};

// ---------------------------------------------------------------------------
// 便捷：连接 + 监听 + 注册 + 认证（返回已认证 client）
// ---------------------------------------------------------------------------
static bool regAuth(ImClient& c, int port, const std::string& uid, const std::string& nick,
                    const std::string& pwd){
    if(!c.connect(port)){ std::cout << "    connect fail\n"; return false; }
    c.listen();
    if(!c.sendRegister(uid, nick, pwd)) return false;
    if(!c.sendAuth(uid, pwd)) return false;
    return c.waitAuthOk();
}

// F1 注册：新 uid 成功（随后 AUTH 收 OK）；重复注册 → S2C_ERROR
static bool f1(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a;
        if(!a.connect(port)) return false;
        a.listen();
        if(!a.sendRegister("f1a", "alice", "pw1")) return false;
        // 无显式注册回执：随后 AUTH 成功即证明注册已生效（服务端契约）
        if(!a.sendAuth("f1a", "pw1")) return false;
        const bool authOk = a.waitAuthOk();
        // 重复注册 → S2C_ERROR
        if(!a.sendRegister("f1a", "alice2", "pw1")) return false;
        const bool dupErr = waitFor([&]{ return a.errCount() > 0; }, 3000) != 0;
        std::cout << "    authOk=" << authOk << " dupRegisterErr=" << dupErr << "\n";
        return authOk && dupErr;
    }();
    std::cout << "[F1] register: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F2 认证：对的口令成功；错口令 / 未注册 → S2C_ERROR
static bool f2(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient c;
        if(!c.connect(port)) return false;
        c.listen();
        c.sendRegister("f2a", "bob", "pw2");
        if(!c.sendAuth("f2a", "pw2")) return false;
        const bool okAuth = c.waitAuthOk();
        // 错口令（另一个 32B 摘要）
        const long long e0 = c.errCount();
        if(!c.sendAuthRaw("f2a", pass32("wrong-password"))) return false;
        const bool wrongRej = waitFor([&]{ return c.errCount() > e0; }, 3000) != 0;
        // 未注册 uid
        const long long e1 = c.errCount();
        if(!c.sendAuthRaw("f2ghost", pass32("pw2"))) return false;
        const bool ghostRej = waitFor([&]{ return c.errCount() > e1; }, 3000) != 0;
        std::cout << "    okAuth=" << okAuth << " wrongPwdRej=" << wrongRej
                  << " ghostRej=" << ghostRej << "\n";
        return okAuth && wrongRej && ghostRej;
    }();
    std::cout << "[F2] auth: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F3 建 DM：A 建 → CREATED 回执；B 侧 CONV_LIST 见同 id；A 再建同 DM → 同 conv_id（幂等）
static bool f3(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b;
        if(!regAuth(a, port, "f3a", "alice", "pw") ||
           !regAuth(b, port, "f3b", "bob",   "pw")) return false;

        if(!a.sendConvCreate(chat::kImConvDm, "", {"f3b"})) return false;
        if(!a.waitCreated()){ std::cout << "    a created timeout\n"; return false; }
        const std::string id1 = a.createdId();

        // B 侧 CONV_LIST 应见同 id
        const int e0 = b.convListEpoch();
        if(!b.sendConvList()) return false;
        if(!b.waitConvListEpoch(e0)){ std::cout << "    b convlist timeout\n"; return false; }
        const bool bSees = b.findConv(id1).type == chat::kImConvDm;

        // A 再建同 DM → 幂等：同 conv_id
        if(!a.sendConvCreate(chat::kImConvDm, "", {"f3b"})) return false;
        if(!a.waitCreated()){ std::cout << "    re-create timeout\n"; return false; }
        const std::string id2 = a.createdId();
        const bool idem = (id1 == id2) && id1 == chat::imDmConvId("f3a", "f3b");
        std::cout << "    id1=" << id1 << " id2=" << id2 << " bSees=" << bSees << " idem=" << idem << "\n";
        return bSees && idem;
    }();
    std::cout << "[F3] DM create idempotent: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F4 DM 收发：A→B 发 3 条，双方各收 3 条（含 sender echo），ts 严格递增
static bool f4(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b;
        if(!regAuth(a, port, "f4a", "alice", "pw") ||
           !regAuth(b, port, "f4b", "bob",   "pw")) return false;
        if(!a.sendConvCreate(chat::kImConvDm, "", {"f4b"})) return false;
        if(!a.waitCreated()) return false;
        const std::string cid = a.createdId();

        a.sendMsg(cid, "m1"); a.sendMsg(cid, "m2"); a.sendMsg(cid, "m3");
        const bool cntA = a.waitMsgCount(3, 5000);
        const bool cntB = b.waitMsgCount(3, 5000);
        auto msA = a.msgs(), msB = b.msgs();
        bool okA = cntA, okB = cntB;
        // ts 单调非降 + 首个 >0（ms 分辨率下同毫秒突发 ts 相等属正常；
        // 核心不变量是「广播 ts 与落库 ts 同源」→ 同批内不会回退）
        auto tsOk = [](const auto& ms){
            uint64_t last = 0; bool first = true, nonDec = true;
            for(auto& [conv, fromUid, fromNick, msg, ts] : ms){
                if(first){ if(ts == 0) return false; first = false; }
                if(ts < last) nonDec = false;
                last = ts;
            }
            return nonDec;
        };
        bool fromA = true;
        for(auto& [conv, fromUid, fromNick, msg, ts] : msA) if(fromUid != "f4a") fromA = false;
        for(auto& [conv, fromUid, fromNick, msg, ts] : msB) if(fromUid != "f4a") fromA = false;
        okA = okA && tsOk(msA) && fromA;
        okB = okB && tsOk(msB) && fromA;
        // 0x96 广播带 seq：新会话前 3 条应为 0,1,2 严格递增 —— 客户端并集去重基线依赖它，
        // 防「广播不带 seq」回归（那样重连会把已读的实时消息再补一遍）。
        const auto bsA = a.bcastSeqs();
        bool bcastOk = bsA.size() == 3 && bsA[0] == 0 && bsA[1] == 1 && bsA[2] == 2;
        std::cout << "    a=" << msA.size() << " b=" << msB.size()
                  << " tsOkA=" << tsOk(msA) << " tsOkB=" << tsOk(msB) << " from=A=" << fromA
                  << " bcastSeq0..2=" << bcastOk << "\n";
        return okA && okB && bcastOk;
    }();
    std::cout << "[F4] DM send/receive: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F5 建群 3 人：全员 CONV_LIST 可见；广播 3/3；B 退群后不再收
static bool f5(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b, c;
        if(!regAuth(a, port, "f5a", "alice", "pw") ||
           !regAuth(b, port, "f5b", "bob",   "pw") ||
           !regAuth(c, port, "f5c", "carol", "pw")) return false;

        if(!a.sendConvCreate(chat::kImConvGrp, "room5", {"f5b", "f5c"})) return false;
        if(!a.waitCreated()) return false;
        const std::string cid = a.createdId();

        // 全员 CONV_LIST 可见
        bool allSee = true;
        for(auto* cli : {&a, &b, &c}){
            const int e0 = cli->convListEpoch();
            if(!cli->sendConvList()) return false;
            if(!cli->waitConvListEpoch(e0)) return false;
            if(cli->findConv(cid).type != chat::kImConvGrp) allSee = false;
        }

        // 广播：a 发 1 条 → 三端各收 1（含 sender echo）
        a.sendMsg(cid, "hi");
        const bool all3 = a.waitMsgCount(1) && b.waitMsgCount(1) && c.waitMsgCount(1);

        // B 退群 → 之后 a 再发，b 不再收；a/c 各 +1。
        // 确定性栅栏：B 反复拉 CONV_LIST 直到该会话从列表消失（B 同连接 FIFO ⇒ leave
        // 必已落注册表与 kv），此时 a 再发则广播成员集必然不含 b —— 跨连接竞态消除。
        if(!b.sendConvLeave(cid)) return false;
        bool bLeft = false;
        for(int i = 0; i < 100 && !bLeft; ++i){
            const int be = b.convListEpoch();
            if(!b.sendConvList()) break;
            b.waitConvListEpoch(be, 500);
            bLeft = b.findConv(cid).type == 0;               // type==0 = 列表已无该会话
            if(!bLeft) std::this_thread::sleep_for(30ms);
        }
        a.sendMsg(cid, "after-leave");
        const bool aGot2 = a.waitMsgCount(2, 5000);
        const bool cGot2 = c.waitMsgCount(2, 5000);
        std::this_thread::sleep_for(250ms);
        const bool bSilent = b.msgCount() == 1;               // 退群后未再收
        std::cout << "    allSee=" << allSee << " bcast3/3=" << all3
                  << " bLeft=" << bLeft
                  << " a=" << a.msgCount() << " c=" << c.msgCount()
                  << " bSilent=" << bSilent << "\n";
        return allSee && all3 && bLeft && aGot2 && cGot2 && bSilent;
    }();
    std::cout << "[F5] group + leave: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F6 未读：B 离线，A 发 3 条 → B 重连 AUTH → CONV_LIST unread=3 → B 打开+READ → 再拉 unread=0
static bool f6(int port, int& pass, int& fail){
    const bool ok = [&]{
        // 阶段 1：A、B 在线建 DM，B 断连
        const std::string cid = [&]{
            ImClient a, b;
            if(!regAuth(a, port, "f6a", "alice", "pw") ||
               !regAuth(b, port, "f6b", "bob",   "pw")) return std::string();
            if(!a.sendConvCreate(chat::kImConvDm, "", {"f6b"})) return std::string();
            if(!a.waitCreated()) return std::string();
            b.close();                                // B 离线（无 LEAVE）
            std::this_thread::sleep_for(100ms);       // 让断线清理落库（成员仍在，仅在线态移除）
            a.sendMsg(a.createdId(), "u1");
            a.sendMsg(a.createdId(), "u2");
            a.sendMsg(a.createdId(), "u3");
            if(!a.waitMsgCount(3, 5000)) return std::string();
            return a.createdId();
        }();
        if(cid.empty()){ std::cout << "    phase1 fail\n"; return false; }

        // 阶段 2：B 重连（全新连接）→ AUTH → CONV_LIST unread=3
        ImClient b;
        if(!b.connect(port)) return false;
        b.listen();
        if(!b.sendAuth("f6b", "pw")) return false;
        if(!b.waitAuthOk()){ std::cout << "    re-auth fail\n"; return false; }
        const int e0 = b.convListEpoch();
        if(!b.sendConvList()) return false;
        if(!b.waitConvListEpoch(e0)) return false;
        const unsigned unread3 = b.unreadOf(cid);

        // 阶段 3：打开 + 拉历史得最新 seq → READ → 再拉 unread=0
        if(!b.sendConvOpen(cid)) return false;
        if(!b.waitOpened()) return false;
        const int e1 = b.historyEpoch();
        if(!b.sendHistory(cid, 200, 0)) return false;
        if(!b.waitHistory(e1)) return false;
        auto seqs = b.histSeqAll();
        if(seqs.empty()) return false;
        const uint64_t lastSeq = seqs.back();
        if(!b.sendRead(cid, lastSeq)) return false;
        // READ 同步到 kv 后再拉列表（FIFO：READ 已落库才回）
        std::this_thread::sleep_for(60ms);
        const int e2 = b.convListEpoch();
        if(!b.sendConvList()) return false;
        if(!b.waitConvListEpoch(e2)) return false;
        const unsigned unread0 = b.unreadOf(cid);
        std::cout << "    unread3=" << unread3 << " lastSeq=" << lastSeq
                  << " unread0=" << unread0 << "\n";
        return unread3 == 3 && unread0 == 0;
    }();
    std::cout << "[F6] unread via cursor: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F7 在线态：B 断连 → A 收 0x9A(bob,0)；B 重建同 uid → A 收 0x9A(bob,1)
static bool f7(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b;
        if(!regAuth(a, port, "f7a", "alice", "pw") ||
           !regAuth(b, port, "f7b", "bob",   "pw")) return false;
        if(!a.sendConvCreate(chat::kImConvDm, "", {"f7b"})) return false;
        if(!a.waitCreated()) return false;

        b.close();                                    // ① B 断连 → A 应收 (bob,0)
        const bool gotOff = a.waitPresence("f7b", false, 5000);

        // ② B 重建同 uid（新连接）→ A 应收 (bob,1)
        ImClient b2;
        if(!b2.connect(port)) return false;
        b2.listen();
        if(!b2.sendAuth("f7b", "pw")) return false;
        if(!b2.waitAuthOk()) return false;
        const bool gotOn = a.waitPresence("f7b", true, 5000);
        std::cout << "    gotOff=" << gotOff << " gotOn=" << gotOn << "\n";
        return gotOff && gotOn;
    }();
    std::cout << "[F7] presence: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F8 历史分页：250 条 → HISTORY(200,0) + HISTORY(50,before) 无重叠无漏 → seq 拼接 = 0..249
static bool f8(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b;
        if(!regAuth(a, port, "f8a", "alice", "pw") ||
           !regAuth(b, port, "f8b", "bob",   "pw")) return false;
        if(!a.sendConvCreate(chat::kImConvDm, "", {"f8b"})) return false;
        if(!a.waitCreated()) return false;
        const std::string cid = a.createdId();

        for(int i = 0; i < 250; ++i) a.sendMsg(cid, "m" + std::to_string(i));
        if(!a.waitMsgCount(250, 15000)){ std::cout << "    sender echo incomplete\n"; return false; }

        // 页 1：最新 200 条（before=0）
        const size_t base = b.historySize();
        const int e0 = b.historyEpoch();
        if(!b.sendHistory(cid, 200, 0)) return false;
        if(!b.waitHistory(e0)) return false;
        auto p1 = b.historySlice(base);               // 最旧→最新：seq 50..249
        const uint64_t firstOfP2 = p1.empty() ? 0 : p1.front().seq;

        // 页 2：严格早于 firstOfP2 的 50 条：seq 0..49
        const size_t base2 = b.historySize();
        const int e1 = b.historyEpoch();
        if(!b.sendHistory(cid, 50, static_cast<uint32_t>(firstOfP2))) return false;
        if(!b.waitHistory(e1)) return false;
        auto p2 = b.historySlice(base2);              // 最旧→最新：seq 0..49

        std::vector<uint64_t> got;
        for(auto& e : p2) got.push_back(e.seq);
        for(auto& e : p1) got.push_back(e.seq);

        bool ok = got.size() == 250;
        for(size_t i = 0; ok && i < got.size(); ++i) ok = (got[i] == i);
        std::cout << "    p1=" << p1.size() << " p2=" << p2.size()
                  << " joint=" << got.size() << " is0..249=" << ok << "\n";
        return ok;
    }();
    std::cout << "[F8] history paging: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F10 非法兜底：未认证发 MSG → ERROR；超大帧不崩；退 dm → ERROR
static bool f10(int port, int& pass, int& fail){
    const bool ok = [&]{
        // ① 未认证连接直接发 MSG → S2C_ERROR
        ImClient ghost;
        if(!ghost.connect(port)) return false;
        ghost.listen();
        ghost.sendMsg("dm:x:y", "unauth");
        const bool unauthedRej = ghost.waitErr(3000);

        // ② 超大帧：长度头超 codec 上限 → 服务端丢弃不崩（随后新连接正常，即未崩）
        ImClient x;
        if(!x.connect(port)) return false;
        x.listen();
        std::string big(70000, 'x');
        big[0] = static_cast<char>(chat::IM_MSG);
        x.sendPayload(big);
        ImClient n;
        if(!regAuth(n, port, "f10n", "neo", "pw")) return false;    // 若服务端已崩，这里必失败
        x.close();

        // ③ 退 dm → ERROR
        ImClient a;
        if(!regAuth(a, port, "f10a", "alice", "pw")) return false;
        if(!a.sendConvCreate(chat::kImConvDm, "", {"f10n"})) return false;
        if(!a.waitCreated()) return false;
        const long long e0 = a.errCount();
        if(!a.sendConvLeave(a.createdId())) return false;
        const bool dmLeaveRej = waitFor([&]{ return a.errCount() > e0; }, 3000) != 0;
        std::cout << "    unauthRej=" << unauthedRej << " oversizedNoCrash=" << true
                  << " dmLeaveRej=" << dmLeaveRej << "\n";
        return unauthedRej && dmLeaveRej;
    }();
    std::cout << "[F10] edge cases: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F11 变更推送：B 在线时 A 建 DM/群 → B（及 C）收 0x9B，随后 0x93 含新会话（无需断连重连）
static bool f11(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b, c;
        if(!regAuth(a, port, "f11a", "alice", "pw") ||
           !regAuth(b, port, "f11b", "bob",   "pw") ||
           !regAuth(c, port, "f11c", "carol", "pw")) return false;

        // A 建 DM(f11b)：B 在线 → B 收 0x9B(dm)
        if(!a.sendConvCreate(chat::kImConvDm, "", {"f11b"})) return false;
        if(!a.waitCreated()){ std::cout << "    dm create timeout\n"; return false; }
        const std::string dm = a.createdId();
        const bool bGotDmPush = b.waitConvChanged(dm, 5000);

        // A 建群(f11b, f11c)：B、C 各收 0x9B
        if(!a.sendConvCreate(chat::kImConvGrp, "room11", {"f11b", "f11c"})) return false;
        if(!a.waitCreatedNew(dm)){ std::cout << "    grp create timeout\n"; return false; } // 等第二批回执
        const std::string g11 = a.createdId();
        const bool bGotGrpPush = b.waitConvChanged(g11, 5000);
        const bool cGotGrpPush = c.waitConvChanged(g11, 5000);

        // B 拉 0x93：DM + 群都可见（不再需要断连重连）
        const int e0 = b.convListEpoch();
        if(!b.sendConvList()) return false;
        if(!b.waitConvListEpoch(e0)){ std::cout << "    b list timeout\n"; return false; }
        const bool bSeesDm  = b.findConv(dm).type  == chat::kImConvDm;
        const bool bSeesGrp = b.findConv(g11).type == chat::kImConvGrp;
        std::cout << "    bGotDmPush=" << bGotDmPush << " bGotGrpPush=" << bGotGrpPush
                  << " cGotGrpPush=" << cGotGrpPush << " bListDm=" << bSeesDm
                  << " bListGrp=" << bSeesGrp << "\n";
        return bGotDmPush && bGotGrpPush && cGotGrpPush && bSeesDm && bSeesGrp;
    }();
    std::cout << "[F11] conv-change push: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F12 入群：非成员 B join → 0x92 收同 cid；A 收 0x9B 且成员含 B；
//     重复 join 幂等（不重复推 0x9B）；不存在的群 / 加 dm → ERROR
static bool f12(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b, c;
        if(!regAuth(a, port, "f12a", "alice", "pw") ||
           !regAuth(b, port, "f12b", "bob",   "pw") ||
           !regAuth(c, port, "f12c", "carol", "pw")) return false;

        // 建群 room12（成员 a、c；b 不在）
        if(!a.sendConvCreate(chat::kImConvGrp, "room12", {"f12c"})) return false;
        if(!a.waitCreated()){ std::cout << "    room12 create timeout\n"; return false; }
        const std::string cid = a.createdId();

        // b 入群：0x92 给 b，0x9B 推给其余在线成员 a
        const bool joined = [&]{
            const long long e0 = b.errCount();        // 隔离旧的 S2C_ERROR
            if(!b.sendConvJoin(cid)) return false;
            if(!b.waitCreated()) return false;
            if(b.createdId() != cid) return false;
            if(b.errCount() != e0) return false;      // join 成功路径不得带 ERROR
            return true;
        }();
        const bool aGotPush = a.waitConvChanged(cid, 5000);

        // A 成员列表含 b
        bool aHasB = false;
        {
            a.sendMembers(cid);
            aHasB = waitFor([&]{
                for(auto& mm : a.members()) if(mm.uid == "f12b") return true;
                return false;
            }, 5000) != 0;
        }

        // 重复 join 幂等：0x92 同 cid 回执，且不重复推 0x9B（A 的 epoch 不再 +1）
        const int ac0 = a.convChangedEpoch();
        const bool idem = [&]{
            if(!b.sendConvJoin(cid)) return false;
            if(!b.waitCreated()) return false;
            if(b.createdId() != cid) return false;
            std::this_thread::sleep_for(200ms);        // 留给服务端可能的（错误）推送
            return a.convChangedEpoch() == ac0;       // 无重复 push
        }();

        // 错误路径：不存在的群 → ERROR；加 dm → ERROR
        const bool errNoSuch = [&]{
            const long long e0 = b.errCount();
            if(!b.sendConvJoin("grp:424242")) return false;
            return waitFor([&]{ return b.errCount() > e0; }, 3000) != 0;
        }();
        const bool errDm = [&]{
            // A 建 dm(a,b)（推 0x9B 给 b，不触发 ERROR）；随后 b 加该 dm → ERROR
            if(!a.sendConvCreate(chat::kImConvDm, "", {"f12b"})) return false;
            if(!a.waitCreatedNew(cid)){ std::cout << "    dm(2nd) create timeout\n"; return false; } // 等 dmid 回执
            const std::string dm = a.createdId();
            const long long e1 = b.errCount();
            if(!b.sendConvJoin(dm)) return false;
            return waitFor([&]{ return b.errCount() > e1; }, 3000) != 0;
        }();
        std::cout << "    joined=" << joined << " aGotPush=" << aGotPush
                  << " aHasB=" << aHasB << " idem=" << idem
                  << " errNoSuch=" << errNoSuch << " errDm=" << errDm << "\n";
        return joined && aGotPush && aHasB && idem && errNoSuch && errDm;
    }();
    std::cout << "[F12] join group: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F13 退群推送：B 退群 → 剩余成员 A 收 0x9B(cid)；A 成员列表不再含 B；B 自己不收 0x9B
static bool f13(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b, c;
        if(!regAuth(a, port, "f13a", "alice", "pw") ||
           !regAuth(b, port, "f13b", "bob",   "pw") ||
           !regAuth(c, port, "f13c", "carol", "pw")) return false;

        // 建群 room13（b、c 是成员）→ 建群推送 b、c 各收 0x9B；创建者 a 不收
        if(!a.sendConvCreate(chat::kImConvGrp, "room13", {"f13b", "f13c"})) return false;
        if(!a.waitCreated()){ std::cout << "    room13 create timeout\n"; return false; }
        const std::string cid = a.createdId();
        const bool bGotCreate = b.waitConvChanged(cid, 5000);
        const bool cGotCreate = c.waitConvChanged(cid, 5000);
        const int b0 = b.convChangedEpoch();           // B 退群前的推送计数

        // B 退群 → A 收 0x9B(cid)（A 自建群时未收过，wait 语义干净）
        if(!b.sendConvLeave(cid)) return false;
        const bool aGotLeavePush = a.waitConvChanged(cid, 5000);

        // A 成员列表不再含 B（A+C 仍在，>=2）
        bool membNoB = false;
        {
            a.sendMembers(cid);
            membNoB = waitFor([&]{
                const auto mm = a.members();
                if(mm.size() < 2) return false;
                for(auto& m : mm) if(m.uid == "f13b") return false;
                return true;
            }, 5000) != 0;
        }
        std::this_thread::sleep_for(200ms);
        const bool bNoPush = b.convChangedEpoch() == b0;   // 退群者被排除：不给自己推 0x9B
        std::cout << "    bGotCreate=" << bGotCreate << " cGotCreate=" << cGotCreate
                  << " aGotLeavePush=" << aGotLeavePush << " aMembNoB=" << membNoB
                  << " bNoPush=" << bNoPush << "\n";
        return bGotCreate && cGotCreate && aGotLeavePush && membNoB && bNoPush;
    }();
    std::cout << "[F13] leave pushes to rest: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// F14 用户查找 + 会话列表预览（v3 UI 前置）：0x1B 查到/查不到/空 uid；0x93 lastMsg
//   （dm=裸消息；grp=服务端拼 `发送者昵称: 消息`）+ lastTsMs>0
static bool f14(int port, int& pass, int& fail){
    const bool ok = [&]{
        ImClient a, b;
        if(!regAuth(a, port, "f14a", "nA", "pw") ||
           !regAuth(b, port, "f14b", "nB", "pw")) return false;

        // ① 查找：f14b 存在 → found=1；ghost 不存在 → found=0（活帧，非 ERROR）；空 uid → ERROR
        const bool lookHit = [&]{
            const int e0 = a.lookupEpoch();
            if(!a.sendUserLookup("f14b")) return false;
            if(!a.waitLookup(e0)){ std::cout << "    lookup-hit timeout\n"; return false; }
            return a.lookFound();
        }();
        const bool lookMiss = [&]{
            const int e0 = a.lookupEpoch();
            if(!a.sendUserLookup("ghost_zz")) return false;
            if(!a.waitLookup(e0)){ std::cout << "    lookup-miss timeout\n"; return false; }
            return !a.lookFound();
        }();
        const bool lookBad = [&]{
            const long long e0 = a.errCount();
            if(!a.sendUserLookup("")) return false;
            return waitFor([&]{ return a.errCount() > e0; }, 3000) != 0;
        }();

        // ② dm 预览：a 发 1 条 → a 的 0x93 该 dm lastMsg=裸消息、lastTsMs>0
        if(!a.sendConvCreate(chat::kImConvDm, "", {"f14b"})) return false;
        if(!a.waitCreated()){ std::cout << "    dm create timeout\n"; return false; }
        const std::string dm = a.createdId();
        if(!a.sendMsg(dm, "hi2b")) return false;
        if(!a.waitMsgCount(1)) return false;
        const bool dmPrev = [&]{
            const int e0 = a.convListEpoch();
            if(!a.sendConvList()) return false;
            if(!a.waitConvListEpoch(e0)) return false;
            const auto c = a.findConv(dm);
            return c.lastMsg == "hi2b" && c.lastTsMs > 0;
        }();

        // ③ 群预览：a 建群(a,b)+发 1 条 → a 的 0x93 该群 lastMsg=`nA: ghi`（服务端拼昵称）
        if(!a.sendConvCreate(chat::kImConvGrp, "g14", {"f14b"})) return false;
        if(!a.waitCreatedNew(dm)){ std::cout << "    grp create timeout\n"; return false; }
        const std::string g14 = a.createdId();
        if(!a.sendMsg(g14, "ghi")) return false;
        const bool grpPrev = [&]{
            const int e0 = a.convListEpoch();
            if(!a.sendConvList()) return false;
            if(!a.waitConvListEpoch(e0)) return false;
            const auto c = a.findConv(g14);
            return c.lastMsg == "nA: ghi" && c.lastTsMs > 0;
        }();
        std::cout << "    lookHit=" << lookHit << " lookMiss=" << lookMiss
                  << " lookBad=" << lookBad << " dmPrev=" << dmPrev
                  << " grpPrev=" << grpPrev << "\n";
        return lookHit && lookMiss && lookBad && dmPrev && grpPrev;
    }();
    std::cout << "[F14] user lookup + last-msg preview: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

// ---------------------------------------------------------------------------
// F9 重启持久化（两阶段，配合 regress 段 (7b)：seed9 → 重启 im_server → verify9）
// ---------------------------------------------------------------------------
static bool seed9(int port, int n){
    ImClient a, b, c;
    if(!regAuth(a, port, "f9a", "alice", "pw") ||
       !regAuth(b, port, "f9b", "bob",   "pw") ||
       !regAuth(c, port, "f9c", "carol", "pw")) return false;
    if(!a.sendConvCreate(chat::kImConvGrp, "persist9", {"f9b", "f9c"})) return false;
    if(!a.waitCreated()){ std::cout << "  created timeout\n"; return false; }
    const std::string cid = a.createdId();
    for(int i = 1; i <= n; ++i) a.sendMsg(cid, "p" + std::to_string(i));
    if(!a.waitMsgCount(n, 15000)){ std::cout << "  echo incomplete\n"; return false; }
    if(!a.sendRead(cid, static_cast<uint64_t>(n))) return false;   // 已读全部 → 重启后 unread=0
    std::this_thread::sleep_for(300ms);               // 让游标落 WAL
    std::cout << "[seed9] cid=" << cid << " msgs=" << n << " READ ok\n";
    return true;
}
static bool verify9(int port, int n){
    ImClient a;
    if(!a.connect(port)) return false;
    a.listen();
    if(!a.sendAuth("f9a", "pw")) return false;
    if(!a.waitAuthOk()){ std::cout << "  re-auth fail\n"; return false; }
    const int e0 = a.convListEpoch();
    if(!a.sendConvList()) return false;
    if(!a.waitConvListEpoch(e0)) return false;
    auto convs = a.convs();
    bool found = false; std::string cid;
    unsigned unread = 0;
    for(auto& cv : convs){
        if(cv.name == "persist9" && cv.type == chat::kImConvGrp){
            found = true; cid = cv.convId; unread = cv.unread;
        }
    }
    // 成员全在
    bool memb3 = false;
    if(found){
        a.sendMembers(cid);
        memb3 = waitFor([&]{ return a.members().size() >= 3; }, 3000) != 0;
    }
    // 历史全在
    bool histN = false;
    if(found){
        const int e1 = a.historyEpoch();
        if(!a.sendHistory(cid, 200, 0)) return false;
        if(!a.waitHistory(e1)) return false;
        histN = a.historySize() == static_cast<size_t>(n);
    }
    std::cout << "  convFound=" << found << " unread=" << unread << " unread0=" << (unread == 0)
              << " members3=" << memb3 << " historyN=" << histN << "\n";
    return found && unread == 0 && memb3 && histN;
}

int main(int argc, char** argv){
    int port = 8891;
    int seed9N = 0, verify9N = 0;
    for(int i = 1; i + 1 < argc; ++i){
        if(std::string(argv[i]) == "--port")    port    = std::atoi(argv[i+1]);
        if(std::string(argv[i]) == "--seed9")   seed9N   = std::atoi(argv[i+1]);
        if(std::string(argv[i]) == "--verify9") verify9N = std::atoi(argv[i+1]);
    }
    if(seed9N > 0)   return seed9(port, seed9N)   ? 0 : 1;
    if(verify9N > 0) return verify9(port, verify9N) ? 0 : 1;

    int pass = 0, fail = 0;
    f1(port, pass, fail);
    f2(port, pass, fail);
    f3(port, pass, fail);
    f4(port, pass, fail);
    f5(port, pass, fail);
    f6(port, pass, fail);
    f7(port, pass, fail);
    f8(port, pass, fail);
    f10(port, pass, fail);
    f11(port, pass, fail);
    f12(port, pass, fail);
    f13(port, pass, fail);
    f14(port, pass, fail);
    std::cout << "[im_test] PASS=" << pass << " FAIL=" << fail << "\n";
    return fail > 0 ? 1 : 0;
}