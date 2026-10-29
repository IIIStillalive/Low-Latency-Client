// Day10 聊天室端到端：G1–G5 收敛断言（配合 chat_server 运行）。
//
//   ./build/chat_server --port 8890 --threads 2 &      # regress.sh 用 2 worker（跨 loop 广播）
//   ./build/chat_room_test --port 8890                 # exit 0 = 全过
//
// 测试客户端用裸阻塞 socket + 每客户端一个读者线程（纯测试 harness，不走事件循环）：
//   帧 = [4B 大端长度][payload]，payload = 聊天帧（1B type，见 message_proto.hpp）。
// 判定矩阵（task_plan_chat_room.md §5）：
//   G1 基本广播：2 客户同房，A 发 3 条 → 双方各收 3 条 S2C_MSG，内容/顺序一致（含发送者 echo）
//   G2 跨 loop 无丢无重不乱序：6 客户 × 各发 200 条 → 每客户收 N×K，且每发送者 seq 单调递增
//   G3 断线清理：成员无 LEAVE 直接 close → 广播不崩溃、不投给死连接；其余成员计数正确
//   G4 非法帧：未知 type / 超大帧 → 服务端不回崩溃，后续正常 join 可用
//   G5 LEAVE + 同名重建：leave 后退房，重新 join 同房同昵称 → 无残留成员、无重复投递
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

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// 裸 socket 帧读写小工具
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
        if(w < 0 && (errno == EINTR)) continue;
        return false;                       // 0 / 真错误
    }
    return true;
}
// 返回 false：FIN / 错误 / 1s 接收超时（out=timeout=true）
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
static std::string framed(const std::string& payload){   // [4B 大端长度][payload]
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

// ---------------------------------------------------------------------------
// 测试客户端：连接 + 读者线程 + 计数
// ---------------------------------------------------------------------------
class ChatClient{
public:
    ChatClient(std::string room, std::string name): room_(std::move(room)), name_(std::move(name)){}
    ~ChatClient(){ close(); }

    bool connect(int port){ fd_ = connectTo(port); return fd_ >= 0; }
    void listen(){ rd_ = std::thread([this]{ reader(); }); }
    void close(){
        if(closed_.exchange(true)) return;
        if(fd_ >= 0){ ::shutdown(fd_, SHUT_RDWR); ::close(fd_); fd_ = -1; }  // shutdown 解开阻塞 recv
        if(rd_.joinable()) rd_.join();
    }
    bool sendPayload(const std::string& payload){
        const std::string f = framed(payload);
        return fd_ >= 0 && writeAll(fd_, f.data(), f.size());
    }
    bool sendMsg(const std::string& msg){ return sendPayload(chat::encodeMsg(room_, msg)); }
    bool sendJoin(){ return sendPayload(chat::encodeJoin(room_, name_)); }
    bool sendLeave(){ return sendPayload(chat::encodeLeave(room_)); }
    bool sendList(){ return sendPayload(chat::encodeList(room_)); }

    bool join(int timeoutMs = 3000){                 // 同步等 S2C_JOINED
        if(!sendJoin()) return false;
        return waitFor([this]{ return joined_.load(); }, timeoutMs) != 0 && joined_.load();
    }
    void resetJoined(){ joined_.store(false); }      // 重建 join 前清零，确保等本次回执
    // 等一条 S2C_MEMBERS 回执 —— 同一连接 FIFO：本连接先前帧（如 LEAVE）必已处理完
    bool waitMembers(int timeoutMs = 3000){
        const int e0 = membersEpoch_.load();
        return waitFor([&]{ return membersEpoch_.load() > e0; }, timeoutMs) != 0;
    }
    long long msgCount() const { return msgCount_.load(); }
    long long errCount() const { return errCount_.load(); }
    bool joined() const { return joined_.load(); }

    // 按接收序返回 (from, 序号) —— 序号取 msg body 的整数值（G2 用 to_string(k) 发送）
    std::vector<std::pair<std::string,int>> recvSeq(){
        std::lock_guard<std::mutex> lk(m_);
        return recv_;
    }
    std::vector<std::string> recvMsgs(){
        std::lock_guard<std::mutex> lk(m_);
        return msgs_;
    }

private:
    void reader(){
        const int fd = fd_;                            // 线程启动后 fd 不再变（close 仅 shutdown+close）
        while(!closed_.load()){
            uint8_t hdr[4];
            bool to = false;
            if(!recvN(fd, hdr, 4, to)){
                if(to && !closed_.load()) continue;    // 1s 无数据：ping 检查关闭标志
                break;                                 // FIN/错误/已关闭
            }
            const uint32_t len = (static_cast<uint32_t>(hdr[0]) << 24) | (static_cast<uint32_t>(hdr[1]) << 16)
                               | (static_cast<uint32_t>(hdr[2]) << 8)  |  static_cast<uint32_t>(hdr[3]);
            if(len > 1u << 20){ break; }               // 异常超大帧：放弃该连接
            std::string payload(len, '\0');
            if(!recvN(fd, &payload[0], len, to)){
                if(to && !closed_.load()) continue;
                break;
            }
            const chat::DecodedMsg d = chat::decode(payload);
            switch(d.type){
            case chat::S2C_JOINED: joined_.store(true); break;
            case chat::S2C_MSG:{
                std::lock_guard<std::mutex> lk(m_);
                msgCount_++;
                recv_.emplace_back(d.name, std::atoi(d.msg.c_str()));
                msgs_.push_back(d.msg);
                break;
            }
            case chat::S2C_ERROR: errCount_++; break;
            case chat::S2C_MEMBERS: membersEpoch_.fetch_add(1); break;
            default: break;                            // LEFT/未知：本测试不断言
            }
        }
    }

    std::string room_, name_;
    int fd_ = -1;
    std::atomic<bool> closed_{false}, joined_{false};
    std::atomic<long long> msgCount_{0}, errCount_{0};
    std::atomic<int> membersEpoch_{0};              // S2C_MEMBERS 到达次数（LIST 栅栏用）
    std::thread rd_;
    std::mutex m_;
    std::vector<std::pair<std::string,int>> recv_;     // (from, seq) 按接收序
    std::vector<std::string> msgs_;
};

// ---------------------------------------------------------------------------
// G1–G5
// ---------------------------------------------------------------------------
static bool g1(int port, int& pass, int& fail){
    const bool ok = [&]{
        ChatClient a("g1", "alice"), b("g1", "bob");
        if(!a.connect(port) || !b.connect(port)){ std::cout << "  connect fail\n"; return false; }
        a.listen(); b.listen();
        if(!a.join() || !b.join()){ std::cout << "  join fail\n"; return false; }
        a.sendMsg("hello1"); a.sendMsg("hello2"); a.sendMsg("hello3");

        const bool cntA = waitFor([&]{ return a.msgCount() == 3; }, 5000);
        const bool cntB = waitFor([&]{ return b.msgCount() == 3; }, 5000);
        const std::vector<std::string> expect = {"hello1", "hello2", "hello3"};
        const bool orderA = cntA && a.recvMsgs() == expect;   // 含发送者 echo + FIFO
        const bool orderB = cntB && b.recvMsgs() == expect;
        std::cout << "  a=" << a.msgCount() << " b=" << b.msgCount()
                  << " orderA=" << orderA << " orderB=" << orderB << "\n";
        return cntA && cntB && orderA && orderB;
    }();
    std::cout << "[G1] basic broadcast: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

static bool g2(int port, int& pass, int& fail){
    const bool ok = [&]{
        constexpr int N = 6, K = 200;
        std::vector<std::unique_ptr<ChatClient>> cs;
        for(int i = 0; i < N; ++i){
            cs.emplace_back(std::make_unique<ChatClient>("g2", "c" + std::to_string(i)));
            if(!cs.back()->connect(port)) return false;
            cs.back()->listen();
            if(!cs.back()->join()) return false;
        }
        // 并发发送：每个客户端各发 K 条（正文 = 序号 k，from = 昵称）
        std::vector<std::thread> senders;
        for(int i = 0; i < N; ++i){
            senders.emplace_back([&, i]{
                for(int k = 0; k < K; ++k) cs[i]->sendMsg(std::to_string(k));
            });
        }
        for(auto& t : senders) t.join();

        bool counts = true;
        for(int i = 0; i < N; ++i){
            if(!waitFor([&]{ return cs[i]->msgCount() == static_cast<long long>(N * K); }, 15000)){
                counts = false;
                std::cout << "  client" << i << " msgCount=" << cs[i]->msgCount()
                          << " want=" << (N * K) << "\n";
            }
        }
        // 逐连接核对：每个发送者恰好出现 K 次且序号严格递增（无丢、无重、单源不乱序）
        bool order = true, complete = true, range = true;
        for(int i = 0; i < N; ++i){
            std::map<std::string,int> count, last;
            for(const auto& [from, seq] : cs[i]->recvSeq()){
                count[from]++;
                if(seq >= K) range = false;
                if(last.count(from) && seq <= last[from]) order = false;   // 严格递增
                last[from] = seq;
            }
            if(static_cast<int>(count.size()) != N) complete = false;
            for(int j = 0; j < N; ++j){
                if(count["c" + std::to_string(j)] != K) complete = false;
            }
        }
        std::cout << "  counts=" << counts << " order=" << order
                  << " complete=" << complete << " range=" << range << "\n";
        return counts && order && complete && range;
    }();
    std::cout << "[G2] 6x200 cross-loop broadcast: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

static bool g3(int port, int& pass, int& fail){
    const bool ok = [&]{
        constexpr int M1 = 50, M2 = 50, M3 = 50, M4 = 30;
        ChatClient a("g3", "a"), b("g3", "b"), c("g3", "c");
        if(!a.connect(port) || !b.connect(port) || !c.connect(port)) return false;
        a.listen(); b.listen(); c.listen();
        if(!a.join() || !b.join() || !c.join()) return false;

        std::thread t1([&]{ for(int i = 0; i < M1; ++i) a.sendMsg("a"); });
        std::thread t2([&]{ for(int i = 0; i < M2; ++i) b.sendMsg("b"); });
        std::thread t3([&]{ for(int i = 0; i < M3; ++i) c.sendMsg("c"); });
        t1.join(); t2.join(); t3.join();

        c.close();                                     // 无 LEAVE 直接断线
        for(int i = 0; i < M4; ++i) a.sendMsg("a2");

        const int total = M1 + M2 + M3 + M4;           // 180
        const bool okA = waitFor([&]{ return a.msgCount() == total; }, 10000);
        const bool okB = waitFor([&]{ return b.msgCount() == total; }, 10000);
        std::cout << "  a=" << a.msgCount() << " b=" << b.msgCount()
                  << " want=" << total << " (c 断线, 其余计数完整)\n";
        return okA && okB;
    }();
    std::cout << "[G3] close without leave: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

static bool g4(int port, int& pass, int& fail){
    const bool ok = [&]{
        ChatClient junk("g4", "x");
        if(!junk.connect(port)) return false;
        junk.listen();
        // ① 未知 type 0xEE（合法帧）→ chat 层静默丢弃，连接不受影响
        std::string unk;
        unk.push_back(static_cast<char>(0xEE));
        unk.append("g4");
        junk.sendPayload(unk);
        // ② 空 payload（len=0 合法帧）→ ChatServer::onMessage 回 S2C_ERROR
        junk.sendPayload("");
        // ③ 超大帧：长度头 70000 > codec kMaxMsgLen(65536) → codec 层丢弃（不进业务回调）
        std::string big(70000, 'x');
        big[0] = static_cast<char>(chat::C2S_MSG);
        junk.sendPayload(big);
        // 判定：② 收到 S2C_ERROR；③ 后服务端仍存活（新客户可 join）；超大帧无消息回显
        ChatClient n("g4", "ok");
        if(!n.connect(port)) return false;
        n.listen();
        const bool alive = n.join();
        const bool err = waitFor([&]{ return junk.errCount() > 0; }, 5000);
        const bool bounced = waitFor([&]{ return junk.msgCount() > 0; }, 300) != 0;
        std::cout << "  serverAlive=" << alive << " emptyPayloadErr=" << err
                  << " noBounceOversized=" << !bounced << "\n";
        return alive && err && !bounced;
    }();
    std::cout << "[G4] garbage frames: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

static bool g5(int port, int& pass, int& fail){
    const bool ok = [&]{
        ChatClient a("g5", "a"), b("g5", "b"), e("g5", "echo");
        if(!a.connect(port) || !b.connect(port) || !e.connect(port)) return false;
        a.listen(); b.listen(); e.listen();
        if(!a.join() || !b.join() || !e.join()) return false;

        e.sendLeave();                                 // ① e 显式离开
        // 栅栏：同一连接上 LEAVE 紧接 LIST → 等到 S2C_MEMBERS 回执即证明 LEAVE 已被服务端
        // 处理完（同连接 FIFO），之后才有 a 的广播 —— 避免跨连接/跨 loop 乱序造成误判。
        e.sendList();
        if(!e.waitMembers(4000)){ std::cout << "  leave fence timeout\n"; return false; }
        a.sendMsg("after-leave");
        const bool leaveOk = waitFor([&]{ return a.msgCount() == 1 && b.msgCount() == 1; }, 5000);
        std::this_thread::sleep_for(200ms);            // 给 e 一点窗口：若未退房会收到
        const bool eSilent = e.msgCount() == 0;
        std::cout << "  after-leave a=" << a.msgCount() << " b=" << b.msgCount()
                  << " e=" << e.msgCount() << " (e 应 = 0)\n";

        e.resetJoined();                               // ② 同名重建：清掉首 join 回执标志
        if(!e.join()) return false;                    //     再入后才继续，避免状态未就绪
        a.sendMsg("after-rejoin");
        // a/b 应 +1；e 应恰收 1（同一昵称的新连接，不是残留成员，无重复投递）
        const bool rejoinOk = waitFor([&]{
            return a.msgCount() == 2 && b.msgCount() == 2 && e.msgCount() == 1;
        }, 5000);
        std::cout << "  after-rejoin a=" << a.msgCount() << " b=" << b.msgCount()
                  << " e=" << e.msgCount() << " (e 应恰 1，无残留)\n";
        return leaveOk && eSilent && rejoinOk;
    }();
    std::cout << "[G5] leave + same-name rejoin: " << (ok ? "PASS" : "FAIL") << "\n";
    ok ? ++pass : ++fail;
    return ok;
}

int main(int argc, char** argv){
    int port = 8890;
    for(int i = 1; i + 1 < argc; ++i){
        if(std::string(argv[i]) == "--port") port = std::atoi(argv[i+1]);
    }
    int pass = 0, fail = 0;
    g1(port, pass, fail);
    g2(port, pass, fail);
    g3(port, pass, fail);
    g4(port, pass, fail);
    g5(port, pass, fail);
    std::cout << "[chat_room_test] PASS=" << pass << " FAIL=" << fail << (fail ? "\n" : "\n");
    return fail > 0 ? 1 : 0;
}