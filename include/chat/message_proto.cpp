#include "chat/message_proto.hpp"

#include <utility>   // std::move（S2C_MEMBERS 收集昵称）

namespace chat {
namespace {

// 帧内小工具：游标式读 / 追加写
bool takeU8(const std::string& s, size_t& i, uint8_t& v){
    if(i >= s.size()) return false;
    v = static_cast<uint8_t>(s[i]);
    ++i;
    return true;
}
bool takeStr(const std::string& s, size_t& i, std::string& out){
    uint8_t n = 0;
    if(!takeU8(s, i, n)) return false;
    if(i + n > s.size()) return false;
    out.assign(s, i, n);
    i += n;
    return true;
}
bool takeU16(const std::string& s, size_t& i, uint16_t& v){
    if(i + 2 > s.size()) return false;
    v = static_cast<uint16_t>((static_cast<uint16_t>(static_cast<uint8_t>(s[i])) << 8)
                            |  static_cast<uint16_t>(static_cast<uint8_t>(s[i+1])));
    i += 2;
    return true;
}
bool takeU32(const std::string& s, size_t& i, uint32_t& v){
    if(i + 4 > s.size()) return false;
    v = (static_cast<uint32_t>(static_cast<uint8_t>(s[i])) << 24)
      | (static_cast<uint32_t>(static_cast<uint8_t>(s[i+1])) << 16)
      | (static_cast<uint32_t>(static_cast<uint8_t>(s[i+2])) <<  8)
      |  static_cast<uint32_t>(static_cast<uint8_t>(s[i+3]));
    i += 4;
    return true;
}
void putU8(std::string& out, uint8_t v){
    out.push_back(static_cast<char>(v));
}
bool putU16(std::string& out, uint16_t v){
    out.push_back(static_cast<char>((v >> 8) & 0xff));
    out.push_back(static_cast<char>( v        & 0xff));
    return true;
}
bool putU32(std::string& out, uint32_t v){
    out.push_back(static_cast<char>((v >> 24) & 0xff));
    out.push_back(static_cast<char>((v >> 16) & 0xff));
    out.push_back(static_cast<char>((v >>  8) & 0xff));
    out.push_back(static_cast<char>( v        & 0xff));
    return true;
}
bool putStr(std::string& out, const std::string& s){   // 长度 ≤ 255，超限失败
    if(s.size() > kMaxNameLen) return false;
    putU8(out, static_cast<uint8_t>(s.size()));
    out.append(s);
    return true;
}
void putU64(std::string& out, uint64_t v){             // 历史 seq：8B 大端（与键编码一致）
    for(int sh = 56; sh >= 0; sh -= 8)
        out.push_back(static_cast<char>((v >> sh) & 0xff));
}
bool takeU64(const std::string& s, size_t& i, uint64_t& v){
    if(i + 8 > s.size()) return false;
    v = 0;
    for(int k = 0; k < 8; ++k)
        v = (v << 8) | static_cast<uint8_t>(s[i + k]);
    i += 8;
    return true;
}

} // namespace

// ---- C2S ----
std::string encodeJoin(const std::string& room, const std::string& name){
    std::string b;
    b.reserve(room.size() + name.size() + 3);
    putU8(b, C2S_JOIN);
    if(!putStr(b, room) || !putStr(b, name)) return {};
    return b;
}
std::string encodeMsg(const std::string& room, const std::string& msg){
    std::string b;
    b.reserve(room.size() + msg.size() + 2);
    putU8(b, C2S_MSG);
    if(!putStr(b, room)) return {};
    b.append(msg);
    return b;
}
std::string encodeLeave(const std::string& room){
    std::string b;
    putU8(b, C2S_LEAVE);
    if(!putStr(b, room)) return {};
    return b;
}
std::string encodeList(const std::string& room){
    std::string b;
    putU8(b, C2S_LIST);
    if(!putStr(b, room)) return {};
    return b;
}
std::string encodeHistory(const std::string& room, uint32_t count, uint32_t before){
    std::string b;
    putU8(b, C2S_HISTORY);
    if(!putStr(b, room)) return {};
    putU32(b, count);
    putU32(b, before);
    return b;
}

// ---- S2C ----
std::string encodeJoined(const std::string& room, const std::string& name){
    std::string b;
    b.reserve(room.size() + name.size() + 3);
    putU8(b, S2C_JOINED);
    if(!putStr(b, room) || !putStr(b, name)) return {};
    return b;
}
std::string encodeBroadcast(const std::string& room, const std::string& from, const std::string& msg){
    std::string b;
    b.reserve(room.size() + from.size() + msg.size() + 3);
    putU8(b, S2C_MSG);
    if(!putStr(b, room) || !putStr(b, from)) return {};
    b.append(msg);
    return b;
}
std::string encodeLeft(const std::string& room, const std::string& name){
    std::string b;
    b.reserve(room.size() + name.size() + 3);
    putU8(b, S2C_LEFT);
    if(!putStr(b, room) || !putStr(b, name)) return {};
    return b;
}
std::string encodeMembers(const std::string& room, const std::vector<std::string>& names){
    std::string b;
    b.reserve(room.size() + names.size() * (kMaxNameLen + 1) + 3);
    putU8(b, S2C_MEMBERS);
    if(!putStr(b, room)) return {};
    if(names.size() > 255) return {};
    putU8(b, static_cast<uint8_t>(names.size()));
    for(const auto& n : names){
        if(!putStr(b, n)) return {};
    }
    return b;
}
std::string encodeHistoryResp(const std::string& room,
                              const std::vector<HistoryEntry>& msgs){
    std::string b;
    b.reserve(room.size() + msgs.size() * (kMaxNameLen + 4 + 8) + 3);
    putU8(b, S2C_HISTORY);
    if(!putStr(b, room)) return {};
    if(msgs.size() > 65535) return {};
    putU16(b, static_cast<uint16_t>(msgs.size()));
    for(const auto& e : msgs){
        if(e.from.size() > kMaxNameLen || e.msg.size() > 65535) return {};
        putU8(b, static_cast<uint8_t>(e.from.size()));
        b.append(e.from);
        putU16(b, static_cast<uint16_t>(e.msg.size()));
        b.append(e.msg);
        putU64(b, e.seq);
    }
    return b;
}
std::string encodeError(uint8_t code, const std::string& reason){
    std::string b;
    putU8(b, S2C_ERROR);
    putU8(b, code);
    if(!putStr(b, reason)) return {};
    return b;
}

DecodedMsg decode(const std::string& payload){
    DecodedMsg d;
    size_t i = 0;
    if(!takeU8(payload, i, d.type)) return d;                 // type 留 0 → 非法
    switch(d.type){
    case C2S_JOIN:
        if(!takeStr(payload, i, d.room) || !takeStr(payload, i, d.name)){ d.type = 0; return d; }
        return d;
    case C2S_MSG:
        if(!takeStr(payload, i, d.room)){ d.type = 0; return d; }
        d.msg.assign(payload, i, payload.size() - i);         // 余下全部（可为空）
        return d;
    case C2S_LEAVE:
    case C2S_LIST:
        if(!takeStr(payload, i, d.room)){ d.type = 0; return d; }
        return d;
    case S2C_JOINED:
        if(!takeStr(payload, i, d.room) || !takeStr(payload, i, d.name)){ d.type = 0; return d; }
        return d;
    case S2C_MSG:
        if(!takeStr(payload, i, d.room) || !takeStr(payload, i, d.name)){ d.type = 0; return d; }
        d.msg.assign(payload, i, payload.size() - i);
        return d;
    case S2C_LEFT:
        if(!takeStr(payload, i, d.room) || !takeStr(payload, i, d.name)){ d.type = 0; return d; }
        return d;
    case S2C_MEMBERS:{
        uint8_t cnt = 0;
        if(!takeStr(payload, i, d.room) || !takeU8(payload, i, cnt)){ d.type = 0; return d; }
        for(uint8_t k = 0; k < cnt; ++k){
            std::string n;
            if(!takeStr(payload, i, n)){ d.type = 0; return d; }
            d.names.push_back(std::move(n));
        }
        return d;
    }
    case C2S_HISTORY:
        if(!takeStr(payload, i, d.room) || !takeU32(payload, i, d.hCount)
           || !takeU32(payload, i, d.hBefore)){ d.type = 0; return d; }
        return d;
    case S2C_HISTORY:{
        uint16_t cnt = 0;
        if(!takeStr(payload, i, d.room) || !takeU16(payload, i, cnt)){ d.type = 0; return d; }
        for(uint16_t k = 0; k < cnt; ++k){
            std::string n, m;
            uint16_t mlen = 0;
            uint64_t seq = 0;
            if(!takeStr(payload, i, n) || !takeU16(payload, i, mlen)){ d.type = 0; return d; }
            if(i + mlen > payload.size()){ d.type = 0; return d; }
            m.assign(payload, i, mlen);
            i += mlen;
            if(!takeU64(payload, i, seq)){ d.type = 0; return d; }
            d.history.emplace_back(HistoryEntry{seq, std::move(n), std::move(m)});
        }
        return d;
    }
    case S2C_ERROR:
        if(!takeU8(payload, i, d.code) || !takeStr(payload, i, d.reason)){ d.type = 0; return d; }
        return d;
    default:                                                     // 未知 type → 非法
        d.type = 0;
        return d;
    }
}

// ============================================================================
// v2 用户层多会话 IM（0x10–0x9A）。
// 编码约束：包体超 kMaxFrameLen 依次返回空串（调用方丢弃）——沿用 v1 encode 契约。
// ============================================================================
std::string imDmConvId(const std::string& a, const std::string& b){
    // 字典序大者靠前：A 找 B 与 B 找 A 生成同一 id → dm 幂等。
    return std::string("dm:") + (a <= b ? a : b) + ":" + (a <= b ? b : a);
}

// ---- v2 C2S 编码 ----
std::string imEncodeRegister(const std::string& uid, const std::string& nick, const std::string& pwd32){
    if(uid.empty() || nick.empty() || pwd32.size() != 32) return {};
    std::string b;
    b.reserve(uid.size() + nick.size() + pwd32.size() + 3);
    putU8(b, IM_REGISTER);
    if(!putStr(b, uid) || !putStr(b, nick)) return {};
    b.append(pwd32);
    return b;
}
std::string imEncodeAuth(const std::string& uid, const std::string& pwd32){
    if(uid.empty() || pwd32.size() != 32) return {};
    std::string b;
    putU8(b, IM_AUTH);
    if(!putStr(b, uid)) return {};
    b.append(pwd32);
    return b;
}
std::string imEncodeConvCreate(uint8_t type, const std::string& name,
                               const std::vector<std::string>& members){
    if((type == kImConvDm && (members.size() != 1 || !name.empty())) ||
       (type == kImConvGrp && members.empty())) return {};
    std::string b;
    b.reserve(name.size() + members.size() * (kMaxUidLen + 1) + 4);
    putU8(b, IM_CONV_CREATE);
    putU8(b, type);
    if(!putStr(b, name)) return {};
    if(members.size() > 255) return {};
    putU8(b, static_cast<uint8_t>(members.size()));
    for(const auto& m : members){
        if(!putStr(b, m)) return {};
    }
    return b;
}
std::string imEncodeConvList(){
    std::string b;
    putU8(b, IM_CONV_LIST);
    return b;
}
std::string imEncodeConvOpen(const std::string& convId){
    std::string b;
    putU8(b, IM_CONV_OPEN);
    if(!putStr(b, convId)) return {};
    return b;
}
std::string imEncodeHistory(const std::string& convId, uint32_t count, uint32_t before){
    std::string b;
    putU8(b, IM_HISTORY);
    if(!putStr(b, convId)) return {};
    putU32(b, count);
    putU32(b, before);
    return b;
}
std::string imEncodeMsg(const std::string& convId, const std::string& msg){
    std::string b;
    b.reserve(convId.size() + msg.size() + 2);
    putU8(b, IM_MSG);
    if(!putStr(b, convId)) return {};
    b.append(msg);
    return b;
}
std::string imEncodeRead(const std::string& convId, uint64_t seq){
    std::string b;
    putU8(b, IM_READ);
    if(!putStr(b, convId)) return {};
    putU64(b, seq);
    return b;
}
std::string imEncodeConvLeave(const std::string& convId){
    std::string b;
    putU8(b, IM_CONV_LEAVE);
    if(!putStr(b, convId)) return {};
    return b;
}
std::string imEncodeMembers(const std::string& convId){
    std::string b;
    putU8(b, IM_MEMBERS);
    if(!putStr(b, convId)) return {};
    return b;
}
std::string imEncodeConvJoin(const std::string& convId){
    std::string b;
    putU8(b, IM_CONV_JOIN);
    if(!putStr(b, convId)) return {};
    return b;
}
std::string imEncodeUserLookup(const std::string& uid){
    if(uid.empty() || uid.size() > kMaxUidLen) return {};
    std::string b;
    putU8(b, IM_USER_LOOKUP);
    if(!putStr(b, uid)) return {};
    return b;
}

// ---- v2 S2C 编码 ----
std::string imEncodeAuthOk(const std::string& uid, const std::string& nick){
    std::string b;
    putU8(b, IM_AUTH_OK);
    if(!putStr(b, uid) || !putStr(b, nick)) return {};
    return b;
}
std::string imEncodeConvCreated(const std::string& convId){
    std::string b;
    putU8(b, IM_CONV_CREATED);
    if(!putStr(b, convId)) return {};
    return b;
}
std::string imEncodeConvListResp(const std::vector<ImConvSummary>& convs){
    std::string b;
    b.reserve(convs.size() * (kMaxUidLen + 2 + 4 + 1) + 2);
    putU8(b, IM_CONV_LIST_R);
    if(convs.size() > 255) return {};
    putU8(b, static_cast<uint8_t>(convs.size()));
    for(const auto& c : convs){
        if(!putStr(b, c.convId)) return {};
        putU8(b, c.type);
        if(!putStr(b, c.name)) return {};
        putU32(b, c.unread);
        if(!putStr(b, c.lastMsg)) return {};     // v3 起追加：最后消息预览（服务端截断 ≤255）
        putU64(b, c.lastTsMs);                   // 最后消息时间（会话列排序锚）
    }
    return b;
}
std::string imEncodeConvOpened(const std::string& convId, uint64_t lastReadCursor){
    std::string b;
    putU8(b, IM_CONV_OPENED);
    if(!putStr(b, convId)) return {};
    putU64(b, lastReadCursor);
    return b;
}
std::string imEncodeHistoryResp(const std::string& convId,
                                const std::vector<ImHistoryEntry>& msgs){
    std::string b;
    b.reserve(convId.size() + msgs.size() * (kMaxUidLen + kMaxMsgLen + 20) + 4);
    putU8(b, IM_HISTORY_R);
    if(!putStr(b, convId)) return {};
    if(msgs.size() > 65535) return {};
    putU16(b, static_cast<uint16_t>(msgs.size()));
    for(const auto& e : msgs){
        if(e.from.size() > kMaxUidLen || e.msg.size() > kMaxMsgLen) return {};
        putU8(b, static_cast<uint8_t>(e.from.size()));
        b.append(e.from);
        putU16(b, static_cast<uint16_t>(e.msg.size()));
        b.append(e.msg);
        putU64(b, e.seq);
        putU64(b, e.tsMs);
    }
    return b;
}
std::string imEncodeMsgBroadcast(const std::string& convId,
                                 const std::string& fromUid, const std::string& fromNick,
                                 const std::string& msg, uint64_t tsMs, uint64_t seq){
    if(fromUid.size() > kMaxUidLen || fromNick.size() > kMaxNameLen || msg.size() > kMaxMsgLen)
        return {};
    std::string b;
    b.reserve(convId.size() + fromUid.size() + fromNick.size() + msg.size() + 28);
    putU8(b, IM_MSG_R);
    if(!putStr(b, convId)) return {};
    if(!putStr(b, fromUid) || !putStr(b, fromNick)) return {};
    putU16(b, static_cast<uint16_t>(msg.size()));
    b.append(msg);
    putU64(b, tsMs);
    putU64(b, seq);
    return b;
}
std::string imEncodeMembersResp(const std::string& convId,
                                const std::vector<ImMember>& members){
    std::string b;
    b.reserve(convId.size() + members.size() * (kMaxUidLen + kMaxNameLen + 3) + 3);
    putU8(b, IM_MEMBERS_R);
    if(!putStr(b, convId)) return {};
    if(members.size() > 255) return {};
    putU8(b, static_cast<uint8_t>(members.size()));
    for(const auto& m : members){
        if(m.uid.size() > kMaxUidLen || m.nick.size() > kMaxNameLen) return {};
        putU8(b, static_cast<uint8_t>(m.uid.size()));
        b.append(m.uid);
        putU8(b, static_cast<uint8_t>(m.nick.size()));
        b.append(m.nick);
        putU8(b, m.online ? 1 : 0);
    }
    return b;
}
std::string imEncodePresence(const std::string& uid, bool online){
    std::string b;
    putU8(b, IM_PRESENCE);
    if(!putStr(b, uid)) return {};
    putU8(b, online ? 1 : 0);
    return b;
}
std::string imEncodeConvChanged(const std::string& convId){
    std::string b;
    putU8(b, IM_CONV_CHANGED);
    if(!putStr(b, convId)) return {};
    return b;
}
std::string imEncodeUserLookupR(bool found, const std::string& nick){
    if(found && nick.empty()) return {};
    if(!found && !nick.empty()) return {};
    std::string b;
    putU8(b, IM_USER_LOOKUP_R);
    putU8(b, found ? 1 : 0);
    if(!putStr(b, nick)) return {};
    return b;
}
std::string imEncodeError(uint8_t code, const std::string& reason){
    return encodeError(code, reason);     // S2C_ERROR 帧格式 v1/v2 相同
}

ImDecoded imDecode(const std::string& payload){
    ImDecoded d;
    size_t i = 0;
    if(!takeU8(payload, i, d.type)) return d;
    switch(d.type){
    case IM_REGISTER:
        if(!takeStr(payload, i, d.uid) || !takeStr(payload, i, d.nick)){ d.type = 0; return d; }
        if(i + 32 > payload.size()){ d.type = 0; return d; }
        d.pwd32.assign(payload, i, 32);
        return d;
    case IM_AUTH:
        if(!takeStr(payload, i, d.uid)){ d.type = 0; return d; }
        if(i + 32 > payload.size()){ d.type = 0; return d; }
        d.pwd32.assign(payload, i, 32);
        return d;
    case IM_CONV_CREATE:{
        uint8_t cnt = 0;
        if(!takeU8(payload, i, d.convType) || !takeStr(payload, i, d.name)
           || !takeU8(payload, i, cnt)){ d.type = 0; return d; }
        for(uint8_t k = 0; k < cnt; ++k){
            std::string m;
            if(!takeStr(payload, i, m)){ d.type = 0; return d; }
            d.members.push_back(std::move(m));
        }
        return d;
    }
    case IM_CONV_LIST:
        return d;                         // 无 body
    case IM_CONV_OPEN:
    case IM_CONV_LEAVE:
    case IM_MEMBERS:
    case IM_CONV_JOIN:
        if(!takeStr(payload, i, d.conv)){ d.type = 0; return d; }
        return d;
    case IM_HISTORY:
        if(!takeStr(payload, i, d.conv) || !takeU32(payload, i, d.hCount)
           || !takeU32(payload, i, d.hBefore)){ d.type = 0; return d; }
        return d;
    case IM_USER_LOOKUP:
        if(!takeStr(payload, i, d.uid)){ d.type = 0; return d; }   // 被查用户
        return d;
    case IM_READ:
        if(!takeStr(payload, i, d.conv)){ d.type = 0; return d; }
        if(!takeU64(payload, i, d.seq)){ d.type = 0; return d; }
        return d;
    case IM_MSG:
        if(!takeStr(payload, i, d.conv)){ d.type = 0; return d; }
        d.msg.assign(payload, i, payload.size() - i);     // 余下全部
        return d;
    case IM_AUTH_OK:
        if(!takeStr(payload, i, d.uid) || !takeStr(payload, i, d.nick)){ d.type = 0; return d; }
        return d;
    case IM_CONV_CREATED:
        if(!takeStr(payload, i, d.conv)){ d.type = 0; return d; }
        return d;
    case IM_CONV_CHANGED:
        if(!takeStr(payload, i, d.conv)){ d.type = 0; return d; }
        return d;
    case IM_CONV_LIST_R:{
        uint8_t cnt = 0;
        if(!takeU8(payload, i, cnt)){ d.type = 0; return d; }
        for(uint8_t k = 0; k < cnt; ++k){
            ImConvSummary c;
            if(!takeStr(payload, i, c.convId) || !takeU8(payload, i, c.type)
               || !takeStr(payload, i, c.name) || !takeU32(payload, i, c.unread)
               || !takeStr(payload, i, c.lastMsg) || !takeU64(payload, i, c.lastTsMs)){ d.type = 0; return d; }
            d.convs.push_back(std::move(c));
        }
        return d;
    }
    case IM_CONV_OPENED:
        if(!takeStr(payload, i, d.conv) || !takeU64(payload, i, d.seq)){ d.type = 0; return d; }
        return d;
    case IM_HISTORY_R:{
        uint16_t cnt = 0;
        if(!takeStr(payload, i, d.conv) || !takeU16(payload, i, cnt)){ d.type = 0; return d; }
        for(uint16_t k = 0; k < cnt; ++k){
            ImHistoryEntry e;
            std::string m;
            uint16_t mlen = 0;
            if(!takeStr(payload, i, e.from) || !takeU16(payload, i, mlen)){ d.type = 0; return d; }
            if(i + mlen > payload.size()){ d.type = 0; return d; }
            m.assign(payload, i, mlen);
            i += mlen;
            if(!takeU64(payload, i, e.seq) || !takeU64(payload, i, e.tsMs)){ d.type = 0; return d; }
            e.msg = std::move(m);
            d.history.push_back(std::move(e));
        }
        return d;
    }
    case IM_MSG_R:{
        if(!takeStr(payload, i, d.conv) || !takeStr(payload, i, d.fromUid)
           || !takeStr(payload, i, d.fromNick)){ d.type = 0; return d; }
        uint16_t mlen = 0;
        if(!takeU16(payload, i, mlen)){ d.type = 0; return d; }
        if(i + mlen > payload.size()){ d.type = 0; return d; }
        d.msg.assign(payload, i, mlen);
        i += mlen;
        if(!takeU64(payload, i, d.tsMs) || !takeU64(payload, i, d.seq)){ d.type = 0; return d; }
        return d;
    }
    case IM_MEMBERS_R:{
        uint8_t cnt = 0;
        if(!takeStr(payload, i, d.conv) || !takeU8(payload, i, cnt)){ d.type = 0; return d; }
        for(uint8_t k = 0; k < cnt; ++k){
            ImMember m;
            if(!takeStr(payload, i, m.uid) || !takeStr(payload, i, m.nick)){ d.type = 0; return d; }
            uint8_t on = 0;
            if(!takeU8(payload, i, on)){ d.type = 0; return d; }
            m.online = on != 0;
            d.membersResp.push_back(std::move(m));
        }
        return d;
    }
    case IM_PRESENCE:
        if(!takeStr(payload, i, d.uid)){ d.type = 0; return d; }
        {
            uint8_t on = 0;
            if(!takeU8(payload, i, on)){ d.type = 0; return d; }
            d.online = on != 0;
        }
        return d;
    case IM_USER_LOOKUP_R:{
        uint8_t f = 0;
        if(!takeU8(payload, i, f) || !takeStr(payload, i, d.nick)){ d.type = 0; return d; }
        d.lookFound = f != 0;
        return d;
    }
    case S2C_ERROR:
        if(!takeU8(payload, i, d.code) || !takeStr(payload, i, d.reason)){ d.type = 0; return d; }
        return d;
    default:
        d.type = 0;
        return d;
    }
}

} // namespace chat