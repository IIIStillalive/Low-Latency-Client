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
void putU8(std::string& out, uint8_t v){
    out.push_back(static_cast<char>(v));
}
bool putStr(std::string& out, const std::string& s){   // 长度 ≤ 255，超限失败
    if(s.size() > kMaxNameLen) return false;
    putU8(out, static_cast<uint8_t>(s.size()));
    out.append(s);
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
    case S2C_ERROR:
        if(!takeU8(payload, i, d.code) || !takeStr(payload, i, d.reason)){ d.type = 0; return d; }
        return d;
    default:                                                     // 未知 type → 非法
        d.type = 0;
        return d;
    }
}

} // namespace chat