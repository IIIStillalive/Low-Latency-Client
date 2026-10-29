#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// 聊天室业务帧协议：盖在 LengthHeaderCodec（4B 长度前缀）的 payload 之上。
// 帧 = [1B type][body]；字符串一律 [1B len][len 字节]（len ≤ 255）。
// 与 RPC（proto/rpc.proto）完全解耦：聊天不引 protobuf，服务端业务独立成层
// （设计见 plan/task_plan_chat_room.md §1）。
namespace chat {

// ---- 帧类型 ----
enum : uint8_t {
    C2S_JOIN    = 0x01,   // 入房：[room][name]
    C2S_MSG     = 0x02,   // 发言：[room][msg 余下全部]
    C2S_LEAVE   = 0x03,   // 离开：[room]
    C2S_LIST    = 0x04,   // 成员表：[room]
    S2C_JOINED  = 0x81,   // 入房回执：[room][name]
    S2C_MSG     = 0x82,   // 广播：[room][from][msg]（含发送者 echo，客户端逻辑统一）
    S2C_LEFT    = 0x83,   // 成员离开通知：[room][name]（只发给留下的人）
    S2C_MEMBERS = 0x84,   // 成员表：[room][cnt][name]*
    S2C_ERROR   = 0x8F,   // 错误：[code][reason]
};

inline constexpr size_t kMaxNameLen  = 255;    // 单字符串字段上限（房间名/昵称/错误理由）
inline constexpr size_t kMaxFrameLen = 65536;  // 单业务帧上限（超限回 S2C_ERROR）

// 编码：只产出 [1B type][body]；4B 长度前缀由外层 LengthHeaderCodec 负责。
// 任一字符串超长 / 条目超 255 → 返回空串（调用方丢弃或回错误）。
std::string encodeJoin(const std::string& room, const std::string& name);
std::string encodeMsg(const std::string& room, const std::string& msg);
std::string encodeLeave(const std::string& room);
std::string encodeList(const std::string& room);

std::string encodeJoined(const std::string& room, const std::string& name);
std::string encodeBroadcast(const std::string& room, const std::string& from, const std::string& msg);
std::string encodeLeft(const std::string& room, const std::string& name);
std::string encodeMembers(const std::string& room, const std::vector<std::string>& names);
std::string encodeError(uint8_t code, const std::string& reason);

// 解码结果；ok()==false 表示非法/截断/未知 type 帧
struct DecodedMsg {
    uint8_t type = 0;                     // 0 = 解析失败
    std::string room;                     // 通用：房间名
    std::string name;                     // JOIN/JOINED：加入者名；S2C_MSG：from；S2C_LEFT：离开者
    std::string msg;                      // MSG/S2C_MSG：正文（C2S_MSG/S2C_MSG 为帧余下全部）
    uint8_t code = 0;                     // S2C_ERROR code
    std::string reason;                   // S2C_ERROR reason
    std::vector<std::string> names;       // S2C_MEMBERS
    bool ok() const { return type != 0; }
};
DecodedMsg decode(const std::string& payload);

} // namespace chat