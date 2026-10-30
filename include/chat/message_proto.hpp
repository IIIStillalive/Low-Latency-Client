#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
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
    C2S_HISTORY = 0x05,   // 拉历史：[room][u32 count][u32 before_seq]
    S2C_JOINED  = 0x81,   // 入房回执：[room][name]
    S2C_MSG     = 0x82,   // 广播：[room][from][msg]（含发送者 echo，客户端逻辑统一）
    S2C_LEFT    = 0x83,   // 成员离开通知：[room][name]（只发给留下的人）
    S2C_MEMBERS = 0x84,   // 成员表：[room][cnt][name]*
    S2C_HISTORY = 0x85,   // 历史批：[room][u16 cnt][(1B name_len][name][u16 msg_len][msg][8B seq)]*
                          //   （最旧→最新，直接供 UI 顺序渲染；seq = 服务端消息号，客户端上翻游标）
    S2C_ERROR   = 0x8F,   // 错误：[code][reason]
};

// ---- v2 用户层多会话 IM 帧（与 v1 零复用：全新 type，编解码独立 imEncode/imDecode）----
enum : uint8_t {
    // C2S（0x10–0x1B）
    IM_REGISTER    = 0x10,  // 注册：[uid][nick][32B pwd_sha256]
    IM_AUTH        = 0x11,  // 登录：[uid][32B pwd_sha256]
    IM_CONV_CREATE = 0x12,  // 建会话：[1B type dm=1/grp=2][u8 name_len][name][u8 cnt][(u8 l][uid)]*
    IM_CONV_LIST   = 0x13,  // (空)：拉我参与的会话列表
    IM_CONV_OPEN   = 0x14,  // 打开会话：[conv_id]
    IM_HISTORY     = 0x15,  // 历史：[conv_id][u32 count][u32 before_seq]
    IM_MSG         = 0x16,  // 发消息：[conv_id][msg 余下全部]
    IM_READ        = 0x17,  // 已读游标上报：[conv_id][8B seq]
    IM_CONV_LEAVE  = 0x18,  // 退群：[conv_id]（dm 不支持 → error）
    IM_MEMBERS     = 0x19,  // 拉成员表：[conv_id]
    IM_CONV_JOIN   = 0x1A,  // 加入群聊：[conv_id]（dm 不可加入 → error）
    IM_USER_LOOKUP = 0x1B,  // 查找用户：[uid]（添加好友前校验存在）
    // S2C（0x90–0x9C）
    IM_AUTH_OK     = 0x90,  // 认证成功：[uid][nick]
    IM_CONV_CREATED= 0x92,  // 会话已建/已存在（dm 幂等）：[conv_id]
    IM_CONV_LIST_R = 0x93,  // 会话列表 + 未读：[u8 cnt][(conv_id][1B type][name][u32 unread)]*
    IM_CONV_OPENED = 0x94,  // 打开回执：[conv_id][8B last_read_cursor]
    IM_HISTORY_R   = 0x95,  // 历史批：[conv_id][u16 cnt][([u8 l]from][u16 l]msg][8B seq][8B ts_ms)]*
    IM_MSG_R       = 0x96,  // 消息广播：[conv_id][u8 l]from_uid][u8 l]from_nick][u16 l]msg][8B ts_ms]
    IM_MEMBERS_R   = 0x99,  // 成员表：[conv_id][u8 cnt][([u8 l]uid][u8 l]nick][1B online)]*
    IM_PRESENCE    = 0x9A,  // 在线状态推送：[uid][1B online]
    IM_CONV_CHANGED= 0x9B,  // 会话变更推送（成员集变：建 DM/入群/退群）：[conv_id] → 收者刷新会话列
    IM_USER_LOOKUP_R=0x9C,  // 用户查找回执：[1B found][u8 l]nick
};

inline constexpr size_t kMaxNameLen  = 255;    // 单字符串字段上限（房间名/昵称/错误理由）
inline constexpr size_t kMaxFrameLen = 65536;  // 单业务帧上限（超限回 S2C_ERROR）

// S2C_HISTORY 单条消息：from + msg + seq（服务端分配的消息号，翻页游标用）
struct HistoryEntry {
    uint64_t seq = 0;
    std::string from;
    std::string msg;
    bool operator==(const HistoryEntry& o) const {
        return seq == o.seq && from == o.from && msg == o.msg;
    }
};

// 编码：只产出 [1B type][body]；4B 长度前缀由外层 LengthHeaderCodec 负责。
// 任一字符串超长 / 条目超 255 → 返回空串（调用方丢弃或回错误）。
std::string encodeJoin(const std::string& room, const std::string& name);
std::string encodeMsg(const std::string& room, const std::string& msg);
std::string encodeLeave(const std::string& room);
std::string encodeList(const std::string& room);
std::string encodeHistory(const std::string& room, uint32_t count, uint32_t before);  // C2S_HISTORY

std::string encodeJoined(const std::string& room, const std::string& name);
std::string encodeBroadcast(const std::string& room, const std::string& from, const std::string& msg);
std::string encodeLeft(const std::string& room, const std::string& name);
std::string encodeMembers(const std::string& room, const std::vector<std::string>& names);
std::string encodeHistoryResp(const std::string& room,
                              const std::vector<HistoryEntry>& msgs);  // S2C_HISTORY
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
    uint32_t hCount = 0;                  // C2S_HISTORY：count（0=默认200） / before_seq（0=最新）
    uint32_t hBefore = 0;
    std::vector<HistoryEntry> history;    // S2C_HISTORY：(from,msg)+seq，最旧→最新
    bool ok() const { return type != 0; }
};
DecodedMsg decode(const std::string& payload);

// ============================================================================
// v2 用户层多会话 IM：独立结构 + 独立编解码（与 v1 类型号/字段零复用）。
// ============================================================================
inline constexpr uint8_t  kImConvDm   = 1;   // 会话类型：单聊
inline constexpr uint8_t  kImConvGrp  = 2;   // 会话类型：群聊
inline constexpr size_t   kMaxUidLen  = 64;  // uid 上限（共识：DM conv_id = "dm:"+两个 uid+":" 仍 <255）
inline constexpr size_t   kMaxMsgLen  = 60000; // 单条消息上限（帧级纵深防御，codec 64KB 之上留余量）

// DM conv_id：两 uid 字典序大者拼接，"dm:"+A+":"+B → 幂等（A找B/B找A同一会话）
std::string imDmConvId(const std::string& a, const std::string& b);

struct ImConvSummary {          // 会话列表条目
    std::string convId;
    uint8_t     type = 0;       // kImConvDm / kImConvGrp
    std::string name;           // 群名 / 单聊对方昵称（或空）
    uint32_t    unread = 0;
    std::string lastMsg;        // 最后消息预览（grp=服务端拼好 `昵称: 消息`；空=无历史）
    uint64_t    lastTsMs = 0;   // 最后消息时间（会话列排序锚；0=无历史）
};
struct ImHistoryEntry {         // v2 历史单条（带发信时间戳）
    uint64_t seq  = 0;
    uint64_t tsMs = 0;
    std::string from;
    std::string msg;
    bool operator==(const ImHistoryEntry& o) const {
        return seq == o.seq && tsMs == o.tsMs && from == o.from && msg == o.msg;
    }
};
struct ImMember {               // v2 成员表单条
    std::string uid;
    std::string nick;
    bool        online = false;
};

// ---- v2 C2S 编码 ----
std::string imEncodeRegister(const std::string& uid, const std::string& nick, const std::string& pwd32);
std::string imEncodeAuth(const std::string& uid, const std::string& pwd32);
std::string imEncodeConvCreate(uint8_t type, const std::string& name,
                               const std::vector<std::string>& members);
std::string imEncodeConvList();                         // 无 body
std::string imEncodeConvOpen(const std::string& convId);
std::string imEncodeHistory(const std::string& convId, uint32_t count, uint32_t before);
std::string imEncodeMsg(const std::string& convId, const std::string& msg);
std::string imEncodeRead(const std::string& convId, uint64_t seq);
std::string imEncodeConvLeave(const std::string& convId);
std::string imEncodeMembers(const std::string& convId);
std::string imEncodeConvJoin(const std::string& convId);      // 0x1A 加入群聊
std::string imEncodeUserLookup(const std::string& uid);        // 0x1B 查找用户（添加好友前校验存在）

// ---- v2 S2C 编码 ----
std::string imEncodeAuthOk(const std::string& uid, const std::string& nick);
std::string imEncodeConvCreated(const std::string& convId);
std::string imEncodeConvListResp(const std::vector<ImConvSummary>& convs);
std::string imEncodeConvOpened(const std::string& convId, uint64_t lastReadCursor);
std::string imEncodeHistoryResp(const std::string& convId,
                                const std::vector<ImHistoryEntry>& msgs);
std::string imEncodeMsgBroadcast(const std::string& convId,
                                 const std::string& fromUid, const std::string& fromNick,
                                 const std::string& msg, uint64_t tsMs, uint64_t seq);  // seq：广播也带，供客户端并集去重
std::string imEncodeMembersResp(const std::string& convId,
                                const std::vector<ImMember>& members);
std::string imEncodePresence(const std::string& uid, bool online);
std::string imEncodeConvChanged(const std::string& convId);   // 0x9B 会话变更推送
std::string imEncodeUserLookupR(bool found, const std::string& nick);  // 0x9C 查找回执

// ---- v2 解码结果 ----
struct ImDecoded {
    uint8_t type = 0;                     // 0 = 非法
    // 注册/认证
    std::string uid;
    std::string nick;
    std::string pwd32;                    // 32B 口令摘要
    // 建会话
    uint8_t     convType = 0;
    std::string name;
    std::vector<std::string> members;
    // 通用 conv id
    std::string conv;                     // 会话 id
    // 历史
    uint32_t hCount = 0, hBefore = 0;
    // 已读 / 打开回执
    uint64_t seq = 0;                     // IM_READ：已读 seq；IM_CONV_OPENED：last_read_cursor
    // 错误
    uint8_t  code = 0;                    // S2C_ERROR code
    std::string reason;                   // S2C_ERROR reason
    // 消息
    std::string msg;
    std::string fromUid, fromNick;        // IM_MSG_R 广播者
    uint64_t    tsMs = 0;
    // S2C 批量
    std::vector<ImConvSummary> convs;
    std::vector<ImHistoryEntry> history;
    std::vector<ImMember> membersResp;
    bool online = false;                  // IM_PRESENCE
    bool lookFound = false;               // IM_USER_LOOKUP_R（0=不存在）；被查 uid 复用 d.uid
    bool ok() const { return type != 0; }
};
ImDecoded imDecode(const std::string& payload);
std::string imEncodeError(uint8_t code, const std::string& reason); // S2C_ERROR（同 v1）

} // namespace chat