#pragma once

// theme.{hpp,cpp} —— v3 客户端调色板与全局 QSS（QQ 风格），所有颜色/字号集中在这里。
// 设计见 plan/task_plan_qt_v3_ui.md §3/§4。

#include <QColor>
#include <QString>

#include <cstdint>

namespace chat::theme {

// ---- 调色板令牌（全局唯一来源）----
inline constexpr const char* kBrand        = "#12B7F5";    // QQ 主蓝：选中行条/发送钮/在线点
inline constexpr const char* kBrandDark    = "#0E9DE1";    // 主蓝 hover
inline constexpr const char* kBadge        = "#FA5151";    // 未读红
inline constexpr const char* kMineBubble   = "#95EC69";    // 我方气泡（QQ 经典绿）
inline constexpr const char* kTheirsBubble = "#FFFFFF";    // 对方气泡
inline constexpr const char* kTheirsBorder = "#DDE3E8";    // 对方气泡边框
inline constexpr const char* kText1        = "#2E2E2E";    // 主文字
inline constexpr const char* kText2        = "#8A9099";    // 次要（预览/时间）
inline constexpr const char* kText3        = "#B6BBC2";    // 弱（时间行/占位）
inline constexpr const char* kRailBg       = "#F0F2F5";    // 左功能条
inline constexpr const char* kPanelBg      = "#F7F8FA";    // 会话面板
inline constexpr const char* kChatBg       = "#F6F7F9";    // 聊天区
inline constexpr const char* kHover        = "#EEF0F3";    // 行悬停
inline constexpr const char* kSel          = "#E8F7FF";    // 行选中（含 QQ 左条色）
inline constexpr const char* kLine         = "#E6E8EC";    // 分隔线
inline constexpr const char* kInputBg      = "#FFFFFF";    // 输入框底
inline constexpr const char* kRed          = "#F56C6C";    // 错误红字

// 全局 QSS：给 QListWidget/按钮/输入框/头像/角标定妆。main() 里 app 级 setStyleSheet 一次。
QString appStyle();

// 时间文本
// 会话列时间：今天→HH:mm；昨天→"昨天"；本周内→周X；更早→MM-dd（跨年→yyyy-MM-dd）
QString timeText(uint64_t tsMs);
// 消息区居中时间行：今天→HH:mm，昨天→昨天 HH:mm，更早→MM-dd HH:mm
QString chatTimeText(uint64_t tsMs);

// 群成员名 / 头像的稳定取色（FNV-1a over UTF-8，与序列无关）。
QColor nameColor(const QString& uid);                  // 群名 6 色盘
QColor avatarColor(const QString& uid);                // 头像 10 色盘

} // namespace chat::theme