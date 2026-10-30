#pragma once

// message_bubble.{hpp,cpp} —— 单条消息气泡（QQ 风格）：头像32 +（群）彩色昵称 + 圆角气泡。
//   我方 → 靠右绿泡，无昵称；对方 → 靠左白泡；群消息才显示发送者昵称（nameColor 取色）。
//   作为 QListWidget item widget 挂载（seq 去重数据位仍在 item 上，与 widget 无关）。
#include <QSize>
#include <QString>
#include <QWidget>

namespace chat {

class MessageBubble : public QWidget {
    Q_OBJECT
public:
    // nick = 已解析显示名；mine = fromUid==自己（右绿）；maxBubbleWidth = 气泡文字区上限（px）
    MessageBubble(const QString& fromUid, const QString& nick, const QString& msg,
                  bool mine, int maxBubbleWidth, QWidget* parent = nullptr);
    QSize sizeHint() const override { return hint_; }

private:
    QSize hint_;
};

} // namespace chat