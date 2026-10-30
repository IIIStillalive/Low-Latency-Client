// message_bubble.cpp —— 气泡组装：布局 + 配色 + sizeHint（wordwrap 行高）。
// 尺寸契约：QLabel(wordWrap) 在 setMaximumWidth 后 sizeHint().height() 即换行后高度；
// 组件 sizeHint = {气泡宽 + 头像 + 间距, 昵称(有则加) + 气泡高 + 上下内距}。
#include "message_bubble.hpp"

#include <QHBoxLayout>
#include <QLabel>
#include <QVBoxLayout>

#include "avatar.hpp"
#include "theme.hpp"

namespace chat {

MessageBubble::MessageBubble(const QString& fromUid, const QString& nick, const QString& msg,
                             bool mine, int maxBubbleWidth, QWidget* parent)
    : QWidget(parent), hint_()
{
    // 不设 WA_TransparentForMouseEvents：保留 QLabel 的文本选中/复制（TextSelectableByMouse）。
    // 滚轮事件会冒泡到 QListWidget 视口处理，不受子 widget 影响。

    auto* root = new QHBoxLayout(this);
    root->setContentsMargins(6, 2, 6, 2);
    root->setSpacing(6);

    auto* avatar = makeAvatarLabel(fromUid, nick, 32, this);

    auto* col = new QVBoxLayout();
    col->setContentsMargins(0, 0, 0, 0);
    col->setSpacing(2);

    QLabel* nameLbl = nullptr;
    if(!mine && !nick.isEmpty()){
        nameLbl = new QLabel(nick, this);
        nameLbl->setStyleSheet(QStringLiteral("color: %1;").arg(theme::nameColor(fromUid).name()));
        QFont nf = nameLbl->font(); nf.setPixelSize(12);
        nameLbl->setFont(nf);
        nameLbl->setContentsMargins(4, 0, 0, 0);
        col->addWidget(nameLbl, 0, Qt::AlignLeft);
    }

    auto* bubble = new QLabel(msg, this);
    bubble->setWordWrap(true);
    bubble->setMaximumWidth(maxBubbleWidth);
    bubble->setTextFormat(Qt::PlainText);                  // 正文按纯文本，HTML 转义
    bubble->setTextInteractionFlags(Qt::TextSelectableByMouse);
    QFont bf = bubble->font(); bf.setPixelSize(14);
    bubble->setFont(bf);
    bubble->setContentsMargins(10, 7, 10, 7);
    bubble->setStyleSheet(mine
        ? QStringLiteral("background:%1;border-radius:8px;color:%2;")
              .arg(theme::kMineBubble, theme::kText1)
        : QStringLiteral("background:%1;border:1px solid %2;border-radius:8px;color:%3;")
              .arg(theme::kTheirsBubble, theme::kTheirsBorder, theme::kText1));
    col->addWidget(bubble, 0, mine ? Qt::AlignRight : Qt::AlignLeft);

    // 我方：[stretch][col][avatar]，对方：[avatar][col][stretch]
    if(mine){
        root->addStretch(1);
        root->addLayout(col);
        root->addWidget(avatar);
    } else {
        root->addWidget(avatar);
        root->addLayout(col);
        root->addStretch(1);
    }

    // sizeHint：以气泡换行高度为基准（width 用实际最大宽 + 头像 38）
    const int bw = bubble->sizeHint().width();
    const int bh = bubble->sizeHint().height();
    const int nameH = nameLbl ? nameLbl->sizeHint().height() + 2 : 0;
    hint_ = QSize(qMax(40, bw + 38), bh + nameH + 4);
    setMinimumSize(hint_);
}

} // namespace chat