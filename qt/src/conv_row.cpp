// conv_row.cpp —— 会话行绘制：背景（选中/悬停）+ 头像 + 两列文本 + 角标。
#include "conv_row.hpp"

#include <QFont>
#include <QFontMetrics>
#include <QPainter>

#include "avatar.hpp"
#include "theme.hpp"

namespace chat {

ConvRow::ConvRow(QWidget* parent) : QWidget(parent){
    setFixedHeight(56);
    setAttribute(Qt::WA_Hover);
    setCursor(Qt::PointingHandCursor);
}

void ConvRow::setData(const QString& convId, bool grp, const QString& name,
                      const QString& preview, uint64_t tsMs, uint32_t unread, bool selected){
    cid_ = convId;
    Q_UNUSED(grp);                        // 行不画类型角标（QQ 会话列不带 dm/grp 标记）
    name_ = name.isEmpty() ? convId : name;
    preview_ = preview;                   // 服务端已拼好预览（grp 带 `昵称: `）
    time_ = tsMs == 0 ? QString() : theme::timeText(tsMs);
    unread_ = unread;
    selected_ = selected;
    av_ = avatarPixmap(convId, name_, 36);
    update();
}

void ConvRow::setSelected(bool sel){ if(selected_ != sel){ selected_ = sel; update(); } }

void ConvRow::paintEvent(QPaintEvent*){
    QPainter p(this);
    p.setRenderHint(QPainter::Antialiasing, true);

    // 背景：选中 → 全宽浅蓝 + 左 3px 品牌条；悬停 → 浅灰。
    if(selected_){
        p.setPen(Qt::NoPen);
        p.setBrush(QColor(theme::kSel));
        p.drawRoundedRect(rect(), 6, 6);
        p.setBrush(QColor(theme::kBrand));
        p.drawRect(QRect(2, 4, 3, height() - 8));
    } else if(hover_){
        p.setPen(Qt::NoPen);
        p.setBrush(QColor(0xFF, 0xFF, 0xFF, 160));
        p.drawRoundedRect(rect(), 6, 6);
    }

    // 头像
    p.drawPixmap(QRect(8, (height() - 36) / 2, 36, 36), av_);

    QFont f = font(); f.setPixelSize(13); f.setBold(true);
    p.setFont(f);
    const int tx = 54, right = width() - 12;
    QFontMetrics fm(f);

    // 首行：昵称（grow 到右侧时间前）
    const int timeW = time_.isEmpty() ? 0 :
        QFontMetrics([&]{ QFont tf = font(); tf.setPixelSize(11); return tf; }()).horizontalAdvance(time_);
    const int nameW = qMax(40, right - tx - timeW - 8);
    const QString nm = fm.elidedText(name_, Qt::ElideRight, nameW);
    p.setPen(QColor(theme::kText1));
    p.drawText(QPoint(tx, 12 + fm.ascent()), nm);

    // 时间（右上）
    if(!time_.isEmpty()){
        QFont tf = font(); tf.setPixelSize(11);
        p.setFont(tf);
        p.setPen(QColor(theme::kText3));
        p.drawText(QPoint(right - QFontMetrics(tf).horizontalAdvance(time_), 12 + fm.ascent()), time_);
    }

    // 次行：预览（elide 到左侧角标前）
    QFont pf = font(); pf.setPixelSize(12); pf.setBold(false);
    p.setFont(pf);
    QFontMetrics pfm(pf);
    const QString badgeText = unread_ > 99 ? QStringLiteral("99+")
                            : unread_ > 0  ? QString::number(unread_)
                                           : QString();
    int badgeW = 0;
    if(!badgeText.isEmpty()){
        QFont bf = font(); bf.setPixelSize(11); bf.setBold(true);
        static const QFontMetrics bfm(bf);
        badgeW = qMax(18, bfm.horizontalAdvance(badgeText) + 10);
    }
    const int prevW = qMax(40, right - tx - (badgeText.isEmpty() ? 0 : badgeW) - 8);
    const QString pv = pfm.elidedText(preview_.isEmpty() ? QStringLiteral("暂无消息") : preview_,
                                      Qt::ElideRight, prevW);
    p.setPen(QColor(theme::kText2));
    p.drawText(QPoint(tx, 12 + fm.ascent() + 26), pv);

    // 未读角标（红药丸，右下）—— 与次行（预览）垂直居中对齐，底不越出行高 56
    if(!badgeText.isEmpty()){
        QFont bf = font(); bf.setPixelSize(11); bf.setBold(true);
        p.setFont(bf);
        QFontMetrics bfm(bf);
        const QRect bd(right - badgeW, 12 + fm.ascent() + 26 - pfm.ascent() / 2 - 8,
                      badgeW, 16);
        p.setPen(Qt::NoPen);
        p.setBrush(QColor(theme::kBadge));
        p.drawRoundedRect(bd, 8, 8);
        p.setPen(QColor(Qt::white));
        p.drawText(bd, Qt::AlignCenter, badgeText);
    }
}

void ConvRow::enterEvent(QEnterEvent*){ hover_ = true; update(); }
void ConvRow::leaveEvent(QEvent*){ hover_ = false; update(); }

} // namespace chat