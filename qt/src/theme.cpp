// theme.cpp —— v3 调色板实现：QSS 组装 + 时间文本 + 稳定取色。
#include "theme.hpp"

#include <QDate>
#include <QDateTime>

namespace chat::theme {
namespace {

uint64_t fnv1aUtf8(const QString& s){
    const QByteArray b = s.toUtf8();
    uint64_t h = 1469598103934665603ULL;
    for(char c : b){ h ^= static_cast<uint8_t>(c); h *= 1099511628211ULL; }
    return h;
}

} // namespace

QString appStyle(){
    QString s = QStringLiteral(R"(
QWidget { font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif; }
QMainWindow, QDialog { background: %CGB; }
QWidget#panel, QWidget#chat, QWidget#member { background: %CG; }

/* 无边框标题栏 */
QWidget#titlebar { background: #FDFDFE; border-bottom: 1px solid %LINE; }
QLabel#logo { background: %BRAND; color: #FFFFFF; border-radius: 6px; font-size: 11px; font-weight: 700; }
QPushButton#ttbtn { background: transparent; border: none; color: %T2; font-size: 13px; }
QPushButton#ttbtn:hover { background: %HOVER; color: %T1; }
QPushButton#ttclose { background: transparent; border: none; color: %T2; font-size: 14px; }
QPushButton#ttclose:hover { background: %BADGE; color: #FFFFFF; }

/* 左功能条 */
QWidget#rail { background: %RAIL; }
QPushButton#railbtn {
    background: transparent; border: none; border-radius: 10px;
    font-size: 18px; color: %T2;
}
QPushButton#railbtn:hover { background: #E2E6EC; }
QPushButton#railbtn:checked { background: #DCEAFF; border-left: 3px solid %BRAND; }

/* 三栏列表：行自绘（ConvRow / MessageBubble），列表本体透明 */
QListWidget#convlist, QListWidget#msglist, QListWidget#memberlist {
    background: transparent; border: none; outline: none;
}
QListWidget::item:selected { background: transparent; }
QListWidget::item:hover { background: transparent; }

QScrollBar:vertical { background: transparent; width: 6px; margin: 0; }
QScrollBar::handle:vertical { background: #C6CBD1; border-radius: 3px; min-height: 30px; }
QScrollBar::handle:vertical:hover { background: #AEB4BB; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: transparent; }

/* 会话面板 */
QLineEdit#search {
    background: #FFFFFF; border: 1px solid %LINE; border-radius: 14px;
    padding: 4px 10px; selection-background-color: %BRAND; color: %T1;
}
QLineEdit#search:focus { border-color: %BRAND; }

/* 聊天区 */
QWidget#chathead { background: #FFFFFF; border-bottom: 1px solid %LINE; }
QLabel#chatname { font-size: 16px; font-weight: 600; color: %T1; }
QLabel#chatsub  { font-size: 12px; color: %T2; }
QLabel#memberhead {
    font-size: 12px; font-weight: 600; color: %T2; background: #FFFFFF;
    border-bottom: 1px solid %LINE;
}
QWidget#member { border-left: 1px solid %LINE; }
QWidget#inputrow { background: #FFFFFF; border-top: 1px solid %LINE; }
QTextEdit#input {
    background: #F6F7F9; border: none; border-radius: 8px;
    padding: 8px 10px; selection-background-color: %BRAND; color: %T1;
}
QTextEdit#input:focus { background: #FFFFFF; border: 1px solid %BRAND; }
QLabel#timeline { color: %T3; font-size: 11px; background: transparent; }

QLabel#hint { font-size: 12px; color: %T3; background: transparent; }

QPushButton#send {
    background: %BRAND; color: #FFFFFF; border: none; border-radius: 6px;
    padding: 6px 22px; font-weight: 600;
}
QPushButton#send:hover { background: %BRANDD; }
QPushButton#send:disabled { background: #B9C3CC; }
QPushButton#ghost {
    background: transparent; border: 1px solid %LINE; border-radius: 6px;
    padding: 5px 14px; color: %T1;
}
QPushButton#ghost:hover { background: %HOVER; }

QMenu { background: #FFFFFF; border: 1px solid %LINE; border-radius: 8px; padding: 4px; }
QMenu::item { padding: 7px 26px; border-radius: 5px; color: %T1; }
QMenu::item:selected { background: %SEL; color: %BRAND; }
QMenu::separator { height: 1px; background: %LINE; margin: 4px 8px; }

QToolTip { background: #2E3438; color: #FFFFFF; border: none; padding: 4px 8px; }
QDialog { background: #FFFFFF; }
)");
    s.replace("%CGB",   kChatBg).replace("%CG",  kPanelBg).replace("%RAIL", kRailBg)
     .replace("%LINE",  kLine).replace("%BRAND", kBrand).replace("%T1", kText1)
     .replace("%BRANDD", kBrandDark).replace("%HOVER", kHover).replace("%SEL", kSel)
     .replace("%T2",    kText2).replace("%T3",  kText3).replace("%BADGE", kBadge);
    return s;
}

QString timeText(uint64_t tsMs){
    const QDateTime dt = QDateTime::fromMSecsSinceEpoch(static_cast<qint64>(tsMs));
    const QDate today = QDate::currentDate();
    const QDate d = dt.date();
    if(d == today)      return dt.toString(QStringLiteral("HH:mm"));
    if(d.addDays(1) == today) return QStringLiteral("昨天");
    const QDate weekAgo = today.addDays(-6);
    if(d >= weekAgo)    return QStringLiteral("周%1").arg(QStringLiteral("一二三四五六日")[dt.date().dayOfWeek() - 1]);
    return dt.date().year() == today.year() ? dt.toString(QStringLiteral("MM-dd"))
                                            : dt.toString(QStringLiteral("yyyy-MM-dd"));   // Qt6: QDateTime::year 已删，用 date().year()
}

QString chatTimeText(uint64_t tsMs){
    const QDateTime dt = QDateTime::fromMSecsSinceEpoch(static_cast<qint64>(tsMs));
    const QDate today = QDate::currentDate();
    if(dt.date() == today)     return dt.toString(QStringLiteral("HH:mm"));
    if(dt.date().addDays(1) == today) return QStringLiteral("昨天 %1").arg(dt.toString(QStringLiteral("HH:mm")));
    return dt.toString(QStringLiteral("MM-dd HH:mm"));
}

QColor nameColor(const QString& uid){
    static const char* kPal[] = {"#5B8FF9", "#E8684A", "#FBBD30", "#5AD8A6", "#5D7092", "#6DC8EC"};
    return QColor(QString::fromLatin1(kPal[fnv1aUtf8(uid) % 6]));
}
QColor avatarColor(const QString& uid){
    static const char* kPal[] = {"#5B8FF9", "#E8684A", "#FBBD30", "#5AD8A6", "#5D7092",
                                 "#6DC8EC", "#9270CA", "#F99BD6", "#00B7A3", "#7FB77E"};
    return QColor(QString::fromLatin1(kPal[fnv1aUtf8(uid) % 10]));
}

} // namespace chat::theme