// avatar.cpp —— 圆形头像实现（离屏绘制 + 缓存）。
#include "avatar.hpp"

#include <QFontMetrics>
#include <QHash>
#include <QPainter>

#include "theme.hpp"

namespace chat {

QPixmap avatarPixmap(const QString& uid, const QString& text, int size){
    static QHash<QString, QPixmap> cache;
    const QString key = uid + QChar(0x1f) + text + QChar(0x1f) + QString::number(size);
    const auto it = cache.constFind(key);
    if(it != cache.cend()) return *it;

    const qreal dpr = 2.0;                         // 固定 2x 渲染（跨屏稳定，代价可忽略）
    QPixmap pm(int(size * dpr), int(size * dpr));
    pm.fill(Qt::transparent);
    pm.setDevicePixelRatio(dpr);

    QPainter p(&pm);
    p.setRenderHint(QPainter::Antialiasing, true);
    p.setBrush(theme::avatarColor(uid));
    p.setPen(Qt::NoPen);
    p.drawEllipse(0, 0, size, size);

    QString ch = text.trimmed().isEmpty() ? QStringLiteral("?") : text.left(1).toUpper();
    QFont f(QStringLiteral("Microsoft YaHei UI"));
    f.setPixelSize(int(size * 0.52));
    p.setFont(f);
    p.setPen(QColor(Qt::white));
    p.drawText(QRect(0, 0, size, size), Qt::AlignCenter, ch);
    p.end();

    cache.insert(key, pm);
    return pm;
}

QLabel* makeAvatarLabel(const QString& uid, const QString& text, int size, QWidget* parent){
    auto* l = new QLabel(parent);
    l->setFixedSize(size, size);
    l->setPixmap(avatarPixmap(uid, text, size));
    l->setScaledContents(false);
    return l;
}

} // namespace chat