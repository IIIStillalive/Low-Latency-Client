#pragma once

// avatar.{hpp,cpp} —— 圆形头像：uid 哈希取色 + 首字符白字（QQ 默认头像同思路，无图片资源）。
// makeAvatarLabel 返回一个已装好圆形位图的 QLabel（HiDPI 感知，位图按设备比例放大）。
#include <QLabel>
#include <QPixmap>
#include <QString>

namespace chat {

// 画 64*scale 圆形位图（scale=设备像素比），缓存按 (uid,text,size) 键。
QPixmap avatarPixmap(const QString& uid, const QString& text, int size);

// 便捷：新建一个 square*? 不——固定 makeAvatarLabel(uid, text, size) → QLabel，尺寸=size。
QLabel* makeAvatarLabel(const QString& uid, const QString& text, int size,
                        QWidget* parent = nullptr);

} // namespace chat