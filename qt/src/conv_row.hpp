#pragma once

// conv_row.{hpp,cpp} —— 会话列单行（QQ 风格）：圆头像 + 昵称/最后消息预览 + 时间/未读角标。
// 自绘背景（悬停/选中 + 选中左 3px 品牌条），QListWidget::setItemWidget 挂载。
#include <QString>
#include <QWidget>

#include <cstdint>

namespace chat {

class ConvRow : public QWidget {
    Q_OBJECT
public:
    explicit ConvRow(QWidget* parent = nullptr);
    void setData(const QString& convId, bool grp, const QString& name,
                 const QString& preview, uint64_t tsMs, uint32_t unread, bool selected);
    QString convId() const { return cid_; }
    void setSelected(bool sel);           // 会话切换时 MainWindow 批量刷
    QSize sizeHint() const override { return QSize(200, 56); }

protected:
    void paintEvent(QPaintEvent*) override;
    void enterEvent(QEnterEvent*) override;
    void leaveEvent(QEvent*) override;

private:
    QString cid_;
    QPixmap av_;
    QString name_, preview_, time_;
    uint32_t unread_ = 0;
    bool selected_ = false, hover_ = false;
};

} // namespace chat