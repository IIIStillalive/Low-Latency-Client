#pragma once

// add_friend_dialog.{hpp,cpp} —— 添加好友/用户（v3 取代「新建单聊」裸输入）。
// 流程：输入 uid → [查找]（0x1B）→ 异步回执（0x9C）→ 存在则结果卡 + [发起聊天]；
//   不存在红字「用户不存在」；也可“直接按 uid 发起”跳过查找（老路径）。
// 与 ImSession 的桥接由 MainWindow 完成（本 dialog 只发信号，不直接碰网络）。
#include <QDialog>
#include <QString>

// Qt 类型必须在此处（全局作用域）前向声明——若放进 chat 命名空间会产生 chat::QWidget 等
// 假类型，污染本 TU 及 include 链上所有后续文件的类名解析。
class QLineEdit;
class QLabel;
class QPushButton;
class QWidget;

namespace chat {

class AddFriendDialog : public QDialog {
    Q_OBJECT
public:
    explicit AddFriendDialog(QWidget* parent = nullptr);

    // MainWindow 把 ImSession::userLookupResult 接到这里（模态 exec 内事件循环照常派发）
    void onLookupResult(bool found, const QString& nick);

signals:
    void lookupRequested(const QString& uid);      // → ImSession::lookupUser
    void startConvRequested(const QString& uid);   // → ImSession::createConv(dm)

public:
    // MainWindow 注入当前登录用户 uid；输入它时直接拒绝（服务端另有权威拦截）
    void setSelfUid(const QString& uid){ selfUid_ = uid; }

private slots:
    void onLookupClicked();
    void onStartClicked();
    void onDirectClicked();

private:
    void setResultVisible(bool show);
    void showStatus(const QString& text, bool ok);

    QLineEdit* uidEdit_ = nullptr;
    QPushButton* lookBtn_ = nullptr;
    QLabel* status_ = nullptr;
    QWidget* resultCard_ = nullptr;
    QLabel* resAvatar_ = nullptr;
    QLabel* resName_ = nullptr;
    QLabel* resUid_ = nullptr;
    QPushButton* chatBtn_ = nullptr;
    QString curUid_;                              // 当前查找/发起目标
    QString selfUid_;                             // 当前登录用户（不能添加/发起自己）
};

} // namespace chat