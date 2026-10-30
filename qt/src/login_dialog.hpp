#pragma once

// login_dialog.hpp —— 登录/注册对话框（纯输入收集，不做网络；网络由 main.cpp 的 ImSession 驱动）。
#include <QDialog>
#include <QLineEdit>

namespace chat {

class LoginDialog : public QDialog {
    Q_OBJECT
public:
    explicit LoginDialog(QWidget* parent = nullptr);

    QString host() const { return hostEdit_->text(); }
    quint16 port() const { return static_cast<quint16>(portEdit_->text().trimmed().toUShort()); }
    QString uid() const { return uidEdit_->text().trimmed(); }
    QString nick() const { return nickEdit_->text().trimmed(); }
    QString pwd() const { return pwdEdit_->text(); }
    bool registerMode() const { return registerMode_; }

public slots:
    void submitLogin();               // 设置 registerMode_=false 后 accept
    void submitRegister();            // 设置 registerMode_=true 后 accept（新用户）

private:
    void accept() override;           // 校验准入（uid/pwd 非空）后 QDialog::accept

    QLineEdit* hostEdit_ = nullptr;
    QLineEdit* portEdit_ = nullptr;
    QLineEdit* uidEdit_ = nullptr;
    QLineEdit* nickEdit_ = nullptr;
    QLineEdit* pwdEdit_ = nullptr;
    bool registerMode_ = false;
};

} // namespace chat