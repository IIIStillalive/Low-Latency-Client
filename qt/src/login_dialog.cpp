// login_dialog.cpp —— 登录/注册输入收集（纯 UI，不碰网络）。
// 布局：host+port 首行，uid+nick+pwd，右侧「注册」（新用户 0x10+0x11）/「登录」（0x11）。
// demo 边界诚实标注：SHA-256 无盐无 TLS，仅防明文口令走线。
#include "login_dialog.hpp"

#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QMessageBox>
#include <QPushButton>

namespace chat {

LoginDialog::LoginDialog(QWidget* parent) : QDialog(parent){
    setWindowTitle(QStringLiteral("IM 登录（demo：SHA-256 无盐，无 TLS）"));

    hostEdit_ = new QLineEdit(QStringLiteral("127.0.0.1"), this);
    portEdit_ = new QLineEdit(QStringLiteral("8891"), this);
    uidEdit_  = new QLineEdit(this);
    pwdEdit_  = new QLineEdit(this);
    pwdEdit_->setEchoMode(QLineEdit::Password);
    nickEdit_ = new QLineEdit(this);
    uidEdit_->setPlaceholderText(QStringLiteral("登录账号（注册时创建）"));
    nickEdit_->setPlaceholderText(QStringLiteral("昵称（仅注册需要）"));

    auto* form = new QFormLayout;
    form->addRow(QStringLiteral("服务器"), hostEdit_);
    form->addRow(QStringLiteral("端口"),   portEdit_);
    form->addRow(QStringLiteral("账号"),   uidEdit_);
    form->addRow(QStringLiteral("口令"),   pwdEdit_);
    form->addRow(QStringLiteral("昵称"),   nickEdit_);
    setLayout(form);

    auto* loginBtn    = new QPushButton(QStringLiteral("登录"), this);
    auto* registerBtn = new QPushButton(QStringLiteral("注册"), this);
    auto* cancelBtn   = new QPushButton(QStringLiteral("取消"), this);
    auto* btnRow      = new QHBoxLayout;
    btnRow->addWidget(loginBtn);
    btnRow->addWidget(registerBtn);
    btnRow->addWidget(cancelBtn);
    btnRow->addStretch(1);
    form->addRow(btnRow);

    connect(loginBtn,    &QPushButton::clicked, this, &LoginDialog::submitLogin);
    connect(registerBtn, &QPushButton::clicked, this, &LoginDialog::submitRegister);
    connect(cancelBtn,   &QPushButton::clicked, this, &QDialog::reject);

    uidEdit_->setFocus();
}

void LoginDialog::submitLogin(){
    registerMode_ = false;
    accept();
}
void LoginDialog::submitRegister(){
    registerMode_ = true;
    accept();
}

void LoginDialog::accept(){
    if(uid().isEmpty() || pwd().isEmpty()){
        QMessageBox::warning(this, QStringLiteral("输入不完整"),
                             QStringLiteral("账号和口令不能为空"));
        return;
    }
    if(port() == 0){
        QMessageBox::warning(this, QStringLiteral("端口非法"),
                             QStringLiteral("端口必须是 1–65535 的数字"));
        return;
    }
    QDialog::accept();
}

} // namespace chat