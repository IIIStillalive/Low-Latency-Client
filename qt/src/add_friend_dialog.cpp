// add_friend_dialog.cpp —— 添加好友对话框（QQ 风格卡片）。
#include "add_friend_dialog.hpp"

#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QVBoxLayout>

#include "avatar.hpp"
#include "theme.hpp"

namespace chat {

AddFriendDialog::AddFriendDialog(QWidget* parent) : QDialog(parent){
    setWindowTitle(QStringLiteral("添加好友 / 用户"));
    setModal(true);
    setFixedWidth(360);

    auto* root = new QVBoxLayout(this);
    root->setContentsMargins(20, 18, 20, 16);
    root->setSpacing(10);

    auto* title = new QLabel(QStringLiteral("添加好友 / 用户"), this);
    QFont tf = title->font(); tf.setPixelSize(16); tf.setBold(true);
    title->setFont(tf);
    root->addWidget(title);

    auto* row = new QHBoxLayout();
    row->setSpacing(8);
    uidEdit_ = new QLineEdit(this);
    uidEdit_->setPlaceholderText(QStringLiteral("对方账号 uid"));
    uidEdit_->setClearButtonEnabled(true);
    lookBtn_ = new QPushButton(QStringLiteral("查找"), this);
    lookBtn_->setObjectName(QStringLiteral("ghost"));
    row->addWidget(uidEdit_, 1);
    row->addWidget(lookBtn_);
    root->addLayout(row);

    status_ = new QLabel(this);
    status_->setObjectName(QStringLiteral("hint"));
    status_->setWordWrap(true);
    root->addWidget(status_);

    // 结果卡：头像 + 昵称 + uid + [发起聊天]
    resultCard_ = new QWidget(this);
    {
        auto* hl = new QHBoxLayout(resultCard_);
        hl->setContentsMargins(12, 10, 12, 10);
        hl->setSpacing(12);
        hl->addWidget(resAvatar_ = new QLabel(resultCard_));
        auto* vl = new QVBoxLayout();
        vl->setSpacing(2);
        resName_ = new QLabel(resultCard_);
        QFont nf = resName_->font(); nf.setPixelSize(14); nf.setBold(true);
        resName_->setFont(nf);
        resUid_ = new QLabel(resultCard_);
        QFont uf = resUid_->font(); uf.setPixelSize(12);
        resUid_->setFont(uf);
        resUid_->setStyleSheet(QStringLiteral("color:%1;").arg(theme::kText2));
        vl->addWidget(resName_);
        vl->addWidget(resUid_);
        hl->addLayout(vl, 1);
        chatBtn_ = new QPushButton(QStringLiteral("发起聊天"), resultCard_);
        chatBtn_->setObjectName(QStringLiteral("send"));
        chatBtn_->setCursor(Qt::PointingHandCursor);
        hl->addWidget(chatBtn_);
        resultCard_->setStyleSheet(QStringLiteral(
            "QWidget#resultCard{background:#F3F5F7;border-radius:8px;}"));
        resultCard_->setObjectName(QStringLiteral("resultCard"));
    }
    root->addWidget(resultCard_);
    setResultVisible(false);

    auto* foot = new QHBoxLayout();
    foot->addStretch(1);
    auto* directBtn = new QPushButton(QStringLiteral("直接按 uid 发起（不查找）"), this);
    directBtn->setFlat(true);
    directBtn->setStyleSheet(QStringLiteral("color:%1;").arg(theme::kBrand));
    directBtn->setCursor(Qt::PointingHandCursor);
    foot->addWidget(directBtn);
    root->addLayout(foot);

    connect(lookBtn_, &QPushButton::clicked, this, &AddFriendDialog::onLookupClicked);
    connect(chatBtn_, &QPushButton::clicked, this, &AddFriendDialog::onStartClicked);
    connect(directBtn, &QPushButton::clicked, this, &AddFriendDialog::onDirectClicked);
    connect(uidEdit_, &QLineEdit::returnPressed, this, &AddFriendDialog::onLookupClicked);
}

void AddFriendDialog::onLookupClicked(){
    const QString uid = uidEdit_->text().trimmed();
    if(uid.isEmpty()){ showStatus(QStringLiteral("先输入对方 uid"), false); return; }
    if(!selfUid_.isEmpty() && uid == selfUid_){
        setResultVisible(false);
        showStatus(QStringLiteral("不能添加自己"), false);
        return;
    }
    curUid_ = uid;
    setResultVisible(false);
    showStatus(QStringLiteral("正在查找 %1 …").arg(uid), true);
    emit lookupRequested(uid);
}

void AddFriendDialog::onLookupResult(bool found, const QString& nick){
    if(curUid_.isEmpty()) return;
    if(found){
        showStatus(QStringLiteral("用户存在"), true);
        resAvatar_->setPixmap(avatarPixmap(curUid_, nick, 44));
        resName_->setText(nick);
        resUid_->setText(QStringLiteral("uid: %1").arg(curUid_));
        setResultVisible(true);
        chatBtn_->setEnabled(true);
    } else {
        setResultVisible(false);
        showStatus(QStringLiteral("用户不存在：%1").arg(curUid_), false);
        chatBtn_->setEnabled(false);
    }
}

void AddFriendDialog::onStartClicked(){
    if(curUid_.isEmpty()) return;
    emit startConvRequested(curUid_);
    accept();
}
void AddFriendDialog::onDirectClicked(){
    const QString uid = uidEdit_->text().trimmed();
    if(uid.isEmpty()){ showStatus(QStringLiteral("先输入对方 uid"), false); return; }
    if(!selfUid_.isEmpty() && uid == selfUid_){
        setResultVisible(false);
        showStatus(QStringLiteral("不能添加自己"), false);
        return;
    }
    curUid_ = uid;
    emit startConvRequested(curUid_);
    accept();
}

void AddFriendDialog::setResultVisible(bool show){
    resultCard_->setVisible(show);
    if(!show) resUid_->clear();
}
void AddFriendDialog::showStatus(const QString& text, bool ok){
    status_->setText(text);
    status_->setStyleSheet(QStringLiteral("color:%1;")
        .arg(ok ? theme::kBrand : theme::kRed));
}

} // namespace chat