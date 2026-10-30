// main_window.cpp —— v3 类 QQ IM 客户端 UI：无边框标题栏 + 三栏 + 会话队列排序 + 气泡。
// 设计见 plan/task_plan_qt_v3_ui.md §2/§4/§5。保留 v2 状态机 / seq 去重 / 触顶续拉。
#include "main_window.hpp"

#include <algorithm>
#include <functional>
#include <set>

#include <QCloseEvent>
#include <QHBoxLayout>
#include <QInputDialog>
#include <QKeyEvent>
#include <QLabel>
#include <QLineEdit>
#include <QListWidget>
#include <QListWidgetItem>
#include <QMenu>
#include <QMessageBox>
#include <QPushButton>
#include <QPoint>
#include <QRegularExpression>
#include <QScrollBar>
#include <QSize>
#include <QTextEdit>
#include <QVBoxLayout>
#include <QWidget>

#if defined(Q_OS_WIN)
#include <qt_windows.h>
#endif

#include "add_friend_dialog.hpp"
#include "avatar.hpp"
#include "conv_row.hpp"
#include "message_bubble.hpp"
#include "theme.hpp"

namespace chat {

// 输入框：Enter 发送（无 Shift）／ Shift+Enter 换行。回调注入避免信号绕圈。
// 定义在 chat 命名空间（非匿名）：与 main_window.hpp 的 class InputEdit 前向声明对齐。
class InputEdit final : public QTextEdit {
public:
    using QTextEdit::QTextEdit;
    std::function<void()> sendCb;
protected:
    void keyPressEvent(QKeyEvent* e) override{
        if((e->key() == Qt::Key_Return || e->key() == Qt::Key_Enter)
           && !(e->modifiers() & Qt::ShiftModifier)){
            if(sendCb) sendCb();
            return;
        }
        QTextEdit::keyPressEvent(e);
    }
};

namespace {

constexpr int kTitleH   = 36;              // 无边框标题栏高（nativeEvent 命中区）
constexpr int kResizeBand = 6;             // 四边/四角缩放带
constexpr int kHistoryChunk  = 200;
constexpr int kMaxTextItemLen = 2000;
constexpr int kTimeLineGapMs = 5 * 60 * 1000;  // >5min 才插时间行

QString truncated(const QString& s){
    return s.size() <= kMaxTextItemLen ? s : s.left(kMaxTextItemLen) + QStringLiteral("…");
}
QString previewOf(bool grp, const QString& fromNick, const QString& msg){
    if(!grp) return msg.left(200);
    return QStringLiteral("%1: %2").arg(fromNick).arg(msg.left(190));   // grp 预览带昵称，与 0x93 同构
}

} // namespace

// ---------------------------------------------------------------------------
// 构造 / UI 组装
// ---------------------------------------------------------------------------
MainWindow::MainWindow(const QString& host, quint16 port, bool registerMode,
                       const QString& uid, const QString& nick, const QString& pwd,
                       QWidget* parent)
    : QMainWindow(parent), host_(host), port_(port), uid_(uid), nick_(nick)
{
    setWindowFlags(Qt::FramelessWindowHint | Qt::Window);
    setMinimumSize(760, 520);
    resize(1040, 660);

    session_ = new ImSession(this);
    connect(session_, &ImSession::connected,        this, &MainWindow::onConnected);
    connect(session_, &ImSession::authOk,           this, &MainWindow::onAuthOk);
    connect(session_, &ImSession::convList,         this, &MainWindow::onConvList);
    connect(session_, &ImSession::convCreated,      this, &MainWindow::onConvCreated);
    connect(session_, &ImSession::convOpened,       this, &MainWindow::onConvOpened);
    connect(session_, &ImSession::historyBatch,     this, &MainWindow::onHistoryBatch);
    connect(session_, &ImSession::message,          this, &MainWindow::onMessage);
    connect(session_, &ImSession::members,          this, &MainWindow::onMembers);
    connect(session_, &ImSession::presence,         this, &MainWindow::onPresence);
    connect(session_, &ImSession::convChanged,      this, &MainWindow::onConvChanged);
    connect(session_, &ImSession::sessionError,     this, &MainWindow::onSessionError);
    connect(session_, &ImSession::disconnected,     this, &MainWindow::onDisconnected);

    // ---- 无边框标题栏 ----
    auto* title = new QWidget(this);
    title->setFixedHeight(kTitleH);
    title->setObjectName(QStringLiteral("titlebar"));
    auto* th = new QHBoxLayout(title);
    th->setContentsMargins(12, 0, 6, 0);
    th->setSpacing(6);
    auto* logo = new QLabel(QStringLiteral("IM"), title);
    logo->setObjectName(QStringLiteral("logo"));
    logo->setAlignment(Qt::AlignCenter);
    logo->setFixedSize(20, 20);
    titleLabel_ = new QLabel(QStringLiteral("IM Chat — 低延迟聊天客户端"), title);
    titleLabel_->setObjectName(QStringLiteral("hint"));
    ttMin_   = new QPushButton(QStringLiteral("─"), title);
    ttMax_   = new QPushButton(QStringLiteral("□"), title);
    ttClose_ = new QPushButton(QStringLiteral("✕"), title);
    for(auto* b : {ttMin_, ttMax_, ttClose_}){
        b->setObjectName(QStringLiteral("ttbtn"));
        b->setFixedSize(30, kTitleH);
        b->setFocusPolicy(Qt::NoFocus);
        b->setCursor(Qt::PointingHandCursor);
    }
    ttClose_->setObjectName(QStringLiteral("ttclose"));
    th->addWidget(logo);
    th->addWidget(titleLabel_);
    th->addStretch(1);
    th->addWidget(ttMin_);
    th->addWidget(ttMax_);
    th->addWidget(ttClose_);
    connect(ttMin_,   &QPushButton::clicked, this, &MainWindow::onTitleMin);
    connect(ttMax_,   &QPushButton::clicked, this, &MainWindow::onTitleMaxToggle);
    connect(ttClose_, &QPushButton::clicked, this, &MainWindow::onTitleClose);

    // ---- 左功能条 ----
    auto* rail = new QWidget(this);
    rail->setFixedWidth(56);
    rail->setObjectName(QStringLiteral("rail"));
    auto* rv = new QVBoxLayout(rail);
    rv->setContentsMargins(0, 10, 0, 8);
    rv->setSpacing(6);
    rv->addWidget(makeAvatarLabel(uid_, nick_.isEmpty() ? uid_ : nick_, 36, rail), 0, Qt::AlignHCenter);
    railChatsBtn_ = new QPushButton(QStringLiteral("💬"), rail);
    railAddBtn_   = new QPushButton(QStringLiteral("➕"), rail);
    railSetBtn_   = new QPushButton(QStringLiteral("⚙"), rail);
    for(auto* b : {railChatsBtn_, railAddBtn_, railSetBtn_}){
        b->setObjectName(QStringLiteral("railbtn"));
        b->setFixedSize(40, 40);
        b->setCursor(Qt::PointingHandCursor);
        b->setFocusPolicy(Qt::NoFocus);
        b->setToolTip(b == railChatsBtn_ ? QStringLiteral("会话")
                     : b == railAddBtn_   ? QStringLiteral("添加 / 新建")
                                          : QStringLiteral("设置（占位）"));
    }
    railChatsBtn_->setCheckable(true);
    railChatsBtn_->setChecked(true);
    rv->addWidget(railChatsBtn_, 0, Qt::AlignHCenter);
    rv->addStretch(1);
    rv->addWidget(railAddBtn_, 0, Qt::AlignHCenter);
    rv->addWidget(railSetBtn_, 0, Qt::AlignHCenter);
    connect(railChatsBtn_, &QPushButton::clicked, this, [this]{ /* 会话本就常驻 */ });
    connect(railAddBtn_,   &QPushButton::clicked, this, &MainWindow::onRailAddMenu);
    connect(railSetBtn_,   &QPushButton::clicked, this, [this]{ setStatus(QStringLiteral("设置暂未实现"), false); });

    // ---- 会话面板 ----
    auto* panel = new QWidget(this);
    panel->setObjectName(QStringLiteral("panel"));
    panel->setFixedWidth(280);
    auto* pv = new QVBoxLayout(panel);
    pv->setContentsMargins(8, 8, 8, 6);
    pv->setSpacing(6);
    searchEdit_ = new QLineEdit(panel);
    searchEdit_->setPlaceholderText(QStringLiteral("搜索会话"));
    searchEdit_->setClearButtonEnabled(true);
    searchEdit_->setEnabled(false);
    searchEdit_->setObjectName(QStringLiteral("search"));
    convList_ = new QListWidget(panel);
    convList_->setObjectName(QStringLiteral("convlist"));
    convList_->setSelectionMode(QAbstractItemView::NoSelection);
    convList_->setVerticalScrollMode(QAbstractItemView::ScrollPerPixel);
    convList_->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    convList_->setContextMenuPolicy(Qt::CustomContextMenu);
    pv->addWidget(searchEdit_);
    pv->addWidget(convList_, 1);
    auto* foot = new QHBoxLayout();
    foot->setSpacing(6);
    statusLabel_ = new QLabel(QStringLiteral("连接中…"), panel);
    statusLabel_->setObjectName(QStringLiteral("hint"));
    statusLabel_->setMinimumWidth(150);
    connBtn_ = new QPushButton(QStringLiteral("重连"), panel);
    connBtn_->setObjectName(QStringLiteral("ghost"));
    connBtn_->setEnabled(false);
    connBtn_->setCursor(Qt::PointingHandCursor);
    foot->addWidget(statusLabel_, 1);
    foot->addWidget(connBtn_);
    pv->addLayout(foot);

    // ---- 聊天区 ----
    auto* chatCol = new QWidget(this);
    chatCol->setObjectName(QStringLiteral("chat"));
    auto* cv = new QVBoxLayout(chatCol);
    cv->setContentsMargins(0, 0, 0, 0);
    cv->setSpacing(0);
    auto* chead = new QWidget(chatCol);
    chead->setObjectName(QStringLiteral("chathead"));
    auto* chv = new QVBoxLayout(chead);
    chv->setContentsMargins(16, 8, 16, 6);
    chv->setSpacing(2);
    chatName_ = new QLabel(QStringLiteral("—"), chead);
    chatName_->setObjectName(QStringLiteral("chatname"));
    chatSub_  = new QLabel(chead);
    chatSub_->setObjectName(QStringLiteral("chatsub"));
    chv->addWidget(chatName_);
    chv->addWidget(chatSub_);
    auto* hint = new QLabel(QStringLiteral("Enter 发送  ·  Shift+Enter 换行  ·  上翻加载更早消息"), chatCol);
    hint->setObjectName(QStringLiteral("hint"));
    hint->setAlignment(Qt::AlignCenter);
    hint->setFixedHeight(22);
    messageList_ = new QListWidget(chatCol);
    messageList_->setObjectName(QStringLiteral("msglist"));
    messageList_->setSelectionMode(QAbstractItemView::NoSelection);
    messageList_->setVerticalScrollMode(QAbstractItemView::ScrollPerPixel);
    messageList_->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    auto* inrow = new QWidget(chatCol);
    inrow->setObjectName(QStringLiteral("inputrow"));
    auto* il = new QHBoxLayout(inrow);
    il->setContentsMargins(10, 8, 10, 8);
    il->setSpacing(8);
    inputEdit_ = new InputEdit(inrow);
    inputEdit_->setObjectName(QStringLiteral("input"));
    inputEdit_->setPlaceholderText(QStringLiteral("输入消息…"));
    inputEdit_->setFixedHeight(72);
    inputEdit_->setEnabled(false);
    sendBtn_ = new QPushButton(QStringLiteral("发送"), inrow);
    sendBtn_->setObjectName(QStringLiteral("send"));
    sendBtn_->setFixedWidth(72);
    sendBtn_->setEnabled(false);
    sendBtn_->setCursor(Qt::PointingHandCursor);
    sendBtn_->setFocusPolicy(Qt::NoFocus);
    il->addWidget(inputEdit_, 1);
    il->addWidget(sendBtn_, 0, Qt::AlignBottom);
    cv->addWidget(chead);
    cv->addWidget(hint);
    cv->addWidget(messageList_, 1);
    cv->addWidget(inrow);
    inputEdit_->sendCb = [this]{ onSendClicked(); };

    // ---- 群成员面板 ----
    memberCol_ = new QWidget(this);
    memberCol_->setObjectName(QStringLiteral("member"));
    memberCol_->setFixedWidth(180);
    auto* mv = new QVBoxLayout(memberCol_);
    mv->setContentsMargins(0, 0, 0, 0);
    mv->setSpacing(0);
    memberHead_ = new QLabel(QStringLiteral("成员"), memberCol_);
    memberHead_->setObjectName(QStringLiteral("memberhead"));
    memberHead_->setContentsMargins(12, 8, 12, 6);
    memberList_ = new QListWidget(memberCol_);
    memberList_->setObjectName(QStringLiteral("memberlist"));
    memberList_->setSelectionMode(QAbstractItemView::NoSelection);
    memberList_->setVerticalScrollMode(QAbstractItemView::ScrollPerPixel);
    memberList_->setFocusPolicy(Qt::NoFocus);
    mv->addWidget(memberHead_);
    mv->addWidget(memberList_, 1);

    // ---- 组装 ----
    auto* body = new QWidget(this);
    auto* bh = new QHBoxLayout(body);
    bh->setContentsMargins(0, 0, 0, 0);
    bh->setSpacing(0);
    bh->addWidget(rail);
    bh->addWidget(panel);
    bh->addWidget(chatCol, 1);
    bh->addWidget(memberCol_);
    memberCol_->setVisible(false);

    auto* central = new QWidget(this);
    auto* vl = new QVBoxLayout(central);
    vl->setContentsMargins(0, 0, 0, 0);
    vl->setSpacing(0);
    vl->addWidget(title);
    vl->addWidget(body, 1);
    setCentralWidget(central);

    // ---- 信号接配 ----
    connect(searchEdit_, &QLineEdit::textChanged, this, &MainWindow::onSearchEdited);
    connect(convList_, &QListWidget::itemClicked, this, &MainWindow::onConvRowClicked);
    connect(convList_, &QWidget::customContextMenuRequested, this, &MainWindow::onConvContextMenu);
    connect(connBtn_, &QPushButton::clicked, this, &MainWindow::onConnBtnClicked);
    connect(sendBtn_, &QPushButton::clicked, this, &MainWindow::onSendClicked);
    connect(messageList_->verticalScrollBar(), &QScrollBar::valueChanged,
            this, &MainWindow::onScrollToTop);

    session_->start(host_, port_, registerMode, uid_, nick_, pwd);
}

void MainWindow::closeEvent(QCloseEvent* event){
    if(session_) session_->disconnectNow();      // 关窗 = 退出客户端
    QMainWindow::closeEvent(event);
}

bool MainWindow::nativeEvent(const QByteArray& eventType, void* message, qintptr* result){
#if defined(Q_OS_WIN)
    if(eventType == "windows_generic_MSG" || eventType == QByteArray("windows_dispatcher_MSG")){
        MSG* msg = static_cast<MSG*>(message);
        if(msg->message == WM_NCHITTEST){
            *result = HTCLIENT;
            if(isMaximized()) return true;               // 最大化态不放边缘命中（无边框无系统框）
            const int gx = static_cast<int>(msg->pt.x);
            const int gy = static_cast<int>(msg->pt.y);
            const QPoint p = mapFromGlobal(QPoint(gx, gy));
            const int w = width(), h = height();
            const bool l = p.x() < kResizeBand, r = p.x() >= w - kResizeBand;
            const bool t = p.y() < kResizeBand, b = p.y() >= h - kResizeBand;
            if(t && l)      *result = HTTOPLEFT;
            else if(t && r) *result = HTTOPRIGHT;
            else if(b && l) *result = HTBOTTOMLEFT;
            else if(b && r) *result = HTBOTTOMRIGHT;
            else if(t)      *result = HTTOP;
            else if(b)      *result = HTBOTTOM;
            else if(l)      *result = HTLEFT;
            else if(r)      *result = HTRIGHT;
            else if(p.y() < kTitleH){
                if(p.x() >= w - 96) *result = HTCLIENT;  // 右上三个标题按钮区：交给按钮
                else                *result = HTCAPTION; // 标题栏：系统级拖动 / 双击最大化
            }
            return true;
        }
    }
#else
    Q_UNUSED(eventType); Q_UNUSED(message); Q_UNUSED(result);
#endif
    return QMainWindow::nativeEvent(eventType, message, result);
}

// ---------------------------------------------------------------------------
// 无边框标题栏动作
// ---------------------------------------------------------------------------
void MainWindow::onTitleMin(){ showMinimized(); }
void MainWindow::onTitleMaxToggle(){
    setWindowState(windowState() ^ Qt::WindowMaximized);
}
void MainWindow::onTitleClose(){ close(); }

// ---------------------------------------------------------------------------
// 左功能条 / 连接
// ---------------------------------------------------------------------------
void MainWindow::onRailAddMenu(){
    if(!authed_) return;
    auto* menu = new QMenu(this);
    menu->addAction(QStringLiteral("添加好友 / 用户"), this, [this]{ onAddFriendDialog(); });
    menu->addAction(QStringLiteral("新建群聊"),         this, [this]{ onNewGrpClicked(); });
    menu->addAction(QStringLiteral("加入群聊"),         this, [this]{ onJoinGrpClicked(); });
    menu->exec(railAddBtn_->mapToGlobal(QPoint(railAddBtn_->width() / 2, railAddBtn_->height())));
    delete menu;
}

void MainWindow::onAddFriendDialog(){
    if(!authed_) return;
    auto* dlg = new AddFriendDialog(this);
    addDlg_ = dlg;
    dlg->setSelfUid(uid_);                          // 输入登录用户自己 → 对话框直接拒绝
    // 查找：对话框 → session；回执：session → 对话框（exec 嵌套事件循环照常派发）
    connect(dlg, &AddFriendDialog::lookupRequested, session_, &ImSession::lookupUser);
    connect(session_, &ImSession::userLookupResult, dlg, &AddFriendDialog::onLookupResult);
    // 发起聊天：dm 建会话（幂等，服务端回 0x92 → onConvCreated 自动刷新并打开）
    connect(dlg, &AddFriendDialog::startConvRequested, this, [this](const QString& peer){
        const QString u = peer.trimmed();
        if(!u.isEmpty()) session_->createConv(kImConvDm, QString(), QStringList{u});
    });
    dlg->exec();
    dlg->deleteLater();
    addDlg_ = nullptr;
}

void MainWindow::onNewGrpClicked(){
    if(!authed_) return;
    bool ok = false;
    const QString name = QInputDialog::getText(this, QStringLiteral("新建群聊"),
        QStringLiteral("群名："), QLineEdit::Normal, QString(), &ok);
    if(!ok || name.trimmed().isEmpty()) return;
    const QString m = QInputDialog::getText(this, QStringLiteral("新建群聊"),
        QStringLiteral("成员 uid（逗号或空格分隔）："), QLineEdit::Normal, QString(), &ok);
    if(!ok || m.trimmed().isEmpty()) return;
    QStringList members;
    const QStringList parts = m.split(QRegularExpression(QStringLiteral("[,\\s]+")),
                                      Qt::SkipEmptyParts);
    for(const auto& p : parts) members.push_back(p);
    if(members.isEmpty()) return;
    for(const auto& u : members){
        if(u == uid_){  // 不得邀请自己入群（服务端同样权威拒绝）
            QMessageBox::warning(this, QStringLiteral("新建群聊"), QStringLiteral("不能邀请自己进群"));
            return;
        }
    }
    session_->createConv(kImConvGrp, name.trimmed(), members);
}
void MainWindow::onJoinGrpClicked(){
    if(!authed_) return;
    bool ok = false;
    QString id = QInputDialog::getText(this, QStringLiteral("加入群聊"),
        QStringLiteral("群 id（会话列 `群名 (grp:N)` 里的 grp:N，可只填数字）："),
        QLineEdit::Normal, QString(), &ok);
    if(!ok) return;
    id = id.trimmed();
    if(id.isEmpty()) return;
    if(!id.startsWith(QStringLiteral("grp:")) && !id.startsWith(QStringLiteral("dm:")))
        id.prepend(QStringLiteral("grp:"));
    session_->joinConv(id);
}

void MainWindow::onConnBtnClicked(){
    if(authed_){                    // 退出登录 → 断开，进入可重连态
        session_->disconnectNow();
        authed_ = false;
        connected_ = false;
        viewReady_ = false;
        activeConv_.clear();
        inputEdit_->setEnabled(false);
        sendBtn_->setEnabled(false);
        connBtn_->setText(QStringLiteral("重连"));
        setStatus(QStringLiteral("已退出登录 —— [重连]可再次登录"), false);
        return;
    }
    session_->reconnect();          // 断线态 → 手动重连（重发 0x11）
    setStatus(QStringLiteral("重连中…"), true);
}

// ---------------------------------------------------------------------------
// 会话队列
// ---------------------------------------------------------------------------
void MainWindow::onSearchEdited(const QString& text){
    searchFilter_ = text.trimmed();
    rebuildConvList();
}

void MainWindow::onConvRowClicked(QListWidgetItem* item){
    const QString cid = item->data(Qt::UserRole).toString();
    if(cid.isEmpty()) return;
    openConv(cid, /*continuation=*/false);
}

void MainWindow::onConvContextMenu(const QPoint& pos){
    QListWidgetItem* item = convList_->itemAt(pos);
    if(!item) return;
    const QString cid = item->data(Qt::UserRole).toString();
    const uint8_t type = static_cast<uint8_t>(item->data(Qt::UserRole + 1).toUInt());
    if(type != kImConvGrp) return;              // dm 无退群（服务端也拒）
    auto* menu = new QMenu(this);
    const QString c = cid;
    menu->addAction(QStringLiteral("退群"), [this, c]{
        session_->leaveConv(c);
        session_->requestConvList();
        if(c == activeConv_){
            activeConv_.clear();
            messageList_->clear();
            inputEdit_->setEnabled(false);
            sendBtn_->setEnabled(false);
            memberList_->clear();
            members_.clear();
        }
    });
    menu->exec(convList_->mapToGlobal(pos));
    delete menu;
}

void MainWindow::rebuildConvList(){
    convList_->clear();
    QList<ConvEntry> sorted = convs_;
    std::sort(sorted.begin(), sorted.end(), [](const ConvEntry& a, const ConvEntry& b){
        if(a.lastTsMs != b.lastTsMs) return a.lastTsMs > b.lastTsMs;   // 最近消息在前
        return a.convId < b.convId;                                     // 同锚稳定序
    });
    for(const auto& c : sorted){
        if(!searchFilter_.isEmpty()
           && !c.name.contains(searchFilter_, Qt::CaseInsensitive))
            continue;
        auto* item = new QListWidgetItem(convList_);
        auto* row = new ConvRow(convList_);
        row->setData(c.convId, c.type == kImConvGrp, c.name, c.lastMsg,
                     c.lastTsMs, c.unread, c.convId == activeConv_);
        item->setSizeHint(row->sizeHint());
        item->setData(Qt::UserRole,     c.convId);
        item->setData(Qt::UserRole + 1, c.type);
        convList_->setItemWidget(item, row);
    }
}

// ---------------------------------------------------------------------------
// 发送
// ---------------------------------------------------------------------------
void MainWindow::onSendClicked(){
    const QString text = inputEdit_->toPlainText().trimmed();
    if(!authed_ || activeConv_.isEmpty() || text.isEmpty()) return;
    session_->sendMessage(activeConv_, text);   // 服务端广播含自己 echo → onMessage 路由
    inputEdit_->clear();
    inputEdit_->setFocus();
}

// ---------------------------------------------------------------------------
// ImSession 信号 → UI
// ---------------------------------------------------------------------------
void MainWindow::onConnected(){
    connected_ = true;
    setStatus(QStringLiteral("已连接，认证中…"), true);
}

void MainWindow::onAuthOk(const QString& uid, const QString& nick){
    authed_ = true;
    uid_ = uid;
    nick_ = nick.isEmpty() ? uid_ : nick;       // 以服务端回执为准
    titleLabel_->setText(QStringLiteral("%1 — IM Chat").arg(nick_));
    searchEdit_->setEnabled(true);
    connBtn_->setText(QStringLiteral("退出登录"));
    connBtn_->setEnabled(true);
    setStatus(QStringLiteral("已登录 —— 拉取会话…"), true);
    session_->requestConvList();
}

void MainWindow::onConvList(const std::vector<ImConvQt>& convs){
    convs_.clear();
    for(const auto& c : convs)
        convs_.push_back(ConvEntry{c.convId, c.type, c.name, c.unread, c.lastMsg, c.lastTsMs});
    rebuildConvList();

    if(needReopen_){
        needReopen_ = false;
        bool found = false;
        for(const auto& c : convs_) if(c.convId == activeConv_) { found = true; break; }
        if(found)
            openConv(activeConv_, /*continuation=*/true);
        else {
            activeConv_.clear();
            messageList_->clear();
            memberList_->clear();
            members_.clear();
            inputEdit_->setEnabled(false);
            sendBtn_->setEnabled(false);
        }
    }
    // 刚建成的会话：0x93 回来时把名字/副标题补正（openConv 时刻尚未知）
    if(!activeConv_.isEmpty()){
        for(const auto& c : convs_){
            if(c.convId == activeConv_){
                chatName_->setText(c.name);
                if(c.type == kImConvGrp) chatSub_->setText(QStringLiteral("消息按群广播 · 群 id：%1").arg(c.convId));
                else                      chatSub_->setText(QStringLiteral("对方账号：%1").arg(c.convId.mid(3)));
                break;
            }
        }
    }
    setStatus(QStringLiteral("已就绪 · %1 个会话").arg(convs_.size()), true);
}

void MainWindow::onConvCreated(const QString& convId){
    session_->requestConvList();               // 0x92：刷新会话列并自动打开新会话
    openConv(convId, /*continuation=*/false);
}

void MainWindow::onConvOpened(const QString&, uint64_t){
    inputEdit_->setEnabled(true);
    sendBtn_->setEnabled(true);
}

void MainWindow::onHistoryBatch(const QString& convId, const std::vector<ImHistoryQt>& msgs){
    if(convId != activeConv_) return;
    if(!viewReady_){
        // ---- 首屏 / 重连续拉 ----
        const bool wasContinuation = continueView_ && messageList_->count() > 0;
        if(continueView_){
            continueView_ = false;
            if(messageList_->count() == 0){
                renderHistoryReplace(msgs);
            } else {
                renderHistoryUnion(msgs);        // seq 并集：只补 seq > 本地最大 的增量
            }
        } else {
            renderHistoryReplace(msgs);
        }
        viewReady_ = true;
        noOlder_ = (msgs.empty() || msgs.size() < static_cast<size_t>(kHistoryChunk));
        if(msgs.empty()){
            oldestSeq_ = 0;
            appendSystemLine(QStringLiteral("暂无消息 —— 说点什么吧"));
            messageList_->scrollToBottom();
        } else if(!wasContinuation){
            oldestSeq_ = msgs.front().seq;
        }
        flushPending();
        sendReadCursor();
        messageList_->scrollToBottom();
        return;
    }
    // ---- 触顶上翻 ----
    const bool exhausted = msgs.empty()
        || msgs.size() < static_cast<size_t>(kHistoryChunk)
        || (!msgs.empty() && msgs.front().seq == 0);
    renderHistoryPrepend(msgs);
    if(!msgs.empty()) oldestSeq_ = msgs.front().seq;
    historyLoading_ = false;
    if(exhausted){
        noOlder_ = true;
        prependSystemLine(QStringLiteral("—— 没有更早的消息 ——"));
    }
}

void MainWindow::onMessage(const QString& convId, const QString& fromUid, const QString& fromNick,
                           const QString& msg, uint64_t seq, uint64_t tsMs){
    if(convId == activeConv_){
        if(!viewReady_){
            pending_.push_back(ImHistoryQt{seq, tsMs, fromUid, msg});
            return;
        }
        appendBubble(fromUid, msg, seq, tsMs);
        messageList_->scrollToBottom();
        if(seq > viewMaxSeq_){
            viewMaxSeq_ = seq;
            session_->sendRead(activeConv_, viewMaxSeq_);
        }
        return;
    }
    // 其他会话：预览 + 未读 +1，实时顶到队首（0x93 下轮对齐权威）
    for(auto& e : convs_){
        if(e.convId == convId){
            e.unread += 1;
            e.lastMsg  = previewOf(e.type == kImConvGrp, fromNick, msg);
            e.lastTsMs = tsMs;
            break;
        }
    }
    rebuildConvList();
}

void MainWindow::onMembers(const QString& convId, const std::vector<ImMemberQt>& members){
    if(convId != activeConv_) return;
    members_ = members;
    memberList_->clear();
    for(const auto& m : members_){
        auto* item = new QListWidgetItem(memberList_);
        item->setText(QStringLiteral("%1 %2").arg(m.online ? QStringLiteral("●") : QStringLiteral("○"),
                                                  m.nick));
        item->setForeground(m.online ? theme::avatarColor(m.uid) : QColor(theme::kText3));
        item->setData(Qt::UserRole,     m.uid);   // 供 onPresence 定位
        item->setData(Qt::UserRole + 1, m.nick);
    }
    updateMembersHeader();
}

void MainWindow::onPresence(const QString& uid, bool online){
    bool changed = false;
    for(auto& m : members_) if(m.uid == uid){ m.online = online; changed = true; break; }
    if(changed){
        for(int i = 0; i < memberList_->count(); ++i){
            QListWidgetItem* item = memberList_->item(i);
            if(item->data(Qt::UserRole).toString() != uid) continue;
            const QString nick = item->data(Qt::UserRole + 1).toString();
            item->setText(QStringLiteral("%1 %2").arg(online ? QStringLiteral("●") : QStringLiteral("○"), nick));
            item->setForeground(online ? theme::avatarColor(uid) : QColor(theme::kText3));
            break;
        }
    }
    updateMembersHeader();
}

void MainWindow::onConvChanged(const QString& convId){
    if(!authed_) return;
    session_->requestConvList();
    if(!activeConv_.isEmpty() && convId == activeConv_)
        session_->requestMembers(activeConv_);
}

void MainWindow::onSessionError(const QString& reason){
    setStatus(QStringLiteral("服务器: %1").arg(reason), false);
    prependSystemLine(QStringLiteral("[错误] %1").arg(reason));
}

void MainWindow::onDisconnected(const QString& reason){
    connected_ = false;
    authed_ = false;
    viewReady_ = false;
    needReopen_ = !activeConv_.isEmpty();
    continueView_ = false;
    inputEdit_->setEnabled(false);
    sendBtn_->setEnabled(false);
    searchEdit_->setEnabled(false);
    connBtn_->setText(QStringLiteral("重连"));
    connBtn_->setEnabled(true);
    setStatus(QStringLiteral("已断开：%1 —— [重连]恢复").arg(reason), false);
}

// ---------------------------------------------------------------------------
// 触顶上翻
// ---------------------------------------------------------------------------
void MainWindow::onScrollToTop(int value){
    if(value != 0) return;
    if(!authed_ || !viewReady_ || historyLoading_ || noOlder_) return;
    requestOlder();
}
void MainWindow::requestOlder(){
    if(oldestSeq_ == 0) return;
    historyLoading_ = true;
    session_->requestHistory(activeConv_, kHistoryChunk, static_cast<uint32_t>(oldestSeq_));
}

// ---------------------------------------------------------------------------
// 打开会话
// ---------------------------------------------------------------------------
void MainWindow::openConv(const QString& cid, bool continuation){
    activeConv_ = cid;
    for(auto& e : convs_) if(e.convId == cid) e.unread = 0;
    rebuildConvList();

    const bool grp = cid.startsWith(QStringLiteral("grp:"));
    QString cname = cid;
    for(const auto& e : convs_) if(e.convId == cid){ cname = e.name; break; }
    chatName_->setText(cname);
    if(grp){
        chatSub_->setText(QStringLiteral("消息按群广播 · 群 id：%1").arg(cid));
        memberCol_->show();
        updateMembersHeader();
    } else {
        chatSub_->setText(QStringLiteral("对方账号：%1").arg(cid.mid(3)));  // dm:N → 对方 uid
        memberCol_->hide();
    }

    if(!continuation){
        messageList_->clear();
        memberList_->clear();
        members_.clear();
        pending_.clear();
        viewReady_ = false;
        continueView_ = false;
        noOlder_ = false;
        historyLoading_ = false;
        oldestSeq_ = 0;
        viewMaxSeq_ = 0;
        lastLineTs_ = 0;
    } else {
        viewReady_ = false;
        continueView_ = true;
    }
    session_->openConv(cid);
    if(!continuation) session_->requestMembers(cid);
    session_->requestHistory(cid, kHistoryChunk, 0);
}

void MainWindow::sendReadCursor(){
    if(viewMaxSeq_ == 0) return;
    session_->sendRead(activeConv_, viewMaxSeq_);
}

// ---------------------------------------------------------------------------
// 渲染
// ---------------------------------------------------------------------------
QString MainWindow::displayName(const QString& uid){
    if(uid == uid_) return nick_.isEmpty() ? uid_ : nick_;
    for(const auto& m : members_)
        if(m.uid == uid) return m.nick.isEmpty() ? uid : m.nick;
    return uid;
}

void MainWindow::maybeTimeLine(uint64_t tsMs){
    if(lastLineTs_ != 0 && tsMs > lastLineTs_ && tsMs - lastLineTs_ < kTimeLineGapMs)
        return;
    lastLineTs_ = tsMs;
    auto* item = new QListWidgetItem(messageList_);
    item->setFlags(Qt::NoItemFlags);                 // 时间行不可选/不可击
    auto* w = new QLabel(theme::chatTimeText(tsMs), messageList_);
    w->setObjectName(QStringLiteral("timeline"));
    w->setAlignment(Qt::AlignCenter);
    w->setFixedHeight(24);
    item->setSizeHint(QSize(qMax(120, messageList_->viewport()->width()), 24));
    messageList_->addItem(item);
    messageList_->setItemWidget(item, w);
}

void MainWindow::appendBubble(const QString& fromUid, const QString& msg, uint64_t seq,
                              uint64_t tsMs){
    maybeTimeLine(tsMs);
    const int bw = qBound(220, messageList_->viewport()->width() - 110, 620);
    auto* item = new QListWidgetItem(messageList_);
    auto* w = new MessageBubble(fromUid, displayName(fromUid), truncated(msg),
                                fromUid == uid_, bw, messageList_);
    item->setSizeHint(w->sizeHint());
    item->setData(Qt::UserRole + 2, QVariant::fromValue(static_cast<qlonglong>(seq)));  // 去重锚
    messageList_->addItem(item);
    messageList_->setItemWidget(item, w);
}

void MainWindow::appendSystemLine(const QString& text){
    auto* item = new QListWidgetItem(messageList_);
    item->setFlags(Qt::NoItemFlags);
    item->setText(text);
    item->setForeground(QColor(theme::kText3));
    item->setTextAlignment(Qt::AlignCenter);
    messageList_->addItem(item);
    messageList_->scrollToBottom();
}
void MainWindow::prependSystemLine(const QString& text){
    auto* item = new QListWidgetItem(messageList_);
    item->setFlags(Qt::NoItemFlags);
    item->setText(text);
    item->setForeground(QColor(theme::kText3));
    item->setTextAlignment(Qt::AlignCenter);
    messageList_->insertItem(0, item);
    messageList_->scrollToTop();
}

void MainWindow::renderHistoryReplace(const std::vector<ImHistoryQt>& msgs){
    messageList_->clear();
    viewMaxSeq_ = 0;
    lastLineTs_ = 0;
    for(const auto& e : msgs){
        appendBubble(e.fromUid, e.msg, e.seq, e.tsMs);
        if(e.seq > viewMaxSeq_) viewMaxSeq_ = e.seq;
    }
}
void MainWindow::renderHistoryUnion(const std::vector<ImHistoryQt>& msgs){
    for(const auto& e : msgs){
        if(e.seq > viewMaxSeq_){
            appendBubble(e.fromUid, e.msg, e.seq, e.tsMs);
            viewMaxSeq_ = e.seq;
        }
    }
}
void MainWindow::renderHistoryPrepend(const std::vector<ImHistoryQt>& msgs){
    if(msgs.empty()) return;
    // 上翻：prepend 批（只补当前位置之前），不做时间行（避免滚动锚漂移）。
    const int bw = qBound(220, messageList_->viewport()->width() - 110, 620);
    QListWidgetItem* anchor = messageList_->count() > 0 ? messageList_->item(0) : nullptr;
    for(auto it = msgs.rbegin(); it != msgs.rend(); ++it){     // 逆序插 0 位 → 批内保持升序
        auto* item = new QListWidgetItem(messageList_);
        auto* w = new MessageBubble(it->fromUid, displayName(it->fromUid), truncated(it->msg),
                                    it->fromUid == uid_, bw, messageList_);
        item->setSizeHint(w->sizeHint());
        item->setData(Qt::UserRole + 2, QVariant::fromValue(static_cast<qlonglong>(it->seq)));
        messageList_->insertItem(0, item);
        messageList_->setItemWidget(item, w);
    }
    if(anchor) messageList_->scrollToItem(anchor, QAbstractItemView::PositionAtTop);
    else       messageList_->scrollToTop();
}

void MainWindow::flushPending(){
    if(pending_.empty()) return;
    std::set<qlonglong> seen;
    for(int i = 0; i < messageList_->count(); ++i){
        const QVariant s = messageList_->item(i)->data(Qt::UserRole + 2);
        if(s.isValid()) seen.insert(s.toLongLong());
    }
    for(const auto& e : pending_){
        if(seen.insert(static_cast<qlonglong>(e.seq)).second)
            appendBubble(e.fromUid, e.msg, e.seq, e.tsMs);
    }
    pending_.clear();
}

void MainWindow::updateMembersHeader(){
    int online = 0;
    for(const auto& m : members_) if(m.online) ++online;
    memberHead_->setText(QStringLiteral("成员 %1 · 在线 %2").arg(members_.size()).arg(online));
}

void MainWindow::setStatus(const QString& text, bool ok){
    statusLabel_->setText(text);
    statusLabel_->setStyleSheet(ok
        ? QStringLiteral("color:%1;").arg(theme::kBrand)
        : QStringLiteral("color:%1;").arg(theme::kRed));
}

} // namespace chat