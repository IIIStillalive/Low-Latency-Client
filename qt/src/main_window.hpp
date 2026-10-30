#pragma once

// main_window.{hpp,cpp} —— v3 类 QQ IM 客户端主窗口（无边框自绘 + 三栏布局）。
// 设计见 plan/task_plan_qt_v3_ui.md §2/§4/§5：
//   无边框：nativeEvent 处理 WM_NCHITTEST —— 上下左右/四角 6px 返回系统缩放；标题栏返回
//   HTCAPTION（系统级拖动/双击最大化）；右上按钮区返回 HTCLIENT（按钮可点）。
//   布局：左功能条(56) | 会话面板(280) | 聊天区 | 群成员面板(180, 仅群聊)。
//   会话队列按 lastTsMs 降序；新消息实时顶到队首（排序锚见 im_session.cpp 0x93/0x96）。
//   保留 v2 状态机：Disconnected → [0x90] Authorized → [0x93] ListLoaded → [0x14] Viewing，
//   断线保留凭据手动重连续拉（seq 并集去重，viewMaxSeq_ 基线）。
#include <QList>
#include <QMainWindow>
#include <QString>

#include <cstdint>

#include "im_session.hpp"

class QCloseEvent;
class QLabel;
class QLineEdit;
class QListWidget;
class QListWidgetItem;
class QMenu;
class QPoint;
class QPushButton;
class QScrollBar;
class QTextEdit;
class QVBoxLayout;
class QWidget;

namespace chat {

class InputEdit;          // 输入框小类（Enter 发送 / Shift+Enter 换行），定义在 main_window.cpp
class AddFriendDialog;

struct ConvEntry {        // 会话列条目（0x93 权威整列 / 0x96 增量实时更新）
    QString convId;
    uint8_t type = 0;     // kImConvDm / kImConvGrp
    QString name;         // 群名 / dm 对方昵称
    uint32_t unread = 0;
    QString lastMsg;      // 最后消息预览（grp=服务端拼好 `昵称: 消息`）
    uint64_t lastTsMs = 0;// 最后消息时间（队列排序锚；0=无历史）
};

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    // creds 由 LoginDialog 收集；内部负责 ImSession->start()（registerMode）或 login。
    explicit MainWindow(const QString& host, quint16 port, bool registerMode,
                        const QString& uid, const QString& nick, const QString& pwd,
                        QWidget* parent = nullptr);
    ~MainWindow() override = default;

protected:
    void closeEvent(QCloseEvent* event) override;          // 关窗前 disconnectNow
    bool nativeEvent(const QByteArray& eventType, void* message,
                     qintptr* result) override;            // WM_NCHITTEST：无边框缩放/拖动

private slots:
    // 无边框标题栏
    void onTitleMin();
    void onTitleMaxToggle();
    void onTitleClose();
    // 左功能条
    void onRailAddMenu();                                   // ➕：添加好友 / 新建群聊 / 加入群聊
    // 会话面板
    void onSearchEdited(const QString& text);
    void onConvRowClicked(QListWidgetItem* item);
    void onConvContextMenu(const QPoint& pos);
    void onConnBtnClicked();                                // 退出登录 / 重连 双态
    // 发送
    void onSendClicked();
    // 添加好友 / 新建（rail ➕ 菜单）
    void onNewGrpClicked();
    void onJoinGrpClicked();
    // ImSession → UI
    void onConnected();
    void onAuthOk(const QString& uid, const QString& nick);
    void onConvList(const std::vector<ImConvQt>& convs);
    void onConvCreated(const QString& convId);
    void onConvOpened(const QString& convId, uint64_t lastReadCursor);
    void onHistoryBatch(const QString& convId, const std::vector<ImHistoryQt>& msgs);
    void onMessage(const QString& convId, const QString& fromUid, const QString& fromNick,
                   const QString& msg, uint64_t seq, uint64_t tsMs);      // 0x96（seq 推进已读基线）
    void onMembers(const QString& convId, const std::vector<ImMemberQt>& members);
    void onPresence(const QString& uid, bool online);
    void onConvChanged(const QString& convId);              // 0x9B：刷新会话列
    void onSessionError(const QString& reason);
    void onDisconnected(const QString& reason);
    // 触顶上翻
    void onScrollToTop(int value);
    // 添加好友流程（AddFriendDialog 桥接）
    void onAddFriendDialog();

private:
    // 会话列 / 视图状态
    void rebuildConvList();                                 // 按 lastTsMs 降序 + 搜索过滤，全量重绘
    void openConv(const QString& convId, bool continueView);
    void requestOlder();
    void updateMembersHeader();                             // `成员 N · 在线 M`
    // 渲染
    QString displayName(const QString& uid);                // uid → nick（members_ 表；自己→nick_；未知→uid）
    void maybeTimeLine(uint64_t tsMs);                      // 距上条 >5min 插居中时间行
    void appendBubble(const QString& fromUid, const QString& msg,
                      uint64_t seq, uint64_t tsMs);         // 4 参；fromUid 做左右/头像/取色
    void appendSystemLine(const QString& text);
    void prependSystemLine(const QString& text);
    void setStatus(const QString& text, bool ok);
    void renderHistoryReplace(const std::vector<ImHistoryQt>& msgs);
    void renderHistoryUnion(const std::vector<ImHistoryQt>& msgs);
    void renderHistoryPrepend(const std::vector<ImHistoryQt>& msgs);
    void flushPending();
    void sendReadCursor();

    ImSession* session_ = nullptr;
    AddFriendDialog* addDlg_ = nullptr;

    QString host_; quint16 port_;
    QString uid_, nick_;
    QString searchFilter_;                  // 本地搜索过滤词

    // 无边框标题栏（nativeEvent 只管命中区；按钮动作在这里）
    QPushButton* ttMin_ = nullptr;
    QPushButton* ttMax_ = nullptr;
    QPushButton* ttClose_ = nullptr;
    QLabel* titleLabel_ = nullptr;
    // 左功能条
    QPushButton* railChatsBtn_ = nullptr;   // 💬 会话（常驻选中）
    QPushButton* railAddBtn_ = nullptr;     // ➕ 菜单
    QPushButton* railSetBtn_ = nullptr;     // ⚙ 设置（占位）
    // 会话面板
    QLineEdit* searchEdit_ = nullptr;
    QLabel* statusLabel_ = nullptr;
    QPushButton* connBtn_ = nullptr;        // 双态：退出登录 / 重连
    // 聊天区
    QLabel* chatName_ = nullptr;
    QLabel* chatSub_ = nullptr;
    QListWidget* convList_ = nullptr;       // 会话列（UserRole=convId; +1=type）
    QListWidget* messageList_ = nullptr;    // 气泡（UserRole+2=seq 去重锚）
    QListWidget* memberList_ = nullptr;     // 群成员（UserRole=uid）
    QWidget* memberCol_ = nullptr;          // 群成员面板容器（仅群聊显示）
    QLabel* memberHead_ = nullptr;          // `成员 N · 在线 M`
    InputEdit* inputEdit_ = nullptr;
    QPushButton* sendBtn_ = nullptr;

    // 会话列数据：全量权威容器，0x93 整列重置，0x96 增量更新预览/排序。
    QList<ConvEntry> convs_;
    QString activeConv_;                  // 当前打开的会话
    bool needReopen_ = false;             // 断线后已登录需续开的原活动会话

    // 当前视图状态（随打开/切换会话重置）
    bool connected_ = false;
    bool authed_ = false;
    bool viewReady_ = false;              // 首屏 HISTORY 已渲染（此前广播缓入 pending_）
    bool continueView_ = false;           // 断线续拉：下个首屏做 seq 并集而非替换
    bool historyLoading_ = false;
    bool noOlder_ = false;
    uint64_t oldestSeq_ = 0;              // 视图最旧 seq（上翻 before 锚）
    uint64_t viewMaxSeq_ = 0;             // 视图最大 seq（已读游标 / 续拉并集锚）
    uint64_t lastLineTs_ = 0;             // 上条显示的时间（时间行去重）
    std::vector<ImHistoryQt> pending_;
    std::vector<ImMemberQt> members_;     // 当前会话成员快照
};

} // namespace chat