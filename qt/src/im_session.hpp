#pragma once

// im_session.hpp —— v2 类飞书 IM 客户端网络层：QTcpSocket + 4B 大端帧切分 + v2 编解码。
// 设计见 plan/task_plan_qt_v2.md §1/§3/§4：
//   - 全部在 GUI 线程事件驱动（readyRead 增量切帧 → imDecode → 信号），无自建收发线程；
//   - message_proto 纯函数（无锁无全局态），GUI 线程直调安全；
//   - 认证：0x10 注册 + 0x11 登录 → 0x90 后 authed_；重连复用同对象（凭据留底，只重发 0x11）。
//   - 对外只暴露 QObject 信号，UI（MainWindow）据此维护状态机。
#include <QMetaType>
#include <QObject>
#include <QString>
#include <QStringList>
#include <QTcpSocket>

#include <string>
#include <vector>

#include "chat/message_proto.hpp"

namespace chat {

// v2 S2C 结构（Qt 友好镜像，供信号出参）
struct ImConvQt {          // 会话列表条目
    QString convId;
    uint8_t type = 0;      // kImConvDm / kImConvGrp
    QString name;          // 群名 / dm 对方昵称
    uint32_t unread = 0;
    QString lastMsg;       // 最后消息预览（grp 已含 `昵称: 消息`；空=无历史）
    uint64_t lastTsMs = 0; // 最后消息时间（会话列排序锚）
};
struct ImHistoryQt {       // 历史/广播单条（seq + 发信时间戳 + 发送者 uid）
    uint64_t seq = 0;
    uint64_t tsMs = 0;
    QString fromUid;       // 发送者 uid（头像取色/左右判定/displayName 解析源）
    QString msg;
};
struct ImMemberQt {        // 成员表单条
    QString uid;
    QString nick;
    bool online = false;
};

class ImSession : public QObject {
    Q_OBJECT
public:
    explicit ImSession(QObject* parent = nullptr);
    ~ImSession() override = default;

    // ---- 连接 / 认证 ----
    // 连接 host:port 并按模式验证；断线重连复用同一对象（start 会 abort 旧连接重连）。
    //   registerMode=true  → 先 0x10 注册再 0x11 登录（新用户）；false → 直接 0x11。
    //   pwd 为明文输入，内部转 32B SHA-256 摘要；内存只留摘要，不存明文。
    void start(const QString& host, quint16 port, bool registerMode,
               const QString& uid, const QString& nick, const QString& pwd);
    void reconnect();                            // 用上次 host/port/凭据重新 start（仅登录）
    void disconnectNow();                        // 主动断开（退出登录）

    // ---- 业务发送（authed_ 后才有语义；未认证静默丢弃） ----
    void requestConvList();                      // 0x13
    void createConv(uint8_t type, const QString& name, const QStringList& members);   // 0x12
    void openConv(const QString& convId);        // 0x14
    void requestHistory(const QString& convId, uint32_t count, uint32_t before);      // 0x15
    void sendMessage(const QString& convId, const QString& msg);                     // 0x16
    void sendRead(const QString& convId, uint64_t seq);                              // 0x17
    void leaveConv(const QString& convId);       // 0x18（dm 会被服务端拒）
    void requestMembers(const QString& convId);  // 0x19
    void joinConv(const QString& convId);        // 0x1A（按固定群 id 入群；回执走 0x92 convCreated）
    void lookupUser(const QString& uid);         // 0x1B 用户查找（异步回执走 0x9C）

    bool authed() const { return authed_; }
    const QString& uid() const { return uid_; }
    const QString& nick() const { return nick_; }

signals:
    void connected();                                                // TCP 建立（尚未认证）
    void authOk(const QString& uid, const QString& nick);            // 0x90
    void convList(const std::vector<chat::ImConvQt>& convs);         // 0x93
    void convCreated(const QString& convId);                         // 0x92
    void convOpened(const QString& convId, uint64_t lastReadCursor); // 0x94
    void historyBatch(const QString& convId, const std::vector<chat::ImHistoryQt>& msgs); // 0x95
    void message(const QString& convId, const QString& fromUid,
                 const QString& fromNick, const QString& msg, uint64_t seq, uint64_t tsMs);  // 0x96（seq：并集去重基线）
    void members(const QString& convId, const std::vector<chat::ImMemberQt>& members); // 0x99
    void presence(const QString& uid, bool online);                  // 0x9A
    void userLookupResult(bool found, const QString& nick);         // 0x9C（0x1B 回执）
    void convChanged(const QString& convId);                       // 0x9B（会话成员集变更 → 收者刷新会话列）
    void sessionError(const QString& reason);                        // 0x8F
    void disconnected(const QString& reason);                        // 断线 / 恶意帧踢出

private slots:
    void onConnected();
    void onReadyRead();
    void onSocketError(QAbstractSocket::SocketError err);

private:
    void writeFrame(const std::string& payload);   // [4B BE len][payload]
    void handlePayload(const std::string& payload);
    void sendAuth();
    void doConnect();                              // 重建/重连 socket（凭据已在成员里）

    QTcpSocket* socket_ = nullptr;
    std::string inBuf_;       // 半包缓冲（readyRead 增量切帧）

    QString host_;
    quint16 port_ = 0;
    QString uid_, nick_;
    std::string pwd32_;       // 32B SHA-256 摘要（不存明文）
    bool doRegister_ = false;  // 本次连接需先 0x10 再 0x11
    bool authed_ = false;
    bool intentionalClose_ = false;   // 主动 abort()：屏蔽其触发的 error/disconnected
};

} // namespace chat

Q_DECLARE_METATYPE(chat::ImConvQt)
Q_DECLARE_METATYPE(chat::ImHistoryQt)
Q_DECLARE_METATYPE(chat::ImMemberQt)
Q_DECLARE_METATYPE(std::vector<chat::ImConvQt>)