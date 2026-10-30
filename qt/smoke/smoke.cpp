// smoke.cpp —— v2 无 GUI 冒烟：QTcpSocket v2 链路全自动断言（不进 regress，手动跑）。
//
// 链路（plan/task_plan_qt_v2.md §7）：
//   注册 f1 → 登录 f1 → 建 DM(f2) → 发 3 条 → 会话列 unread=3(stamped server 0x93)
//   → 打开 → 拉最新页(seq 0..2 升序) → 上翻 before=1 → 批仅 seq0 → 已读 0x17(2)
//   → 会话列 unread=0 → exit 0。
//   直接驱动 ImSession（不经过 GUI），网络+切帧+业务编解码+未读流转全链路。
//
// 前置：本地 im_server 已起（需 --kv-path）：
//   WSL:  ./build/im_server --port 8891 --threads 2 --kv-path /tmp/im_smoke.wal
// 运行：
//   Windows: build-qt\qt_smoke.exe --host 127.0.0.1 --port 8891
//   Linux:   ./build-qt/qt_smoke --host 127.0.0.1 --port 8891
#include <QCoreApplication>
#include <QTimer>
#include <QString>
#include <QStringList>

#include <cinttypes>
#include <cstdio>
#include <string>
#include <vector>

#include "im_session.hpp"

namespace {
enum class Stage { AuthWait, ConvCreateWait, EchoWait, ConvListUnread, OpenHistory, PageTop, ConvListRead, Done };
Stage g_stage = Stage::AuthWait;
int   g_echoed = 0;
bool  g_failed = false;
bool  g_done   = false;
QString g_dmId;

void fail(const char* why){
    if(g_done) return;
    std::printf("[smoke] FAIL: %s\n", why);
    g_failed = true;
    QCoreApplication::exit(1);
}
void pass(const char* tag){
    std::printf("[smoke] PASS: %s\n", tag);
}
} // namespace

int main(int argc, char** argv){
    QCoreApplication app(argc, argv);

    QString host = QStringLiteral("127.0.0.1");
    quint16 port = 8891;
    for(int i = 1; i + 1 < argc; ++i){
        if(std::string(argv[i]) == "--host") host = QString::fromUtf8(argv[i + 1]);
        if(std::string(argv[i]) == "--port") port = QString::fromUtf8(argv[i + 1]).toUShort();
    }

    // 唯一用户：pid 后缀隔离残留 kv（重复注册幂等，登录态可续）
    const QString uid = QStringLiteral("f_%1").arg(quint64(QCoreApplication::applicationPid()));
    const QString peer = QStringLiteral("g_%1").arg(quint64(QCoreApplication::applicationPid()));
    chat::ImSession sess;

    QTimer watchdog(&app);
    watchdog.setSingleShot(true);
    QObject::connect(&watchdog, &QTimer::timeout, &app, [&]{
        fail(qPrintable(QStringLiteral("watchdog timeout at stage %1")
              .arg(static_cast<int>(g_stage))));
    });
    watchdog.start(15000);

    // 注册新用户并登录（0x10+0x11 → 0x90）
    QObject::connect(&sess, &chat::ImSession::connected, &app, []{
        std::printf("[smoke] tcp connected, auth sent\n");
    });
    QObject::connect(&sess, &chat::ImSession::authOk, &app, [&](const QString& u, const QString&){
        if(g_done || g_failed) return;
        if(u != uid){ fail("authOk uid mismatch"); return; }
        pass("auth");
        g_stage = Stage::ConvCreateWait;

        // 建 DM：先注册 peer（0x10，无回执），再建会话
        sess.createConv(chat::kImConvDm, QString(),
                        QStringList{peer});
    });

    // 会话已建 → 发 3 条（等 0x96 echo 计数）
    QObject::connect(&sess, &chat::ImSession::convCreated, &app, [&](const QString& cid){
        if(g_done || g_failed) return;
        if(g_stage != Stage::ConvCreateWait){ fail("unexpected convCreated"); return; }
        if(cid.isEmpty()){ fail("empty conv id"); return; }
        pass(qPrintable(QStringLiteral("conv created %1").arg(cid)));
        g_dmId = cid;
        g_stage = Stage::EchoWait;
        for(int i = 1; i <= 3; ++i)
            sess.sendMessage(cid, QStringLiteral("smoke-%1").arg(i));
    });

    // 3 条广播收齐 → 拉会话列，验 server 0x93 unread=3
    QObject::connect(&sess, &chat::ImSession::message, &app, [&](const QString&, const QString&,
                                                                 const QString&, const QString&, uint64_t){
        if(g_done || g_failed) return;
        if(g_stage != Stage::EchoWait) return;       // 只认发送期间的 echo
        if(++g_echoed < 3) return;
        g_stage = Stage::ConvListUnread;
        sess.requestConvList();
    });

    QObject::connect(&sess, &chat::ImSession::convList, &app,
                     [&](const std::vector<chat::ImConvQt>& convs){
        if(g_done || g_failed) return;
        for(const auto& c : convs){
            if(c.convId != g_dmId) continue;
            if(g_stage == Stage::ConvListUnread){
                if(c.unread != 3){ fail(qPrintable(QStringLiteral("unread=%1 want=3").arg(c.unread))); return; }
                pass("convList unread=3 (server 0x93)");
                g_stage = Stage::OpenHistory;
                sess.openConv(g_dmId);
                sess.requestHistory(g_dmId, 200, 0);   // 打开 → 拉最新页
                return;
            }
            if(g_stage == Stage::ConvListRead){
                if(c.unread != 0){ fail(qPrintable(QStringLiteral("unread after read=%1 want=0").arg(c.unread))); return; }
                pass("convList unread=0 after 0x17(2)");
                g_stage = Stage::Done;
                g_done = true;
                std::printf("[smoke] ALL PASS\n");
                QCoreApplication::exit(0);
                return;
            }
        }
        if(g_stage == Stage::ConvListUnread) fail("dm missing from conv list");
        if(g_stage == Stage::ConvListRead) fail("dm missing from conv list after read");
    });

    // 历史：最新页 3 条 seq0..2 → 上翻 before=1 仅 seq0 → 已读 0x17(2)
    QObject::connect(&sess, &chat::ImSession::historyBatch, &app,
                     [&](const QString& cid, const std::vector<chat::ImHistoryQt>& msgs){
        if(g_done || g_failed) return;
        if(cid != g_dmId) return;
        switch(g_stage){
        case Stage::OpenHistory:
            if(msgs.size() != 3){ fail(qPrintable(QStringLiteral("first page size=%1 want=3").arg(msgs.size()))); return; }
            for(size_t i = 0; i < msgs.size(); ++i){
                if(msgs[i].seq != i){ fail("first page seq order"); return; }
            }
            pass("first page (C200,before=0): 3 entries seq0..2");
            sess.sendRead(g_dmId, msgs.back().seq);    // 0x17(2)：打开即整批已读
            g_stage = Stage::PageTop;
            sess.requestHistory(g_dmId, 200, 1);       // 上翻：严格早于 1 → 仅 seq0
            break;
        case Stage::PageTop:
            if(msgs.size() != 1 || msgs[0].seq != 0){
                fail(qPrintable(QStringLiteral("page-top size=%1 want seq0 only").arg(msgs.size())));
                return;
            }
            pass("upper batch (C200,before=1): only seq0");
            g_stage = Stage::ConvListRead;
            sess.requestConvList();                    // 验已读后 unread=0
            break;
        default:
            fail("unexpected history batch");
        }
    });

    QObject::connect(&sess, &chat::ImSession::sessionError, &app, [&](const QString& r){
        fail(qPrintable(QStringLiteral("server error: ") + r +
                        QStringLiteral(" —— 服务端需 --kv-path；dm 会话需 peer 已注册")));
    });
    QObject::connect(&sess, &chat::ImSession::disconnected, &app, [&](const QString& r){
        fail(qPrintable(QStringLiteral("disconnected: ") + r));
    });

    std::printf("[smoke] u=%s peer=%s target %s:%u\n",
                qPrintable(uid), qPrintable(peer), qPrintable(host), port);

    // 注册 f1 → 登录（0x10+0x11 → 0x90）；DM 幂等地建，peer 无需预注册
    sess.start(host, port, /*registerMode=*/true, uid, QStringLiteral("探针"), QStringLiteral("p"));

    return QCoreApplication::exec();
}