// main.cpp —— Qt6 Widgets v3 类 QQ IM 客户端入口（QQ 风格无边框三栏）。
// 设计见 plan/task_plan_qt_v3_ui.md；构建见 qt/CMakeLists.txt。
#include <QApplication>
#include <QFont>

#include "login_dialog.hpp"
#include "main_window.hpp"
#include "theme.hpp"

int main(int argc, char** argv){
    QApplication app(argc, argv);
    QApplication::setApplicationName(QStringLiteral("qt_im"));
    QApplication::setFont(QFont(QStringLiteral("Microsoft YaHei UI"), 9));   // QQ 同款中文界面字体
    app.setStyleSheet(chat::theme::appStyle());                              // 全局定妆

    chat::LoginDialog login;
    if(login.exec() != QDialog::Accepted) return 0;

    chat::MainWindow w(login.host(), login.port(), login.registerMode(),
                       login.uid(), login.nick(), login.pwd());
    w.show();
    return QApplication::exec();
}