#pragma once

#include <atomic>
#include <mutex>
#include <sstream>
#include <utility>
#include <string>
#include "net/noncopyable.hpp"
namespace server
{

enum class Level{ TRACE, DEBUG, INFO, WARN, ERROR};

// 一对自由函数: 等级->字符串（实现已移 log.cpp）
const char* level_str(Level lv);

class Logger: public Noncopyable
{
public:
    static Logger& instance();
    void set_level(Level lv);
    template<typename... Args>
    void log(Level lv, Args&&... args)
    {
        std::ostringstream os;
        os << level_str(lv) << ":";
        (os << ... << std::forward<Args>(args));
        do_log(lv, os.str());

    }
private:
    Logger() = default; //私有只能由instance 创建
    void do_log(Level lv, const std::string& msg);
    // 多线程（服务端每个 IO 线程、客户端每条连接的 loop）会并发打日志：
    // level_ 用原子读，整行的拼接/输出用 mutex 串起来 —— 否则一条消息会被别的
    // 线程从中间插进去，日志里出现残缺行、按关键字 grep 还会漏计。
    std::atomic<Level> level_{Level::INFO};
    std::mutex         mutex_;
};

}