#include "net/log.hpp"

#include <iostream>

namespace server{

const char* level_str(Level lv){
    switch (lv) {
        case Level::TRACE: return "TRACE";
        case Level::DEBUG: return "DEBUG";
        case Level::INFO:  return "INFO";
        case Level::WARN:  return "WARN";
        case Level::ERROR: return "ERROR";
        default:           return "UNKNOWN";
    }
}

Logger& Logger::instance(){
    static Logger inst;
    return inst;
}

void Logger::do_log(Level lv, const std::string& msg){
    if(lv < level_.load(std::memory_order_relaxed)) return;   // 低于当前级别 → 丢弃（set_level 才真正生效）
    std::lock_guard<std::mutex> lk(mutex_);   // 整行原子：消息拼接已完成，这里只防两行交错
    std::cout << msg << '\n';
}

void Logger::set_level(Level lv){
    level_.store(lv, std::memory_order_relaxed);
}

}