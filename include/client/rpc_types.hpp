#pragma once

// RPC 公共类型：状态码 / 结果 / 调用选项
//
// 状态码同时是 RpcResponse.status 的线上取值（proto int32），新增只能追加，不能改编号。

#include <cstdint>
#include <string>

namespace server {

enum class RpcStatus : uint8_t {
    Ok                = 0,  // 成功
    AppError          = 1,  // 业务错误（服务端 method 返回失败）
    UnknownReq        = 2,  // 服务端不认识该 req_id（保守：正常流程不应出现）
    DeadlineExceeded  = 3,  // 本地超时（响应未在 deadline 内返回）
    Cancelled         = 4,  // 调用方 cancel()
    ResourceExhausted = 5,  // 背压：连接/池 in-flight 超过上限，fail-fast
    Unavailable       = 6,  // 连接不可用 / 客户端已停止
};

inline const char* rpcStatusName(RpcStatus s){
    switch(s){
        case RpcStatus::Ok:                return "Ok";
        case RpcStatus::AppError:          return "AppError";
        case RpcStatus::UnknownReq:        return "UnknownReq";
        case RpcStatus::DeadlineExceeded:  return "DeadlineExceeded";
        case RpcStatus::Cancelled:         return "Cancelled";
        case RpcStatus::ResourceExhausted: return "ResourceExhausted";
        case RpcStatus::Unavailable:       return "Unavailable";
    }
    return "Unknown";
}

struct RpcResult {
    RpcStatus   status = RpcStatus::Ok;
    std::string payload;   // 业务返回值（bytes，demo 用 UTF-8 文本）
    std::string error;     // 非 Ok 时的描述

    bool ok() const { return status == RpcStatus::Ok; }
};

struct RpcCallOptions {
    double deadlineSec = 5.0;    // 从 call() 发起时刻起算；<= 0 表示不超时
    bool   idempotent  = false;  // 允许服务端按 req_id 缓存响应并重放（断线重发去重）
};

} // namespace server
