// sha256.hpp —— 纯头 SHA-256 摘要：口令落库/落线用（demo 级）。
//
// 为什么自造：项目零外部依赖（聊天协议不引 openssl/protobuf），v2 用户层要口令摘要，
// 客户端（Qt）与服务端（im_server）必须算同一函数 → 放 include/chat 纯头，双方 include。
// 明确边界：无盐无 HMAC、无迭代，只防「明文口令落盘/走线」这一层；正式安全
// （加盐 KDF + TLS）是 v3，见 plan/task_plan_im_v2.md §8。算法对照 FIPS 180-4 附录 B。
#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace chat {

// 返回 32 字节原始摘要（非 hex）。输入可为任意二进制串。
inline std::string sha256(const std::string& data){
    // ---- 1. 填充：0x80 + 0* + [8B 大端比特数]，总长 ≡ 0 (mod 64) ----
    const size_t origLen = data.size();
    const uint64_t bitLen = static_cast<uint64_t>(origLen) * 8;
    std::string msg = data;
    msg.push_back(static_cast<char>(0x80));
    {
        const size_t rem = msg.size() % 64;
        const size_t pad = (rem <= 56) ? (56 - rem) : (120 - rem);
        msg.append(pad, '\0');
    }
    for(int sh = 56; sh >= 0; sh -= 8)
        msg.push_back(static_cast<char>((bitLen >> sh) & 0xff));

    // ---- 2. 常数与初始向量 ----
    static const uint32_t K[64] = {
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2};
    uint32_t h[8] = {0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,
                     0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19};

    auto rotr = [](uint32_t x, int n){ return (x >> n) | (x << (32 - n)); };

    // ---- 3. 逐 64B 块压缩 ----
    for(size_t off = 0; off < msg.size(); off += 64){
        uint32_t w[64];
        for(int i = 0; i < 16; ++i){
            const size_t p = off + static_cast<size_t>(i) * 4;
            w[i] = (static_cast<uint32_t>(static_cast<uint8_t>(msg[p])) << 24)
                 | (static_cast<uint32_t>(static_cast<uint8_t>(msg[p+1])) << 16)
                 | (static_cast<uint32_t>(static_cast<uint8_t>(msg[p+2])) <<  8)
                 |  static_cast<uint32_t>(static_cast<uint8_t>(msg[p+3]));
        }
        for(int i = 16; i < 64; ++i){
            const uint32_t s0 = rotr(w[i-15], 7) ^ rotr(w[i-15], 18) ^ (w[i-15] >> 3);
            const uint32_t s1 = rotr(w[i-2], 17) ^ rotr(w[i-2], 19) ^ (w[i-2] >> 10);
            w[i] = w[i-16] + s0 + w[i-7] + s1;
        }
        uint32_t a = h[0], b = h[1], c = h[2], d = h[3];
        uint32_t e = h[4], f = h[5], g = h[6], xx = h[7];
        for(int i = 0; i < 64; ++i){
            const uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            const uint32_t ch  = (e & f) ^ (~e & g);
            const uint32_t t1  = xx + S1 + ch + K[i] + w[i];
            const uint32_t S0  = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            const uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t t2  = S0 + maj;
            xx = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
        }
        h[0]+=a; h[1]+=b; h[2]+=c; h[3]+=d; h[4]+=e; h[5]+=f; h[6]+=g; h[7]+=xx;
    }

    // ---- 4. 输出 8 × 4B 大端原始字节 ----
    std::string out;
    out.reserve(32);
    for(int i = 0; i < 8; ++i)
        for(int sh = 24; sh >= 0; sh -= 8)
            out.push_back(static_cast<char>((h[i] >> sh) & 0xff));
    return out;
}

} // namespace chat