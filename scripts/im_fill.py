#!/usr/bin/env python3
# im_fill.py —— 给 v2 IM 会话灌历史消息（触顶上翻验证用，qt/README.md「步骤 3」）。
#
# 前置：
#   1. im_server 起着且必须开 --kv-path：
#        ./build/im_server --port 8891 --threads 2 --kv-path /tmp/im_qt.wal
#   2. 目标会话已存在 / 你是成员：
#        --peer 模式自动「注册→建 DM」拿 cid（省去手算 dm:A:B 字典序）；
#        --conv 模式直接指定会话 id（群聊 id 在 GUI 会话列看 `群名 (grp:N)`，须已是成员）。
#   3. 仅需 Python 3 标准库（socket/struct/hashlib），Windows / WSL 都能跑。
#
# 协议（与 include/chat/message_proto.hpp 一致）：
#   帧 = [4B 大端长度][payload]；payload = [1B type][body]；字符串 = [1B len][bytes](≤255)。
#   注册 0x10 / 登录 0x11 / 建会话 0x12 / 发消息 0x16；登录成功回 0x90、建会话回 0x92。
import argparse
import hashlib
import socket
import struct
import sys
import time


def frm(payload: bytes) -> bytes:
    """帧外壳：4B 大端长度 + payload。"""
    return struct.pack(">I", len(payload)) + payload


def s1(b: bytes) -> bytes:
    """1B 长度前缀字符串（≤255）。"""
    if len(b) > 255:
        raise ValueError("string too long (len>255)")
    return bytes([len(b)]) + b


class ImConn:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.buf = b""

    def send(self, payload: bytes) -> None:
        self.sock.sendall(frm(payload))

    def recv_payload(self, timeout: float = 2.0) -> bytes:
        """收一帧 payload（含首字节 type）；半包攒、粘包拆；超时抛 socket.timeout。"""
        self.sock.settimeout(timeout)
        while True:
            if len(self.buf) >= 4:
                need = struct.unpack(">I", self.buf[:4])[0]
                if len(self.buf) >= 4 + need:
                    frame = self.buf[4:4 + need]
                    self.buf = self.buf[4 + need:]
                    return frame
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("server closed")
            self.buf += chunk


def wait_until(self: ImConn, want_type: int, note: str, tries: int = 3) -> bytes:
    """读到指定 type 的帧（跳过中间的 S2C_ERROR 等）；全失败则报错退出。"""
    for _ in range(tries):
        f = self.recv_payload()
        if f and f[0] == want_type:
            return f
    print(f"{note}：未收到 0x{want_type:02x}（检查服务端开着且 --kv-path；口令对准；会话已存在且你是成员）")
    sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser(description="给 v2 IM 会话灌历史消息（0x16 发 fill-N）")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8891)
    ap.add_argument("--uid", required=True, help="灌消息账号（不存在则自动注册）")
    ap.add_argument("--pwd", default="x")
    ap.add_argument("--conv", default=None, help="会话 id（dm:alice:bob 或 grp:N）")
    ap.add_argument("--peer", default=None, help="填对方 uid 则自动[注册→建 DM]拿 cid，免手算字典序")
    ap.add_argument("--count", type=int, default=300)
    ap.add_argument("--interval", type=float, default=0.01, help="每条间隔秒（0=连发）")
    args = ap.parse_args()
    if not args.conv and not args.peer:
        ap.error("需要 --conv 或 --peer 之一")

    c = ImConn(args.host, args.port)
    uid = args.uid.encode()
    pwd = hashlib.sha256(args.pwd.encode()).digest()

    # 注册（幂等：已存在会收 S2C_ERROR user-exists，跳过继续）→ 登录 → 等 0x90
    c.send(bytes([0x10]) + s1(uid) + s1(uid) + pwd)
    c.send(bytes([0x11]) + s1(uid) + pwd)
    wait_until(c, 0x90, "登录")

    cid = args.conv
    if args.peer:
        # 建 DM（幂等）：[0x12][type=1][空名串][cnt=1][peer]
        payload = bytes([0x12, 1, 0, 1]) + s1(args.peer.encode())
        c.send(payload)
        f = wait_until(c, 0x92, "建 DM")
        ln = f[1]
        cid = f[2:2 + ln].decode()
        print(f"DM 会话 = {cid}")

    target = cid.encode()
    for i in range(args.count):
        c.send(bytes([0x16]) + s1(target) + ("fill-%d" % i).encode())
        if args.interval > 0:
            time.sleep(args.interval)
    print(f"发送完成：{args.count} 条 → {cid}（uid={args.uid}）")


if __name__ == "__main__":
    main()