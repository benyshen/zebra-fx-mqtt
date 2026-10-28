# -*- coding: utf-8 -*-
"""生成程序图标（无第三方依赖）：深色圆角方块 + 绿色 RFID 电波弧。"""
import math
import os
import struct
import zlib

S = 256
BG = (26, 32, 39)        # #1a2027
BG2 = (18, 22, 27)       # #12161b
GRN = (57, 181, 74)      # #39b54a
GRN2 = (46, 160, 67)     # #2ea043

px = bytearray(S * S * 4)


def put(x, y, rgb, a=255):
    i = (y * S + x) * 4
    px[i:i + 4] = bytes((rgb[2], rgb[1], rgb[0], a))


def rrect_inside(x, y, x0, y0, x1, y1, r):
    if x < x0 or x > x1 or y < y0 or y > y1:
        return False
    cx = min(max(x, x0 + r), x1 - r)
    cy = min(max(y, y0 + r), y1 - r)
    return (x - cx) ** 2 + (y - cy) ** 2 <= r * r


CX, CY = 74, 128
for y in range(S):
    for x in range(S):
        if not rrect_inside(x, y, 8, 8, 247, 247, 46):
            continue
        t = y / S
        base = tuple(int(BG2[i] + (BG[i] - BG2[i]) * (1 - t)) for i in range(3))
        put(x, y, base)

# 电波弧
for r in (44, 74, 104):
    for y in range(S):
        for x in range(S):
            dx, dy = x - CX, y - CY
            d = math.hypot(dx, dy)
            if abs(d - r) > 7:
                continue
            ang = math.degrees(math.atan2(dy, dx))
            if -48 <= ang <= 48:
                put(x, y, GRN if r != 74 else GRN2)
# 中心点
for y in range(S):
    for x in range(S):
        if (x - CX) ** 2 + (y - CY) ** 2 <= 15 ** 2:
            put(x, y, GRN)


def chunk(tag, data):
    return (struct.pack(">I", len(data)) + tag + data +
            struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))


raw = b"".join(b"\x00" + bytes(px[y * S * 4:(y + 1) * S * 4]) for y in range(S))
png = (b"\x89PNG\r\n\x1a\n" +
       chunk(b"IHDR", struct.pack(">IIBBBBB", S, S, 8, 6, 0, 0, 0)) +
       chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "FXR90Reader.ico")
ico = (struct.pack("<HHH", 0, 1, 1) +
       struct.pack("<BBBBHHII", 0, 0, 0, 0, 1, 32, len(png), 22) + png)
with open(out, "wb") as f:
    f.write(ico)
print("icon:", out, len(ico), "bytes")
