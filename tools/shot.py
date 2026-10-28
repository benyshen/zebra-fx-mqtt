# -*- coding: utf-8 -*-
"""用 GDI PrintWindow 截指定 Tk 窗口（含被遮挡/后台），输出 PNG。纯标准库。

python tools/shot.py   -> 生成 docs/images/{main,settings,raw}.png
"""
import ctypes
import ctypes.wintypes as wt
import os
import struct
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import fxr90_reader as R  # noqa: E402

user32 = ctypes.windll.user32
gdi32 = ctypes.windll.gdi32
user32.SetProcessDPIAware()

SRCCOPY = 0x00CC0020
DIB_RGB_COLORS = 0


class BITMAPINFOHEADER(ctypes.Structure):
    _fields_ = [("biSize", wt.DWORD), ("biWidth", wt.LONG), ("biHeight", wt.LONG),
                ("biPlanes", wt.WORD), ("biBitCount", wt.WORD),
                ("biCompression", wt.DWORD), ("biSizeImage", wt.DWORD),
                ("biXPelsPerMeter", wt.LONG), ("biYPelsPerMeter", wt.LONG),
                ("biClrUsed", wt.DWORD), ("biClrImportant", wt.DWORD)]


class BITMAPINFO(ctypes.Structure):
    _fields_ = [("bmiHeader", BITMAPINFOHEADER), ("bmiColors", wt.DWORD * 3)]


def toplevel(widget):
    """优先用 Tk 的 wm_frame() 拿到真正的顶层窗口句柄（对话框的父链会指回主窗口）。"""
    try:
        f = widget.wm_frame()
        if f:
            return int(f, 16)
    except Exception:
        pass
    h = widget.winfo_id()
    while True:
        p = user32.GetParent(h)
        if not p:
            return h
        h = p


def capture(hwnd, path):
    rect = wt.RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    w, h = rect.right - rect.left, rect.bottom - rect.top
    print("capture hwnd=%s rect=%s size=%dx%d" % (hwnd, (rect.left, rect.top, rect.right, rect.bottom), w, h))
    hdc_screen = user32.GetDC(0)
    hdc = gdi32.CreateCompatibleDC(hdc_screen)
    bmp = gdi32.CreateCompatibleBitmap(hdc_screen, w, h)
    gdi32.SelectObject(hdc, bmp)
    user32.PrintWindow(hwnd, hdc, 2)

    bi = BITMAPINFO()
    bi.bmiHeader.biSize = ctypes.sizeof(BITMAPINFOHEADER)
    bi.bmiHeader.biWidth = w
    bi.bmiHeader.biHeight = -h          # 负数 = 自上而下
    bi.bmiHeader.biPlanes = 1
    bi.bmiHeader.biBitCount = 32
    buf = ctypes.create_string_buffer(w * h * 4)
    gdi32.GetDIBits(hdc, bmp, 0, h, buf, ctypes.byref(bi), DIB_RGB_COLORS)

    raw = bytearray(w * h * 3)
    src = buf.raw
    for i in range(w * h):
        b, g, r = src[i * 4], src[i * 4 + 1], src[i * 4 + 2]
        raw[i * 3:i * 3 + 3] = bytes((r, g, b))
    rows = b"".join(b"\x00" + bytes(raw[y * w * 3:(y + 1) * w * 3]) for y in range(h))

    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data +
                struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))

    png = (b"\x89PNG\r\n\x1a\n" +
           chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)) +
           chunk(b"IDAT", zlib.compress(rows, 6)) + chunk(b"IEND", b""))
    with open(path, "wb") as f:
        f.write(png)

    gdi32.DeleteObject(bmp)
    gdi32.DeleteDC(hdc)
    user32.ReleaseDC(0, hdc_screen)
    print("saved", path, "%dx%d" % (w, h))


def main():
    out = os.path.join(R.app_dir(), "docs", "images")
    os.makedirs(out, exist_ok=True)
    live = "--live" in sys.argv

    app = R.App(R.load_config())
    app.update()
    app.update_idletasks()

    if live:
        # 真机：连 broker、点开始、等几秒收标签，再截（真实连接参数取自 dist/config.json）
        import json
        import time
        alt = os.path.join(R.app_dir(), "dist", "config.json")
        if os.path.exists(alt):
            with open(alt, encoding="utf-8") as f:
                app.cfg.update(json.load(f))
        app.connect_mqtt()
        t0 = time.time()
        while time.time() - t0 < 12 and not app.connected:
            app.update()
            time.sleep(0.05)
        app.start_reading()
        t0 = time.time()
        while time.time() - t0 < 14:
            app.update()
            time.sleep(0.05)
        print("live:", app.connected, len(app.store.tags), "tags")
    else:
        # 合成数据：塞几条样例，界面看起来有内容
        for s in R.SAMPLES:
            for ev in R.parse_tag_event(s):
                rec, new = app.store.add(ev)
                app._ensure_row(rec, new)
        app.session_start = __import__("time").time() - 137
        app.session_active = True
        app._set_running(True)
        app.store.total_reads = 342
        app._update_stats()
        app.update()
    if "--mask" in sys.argv:
        # 公开发布用：界面上的 Broker 地址脱敏（数据仍是真机抓的）
        app.cfg["host"] = "broker.example.com"
        app._load_settings_into_view()
        app.update()
    capture(toplevel(app), os.path.join(out, "main.png"))

    # 设置对话框
    dlg = R.SettingsDialog(app, app.cfg, lambda c: None)
    app.update()
    dlg.update()
    capture(toplevel(dlg), os.path.join(out, "settings.png"))
    dlg.destroy()
    app.update()

    # 原始 JSON
    first = app.tree.get_children()[0]
    app.tree.selection_set(first)
    app._show_raw()
    app.update()
    for w in app.winfo_children():
        if isinstance(w, __import__("tkinter").Toplevel):
            w.update()
            capture(toplevel(w), os.path.join(out, "raw.png"))
            w.destroy()
            break

    app.destroy()


if __name__ == "__main__":
    main()
