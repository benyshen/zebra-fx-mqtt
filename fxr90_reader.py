#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zebra FX·MQTT 标签读取器 (Windows 桌面端)

通过 MQTT(S) 订阅 Zebra IoT Connector 输出的标签事件主题，实时汇总 EPC，
提供 开始读取 / 结束读取、工作时长、标签总数统计与 CSV 导出。

协议依据: https://zebradevs.github.io/rfid-ziotc-docs/other_cloud_support/MQTT/
  - 标签事件:   {"data":{"CRC":"1947","PC":"3000","antenna":1,"channel":924.375,
                 "eventNum":665,"format":"epc","idHex":"e2806894...","peakRssi":-60,
                 "phase":176.6,"reads":1},"timestamp":"...","type":"CUSTOM"}
  - 控制命令:   {"command":"start","command_id":"abcd1432","payload":{"doNotPersistState":true}}
                {"command":"stop","command_id":"abcd1324","payload":{}}
                响应回到 response topic，形如 {"command":"start","command_id":"...","response":"success",...}
"""

import argparse
import csv
import json
import os
import queue
import ssl
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover
    mqtt = None

APP_NAME = "Zebra FX·MQTT 标签读取器"
APP_VER = "1.0.0"

# ---------------------------------------------------------------- 外观常量
BG = "#12161b"
PANEL = "#1a2027"
CARD = "#212a33"
CARD_HI = "#2b3742"
FIELD = "#0f1419"
FG = "#e6edf3"
MUTED = "#8b98a5"
ACCENT = "#2f81f7"
GREEN = "#2ea043"
GREEN_HI = "#39b54a"
RED = "#c9483f"
RED_HI = "#d95c53"
BORDER = "#2d3843"

DEFAULT_CONFIG = {
    "host": "broker.example.com",
    "port": 8883,
    "tls": True,
    "verify_cert": True,
    "ca_file": "",
    "username": "mqtt",
    "password": "mqtt",
    "client_id": "fxr90_win_001",
    "mqtt_version": "5",
    "keepalive": 60,
    "tag_topic": "/FXR90/tag",
    "response_topic": "/FXR90/response",
    "management_topic": "/FXR90/meresponse",
    "command_topic": "/FXR90/command",
    "send_commands": True,
    "auto_connect": False,
    "autoscroll": True,
    "max_rows_ui": 5000,
    "_说明": "主题由读头端 MQTT 端点配置决定：标签 /FXR90/tag、指令响应 /FXR90/response、管理事件 /FXR90/meresponse、控制指令 /FXR90/command（也支持 + 与 # 通配）",
}

# 指令主题探测用的候选名（在未收到响应时可点“探测”自动找）
PROBE_SUFFIXES = ["merequest", "mrequest", "cmds", "command", "commands", "req",
                  "request", "mgmt", "mcmd", "meremote", "request/command",
                  "command/request"]


# ---------------------------------------------------------------- 基础工具
def app_dir() -> str:
    """脚本/打包后可执行文件所在目录（配置、导出放这里）。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def eprint(*args):
    """窗口模式下 sys.stderr 可能为 None，打印不能抛。"""
    try:
        print(*args, file=sys.stderr)
    except Exception:
        pass


CONFIG_NAME = "config.json"


def config_dir() -> str:
    """配置放 exe 同级目录；目录不可写（如 Program Files）时回落到 %APPDATA%。"""
    d = app_dir()
    try:
        probe = os.path.join(d, ".write_test")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(probe)
        return d
    except Exception:
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
        p = os.path.join(base, "FXR90Reader")
        os.makedirs(p, exist_ok=True)
        return p


CONFIG_PATH = os.path.join(config_dir(), CONFIG_NAME)


def load_config() -> dict:
    """读取 config.json；文件不存在就用内置默认值并落盘，保证它始终是可编辑的配置接口。"""
    cfg = dict(DEFAULT_CONFIG)
    if not os.path.exists(CONFIG_PATH):
        try:
            save_config(cfg)
        except Exception:
            pass
        return cfg
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            cfg.update({k: v for k, v in data.items() if not k.startswith("_")})
    except Exception as e:
        eprint("读取配置失败，使用默认值:", e)
    return cfg


def save_config(cfg: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def topic_matches(topic_filter: str, topic: str) -> bool:
    """MQTT 主题通配匹配（+/#），用于判断消息该走标签/响应/管理事件哪条通路。"""
    if not topic_filter:
        return False
    f = topic_filter.strip("/").split("/")
    t = topic.strip("/").split("/")
    for i, part in enumerate(f):
        if part == "#":
            return True
        if i >= len(t):
            return False
        if part != "+" and part != t[i]:
            return False
    return len(f) == len(t)


def ts_epoch(iso: str) -> float:
    """读取器时间戳(UTC ISO) -> epoch 秒，失败返回当前时间。"""
    if not iso:
        return time.time()
    for f in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
              "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(iso, f).timestamp()
        except Exception:
            continue
    return time.time()


def fmt_dur(sec: float) -> str:
    sec = max(0, int(sec))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return "%02d:%02d:%02d" % (h, m, s)


def parse_tag_event(payload: str):
    """解析一条标签消息，返回事件 dict 列表（一条消息可能带多个标签）。"""
    try:
        j = json.loads(payload)
    except Exception:
        return []
    items = j if isinstance(j, list) else [j]
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        data = it.get("data", it)
        if isinstance(data, list):
            sub = data
        else:
            sub = [data]
        for d in sub:
            if not isinstance(d, dict):
                continue
            tid = d.get("idHex") or d.get("id") or d.get("epc") or d.get("EPC")
            if not tid:
                continue
            tid = str(tid).strip().lower()
            if tid.startswith("0x"):
                tid = tid[2:]
            if not tid:
                continue
            out.append({
                "id": tid,
                "format": (d.get("format") or "epc").lower(),
                "antenna": d.get("antenna"),
                "rssi": d.get("peakRssi", d.get("rssi")),
                "reads": d.get("reads", 1) or 1,
                "channel": d.get("channel"),
                "phase": d.get("phase"),
                "pc": d.get("PC") or d.get("pc"),
                "crc": d.get("CRC") or d.get("crc"),
                "tid": d.get("TID") or d.get("tid"),
                "type": it.get("type", ""),
                "timestamp": it.get("timestamp", ""),
                "raw": json.dumps(it, ensure_ascii=False),
            })
    return out


# ---------------------------------------------------------------- 数据模型
class TagStore:
    def __init__(self):
        self.tags = {}          # id -> dict(聚合结果)
        self.total_reads = 0
        self.session_reads = 0
        self.seq = 0

    def add(self, ev: dict):
        self.seq += 1
        tid = ev["id"]
        self.total_reads += int(ev["reads"] or 1)
        self.session_reads += int(ev["reads"] or 1)
        e = ts_epoch(ev.get("timestamp", ""))
        rec = self.tags.get(tid)
        if rec is None:
            rec = {
                "id": tid, "tid": ev.get("tid") or "",
                "antenna": ev.get("antenna"), "rssi": ev.get("rssi"),
                "rssi_best": ev.get("rssi"), "reads": int(ev["reads"] or 1),
                "first_ts": e, "last_ts": e,
                "first": datetime.fromtimestamp(e).strftime("%H:%M:%S"),
                "last": datetime.fromtimestamp(e).strftime("%H:%M:%S"),
                "channel": ev.get("channel"), "phase": ev.get("phase"),
                "pc": ev.get("pc"), "crc": ev.get("crc"),
                "type": ev.get("type"), "raw": ev.get("raw"),
                "antennas": {str(ev.get("antenna"))} if ev.get("antenna") else set(),
            }
            self.tags[tid] = rec
            return rec, True
        rec["reads"] += int(ev["reads"] or 1)
        if e < rec["first_ts"]:
            rec["first_ts"] = e
            rec["first"] = datetime.fromtimestamp(e).strftime("%H:%M:%S")
        if e >= rec["last_ts"]:
            rec["last_ts"] = e
            rec["last"] = datetime.fromtimestamp(e).strftime("%H:%M:%S")
        r = ev.get("rssi")
        if r is not None:
            rec["rssi"] = r
            if rec["rssi_best"] is None or r > rec["rssi_best"]:
                rec["rssi_best"] = r
        if ev.get("antenna"):
            rec["antennas"].add(str(ev.get("antenna")))
            rec["antenna"] = ev.get("antenna")
        if ev.get("channel") is not None:
            rec["channel"] = ev.get("channel")
        if ev.get("tid") and not rec["tid"]:
            rec["tid"] = ev.get("tid")
        rec["raw"] = ev.get("raw")
        return rec, False

    def clear(self):
        self.tags.clear()
        self.total_reads = 0
        self.session_reads = 0
        self.seq = 0


# ---------------------------------------------------------------- MQTT 层
class MqttLink:
    """paho 客户端封装：所有回调只往队列里塞事件，UI 线程消费。"""

    def __init__(self, cfg: dict, ui_q: queue.Queue):
        self.cfg = cfg
        self.q = ui_q
        self.client = None
        self.connected = False
        self._want = False

    def start(self):
        if mqtt is None:
            raise RuntimeError("缺少 paho-mqtt，请先执行: uv pip install paho-mqtt")
        cid = self.cfg.get("client_id") or ("fxr90_win_" + uuid.uuid4().hex[:6])
        v5 = str(self.cfg.get("mqtt_version")) == "5"
        proto = mqtt.MQTTv5 if v5 else mqtt.MQTTv311
        cli = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                          client_id=cid, protocol=proto,
                          clean_session=None if v5 else True)
        if self.cfg.get("username"):
            cli.username_pw_set(self.cfg["username"], self.cfg.get("password") or None)
        if self.cfg.get("tls"):
            if self.cfg.get("verify_cert", True):
                ctx = ssl.create_default_context(cafile=self.cfg.get("ca_file") or None)
                cli.tls_set_context(ctx)
            else:
                cli.tls_set(cert_reqs=ssl.CERT_NONE)
                cli.tls_insecure_set(True)
        cli.reconnect_delay_set(2, 15)
        cli.on_connect = self._on_connect
        cli.on_disconnect = self._on_disconnect
        cli.on_message = self._on_message
        self.client = cli
        self._want = True
        self.q.put(("log", "连接 %s:%s (MQTT%s%s) ..." % (
            self.cfg["host"], self.cfg["port"], self.cfg.get("mqtt_version", "5"),
            " / TLS" if self.cfg.get("tls") else "")))
        cli.connect_async(self.cfg["host"], int(self.cfg["port"]),
                          int(self.cfg.get("keepalive", 60)))
        cli.loop_start()

    def stop(self):
        self._want = False
        if self.client:
            try:
                self.client.disconnect()
            except Exception:
                pass
            try:
                self.client.loop_stop()
            except Exception:
                pass
            self.client = None
        self.connected = False
        self.q.put(("conn", False, "已断开"))

    # --- paho 回调（网络线程）
    def _on_connect(self, cli, userdata, flags, rc, props=None):
        ok = (rc == 0) or (getattr(rc, "value", 1) == 0)
        if not ok:
            self.q.put(("conn", False, "连接被拒绝: %s" % rc))
            return
        self.connected = True
        self.q.put(("conn", True, "已连接"))
        for key, label in (("response_topic", "响应"), ("management_topic", "管理事件")):
            t = (self.cfg.get(key) or "").strip()
            if not t:
                continue
            try:
                cli.subscribe(t, 1)
                self.q.put(("log", "订阅%s主题 %s" % (label, t)))
            except Exception as e:
                self.q.put(("log", "订阅%s主题失败: %s" % (label, e)))

    def _on_disconnect(self, cli, userdata, flags, rc, props=None):
        self.connected = False
        self.q.put(("conn", False, "连接断开 (rc=%s)" % rc))

    def _on_message(self, cli, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8", "replace")
        cfg = self.cfg
        if topic_matches(cfg.get("response_topic", ""), topic):
            self.q.put(("resp", payload, topic))
        elif topic_matches(cfg.get("management_topic", ""), topic):
            self.q.put(("mgmt", payload, topic))
        elif topic_matches(cfg.get("tag_topic", ""), topic):
            self.q.put(("tag", payload, topic))
        else:
            self.q.put(("other", payload, topic))

    # --- 供 UI 线程调用
    def subscribe(self, topic: str):
        if self.client and self.connected:
            self.client.subscribe(topic, 1)
            self.q.put(("log", "订阅 %s" % topic))

    def unsubscribe(self, topic: str):
        if self.client and self.connected:
            self.client.unsubscribe(topic)
            self.q.put(("log", "取消订阅 %s" % topic))

    def publish(self, topic: str, payload: str):
        if self.client and self.connected:
            self.client.publish(topic, payload, qos=1)
            self.q.put(("log", "→ %s  %s" % (topic, payload)))
            return True
        return False


# ---------------------------------------------------------------- 设置对话框
class SettingsDialog(tk.Toplevel):
    FIELDS = [
        ("host", "服务器地址", "str"),
        ("port", "端口", "int"),
        ("tls", "TLS 加密 (mqtts)", "bool"),
        ("verify_cert", "校验服务器证书", "bool"),
        ("ca_file", "CA 证书（可留空）", "file"),
        ("username", "用户名", "str"),
        ("password", "密码", "str"),
        ("client_id", "Client ID", "str"),
        ("mqtt_version", "MQTT 版本", "choice"),
        ("keepalive", "Keep Alive (秒)", "int"),
        ("tag_topic", "标签数据主题", "str"),
        ("response_topic", "指令响应主题", "str"),
        ("management_topic", "管理事件主题", "str"),
        ("command_topic", "命令下发主题", "str"),
        ("send_commands", "开始/结束同时下发读写器指令", "bool"),
        ("auto_connect", "启动时自动连接", "bool"),
    ]

    def __init__(self, master, cfg, on_save):
        super().__init__(master)
        self.title("连接设置")
        self.configure(bg=BG)
        self.resizable(False, False)
        self.cfg = dict(cfg)
        self.on_save = on_save
        self.vars = {}
        self.transient(master)
        self.grab_set()

        body = ttk.Frame(self, style="Panel.TFrame", padding=18)
        body.pack(fill="both", expand=True, padx=12, pady=12)

        ttk.Label(body, text="MQTT 连接与主题", style="PanelTitle.TLabel").grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 12))

        r = 1
        for key, label, kind in self.FIELDS:
            if key in ("tag_topic", "response_topic") and r == 11:
                ttk.Separator(body, orient="horizontal").grid(
                    row=r, column=0, columnspan=3, sticky="ew", pady=8)
                r += 1
            ttk.Label(body, text=label, style="Panel.TLabel").grid(
                row=r, column=0, sticky="w", pady=5, padx=(0, 12))
            if kind == "bool":
                v = tk.BooleanVar(value=bool(self.cfg.get(key)))
                tk.Checkbutton(body, variable=v, text="启用", bg=PANEL, fg=FG,
                               activebackground=PANEL, activeforeground=FG,
                               selectcolor=FIELD, highlightthickness=0, bd=0,
                               font=("Microsoft YaHei UI", 9)).grid(
                    row=r, column=1, sticky="w")
            elif kind == "choice":
                v = tk.StringVar(value=str(self.cfg.get(key, "5")))
                cb = ttk.Combobox(body, textvariable=v, values=["5", "3.1.1"],
                                  state="readonly", width=34)
                cb.grid(row=r, column=1, sticky="w")
            elif kind == "file":
                v = tk.StringVar(value=str(self.cfg.get(key, "")))
                fr = ttk.Frame(body, style="Panel.TFrame")
                fr.grid(row=r, column=1, sticky="w")
                ttk.Entry(fr, textvariable=v, width=28).pack(side="left")
                ttk.Button(fr, text="…", width=3,
                           command=lambda vv=v: self._pick_ca(vv)).pack(side="left", padx=4)
            else:
                v = tk.StringVar(value=str(self.cfg.get(key, "")))
                e = ttk.Entry(body, textvariable=v, width=36)
                if key == "password":
                    e.configure(show="•")
                e.grid(row=r, column=1, sticky="w")
            self.vars[key] = (v, kind)
            r += 1

        tip = ("说明：Zebra 读取器的三个主题由读取器端 MQTT 端点配置决定。\n"
               "若“开始/结束”没有真正控制读写器，请点“探测命令主题”或向厂商确认主题名。\n"
               "配置文件：%s" % CONFIG_PATH)
        ttk.Label(body, text=tip, style="Hint.TLabel", justify="left").grid(
            row=r, column=0, columnspan=3, sticky="w", pady=(12, 0))

        self.probe_out = tk.Text(body, height=4, bg=FIELD, fg=MUTED, bd=0,
                                 font=("Consolas", 9), wrap="word")
        self.probe_out.grid(row=r + 1, column=0, columnspan=3, sticky="ew", pady=(8, 0))
        self.probe_out.configure(state="disabled")

        btns = ttk.Frame(body, style="Panel.TFrame")
        btns.grid(row=r + 2, column=0, columnspan=3, sticky="ew", pady=(14, 0))
        ttk.Button(btns, text="从 MQTTX 配置导入", command=self._import_mqttx).pack(side="left")
        ttk.Button(btns, text="探测命令主题", command=self._probe).pack(side="left", padx=8)
        ttk.Button(btns, text="取消", command=self.destroy).pack(side="right")
        ttk.Button(btns, text="保存", style="Accent.TButton",
                   command=self._save).pack(side="right", padx=8)

    # ---- 辅助
    def _pick_ca(self, var):
        p = filedialog.askopenfilename(title="选择 CA 证书", filetypes=[
            ("证书", "*.pem *.crt *.cer"), ("所有文件", "*.*")])
        if p:
            var.set(p)

    def _import_mqttx(self):
        p = filedialog.askopenfilename(title="选择 MQTTX 导出的 JSON",
                                       filetypes=[("JSON", "*.json"), ("所有文件", "*.*")])
        if not p:
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            conns = data if isinstance(data, list) else [data]
            conn = conns[0]
            self.vars["host"][0].set(str(conn.get("host", "")))
            self.vars["port"][0].set(str(conn.get("port", 8883)))
            proto = str(conn.get("protocol", "mqtt")).lower()
            self.vars["tls"][0].set(proto.endswith("s") or bool(conn.get("ssl")))
            self.vars["verify_cert"][0].set(bool(conn.get("rejectUnauthorized", True)))
            self.vars["username"][0].set(str(conn.get("username") or ""))
            self.vars["password"][0].set(str(conn.get("password") or ""))
            self.vars["client_id"][0].set(str(conn.get("clientId") or "fxr90_win_001"))
            self.vars["keepalive"][0].set(str(conn.get("keepalive", 60)))
            self.vars["mqtt_version"][0].set(
                "5" if str(conn.get("mqttVersion", "5")).startswith("5") else "3.1.1")
            # 从历史消息里的 topic 反推标签/响应主题
            topics = {m.get("topic") for m in (conn.get("messages") or []) if m.get("topic")}
            for t in topics:
                low = t.lower()
                if "tag" in low:
                    self.vars["tag_topic"][0].set(t)
                elif "meresponse" in low or "mevent" in low:
                    self.vars["management_topic"][0].set(t)
                elif "response" in low or "rsp" in low:
                    self.vars["response_topic"][0].set(t)
            self._say("已从 %s 导入（%s:%s）" % (os.path.basename(p), conn.get("host"), conn.get("port")))
        except Exception as e:
            messagebox.showerror("导入失败", str(e), parent=self)

    def _say(self, text):
        self.probe_out.configure(state="normal")
        self.probe_out.insert("end", text + "\n")
        self.probe_out.see("end")
        self.probe_out.configure(state="disabled")

    def _probe(self):
        """向候选主题各发一条 get_version，看哪个能收到读取器响应。"""
        cfg = self._collect()
        resp_topic = cfg["response_topic"]
        base = resp_topic.rsplit("/", 1)[0] if "/" in resp_topic else "/FXR90"
        cands = [cfg["command_topic"]] + ["%s/%s" % (base, s) for s in PROBE_SUFFIXES]
        cands += [resp_topic.replace("response", "request"),
                  resp_topic.replace("meresponse", "merequest"),
                  resp_topic.replace("rsp", "cmd")]
        seen, uniq = set(), []
        for c in cands:
            if c and c not in seen and c != resp_topic:
                seen.add(c)
                uniq.append(c)

        self._say("探测 %d 个候选主题 ..." % len(uniq))
        threading.Thread(target=self._probe_worker, args=(cfg, uniq), daemon=True).start()

    def _probe_worker(self, cfg, topics):
        got = []
        pending = {}
        lock = threading.Lock()

        def on_connect(c, u, f, rc, props=None):
            c.subscribe(cfg["response_topic"], 1)

        def on_message(c, u, msg):
            try:
                j = json.loads(msg.payload.decode("utf-8", "replace"))
            except Exception:
                return
            if isinstance(j, dict) and "response" in j:
                with lock:
                    if j.get("command_id") in pending:
                        got.append((pending[j["command_id"]], j.get("command"), j.get("response")))

        try:
            proto = mqtt.MQTTv5 if cfg.get("mqtt_version") == "5" else mqtt.MQTTv311
            c = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="fxr90_probe_" + uuid.uuid4().hex[:6], protocol=proto)
            c.username_pw_set(cfg["username"], cfg.get("password") or None)
            if cfg.get("tls"):
                if cfg.get("verify_cert", True):
                    c.tls_set_context(ssl.create_default_context(cafile=cfg.get("ca_file") or None))
                else:
                    c.tls_set(cert_reqs=ssl.CERT_NONE)
                    c.tls_insecure_set(True)
            c.on_connect, c.on_message = on_connect, on_message
            c.connect(cfg["host"], int(cfg["port"]), 20)
            c.loop_start()
            time.sleep(1.5)
            for t in topics:
                cid = uuid.uuid4().hex[:8]
                with lock:
                    pending[cid] = t
                c.publish(t, json.dumps({"command": "get_version", "command_id": cid,
                                         "payload": {}}), qos=1)
                time.sleep(1.2)
            time.sleep(2.0)
            c.loop_stop()
            c.disconnect()
            self.after(0, self._probe_done, got)
        except Exception as e:
            self.after(0, self._say, "探测失败: %s" % e)

    def _probe_done(self, got):
        if not got:
            self._say("未收到任何响应。命令主题可能不在候选内，或该账号无发布权限。")
            return
        t = got[0][0]
        self.vars["command_topic"][0].set(t)
        self._say("✔ 命中命令主题: %s（response=%s）已填入" % (t, got[0][2]))

    def _collect(self):
        cfg = dict(self.cfg)
        for key, (v, kind) in self.vars.items():
            if kind == "bool":
                cfg[key] = bool(v.get())
            elif kind == "int":
                try:
                    cfg[key] = int(str(v.get()).strip())
                except Exception:
                    cfg[key] = DEFAULT_CONFIG.get(key)
            else:
                val = v.get().strip()
                cfg[key] = val
        if cfg.get("mqtt_version") == "3.1.1":
            cfg["mqtt_version"] = "3.1.1"
        else:
            cfg["mqtt_version"] = "5"
        return cfg

    def _save(self):
        cfg = self._collect()
        if not cfg["host"]:
            messagebox.showwarning("提示", "服务器地址不能为空", parent=self)
            return
        self.on_save(cfg)
        self.destroy()


# ---------------------------------------------------------------- 主窗口
class App(tk.Tk):
    COLS = [
        ("seq", "#", 52, "center"),
        ("id", "EPC / 标签ID", 250, "w"),
        ("tid", "TID", 170, "w"),
        ("ant", "天线", 56, "center"),
        ("rssi", "RSSI(dBm)", 84, "center"),
        ("reads", "次数", 66, "center"),
        ("first", "首次", 84, "center"),
        ("last", "最近", 84, "center"),
        ("chan", "频道(MHz)", 90, "center"),
        ("pc", "PC", 60, "center"),
        ("crc", "CRC", 60, "center"),
    ]

    def __init__(self, cfg):
        super().__init__()
        self.cfg = cfg
        self.store = TagStore()
        self.q = queue.Queue()
        self.link = MqttLink(cfg, self.q)

        self.session_active = False
        self.session_start = 0.0
        self.session_elapsed = 0.0
        self.connected = False
        self.item_by_id = {}
        self.sort_col = None
        self.sort_desc = False
        self.rate_win = deque()
        self.pending_cmd = {}
        self.dirty = False
        self._pending_start = False

        self.title("%s  v%s" % (APP_NAME, APP_VER))
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        w = max(980, min(1220, sw - 80))
        h = max(600, min(780, sh - 120))
        self.geometry("%dx%d+%d+%d" % (w, h, max(0, (sw - w) // 2), max(0, (sh - h) // 2 - 20)))
        self.minsize(min(980, w), min(600, h))
        self.configure(bg=BG)
        self._init_style()
        self._build()
        self._load_settings_into_view()

        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.bind("<F5>", lambda e: self.start_reading())
        self.bind("<F6>", lambda e: self.stop_reading())
        self.after(80, self._pump)
        self.after(200, self._tick)
        if cfg.get("auto_connect"):
            self.after(400, self.connect_mqtt)

    # ---------- 样式
    def _init_style(self):
        st = ttk.Style(self)
        st.theme_use("clam")
        st.configure(".", background=BG, foreground=FG, fieldbackground=FIELD,
                     bordercolor=BORDER, lightcolor=PANEL, darkcolor=PANEL,
                     focuscolor=ACCENT, font=("Microsoft YaHei UI", 10))
        st.configure("TFrame", background=BG)
        st.configure("Panel.TFrame", background=PANEL)
        st.configure("Card.TFrame", background=CARD)
        st.configure("TLabel", background=BG, foreground=FG)
        st.configure("Panel.TLabel", background=PANEL, foreground=FG)
        st.configure("PanelTitle.TLabel", background=PANEL, foreground=FG,
                     font=("Microsoft YaHei UI", 11, "bold"))
        st.configure("Hint.TLabel", background=PANEL, foreground=MUTED,
                     font=("Microsoft YaHei UI", 9))
        st.configure("Title.TLabel", background=BG, foreground=FG,
                     font=("Microsoft YaHei UI", 15, "bold"))
        st.configure("Sub.TLabel", background=BG, foreground=MUTED,
                     font=("Microsoft YaHei UI", 9))
        st.configure("Stat.TLabel", background=FIELD, foreground=MUTED,
                     font=("Microsoft YaHei UI", 9))
        st.configure("StatVal.TLabel", background=FIELD, foreground=FG,
                     font=("Microsoft YaHei UI", 11, "bold"))

        st.configure("TButton", background=CARD, foreground=FG, borderwidth=0,
                     focusthickness=0, padding=(16, 9), relief="flat",
                     font=("Microsoft YaHei UI", 10))
        st.map("TButton",
               background=[("disabled", "#1b2127"), ("pressed", CARD_HI), ("active", CARD_HI)],
               foreground=[("disabled", "#5b6570")])
        st.configure("Accent.TButton", background=ACCENT, foreground="#ffffff")
        st.map("Accent.TButton", background=[("active", "#4a92f8"), ("pressed", "#2670e0")])
        st.configure("Start.TButton", background=GREEN, foreground="#ffffff",
                     font=("Microsoft YaHei UI", 10, "bold"))
        st.map("Start.TButton", background=[("active", GREEN_HI), ("pressed", "#278a3c"),
                                            ("disabled", "#22422c")])
        st.configure("Stop.TButton", background=RED, foreground="#ffffff",
                     font=("Microsoft YaHei UI", 10, "bold"))
        st.map("Stop.TButton", background=[("active", RED_HI), ("pressed", "#b03d35"),
                                           ("disabled", "#3d2523")])
        st.configure("Ghost.TButton", background=PANEL, foreground=MUTED)
        st.map("Ghost.TButton", background=[("active", CARD)], foreground=[("active", FG)])

        st.configure("TEntry", fieldbackground=FIELD, foreground=FG, insertcolor=FG,
                     bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER, padding=6)
        st.configure("TCombobox", fieldbackground=FIELD, background=FIELD, foreground=FG,
                     arrowcolor=MUTED, bordercolor=BORDER, padding=5)
        st.map("TCombobox", fieldbackground=[("readonly", FIELD)],
               foreground=[("readonly", FG)])
        self.option_add("*TCombobox*Listbox.background", FIELD)
        self.option_add("*TCombobox*Listbox.foreground", FG)
        self.option_add("*TCombobox*Listbox.selectBackground", ACCENT)

        st.configure("Switch.TCheckbutton", background=PANEL, foreground=FG,
                     focuscolor=PANEL)
        st.map("Switch.TCheckbutton", background=[("active", PANEL)])
        st.configure("Dark.TCheckbutton", background=BG, foreground=MUTED, focuscolor=BG)
        st.map("Dark.TCheckbutton", background=[("active", BG)])

        st.configure("Tags.Treeview", background=PANEL, fieldbackground=PANEL,
                     foreground=FG, rowheight=27, borderwidth=0,
                     font=("Consolas", 9))
        st.configure("Tags.Treeview.Heading", background="#26303a", foreground=MUTED,
                     relief="flat", padding=(6, 8),
                     font=("Microsoft YaHei UI", 9, "bold"))
        st.map("Tags.Treeview", background=[("selected", "#1f4d7a")],
               foreground=[("selected", "#ffffff")])
        st.map("Tags.Treeview.Heading", background=[("active", "#2e3a46")],
               foreground=[("active", FG)])

        st.configure("Vertical.TScrollbar", background=CARD, troughcolor=BG,
                     bordercolor=BG, arrowcolor=MUTED, darkcolor=CARD,
                     lightcolor=CARD, gripcount=0, arrowsize=12)
        st.configure("Horizontal.TScrollbar", background=CARD, troughcolor=BG,
                     bordercolor=BG, arrowcolor=MUTED, darkcolor=CARD,
                     lightcolor=CARD, arrowsize=12)
        st.configure("TSeparator", background=BORDER)

    # ---------- 界面
    def _build(self):
        # 顶部标题栏
        head = ttk.Frame(self, style="TFrame")
        head.pack(fill="x", padx=18, pady=(14, 6))
        left = ttk.Frame(head, style="TFrame")
        left.pack(side="left")
        ttk.Label(left, text="Zebra FX·MQTT 标签读取器", style="Title.TLabel").pack(anchor="w")
        self.sub_lbl = ttk.Label(left, text="—", style="Sub.TLabel")
        self.sub_lbl.pack(anchor="w", pady=(2, 0))

        right = ttk.Frame(head, style="TFrame")
        right.pack(side="right")
        self.dot = tk.Canvas(right, width=12, height=12, bg=BG, highlightthickness=0)
        self.dot.pack(side="left", padx=(0, 6))
        self._dot_id = self.dot.create_oval(2, 2, 10, 10, fill=MUTED, outline="")
        self.conn_lbl = ttk.Label(right, text="未连接", style="Sub.TLabel")
        self.conn_lbl.pack(side="left", padx=(0, 14))
        self.btn_settings = ttk.Button(right, text="设置", style="Ghost.TButton",
                                       command=self.open_settings)
        self.btn_settings.pack(side="left", padx=(0, 8))
        self.btn_conn = ttk.Button(right, text="连接", style="Accent.TButton",
                                   command=self.toggle_connect)
        self.btn_conn.pack(side="left")

        # 操作条
        bar = ttk.Frame(self, style="TFrame")
        bar.pack(fill="x", padx=18, pady=(6, 10))
        self.btn_start = ttk.Button(bar, text="▶  开始读取   F5", style="Start.TButton",
                                    command=self.start_reading)
        self.btn_start.pack(side="left")
        self.btn_stop = ttk.Button(bar, text="■  结束读取   F6", style="Stop.TButton",
                                   command=self.stop_reading, state="disabled")
        self.btn_stop.pack(side="left", padx=8)
        ttk.Button(bar, text="清空列表", command=self.clear_list).pack(side="left", padx=(16, 0))
        ttk.Button(bar, text="导出 CSV", command=self.export_csv).pack(side="left", padx=8)

        self.autoscroll = tk.BooleanVar(value=bool(self.cfg.get("autoscroll", True)))
        tk.Checkbutton(bar, text="自动滚动", variable=self.autoscroll, bg=BG, fg=MUTED,
                       activebackground=BG, activeforeground=FG, selectcolor=FIELD,
                       highlightthickness=0, bd=0,
                       font=("Microsoft YaHei UI", 9)).pack(side="right")

        # 列表
        table = ttk.Frame(self, style="Panel.TFrame")
        table.pack(fill="both", expand=True, padx=18, pady=(0, 10))
        self.tree = ttk.Treeview(table, columns=[c[0] for c in self.COLS],
                                 show="headings", style="Tags.Treeview")
        for key, title, width, anchor in self.COLS:
            self.tree.heading(key, text=title, command=lambda k=key: self.sort_by(k))
            self.tree.column(key, width=width, anchor=anchor,
                             stretch=(key in ("id", "tid")))
        vsb = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table.rowconfigure(0, weight=1)
        table.columnconfigure(0, weight=1)
        self.tree.tag_configure("odd", background="#1d242c")
        self.tree.tag_configure("even", background=PANEL)
        self.tree.tag_configure("new", background="#1f3a2a")

        self.menu = tk.Menu(self, tearoff=0, bg=PANEL, fg=FG,
                            activebackground=ACCENT, activeforeground="#fff", bd=0)
        self.menu.add_command(label="复制 EPC", command=self._copy_id)
        self.menu.add_command(label="复制整行", command=self._copy_row)
        self.menu.add_command(label="查看原始 JSON", command=self._show_raw)
        self.tree.bind("<Button-3>", self._popup)
        self.tree.bind("<Double-1>", lambda e: self._show_raw())

        # 日志
        logwrap = ttk.Frame(self, style="Panel.TFrame")
        logwrap.pack(fill="x", padx=18, pady=(0, 8))
        hdr = ttk.Frame(logwrap, style="Panel.TFrame")
        hdr.pack(fill="x", padx=10, pady=(6, 0))
        ttk.Label(hdr, text="通信日志", style="Hint.TLabel").pack(side="left")
        ttk.Button(hdr, text="清空", style="Ghost.TButton",
                   command=self._clear_log).pack(side="right")
        self.logbox = tk.Text(logwrap, height=6, bg=FIELD, fg=MUTED, bd=0,
                              font=("Consolas", 9), wrap="none", padx=8, pady=6,
                              insertbackground=FG, state="disabled")
        self.logbox.pack(fill="x", padx=10, pady=(4, 10))
        self.logbox.tag_configure("tx", foreground="#6cb6ff")
        self.logbox.tag_configure("rx", foreground="#8b98a5")
        self.logbox.tag_configure("ok", foreground=GREEN_HI)
        self.logbox.tag_configure("err", foreground="#ff7b72")
        self.logbox.tag_configure("cmd", foreground="#e3b341")

        # 状态栏
        sb = tk.Frame(self, bg=FIELD, height=60)
        sb.pack(fill="x", side="bottom")
        sb.pack_propagate(False)
        self.st_conn = self._stat(sb, "连接状态", "未连接", 0)
        self.st_time = self._stat(sb, "工作时长", "00:00:00", 1)
        self.st_tags = self._stat(sb, "标签总数", "0", 2)
        self.st_reads = self._stat(sb, "读取次数", "0", 3)
        self.st_rate = self._stat(sb, "读取速率", "0 /s", 4)
        self.st_topic = self._stat(sb, "标签主题", "—", 5, expand=True)

    def _stat(self, parent, title, value, idx, expand=False):
        f = tk.Frame(parent, bg=FIELD)
        f.pack(side="left", fill="x", expand=expand, padx=(18 if idx == 0 else 22, 0))
        tk.Label(f, text=title, bg=FIELD, fg=MUTED,
                 font=("Microsoft YaHei UI", 8)).pack(anchor="w", pady=(9, 0))
        v = tk.Label(f, text=value, bg=FIELD, fg=FG,
                     font=("Microsoft YaHei UI", 11, "bold"))
        v.pack(anchor="w", pady=(1, 6))
        return v

    # ---------- 配置展示
    def _load_settings_into_view(self):
        c = self.cfg
        scheme = "mqtts" if c.get("tls") else "mqtt"
        self.sub_lbl.configure(text="%s://%s:%s   ·   %s" % (
            scheme, c.get("host"), c.get("port"), c.get("client_id", "")))
        self.st_topic.configure(text=c.get("tag_topic", "—"))

    def open_settings(self):
        SettingsDialog(self, self.cfg, self._apply_settings)

    def _apply_settings(self, cfg):
        was_connected = self.connected
        session = self.session_active
        if was_connected:
            self.link.stop()
            self.connected = False
        if session:
            self.session_active = False
            self._set_running(False)
        self.cfg = cfg
        save_config(cfg)
        self.link.cfg = cfg
        self._load_settings_into_view()
        self.log("设置已保存", "ok")
        if was_connected:
            self.after(300, self.connect_mqtt)

    # ---------- 连接
    def toggle_connect(self):
        if self.connected or (self.link.client and self.link._want):
            self.disconnect_mqtt()
        else:
            self.connect_mqtt()

    def connect_mqtt(self):
        if self.connected:
            return
        try:
            self.btn_conn.configure(text="连接中…", state="disabled")
            self.link.cfg = self.cfg
            self.link = MqttLink(self.cfg, self.q)
            self.link.start()
        except Exception as e:
            self.btn_conn.configure(text="连接", state="normal")
            self.log("连接失败: %s" % e, "err")
            messagebox.showerror("连接失败", str(e), parent=self)

    def disconnect_mqtt(self):
        self._pending_start = False
        if self.session_active:
            self.stop_reading(publish=False)
        self.link.stop()
        self.connected = False
        self._refresh_conn()
        self.btn_conn.configure(text="连接")

    def _refresh_conn(self):
        on = self.connected
        self.dot.itemconfigure(self._dot_id, fill=GREEN_HI if on else MUTED)
        self.conn_lbl.configure(text="已连接" if on else "未连接",
                                foreground=FG if on else MUTED)
        self.st_conn.configure(text="已连接" if on else "未连接",
                               fg=GREEN_HI if on else FG)
        self.btn_conn.configure(text="断开" if on else "连接", state="normal")

    # ---------- 读取控制
    def start_reading(self):
        if self.session_active:
            return
        if not self.connected:
            self._pending_start = True
            self.log("尚未连接，先建立 MQTT 连接，连上后自动开始读取…", "tx")
            if not (self.link.client and self.link._want):
                self.connect_mqtt()
            return
        self._begin_session()

    def _begin_session(self):
        self.store.clear()
        self.item_by_id.clear()
        self.tree.delete(*self.tree.get_children())
        self.rate_win.clear()
        self.session_start = time.time()
        self.session_elapsed = 0.0
        self.session_active = True
        self._set_running(True)

        topic = self.cfg.get("tag_topic", "/FXR90/tag/#")
        self.link.subscribe(topic)
        if self.cfg.get("send_commands", True):
            cid = self.send_command("start", {"doNotPersistState": True})
            self._cmd_timeout = self.after(3500, lambda: self._cmd_warn(cid, "start"))
        self.log("开始读取，订阅 %s" % topic, "ok")
        self._update_stats()

    def stop_reading(self, publish=True):
        if not self.session_active:
            return
        self.session_elapsed = time.time() - self.session_start
        self.session_active = False
        self.link.unsubscribe(self.cfg.get("tag_topic", "/FXR90/tag/#"))
        if publish and self.cfg.get("send_commands", True):
            cid = self.send_command("stop", {})
            self.after(3500, lambda: self._cmd_warn(cid, "stop"))
        self._set_running(False)
        self.log("结束读取：本次 %s，标签 %d 个，读取 %d 次" % (
            fmt_dur(self.session_elapsed), len(self.store.tags), self.store.session_reads), "ok")
        self._update_stats()

    def _set_running(self, running):
        self.btn_start.configure(state="disabled" if running else "normal")
        self.btn_stop.configure(state="normal" if running else "disabled")

    def send_command(self, cmd, payload):
        cid = uuid.uuid4().hex[:8]
        topic = self.cfg.get("command_topic") or ""
        body = json.dumps({"command": cmd, "command_id": cid, "payload": payload},
                          ensure_ascii=False, separators=(",", ":"))
        if not topic:
            self.log("未配置命令主题，跳过 %s 指令" % cmd, "err")
            return None
        ok = self.link.publish(topic, body)
        if not ok:
            self.log("指令未发出（未连接）", "err")
            return None
        self.pending_cmd[cid] = cmd
        self.log("下发指令 %s  (id=%s)" % (cmd, cid), "cmd")
        return cid

    def _cmd_warn(self, cid, cmd):
        if cid and cid in self.pending_cmd:
            self.pending_cmd.pop(cid, None)
            self.log("未收到 %s 指令响应：读取器可能未订阅该主题（%s），"
                     "本地采集不受影响" % (cmd, self.cfg.get("command_topic")), "err")

    # ---------- 事件泵
    def _pump(self):
        n_tag = 0
        try:
            while True:
                kind, *rest = self.q.get_nowait()
                if kind == "tag":
                    for ev in parse_tag_event(rest[0]):
                        rec, new = self.store.add(ev)
                        self.rate_win.append(time.time())
                        self._ensure_row(rec, new)
                        n_tag += 1
                elif kind == "resp":
                    self._on_response(rest[0], rest[1] if len(rest) > 1 else "")
                elif kind == "mgmt":
                    payload = rest[0]
                    topic = rest[1] if len(rest) > 1 else ""
                    try:
                        j = json.loads(payload)
                        comp = j.get("component", "?") if isinstance(j, dict) else "?"
                    except Exception:
                        comp = "?"
                    self.log("← 管理事件 %s  component=%s (%d 字节)" % (
                        topic.split("/")[-1] or topic, comp, len(payload)), "rx")
                elif kind == "other":
                    payload = rest[0]
                    topic = rest[1] if len(rest) > 1 else ""
                    if topic not in getattr(self, "_other_topics", set()):
                        self._other_topics = getattr(self, "_other_topics", set()) | {topic}
                        self.log("← 未分类主题 %s  %s" % (topic, payload[:120]), "rx")
                elif kind == "conn":
                    ok, msg = rest[0], rest[1]
                    self.connected = ok
                    self._refresh_conn()
                    self.log(("✔ " if ok else "✘ ") + msg, "ok" if ok else "err")
                    if ok and getattr(self, "_pending_start", False) \
                            and not self.session_active:
                        self._pending_start = False
                        self.after(200, self._begin_session)
                    elif not ok:
                        self.btn_conn.configure(text="连接", state="normal")
                        if self.session_active:
                            self.session_elapsed = time.time() - self.session_start
                            self.session_active = False
                            self._pending_start = True
                            self._set_running(False)
                            self.log("连接中断，已暂停本次读取（重连后自动继续）", "err")
                elif kind == "log":
                    self.log(rest[0])
        except queue.Empty:
            pass
        if n_tag:
            self.dirty = True
            self._update_stats()
        self.after(80, self._pump)

    def _ensure_row(self, rec, new):
        iid = self.item_by_id.get(rec["id"])
        vals = self._row_values(rec)
        if iid is None:
            iid = self.tree.insert("", "end", values=vals,
                                   tags=("new",) if new else ())
            self.item_by_id[rec["id"]] = iid
            if self.autoscroll.get():
                self.tree.see(iid)
            self.after(1200, lambda i=iid: self._unhighlight(i))
        else:
            self.tree.item(iid, values=vals)
        if self.sort_col:
            self._need_sort = True

    def _unhighlight(self, iid):
        try:
            self.tree.item(iid, tags=())
        except Exception:
            pass

    def _row_values(self, r):
        rssi = r["rssi_best"] if r.get("rssi_best") is not None else r["rssi"]
        iid = self.item_by_id.get(r["id"])
        try:
            seq = (self.tree.index(iid) + 1) if iid else len(self.item_by_id) + 1
        except Exception:
            seq = ""
        return (
            seq,
            r["id"].upper(),
            (r["tid"] or "").upper(),
            ",".join(sorted(r["antennas"])) if r["antennas"] else (r["antenna"] or ""),
            "" if rssi is None else ("%d" % rssi),
            r["reads"],
            r["first"], r["last"],
            "" if r["channel"] is None else ("%.3f" % float(r["channel"])),
            r.get("pc") or "", r.get("crc") or "",
        )

    def _on_response(self, payload, topic=""):
        tag = (" [%s]" % topic) if topic else ""
        try:
            j = json.loads(payload)
        except Exception:
            self.log("←%s %s" % (tag, payload[:200]), "rx")
            return
        if isinstance(j, dict) and "response" in j:
            cid = j.get("command_id")
            cmd = j.get("command", "")
            resp = j.get("response")
            pl = j.get("payload")
            msg = str(pl.get("message") or "") if isinstance(pl, dict) else ""
            low = msg.lower()
            # 读头已在读取/已停止时，start/stop 会返回失败但其实状态已符合意图
            benign = ("currently ongoing" in low or "currently not ongoing" in low
                      or "not ongoing" in low or "already" in low)
            self.pending_cmd.pop(cid, None)
            self.log("← 指令 %s 响应: %s  (id=%s)%s" % (cmd, resp, cid, tag),
                     "ok" if (resp == "success" or benign) else "err")
            if benign:
                short = msg.split("Description:", 1)[-1].strip() or msg
                self.log("   读头状态已符合要求（%s），按成功处理" % short[:110], "ok")
            elif resp != "success":
                self.log("   %s" % (msg or payload)[:400], "err")
        else:
            comp = j.get("component") if isinstance(j, dict) else None
            if comp:
                self.log("← 管理事件 component=%s%s (%d 字节)" % (comp, tag, len(payload)), "rx")
            else:
                self.log("←%s %s" % (tag, payload[:160]), "rx")

    def _tick(self):
        if getattr(self, "_need_sort", False) and self.sort_col:
            self._need_sort = False
            self._apply_sort()
        if self.session_active:
            self.session_elapsed = time.time() - self.session_start
        now = time.time()
        while self.rate_win and now - self.rate_win[0] > 5:
            self.rate_win.popleft()
        if self.session_active:
            rate = len(self.rate_win) / 5.0
            self.st_rate.configure(text="%.0f /s" % rate)
            self.st_time.configure(text=fmt_dur(self.session_elapsed))
        else:
            self.st_time.configure(text=fmt_dur(self.session_elapsed))
            self.st_rate.configure(text="0 /s")
        self.after(250, self._tick)

    def _update_stats(self):
        self.st_tags.configure(text=str(len(self.store.tags)))
        self.st_reads.configure(text=str(self.store.total_reads))

    # ---------- 列表操作
    def sort_by(self, col):
        if self.sort_col == col:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_col, self.sort_desc = col, False
        self._apply_sort()
        for key, title, _, _ in self.COLS:
            arrow = " ▲" if (key == col and not self.sort_desc) else (
                " ▼" if key == col else "")
            self.tree.heading(key, text=title + arrow)

    def _apply_sort(self):
        col = self.sort_col
        if not col:
            return
        tags = list(self.store.tags.values())

        def key(r):
            if col == "id":
                return r["id"]
            if col == "tid":
                return r["tid"] or ""
            if col == "reads":
                return r["reads"]
            if col == "rssi":
                return r["rssi_best"] if r["rssi_best"] is not None else -999
            if col == "ant":
                return r["antenna"] or 0
            if col == "chan":
                return r["channel"] or 0
            if col == "first":
                return r["first"]
            if col == "last":
                return r["last"]
            if col == "pc":
                return r["pc"] or ""
            if col == "crc":
                return r["crc"] or ""
            return r["id"]

        try:
            tags.sort(key=key, reverse=self.sort_desc)
        except Exception:
            return
        for i, r in enumerate(tags):
            iid = self.item_by_id.get(r["id"])
            if iid:
                self.tree.move(iid, "", i)
                self.tree.set(iid, "seq", str(i + 1))

    def clear_list(self):
        if self.session_active:
            if not messagebox.askyesno("确认", "正在读取中，清空当前列表并重新计数？", parent=self):
                return
            self._begin_session()
            return
        self.store.clear()
        self.item_by_id.clear()
        self.tree.delete(*self.tree.get_children())
        self._update_stats()
        self.log("列表已清空")

    def export_csv(self):
        if not self.store.tags:
            messagebox.showinfo("提示", "当前没有可导出的标签数据", parent=self)
            return
        default = "fxr90_tags_%s.csv" % datetime.now().strftime("%Y%m%d_%H%M%S")
        path = filedialog.asksaveasfilename(
            title="导出标签数据", defaultextension=".csv", initialfile=default,
            initialdir=os.path.join(os.path.expanduser("~"), "Desktop"),
            filetypes=[("CSV 文件", "*.csv")])
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow(["序号", "EPC", "TID", "天线", "RSSI峰值(dBm)", "读取次数",
                            "首次读取", "最近读取", "频道(MHz)", "PC", "CRC", "原始JSON"])
                for i, r in enumerate(self.store.tags.values(), 1):
                    w.writerow([i, r["id"].upper(), (r["tid"] or "").upper(),
                                ",".join(sorted(r["antennas"])) or r["antenna"] or "",
                                r["rssi_best"], r["reads"], r["first"], r["last"],
                                r["channel"], r["pc"] or "", r["crc"] or "", r["raw"] or ""])
            self.log("已导出 %d 条到 %s" % (len(self.store.tags), path), "ok")
            messagebox.showinfo("导出完成", "已导出 %d 个标签\n%s" % (len(self.store.tags), path),
                                parent=self)
        except Exception as e:
            messagebox.showerror("导出失败", str(e), parent=self)

    # ---------- 右键菜单
    def _selected_id(self):
        sel = self.tree.selection()
        if not sel:
            return None
        vals = self.tree.item(sel[0], "values")
        return vals[1].lower() if len(vals) > 1 else None

    def _popup(self, event):
        iid = self.tree.identify_row(event.y)
        if iid:
            self.tree.selection_set(iid)
            self.menu.tk_popup(event.x_root, event.y_root)

    def _copy_id(self):
        tid = self._selected_id()
        if tid:
            self.clipboard_clear()
            self.clipboard_append(tid.upper())
            self.log("已复制 EPC %s" % tid.upper())

    def _copy_row(self):
        sel = self.tree.selection()
        if sel:
            vals = self.tree.item(sel[0], "values")
            self.clipboard_clear()
            self.clipboard_append("\t".join(str(v) for v in vals))
            self.log("已复制整行")

    def _show_raw(self):
        tid = self._selected_id()
        if not tid or tid not in self.store.tags:
            return
        r = self.store.tags[tid]
        win = tk.Toplevel(self)
        win.title("标签原始数据 · %s" % tid.upper())
        win.configure(bg=BG)
        win.geometry("680x420")
        txt = tk.Text(win, bg=FIELD, fg=FG, bd=0, font=("Consolas", 10),
                      wrap="word", padx=12, pady=12)
        txt.pack(fill="both", expand=True, padx=12, pady=12)
        try:
            pretty = json.dumps(json.loads(r["raw"] or "{}"), indent=2, ensure_ascii=False)
        except Exception:
            pretty = r["raw"] or ""
        txt.insert("1.0", pretty)
        txt.configure(state="disabled")

    # ---------- 日志
    def log(self, msg, kind="rx"):
        ts = datetime.now().strftime("%H:%M:%S")
        self.logbox.configure(state="normal")
        self.logbox.insert("end", "[%s] %s\n" % (ts, msg), kind)
        lines = int(self.logbox.index("end-1c").split(".")[0])
        if lines > 400:
            self.logbox.delete("1.0", "120.0")
        self.logbox.see("end")
        self.logbox.configure(state="disabled")

    def _clear_log(self):
        self.logbox.configure(state="normal")
        self.logbox.delete("1.0", "end")
        self.logbox.configure(state="disabled")

    # ---------- 关闭
    def on_close(self):
        try:
            if self.session_active:
                self.stop_reading()
            self.link.stop()
        except Exception:
            pass
        self.destroy()


# ---------------------------------------------------------------- 自检 / 演示
SAMPLES = [
    '{"data":{"CRC":"1947","PC":"3000","antenna":1,"channel":924.375,"eventNum":665,'
    '"format":"epc","idHex":"e280689400005007e7e11d1c","peakRssi":-60,'
    '"phase":176.62159729003906,"reads":1},"timestamp":"2026-10-28T10:26:58.327+0000","type":"CUSTOM"}',
    '{"data":{"CRC":"195c","PC":"3000","antenna":1,"channel":922.625,"eventNum":654,'
    '"format":"epc","idHex":"e280689400004012e19300dc","peakRssi":-64,"phase":77.5,'
    '"reads":1},"timestamp":"2026-10-28T08:37:54.134+0000","type":"CUSTOM"}',
    '{"data":[{"idHex":"E280689400005007E7E11D1C","antenna":2,"peakRssi":-55,"reads":3,'
    '"channel":926.375}],"timestamp":"2026-10-28T08:37:55.000+0000","type":"INVENTORY"}',
]


def selftest():
    ok = True
    store = TagStore()
    evs = []
    for s in SAMPLES:
        e = parse_tag_event(s)
        assert e, "解析失败: " + s[:50]
        evs.extend(e)
    for e in evs:
        store.add(e)
    print("事件数:", len(evs), "唯一标签:", len(store.tags),
          "读取次数:", store.total_reads)
    print("标签:", sorted(store.tags))
    assert len(store.tags) == 2, store.tags
    assert store.total_reads == 5, store.total_reads
    assert store.tags["e280689400005007e7e11d1c"]["reads"] == 4
    assert store.tags["e280689400005007e7e11d1c"]["rssi_best"] == -55
    assert "2" in store.tags["e280689400005007e7e11d1c"]["antennas"]
    # 坏数据
    for bad in ["", "not json", "{}", '{"data":{}}', "[]"]:
        assert parse_tag_event(bad) == [], bad
    print("CSV 字段:", len(["序号", "EPC", "TID", "天线", "RSSI峰值(dBm)", "读取次数",
                            "首次读取", "最近读取", "频道(MHz)", "PC", "CRC", "原始JSON"]))
    # 主题通配匹配
    cases = [
        ("/FXR90/tag/#", "/FXR90/tag", True),
        ("/FXR90/tag/#", "/FXR90/tag/1", True),
        ("/FXR90/response/#", "/FXR90/response", True),
        ("/FXR90/response/#", "/FXR90/response/x", True),
        ("/FXR90/meresponse/#", "/FXR90/response", False),
        ("/FXR90/command", "/FXR90/command", True),
        ("/FXR90/command", "/FXR90/command/1", False),
        ("/a/+/c", "/a/b/c", True),
    ]
    for filt, topic, want in cases:
        got = topic_matches(filt, topic)
        assert got == want, (filt, topic, got, want)
    print("主题匹配用例:", len(cases), "全部通过")
    print("SELFTEST OK" if ok else "FAIL")


def demo(app):
    """把样例标签灌进界面，用于没接通读头时验证 UI。"""
    import random
    ids = ["e280689400005007e7e11d1c", "e280689400004012e19300dc",
           "e28068940000a001b2c3d4e5", "e28068940000b002c3d4e5f6"]

    def feed():
        if not app.session_active:
            return
        i = random.choice(ids)
        j = json.dumps({"data": {"CRC": "1947", "PC": "3000",
                                 "antenna": random.randint(1, 4),
                                 "channel": random.choice([921.125, 922.625, 924.375]),
                                 "format": "epc", "idHex": i,
                                 "peakRssi": random.randint(-75, -45),
                                 "reads": 1},
                        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000",
                        "type": "CUSTOM"}, ensure_ascii=False)
        app.q.put(("tag", j))
        app.after(random.randint(120, 500), feed)

    app.log("演示数据已启动", "ok")
    feed()


def enable_dpi_awareness():
    """让窗口按物理像素排版，避免 125% 缩放下界面被放大截断。"""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            ctypes.windll.user32.SetProcessDPIAware()
    except Exception:
        pass


def install_crash_handler():
    """打包后没有控制台：异常写 error.log 并弹窗，别让程序静默消失。"""
    def hook(exc_type, exc, tb):
        import traceback
        text = "".join(traceback.format_exception(exc_type, exc, tb))
        try:
            with open(os.path.join(app_dir(), "error.log"), "a", encoding="utf-8") as f:
                f.write("[%s]\n%s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text))
        except Exception:
            pass
        eprint(text)
        try:
            messagebox.showerror(APP_NAME, "%s: %s\n\n详情见 error.log" % (exc_type.__name__, exc))
        except Exception:
            pass
    sys.excepthook = hook


def main():
    ap = argparse.ArgumentParser(description=APP_NAME)
    ap.add_argument("--selftest", action="store_true", help="只跑解析自检，不开窗口")
    ap.add_argument("--demo", action="store_true", help="连接后自动开始并注入演示数据")
    ap.add_argument("--auto", action="store_true", help="启动后自动连接并开始读取（无人值守）")
    ap.add_argument("--host", help="覆盖服务器地址")
    ap.add_argument("--config", help="指定配置文件路径（默认 exe 同目录 config.json）")
    args = ap.parse_args()
    if args.config:
        global CONFIG_PATH
        CONFIG_PATH = os.path.abspath(args.config)
    if args.selftest:
        import contextlib
        import io
        buf = io.StringIO()
        if sys.stdout is None:          # 窗口模式 exe 无 stdout
            with contextlib.redirect_stdout(buf):
                selftest()
            try:
                with open(os.path.join(app_dir(), "selftest.txt"), "w", encoding="utf-8") as f:
                    f.write(buf.getvalue())
            except Exception:
                pass
        else:
            selftest()
        return
    if mqtt is None:
        eprint("缺少 paho-mqtt：请执行 uv pip install paho-mqtt")
        sys.exit(2)
    enable_dpi_awareness()
    install_crash_handler()
    cfg = load_config()
    if args.host:
        cfg["host"] = args.host
    app = App(cfg)
    if args.demo:
        app.after(1500, lambda: (app.connect_mqtt(), app.after(1800, app.start_reading),
                                 app.after(2000, lambda: demo(app))))
    elif args.auto:
        app.after(400, app.start_reading)
    app.mainloop()


if __name__ == "__main__":
    main()
