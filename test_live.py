# -*- coding: utf-8 -*-
"""真实链路验证：用 App 的 MQTT 层订阅新主题，发 start/stop，看读头回什么。"""
import json
import queue
import sys
import time

import fxr90_reader as R

cfg = R.load_config()
print("主题:", cfg["tag_topic"], "|", cfg["response_topic"], "|",
      cfg["management_topic"], "| 命令:", cfg["command_topic"])

q = queue.Queue()
link = R.MqttLink(cfg, q)
link.start()
t0 = time.time()
while time.time() - t0 < 10 and not link.connected:
    time.sleep(0.1)
print("连接:", link.connected)
assert link.connected, "连接失败"

# 等健康事件
time.sleep(2)
link.subscribe(cfg["tag_topic"])

cid = "live" + str(int(time.time()) % 100000)
body = json.dumps({"command": "start", "command_id": cid,
                   "payload": {"doNotPersistState": True}}, separators=(",", ":"))
link.publish(cfg["command_topic"], body)
print("已发 start:", cid)

seen = {"resp": 0, "mgmt": 0, "tag": 0, "other": 0}
samples = []
t0 = time.time()
while time.time() - t0 < 20:
    try:
        kind, *rest = q.get(timeout=0.5)
    except queue.Empty:
        continue
    seen[kind] = seen.get(kind, 0) + 1
    if kind in ("resp", "log"):
        print("[%s]" % kind, str(rest)[:220])
    elif kind in ("mgmt", "tag", "other") and seen[kind] <= 2:
        print("[%s]" % kind, str(rest[0])[:260])
    if kind in ("tag", "resp") and len(samples) < 4:
        samples.append((kind, rest[:2]))
    if seen["tag"] >= 30:
        break

print("统计:", seen)
link.stop()
print("LIVE OK" if (seen["resp"] or seen["tag"]) else "LIVE: 没有收到响应/标签")
