# -*- coding: utf-8 -*-
"""GUI 冒烟测试（在现场实时数据下也要稳）：真实连 broker + 注入样例 + 校验界面/CSV。

读头可能正在推真实标签，所以只断言“注入的样例 EPC 必须出现且计数正确”，
不断言全表行数与总数。
"""
import csv
import os
import time

import fxr90_reader as R

R.filedialog.asksaveasfilename = lambda **kw: os.path.join(R.app_dir(), "_test_export.csv")
R.messagebox.showinfo = lambda *a, **k: None
R.messagebox.showerror = lambda *a, **k: None

SAMPLE_IDS = ["E280689400005007E7E11D1C", "E280689400004012E19300DC"]

app = R.App(R.load_config())
app.update()
print("窗口:", app.title(), app.winfo_width(), "x", app.winfo_height())

app.connect_mqtt()
t0 = time.time()
while time.time() - t0 < 15 and not app.connected:
    app.update()
    time.sleep(0.05)
print("连接:", app.connected, "|", app.conn_lbl.cget("text"))
assert app.connected, "未能连接 broker"

app.start_reading()
app.update()
print("会话激活:", app.session_active, "| 开始按钮:", str(app.btn_start["state"]),
      "| 结束按钮:", str(app.btn_stop["state"]))
assert app.session_active

# 停掉现场数据流，注入确定样例
app.link.unsubscribe(app.cfg["tag_topic"])
time.sleep(0.4)
app.store.clear()
app.item_by_id.clear()
app.tree.delete(*app.tree.get_children())
for s in R.SAMPLES:
    app.q.put(("tag", s))
t0 = time.time()
while time.time() - t0 < 1.5:
    app.update()
    time.sleep(0.02)
rows = app.tree.get_children()
vals = {app.tree.item(r, "values")[1]: app.tree.item(r, "values") for r in rows}
print("注入后行数:", len(rows))
for k in sorted(vals):
    print("  ", vals[k])
for sid in SAMPLE_IDS:
    assert sid in vals, "样例 %s 未出现在列表" % sid
assert vals[SAMPLE_IDS[0]][5] == "4", vals[SAMPLE_IDS[0]]
assert vals[SAMPLE_IDS[0]][4] == "-55", vals[SAMPLE_IDS[0]]
assert vals[SAMPLE_IDS[0]][3] == "1,2", vals[SAMPLE_IDS[0]]

before = len(app.tree.get_children())
app.q.put(("tag", R.SAMPLES[0]))
for _ in range(15):
    app.update(); time.sleep(0.02)
print("再次注入后行数:", len(app.tree.get_children()), "读取次数:", app.st_reads.cget("text"))
assert len(app.tree.get_children()) == before
assert app.tree.item(app.item_by_id[SAMPLE_IDS[0].lower()], "values")[5] == "5"

app.sort_by("reads")
app.update()
print("按次数排序首行:", app.tree.item(app.tree.get_children()[0], "values")[:2])

app.export_csv()
p = os.path.join(R.app_dir(), "_test_export.csv")
with open(p, encoding="utf-8-sig") as f:
    data = list(csv.reader(f))
print("CSV 行:", len(data), "表头:", data[0][:5])
assert data[0][1] == "EPC" and len(data) >= 3

app.stop_reading()
app.update()
print("停止后: 会话", app.session_active, "时长", app.st_time.cget("text"),
      "| 开始按钮", str(app.btn_start["state"]), "| 结束按钮", str(app.btn_stop["state"]))
assert not app.session_active and str(app.btn_start["state"]) == "normal"

dlg = R.SettingsDialog(app, app.cfg, lambda c: None)
app.update()
cfg = dlg._collect()
print("设置字段:", len(cfg), "| 命令:", cfg["command_topic"],
      "| 响应:", cfg["response_topic"], "| 管理:", cfg["management_topic"])
assert cfg["command_topic"] == "/FXR90/command"
dlg.destroy()

app.clear_list()
app.update()
print("清空后行数:", len(app.tree.get_children()))
app.disconnect_mqtt()
for _ in range(10):
    app.update(); time.sleep(0.05)
print("断开后:", app.conn_lbl.cget("text"), "| 按钮:", app.btn_conn.cget("text"))
app.destroy()
os.remove(p)
print("GUI TEST OK")
