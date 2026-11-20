"""
TC002 X 粉丝数 —— MQTT 版 / X follower counter for Ulanzi TC002 via MQTT
纯标准库，无需 pip 安装。可在任意联网机器上运行（不需要和时钟同一局域网）。

用法 / Usage:
    python x_followers_mqtt.py            # 持续运行，每 5 分钟刷新
    python x_followers_mqtt.py --once     # 推送一次后退出（GitHub Actions 用）

环境变量（可选，优先于下面的默认值）/ Env overrides:
    X_USERNAME, MQTT_HOST, MQTT_PORT, MQTT_TOPIC_PREFIX, X_BEARER_TOKEN
"""
import argparse
import json
import os
import socket
import struct
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime

# ===== 配置 / Config =====
X_USERNAME = os.environ.get("X_USERNAME") or "VIVISTAYGOLD"
MQTT_HOST = os.environ.get("MQTT_HOST") or "broker.hivemq.com"
MQTT_PORT = int(os.environ.get("MQTT_PORT") or 1883)
TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX") or ""  # 必填：mqtt_prefix + "_" + MAC 后4位（放在 GitHub Secret）
APP_NAME = "xfollowers"
INTERVAL_SEC = 300
FLASH_SEC = 60
X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN", "")

COLOR_NORMAL, COLOR_UP, COLOR_DOWN = "#FFFFFF", "#3DDC84", "#FF5A5A"
UA = {"User-Agent": "tc002-x-followers/1.1"}


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ===== 数据源 / Data sources =====
def http_json(url, headers=None, timeout=15):
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def get_followers(user):
    try:
        d = http_json(f"https://api.fxtwitter.com/{urllib.parse.quote(user)}")
        return int(d["user"]["followers"])
    except Exception as e:
        if not X_BEARER_TOKEN:
            raise
        log(f"fxtwitter 失败，改用官方 API: {e}")
        d = http_json(
            f"https://api.x.com/2/users/by/username/{urllib.parse.quote(user)}"
            "?user.fields=public_metrics",
            headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"},
        )
        return int(d["data"]["public_metrics"]["followers_count"])


# ===== 画面 / Rendering =====
def x_logo_bitmap(size=10, color=0xFFFFFF):
    px = [0] * (size * size)
    for y in range(size):
        for x in (y, y + 1):
            if x < size:
                px[y * size + x] = color
        px[y * size + (size - 1 - y)] = color
    return px


def fmt(n):
    if n < 10_000:
        return str(n)
    if n < 1_000_000:
        return f"{n / 1000:.1f}K".replace(".0K", "K")
    return f"{n / 1_000_000:.1f}M".replace(".0M", "M")


def build_payload(count, color):
    return {
        "draw": [{"db": [1, 3, 10, 10, x_logo_bitmap()]}],
        "text": [{
            "content": fmt(count) if count is not None else "--",
            "fontHeight": 10, "x": -1000, "y": -1000,
            "align": "center", "valign": "middle",
            "rect": [13, 0, 39, 16], "color": color, "charSpacing": 1,
        }],
    }


# ===== 极简 MQTT 3.1.1 发布（QoS 0）/ Minimal MQTT publisher =====
def _varlen(n):
    out = bytearray()
    while True:
        b, n = n % 128, n // 128
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _str(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def mqtt_publish(messages):
    """messages: list of (topic, payload_str, retain)"""
    client_id = f"tc002x-{os.getpid()}-{int(time.time())}"
    var = _str("MQTT") + bytes([4, 0x02]) + struct.pack("!H", 30)
    body = var + _str(client_id)
    with socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=15) as s:
        s.sendall(b"\x10" + _varlen(len(body)) + body)
        ack = s.recv(4)
        if len(ack) < 4 or ack[0] != 0x20 or ack[3] != 0:
            raise RuntimeError(f"MQTT CONNACK 异常: {ack!r}")
        for topic, payload, retain in messages:
            pb = _str(topic) + payload.encode()
            s.sendall(bytes([0x30 | (1 if retain else 0)]) + _varlen(len(pb)) + pb)
        s.sendall(b"\xe0\x00")  # DISCONNECT


def push(count, color, switch=False):
    msgs = [(f"{TOPIC_PREFIX}/custom/{APP_NAME}",
             json.dumps(build_payload(count, color), separators=(",", ":")),
             True)]  # retain：时钟重启/重连后立即拿到最新画面
    if switch:
        msgs.append((f"{TOPIC_PREFIX}/switchDiyApp", json.dumps({"name": APP_NAME}), False))
    mqtt_publish(msgs)


def main():
    if not TOPIC_PREFIX:
        sys.exit("缺少 MQTT_TOPIC_PREFIX 环境变量")
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="推送一次后退出")
    ap.add_argument("--switch", action="store_true", help="同时把时钟切到该页面")
    args = ap.parse_args()

    last, flash_until, flash_color = None, 0.0, COLOR_NORMAL
    switch = args.switch or not args.once
    while True:
        try:
            n = get_followers(X_USERNAME)
            if last is not None and n != last:
                flash_color = COLOR_UP if n > last else COLOR_DOWN
                flash_until = time.time() + FLASH_SEC
                log(f"粉丝变化: {last} -> {n}")
            color = flash_color if time.time() < flash_until else COLOR_NORMAL
            push(n, color, switch=switch)
            switch = False
            log(f"已推送: {n}")
            last = n
        except Exception as e:
            log(f"本轮失败: {e}")
            if args.once:
                sys.exit(1)
        if args.once:
            return
        wait = INTERVAL_SEC
        if flash_until > time.time():
            wait = min(wait, flash_until - time.time() + 1)
        time.sleep(wait)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
