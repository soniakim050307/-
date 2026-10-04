#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
스탭 허브 (Stage Hub) v0.1
배우 · 관객 · 스탭 3자 정서 피드백 루프의 노트북 쪽 프로그램.

- 좌석 보드(USB 시리얼), 배우 보드(UDP/시리얼), 또는 노트북에서 폴라 직접 연결(BLE)로 신호를 받는다.
- 개인 기준선 대비 z점수를 0~1로 바꿔 낮음/중간/높음 단계를 만든다(히스테리시스).
- 배우->관객은 스탭 승인형, 관객->배우는 스탭 차단형으로 진동 명령을 보낸다.
- 브라우저 화면(dashboard.html)에서 그래프, 승인/거부, 큐, 대본을 본다.
- 모든 이벤트와 원신호는 logs/<날짜_시각>/ 에 CSV로 저장된다.

표준 라이브러리만으로 돌아간다. 시리얼은 pyserial, 폴라 직접 연결은 bleak가 있을 때만 쓴다.
"""
import argparse
import csv
import json
import math
import os
import queue
import random
import re
import socket
import sys
import threading
import time
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "0.1"
HERE = os.path.dirname(os.path.abspath(__file__))
LEVEL_KO = {"low": "낮음", "mid": "중간", "high": "높음"}

DEFAULT_CFG = {
    # 단계 경계(0~1 점수) 와 히스테리시스
    "t_low": 0.45, "t_high": 0.70, "hyst": 0.03,
    # z점수 -> 0~1: s = (z - z0) / (z1 - z0)
    "z0": -1.0, "z1": 3.0,
    "smooth_s": 3.0,
    # 배우 -> 관객 (승인형)
    "dwell_s": 5.0, "cooldown_s": 30.0, "cand_expire_s": 45.0, "cand_leave_s": 8.0,
    "seat_pattern": "strong",
    # 관객 -> 배우 (차단형)
    "veto_s": 2.0, "pulse_gap_s": 6.0, "pulse_per_min": 6,
    "pattern_weak": "weak", "pattern_strong": "strong",
    # 관객 종합 점수 가중치(채널이 없으면 자동으로 빠지고 나머지로 다시 나눔)
    "w_hr": 1.0, "w_gsr": 1.0, "w_seat": 1.0,
    # 배우 각성 가중치: 심박 상승 + HRV(RMSSD) 저하
    "w_act_hr": 1.0, "w_act_rr": 1.0, "rr_win_s": 20.0,
    "gsr_sign": 1,
    # 큐(채널 3)
    "cue_gap_s": 30.0, "cue_max": 5,
    # 기타
    "stale_s": 5.0, "baseline_s": 120.0,
}
CFG_NOTES = {  # 화면 설정창에 쓰는 정보: 라벨, 최소, 최대, 간격
    "t_low": ("낮음/중간 경계", 0.05, 0.95, 0.01),
    "t_high": ("중간/높음 경계", 0.05, 0.99, 0.01),
    "hyst": ("히스테리시스", 0.0, 0.15, 0.01),
    "smooth_s": ("점수 평활(초)", 0.5, 15, 0.5),
    "dwell_s": ("배우 '높음' 유지(초)", 1, 30, 1),
    "cooldown_s": ("승인 후보 쿨다운(초)", 5, 120, 5),
    "cand_expire_s": ("후보 만료(초)", 10, 120, 5),
    "veto_s": ("차단 대기(초, 0=즉시)", 0, 6, 0.5),
    "pulse_gap_s": ("관객 펄스 최소 간격(초)", 0, 60, 1),
    "pulse_per_min": ("관객 펄스 분당 최대", 1, 20, 1),
    "w_hr": ("관객 가중치: 심박", 0, 3, 0.5),
    "w_gsr": ("관객 가중치: GSR", 0, 3, 0.5),
    "w_seat": ("관객 가중치: 좌석", 0, 3, 0.5),
    "w_act_hr": ("배우 가중치: 심박", 0, 3, 0.5),
    "w_act_rr": ("배우 가중치: HRV 저하", 0, 3, 0.5),
    "rr_win_s": ("배우 HRV 창(초)", 10, 60, 5),
    "cue_gap_s": ("큐 최소 간격(초)", 0, 120, 5),
    "cue_max": ("큐 최대 횟수", 1, 10, 1),
}


def clamp(x, a, b):
    return a if x < a else (b if x > b else x)


def median(xs):
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return None
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def mean(xs):
    return sum(xs) / len(xs)


def pstdev(xs):
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / len(xs))


def mmss(sec):
    if sec is None:
        return ""
    sign = "-" if sec < 0 else ""
    sec = abs(int(sec))
    return "%s%d:%02d" % (sign, sec // 60, sec % 60)


# ---------------------------------------------------------------- 대본 읽기
BEAT_RE = re.compile(
    r'^(?:#{1,6}\s*)?(.+?)\s*[(（]\s*(\d+):(\d{2})\s*[~\-–—]\s*(\d+):(\d{2})\s*[)）]\s*(?:[—–\-:]+\s*(.*))?$')
ANCHOR_RE = re.compile(r'^\[(\d+):(\d{2})\]\s*(.*)$')
TIME_RE = re.compile(r'(\d+):(\d{2})\s*[~\-–—]\s*(\d+):(\d{2})')


def parse_script(text):
    """마크다운/텍스트 대본을 구간(비트)과 본문 블록으로 나눈다.
    '### 비트 1 · 도입 (0:00~1:20) — 메모' 형식의 줄이 구간이고,
    '[1:20] 대사' 형식은 시각 앵커, '| 비트 | 시간 | 예상 배우 각성 | ...' 표는 비트 시트다."""
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks, beats, table_rows = [], [], []
    cur = None
    for raw in lines:
        s = raw.strip()
        if not s:
            continue
        if s.startswith("|"):
            table_rows.append(s)
            continue
        m = BEAT_RE.match(s)
        if m:
            t0 = int(m.group(2)) * 60 + int(m.group(3))
            t1 = int(m.group(4)) * 60 + int(m.group(5))
            beat = {"i": len(beats), "title": m.group(1).strip(), "t0": t0, "t1": t1,
                    "note": (m.group(6) or "").strip()}
            beats.append(beat)
            cur = beat["i"]
            blocks.append({"k": "beat", "text": beat["title"], "note": beat["note"], "beat": cur, "t": t0})
            continue
        t = None
        body = s
        am = ANCHOR_RE.match(s)
        if am:
            t = int(am.group(1)) * 60 + int(am.group(2))
            body = am.group(3)
        hm = re.match(r'^(#{1,6})\s+(.*)$', body)
        if hm:
            kind, body = "h", hm.group(2)
        elif body.startswith("(") or body.startswith("（"):
            kind = "dir"
        else:
            kind = "p"
        blocks.append({"k": kind, "text": body, "beat": cur, "t": t})
    _apply_sheet(table_rows, beats)
    return {"blocks": blocks, "beats": beats}


def _apply_sheet(rows, beats):
    if not rows or not beats:
        return
    cells = [[c.strip() for c in r.strip().strip("|").split("|")] for r in rows]
    head_i = None
    for i, c in enumerate(cells):
        joined = " ".join(c)
        if "비트" in joined and "시간" in joined:
            head_i = i
            break
    if head_i is None:
        return
    head = cells[head_i]

    def col(*keys):
        for j, h in enumerate(head):
            if all(k in h for k in keys):
                return j
        return None
    c_time, c_act, c_aud, c_rule = col("시간"), col("배우"), col("관객"), col("판단")
    n = 0
    for row in cells[head_i + 1:]:
        if all(re.fullmatch(r':?-{2,}:?', x or "---") for x in row):
            continue
        target = None
        if c_time is not None and c_time < len(row):
            tm = TIME_RE.search(row[c_time])
            if tm:
                t0 = int(tm.group(1)) * 60 + int(tm.group(2))
                for b in beats:
                    if b["t0"] == t0:
                        target = b
        if target is None and n < len(beats):
            target = beats[n]
        n += 1
        if target is None:
            continue

        def get(j):
            return row[j] if (j is not None and j < len(row)) else ""
        target["exp_actor"] = get(c_act)
        target["exp_aud"] = get(c_aud)
        target["rule"] = get(c_rule)


# ---------------------------------------------------------------- 기록
class SessionLog:
    def __init__(self, base, enabled=True):
        self.enabled = enabled
        self.lock = threading.Lock()
        self.files = {}
        self.dir = None
        if enabled:
            self.dir = os.path.join(base, datetime.now().strftime("%Y%m%d_%H%M%S"))
            os.makedirs(self.dir, exist_ok=True)

    def _w(self, name, header):
        if name not in self.files:
            f = open(os.path.join(self.dir, name), "w", newline="", encoding="utf-8-sig")
            w = csv.writer(f)
            w.writerow(header)
            self.files[name] = (f, w)
        return self.files[name]

    def row(self, name, header, values, flush=False):
        if not self.enabled:
            return
        with self.lock:
            f, w = self._w(name, header)
            w.writerow(values)
            if flush:
                f.flush()

    def text(self, name, content):
        if not self.enabled:
            return
        with self.lock:
            with open(os.path.join(self.dir, name), "w", encoding="utf-8") as f:
                f.write(content)

    def flush(self):
        with self.lock:
            for f, _ in self.files.values():
                f.flush()

    def close(self):
        with self.lock:
            for f, _ in self.files.values():
                try:
                    f.close()
                except Exception:
                    pass
            self.files = {}


class Bus:
    def __init__(self):
        self.lock = threading.Lock()
        self.subs = set()

    def subscribe(self):
        q = queue.Queue(maxsize=2000)
        with self.lock:
            self.subs.add(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            self.subs.discard(q)

    def publish(self, kind, data):
        msg = "event: %s\ndata: %s\n\n" % (kind, json.dumps(data, ensure_ascii=False))
        with self.lock:
            subs = list(self.subs)
        for q in subs:
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass


class Clock:
    """허브 시간(초). 데모에서 speed를 올리면 허브 안의 모든 시간 규칙이 같은 배율로 빨라진다."""

    def __init__(self, speed=1.0):
        self.speed = float(speed)
        self.t0 = time.monotonic()

    def now(self):
        return (time.monotonic() - self.t0) * self.speed


# ---------------------------------------------------------------- 장치 연결
class Link:
    kind = "none"
    stop_line = "off"

    def __init__(self, hub, party):
        self.hub = hub
        self.party = party
        self.alive = True
        self.error = None

    def start(self):
        pass

    def send(self, cid, pattern):
        return False

    def stop_all(self):
        pass

    def close(self):
        self.alive = False


class SerialLink(Link):
    kind = "serial"

    def __init__(self, hub, party, port, baud=115200):
        Link.__init__(self, hub, party)
        self.port, self.baud = port, int(baud)
        self.ser = None
        self.wlock = threading.Lock()

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        try:
            import serial  # pyserial
        except ImportError:
            self.error = "pyserial이 없습니다: pip3 install pyserial"
            print("[오류]", self.error)
            return
        while self.alive:
            try:
                self.ser = serial.Serial(self.port, self.baud, timeout=0.2)
                self.error = None
                print("[연결] %s 시리얼 %s" % (self.party, self.port))
                buf = b""
                while self.alive:
                    data = self.ser.read(256)
                    if not data:
                        continue
                    buf += data
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        self.hub.ingest(self.party, line.decode("utf-8", "ignore"))
            except Exception as e:
                self.error = str(e)
                self.ser = None
                time.sleep(2)

    def _write(self, text):
        with self.wlock:
            if self.ser is None:
                return False
            try:
                self.ser.write((text + "\n").encode())
                return True
            except Exception as e:
                self.error = str(e)
                return False

    def send(self, cid, pattern):
        return self._write("V,%s,%s" % (cid, pattern))

    def stop_all(self):
        self._write("X")


class UdpLink(Link):
    kind = "udp"

    def __init__(self, hub, party, listen_port, remote_ip=None, remote_port=4210):
        Link.__init__(self, hub, party)
        self.listen_port, self.remote_ip, self.remote_port = int(listen_port), remote_ip, int(remote_port)
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.listen_port))
        self.sock.settimeout(0.5)
        threading.Thread(target=self._run, daemon=True).start()
        print("[대기] %s UDP 포트 %d" % (self.party, self.listen_port))

    def _run(self):
        while self.alive:
            try:
                data, addr = self.sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                return
            if self.remote_ip is None:
                self.remote_ip = addr[0]
                print("[연결] %s 장치 주소 %s" % (self.party, self.remote_ip))
            for line in data.decode("utf-8", "ignore").splitlines():
                self.hub.ingest(self.party, line)

    def _tx(self, text):
        if not self.remote_ip or not self.sock:
            return False
        try:
            self.sock.sendto(text.encode(), (self.remote_ip, self.remote_port))
            return True
        except OSError as e:
            self.error = str(e)
            return False

    def send(self, cid, pattern):
        return self._tx("%s,%s" % (cid, pattern))

    def stop_all(self):
        self._tx("off")

    def close(self):
        self.alive = False
        try:
            self.sock.close()
        except Exception:
            pass


class LampLink(UdpLink):
    """채널 3 램프(장치 미정). 주소를 주면 'Q1,up' 같은 줄을 UDP로 보낸다."""
    kind = "lamp"

    def __init__(self, hub, ip, port=4211):
        UdpLink.__init__(self, hub, "lamp", 0, ip, port)

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def stop_all(self):
        self._tx("off")


def parse_hr_measurement(data):
    flags = data[0]
    if flags & 0x01:
        bpm = data[1] | (data[2] << 8)
        i = 3
    else:
        bpm = data[1]
        i = 2
    if flags & 0x08:
        i += 2
    rrs = []
    if flags & 0x10:
        while i + 1 < len(data):
            rrs.append(int((data[i] | (data[i + 1] << 8)) * 1000 // 1024))
            i += 2
    return bpm, rrs


class BleHrLink(Link):
    """노트북이 폴라에 직접 BLE로 붙는 방식(bleak 필요). 배우 보드가 와이파이와 블루투스를 같이 못 쓸 때의 대안.
    진동 명령은 보내지 못하므로 --actor-ip 와 같이 쓰려면 UdpLink 가 따로 필요하다. (실험적)"""
    kind = "ble"

    def __init__(self, hub, party, name, cmd_link=None):
        Link.__init__(self, hub, party)
        self.name = name
        self.cmd_link = cmd_link

    def start(self):
        threading.Thread(target=self._thread, daemon=True).start()

    def send(self, cid, pattern):
        return self.cmd_link.send(cid, pattern) if self.cmd_link else False

    def stop_all(self):
        if self.cmd_link:
            self.cmd_link.stop_all()

    def _thread(self):
        import asyncio
        try:
            from bleak import BleakScanner, BleakClient
        except ImportError:
            self.error = "bleak이 없습니다: pip3 install bleak"
            print("[오류]", self.error)
            return
        HR = "00002a37-0000-1000-8000-00805f9b34fb"

        async def main():
            while self.alive:
                try:
                    dev = await BleakScanner.find_device_by_filter(
                        lambda d, ad: bool(d.name) and self.name in d.name, timeout=10)
                    if dev is None:
                        continue
                    print("[연결 시도] %s" % dev.name)

                    def cb(_, data):
                        try:
                            bpm, rrs = parse_hr_measurement(bytes(data))
                        except Exception:
                            return
                        ms = int(self.hub.clock.now() * 1000)
                        self.hub.ingest(self.party, "H,%d,%d,1" % (ms, bpm))
                        for r in rrs:
                            self.hub.ingest(self.party, "R,%d,%d" % (ms, r))
                    async with BleakClient(dev) as cli:
                        await cli.start_notify(HR, cb)
                        self.error = None
                        print("[연결] %s BLE %s" % (self.party, dev.name))
                        while cli.is_connected and self.alive:
                            await asyncio.sleep(1)
                except Exception as e:
                    self.error = str(e)
                    await asyncio.sleep(2)
        asyncio.run(main())


class DemoLink(Link):
    """가짜 장치: 명령을 받으면 40~140ms 뒤에 확인(ack)을 돌려준다."""
    kind = "demo"

    def send(self, cid, pattern):
        delay = random.uniform(0.04, 0.14) / self.hub.clock.speed
        line = ("A,%s,%s" if self.party == "seat" else "ack,%s,%s") % (cid, pattern)
        threading.Timer(delay, lambda: self.hub.ingest(self.party, line)).start()
        return True


# ---------------------------------------------------------------- 허브 본체
class Hub:
    FEATS = ("act_hr", "act_rr", "aud_hr", "aud_gsr", "aud_seat")

    def __init__(self, clock, cfg, log, bus):
        self.clock, self.log, self.bus = clock, log, bus
        self.cfg = dict(DEFAULT_CFG)
        self.cfg.update(cfg or {})
        self.lock = threading.RLock()
        self.links = {}
        self.phase = "idle"
        self.baseline_t0 = None
        self.perf_t0 = None
        self.buf = {"act_hr": deque(maxlen=400), "act_rr": deque(maxlen=400), "aud_hr": deque(maxlen=400),
                    "aud_gsr": deque(maxlen=400), "aud_seat": deque(maxlen=400)}
        self.last_raw = {"act_hr": None, "act_rmssd": None, "aud_hr": None, "aud_gsr": None, "aud_front": None, "aud_back": None}
        self.last_rx = {"seat": None, "actor": None}
        self.pools = {k: deque(maxlen=2400) for k in self.FEATS}
        self.stats = {}
        self.baseline_provisional = False
        self.ema = {"aud": None, "act": None}
        self.ema_t = {"aud": None, "act": None}
        self.levels = {"aud": None, "act": None}
        self.comp = {"aud_hr": None, "aud_gsr": None, "aud_seat": None}
        self.act_comp = {"act_hr": None, "act_rr": None}
        self.last_tick = None
        self.new_pts = []
        self.hist = deque(maxlen=14400)
        self.events = deque(maxlen=3000)
        self.ev_n = 0
        self.act_high_since = None
        self.cand_made = False
        self.cooldown_until = 0.0
        self.pending_cand = None
        self.pending_pulses = []
        self.pulse_sent = deque(maxlen=50)
        self.n_cand = self.n_pulse = self.n_cmd = self.n_cue = 0
        self.cmds = {}
        self.latencies = []
        self.cue_count = 0
        self.last_cue_t = None
        self.halted = False
        self.feedback_on = True
        self.relay_on = True
        self.script = None
        self.script_manual = None
        self.bad_lines = 0

    # -------- 시간
    def pt(self, now):
        if self.perf_t0 is None or self.phase not in ("performance", "ended"):
            return None
        return now - self.perf_t0

    # -------- 이벤트 기록
    def emit(self, typ, now=None, **kw):
        now = self.clock.now() if now is None else now
        with self.lock:
            self.ev_n += 1
            ev = {"n": self.ev_n, "t": round(now, 3), "pt": None, "type": typ}
            p = self.pt(now)
            ev["pt"] = None if p is None else round(p, 3)
            ev.update(kw)
            self.events.append(ev)
        self.log.row("events.csv", ["hub_t", "perf_t", "type", "detail_json", "wall"],
                     ["%.3f" % now, "" if ev["pt"] is None else "%.3f" % ev["pt"], typ,
                      json.dumps(kw, ensure_ascii=False), datetime.now().isoformat(timespec="milliseconds")],
                     flush=True)
        self.bus.publish("event", ev)
        return ev

    # -------- 입력
    def ingest(self, party, line, t=None):
        line = line.strip()
        if not line:
            return
        if line.startswith("#"):  # 보드 상태 줄(# scan, # polar connected ...)은 터미널에만 보여준다
            print("[%s 보드] %s" % (party, line))
            return
        p = [x.strip() for x in line.split(",")]
        k = p[0]
        now = self.clock.now() if t is None else t
        try:
            if k == "S" and len(p) >= 7:
                ms, fr, bk, gs, hr, ok = (int(float(x)) for x in p[1:7])
                with self.lock:
                    self.buf["aud_seat"].append((now, fr, bk))
                    self.buf["aud_gsr"].append((now, gs))
                    self.last_raw.update(aud_front=fr, aud_back=bk, aud_gsr=gs)
                    if ok and hr > 0:
                        self.buf["aud_hr"].append((now, hr))
                        self.last_raw["aud_hr"] = hr
                    self.last_rx["seat"] = now
                self.log.row("raw_seat.csv", ["hub_t", "board_ms", "front", "back", "gsr", "hr", "hr_ok"],
                             ["%.3f" % now, ms, fr, bk, gs, hr, ok])
            elif k == "H" and len(p) >= 4:
                ms, hr, ok = int(float(p[1])), int(float(p[2])), int(float(p[3]))
                key = "act_hr" if party == "actor" else "aud_hr"
                with self.lock:
                    if ok and hr > 0:
                        self.buf[key].append((now, hr))
                        self.last_raw[key] = hr
                    self.last_rx["actor" if party == "actor" else "seat"] = now
                self.log.row("raw_%s.csv" % party, ["hub_t", "board_ms", "hr", "hr_ok"],
                             ["%.3f" % now, ms, hr, ok])
            elif k == "R" and len(p) >= 3:
                if party == "actor":
                    rr = int(float(p[2]))
                    if 300 <= rr <= 2000:
                        with self.lock:
                            self.buf["act_rr"].append((now, rr))
                self.log.row("rr_%s.csv" % party, ["hub_t", "board_ms", "rr_ms"],
                             ["%.3f" % now, int(float(p[1])), int(float(p[2]))])
            elif k == "M":
                self.emit("marker", now, src=party, label="장치 버튼")
            elif k in ("A", "ack") and len(p) >= 3:
                self._on_ack(p[1], p[2], now)
        except ValueError:
            self.bad_lines += 1

    def _on_ack(self, cid, pattern, now):
        with self.lock:
            rec = self.cmds.get(cid)
            if not rec or rec.get("rtt_ms") is not None:
                return
            rec["rtt_ms"] = round((time.monotonic() - rec["real"]) * 1000.0)
            self.latencies.append(rec["rtt_ms"])
        self.emit("ack", now, id=cid, target=rec["target"], pattern=pattern, rtt_ms=rec["rtt_ms"])

    # -------- 특징값과 점수
    def _recent(self, key, now, win):
        return [x for x in self.buf[key] if now - x[0] <= win]

    def _features(self, now):
        sign = 1 if self.cfg["gsr_sign"] >= 0 else -1
        stale = self.cfg["stale_s"]
        f = {k: None for k in self.FEATS}
        seat_fresh = self.last_rx["seat"] is not None and now - self.last_rx["seat"] <= stale
        act_fresh = self.last_rx["actor"] is not None and now - self.last_rx["actor"] <= stale
        if act_fresh:
            v = [x[1] for x in self._recent("act_hr", now, 5.0)]
            f["act_hr"] = median(v) if v else None
            f["act_rr"] = self._act_hrv(now)
        if seat_fresh:
            v = [x[1] for x in self._recent("aud_hr", now, 5.0)]
            f["aud_hr"] = median(v) if v else None
            v = [x[1] for x in self._recent("aud_gsr", now, 5.0)]
            f["aud_gsr"] = sign * median(v) if v else None
            rows = self._recent("aud_seat", now, 3.0)
            if len(rows) >= 5:
                e = [abs(rows[i + 1][1] - rows[i][1]) + abs(rows[i + 1][2] - rows[i][2]) for i in range(len(rows) - 1)]
                f["aud_seat"] = mean(e)
        return f

    def _act_hrv(self, now):
        """최근 rr_win_s 초의 RMSSD를 부호 반대로 돌려준다(HRV가 떨어질수록 값이 커져 각성으로 읽힘).
        중앙값에서 25% 넘게 벗어난 RR은 잡음으로 보고 뺀다. 폴라 알림 누락(6~7%)은 이웃 차이만 건너뛴다."""
        rows = self._recent("act_rr", now, self.cfg["rr_win_s"])
        if len(rows) < 8:
            self.last_raw["act_rmssd"] = None
            return None
        med = median([r[1] for r in rows])
        ok = [r for r in rows if abs(r[1] - med) <= 0.25 * med]
        d2 = []
        for a, b in zip(ok, ok[1:]):
            if b[0] - a[0] <= 3.0:  # 알림 누락으로 간격이 벌어지면 그 차이는 안 씀
                d2.append((b[1] - a[1]) ** 2)
        if len(d2) < 6:
            self.last_raw["act_rmssd"] = None
            return None
        rmssd = math.sqrt(sum(d2) / len(d2))
        self.last_raw["act_rmssd"] = round(rmssd, 1)
        return -rmssd

    def _compute_stats(self, provisional):
        floors = {"act_hr": lambda mu: 3.0, "act_rr": lambda mu: max(0.15 * abs(mu), 3.0), "aud_hr": lambda mu: 3.0,
                  "aud_gsr": lambda mu: max(0.02 * abs(mu), 5.0), "aud_seat": lambda mu: max(0.5 * mu, 2.0)}
        stats = {}
        for k, vals in self.pools.items():
            vals = list(vals)
            if len(vals) >= 20:
                mu = mean(vals)
                stats[k] = (mu, max(pstdev(vals), floors[k](mu)))
        self.stats = stats
        self.baseline_provisional = provisional

    def _score(self, key, x):
        st = self.stats.get(key)
        if st is None or x is None:
            return None
        z = (x - st[0]) / st[1]
        return clamp((z - self.cfg["z0"]) / (self.cfg["z1"] - self.cfg["z0"]), 0.0, 1.0)

    @staticmethod
    def _next_level(prev, s, lo, hi, h):
        if prev is None:
            return "high" if s >= hi else ("mid" if s >= lo else "low")
        if prev == "low":
            if s >= hi + h:
                return "high"
            if s >= lo + h:
                return "mid"
        elif prev == "mid":
            if s >= hi + h:
                return "high"
            if s < lo - h:
                return "low"
        else:
            if s < lo - h:
                return "low"
            if s < hi - h:
                return "mid"
        return prev

    def _smooth(self, key, raw, now, dt):
        if raw is None:
            if self.ema_t[key] is not None and now - self.ema_t[key] > self.cfg["stale_s"]:
                self.ema[key] = None
            return
        prev = self.ema[key]
        a = 1.0 - math.exp(-max(dt, 0.01) / max(self.cfg["smooth_s"], 0.1))
        self.ema[key] = raw if prev is None else prev + a * (raw - prev)
        self.ema_t[key] = now

    # -------- 한 번 계산
    def tick(self, now=None):
        with self.lock:
            now = self.clock.now() if now is None else now
            dt = 0.25 if self.last_tick is None else now - self.last_tick
            self.last_tick = now
            feats = self._features(now)
            if self.phase in ("idle", "baseline"):
                for k, v in feats.items():
                    if v is not None:
                        self.pools[k].append(v)
            sc = {k: self._score(k, v) for k, v in feats.items()}
            self.comp = {k: sc[k] for k in ("aud_hr", "aud_gsr", "aud_seat")}
            wts = {"aud_hr": self.cfg["w_hr"], "aud_gsr": self.cfg["w_gsr"], "aud_seat": self.cfg["w_seat"]}
            num = den = 0.0
            for k, s in self.comp.items():
                if s is not None and wts[k] > 0:
                    num += wts[k] * s
                    den += wts[k]
            self._smooth("aud", num / den if den > 0 else None, now, dt)
            aw = {"act_hr": self.cfg["w_act_hr"], "act_rr": self.cfg["w_act_rr"]}
            self.act_comp = {k: sc[k] for k in aw}
            anum = aden = 0.0
            for k, w in aw.items():
                if sc[k] is not None and w > 0:
                    anum += w * sc[k]
                    aden += w
            self._smooth("act", anum / aden if aden > 0 else None, now, dt)
            prev = dict(self.levels)
            if self.phase in ("ready", "performance"):
                for key in ("aud", "act"):
                    s = self.ema[key]
                    if s is None:
                        self.levels[key] = None
                    else:
                        self.levels[key] = self._next_level(self.levels[key], s, self.cfg["t_low"],
                                                            self.cfg["t_high"], self.cfg["hyst"])
                    if prev[key] != self.levels[key] and self.levels[key] is not None and prev[key] is not None:
                        self.emit("level", now, party=key, frm=prev[key], to=self.levels[key],
                                  score=round(self.ema[key], 3))
                if self.phase == "performance":
                    self._rules(now, prev)
            pt = {"t": round(now, 2), "pt": None if self.pt(now) is None else round(self.pt(now), 2),
                  "act": _r(self.ema["act"]), "aud": _r(self.ema["aud"]),
                  "aud_hr": _r(self.comp["aud_hr"]), "aud_gsr": _r(self.comp["aud_gsr"]),
                  "aud_seat": _r(self.comp["aud_seat"]),
                  "act_s_hr": _r(self.act_comp["act_hr"]), "act_s_rr": _r(self.act_comp["act_rr"]),
                  "act_lvl": self.levels["act"], "aud_lvl": self.levels["aud"]}
            self.new_pts.append(pt)
            self.hist.append(pt)
            if int(now * 4) % 4 == 0:
                lr = self.last_raw
                self.log.row("signals.csv", ["hub_t", "perf_t", "phase", "act_hr", "act_score", "act_level", "aud_hr",
                                             "aud_gsr", "aud_front", "aud_back", "aud_score", "aud_level", "s_hr",
                                             "s_gsr", "s_seat", "act_rmssd", "act_s_hr", "act_s_rr"],
                             ["%.2f" % now, "" if pt["pt"] is None else pt["pt"], self.phase, lr["act_hr"], pt["act"],
                              self.levels["act"], lr["aud_hr"], lr["aud_gsr"], lr["aud_front"], lr["aud_back"],
                              pt["aud"], self.levels["aud"], pt["aud_hr"], pt["aud_gsr"], pt["aud_seat"], lr["act_rmssd"], pt["act_s_hr"],
                              pt["act_s_rr"]])

    # -------- 규칙
    def _rules(self, now, prev):
        c = self.cfg
        order = {"low": 0, "mid": 1, "high": 2}
        pa, na = prev["aud"], self.levels["aud"]
        if pa and na and order[na] > order[pa]:
            self._make_pulse(now, "weak" if na == "mid" else "strong", pa, na)
        # 배우 높음 유지 -> 승인 후보
        lvl = self.levels["act"]
        if lvl == "high":
            if self.act_high_since is None:
                self.act_high_since = now
                self.cand_made = False
            if (not self.cand_made and now - self.act_high_since >= c["dwell_s"]
                    and self.pending_cand is None and now >= self.cooldown_until):
                self._make_candidate(now)
        else:
            self.act_high_since = None
        # 후보 만료
        pc = self.pending_cand
        if pc:
            pc["left_since"] = (pc["left_since"] or now) if lvl != "high" else None
            if now > pc["expires"] or (pc["left_since"] and now - pc["left_since"] >= c["cand_leave_s"]):
                self._close_cand("expired", now)
        # 차단 대기 중인 펄스 전송
        for p in list(self.pending_pulses):
            if now >= p["due"]:
                self._send_pulse(p, now, "대기 시간 경과")

    def _blocked_reason(self):
        if self.halted:
            return "halt"
        if not self.feedback_on:
            return "off"
        return None

    def _make_pulse(self, now, kind, frm, to):
        c = self.cfg
        self.n_pulse += 1
        p = {"id": "P%d" % self.n_pulse, "kind": kind, "frm": frm, "to": to, "t": now,
             "due": now + c["veto_s"], "score": _r(self.ema["aud"])}
        why = self._blocked_reason()
        if why:
            self.emit("pulse_suppressed", now, id=p["id"], kind=kind, frm=frm, to=to, reason=why)
            return
        if not self.relay_on:
            self.emit("pulse_suppressed", now, id=p["id"], kind=kind, frm=frm, to=to, reason="relay_off")
            return
        if self.pulse_sent and now - self.pulse_sent[-1] < c["pulse_gap_s"]:
            self.emit("pulse_skipped", now, id=p["id"], kind=kind, frm=frm, to=to, reason="gap")
            return
        if sum(1 for t in self.pulse_sent if now - t < 60.0) >= c["pulse_per_min"]:
            self.emit("pulse_skipped", now, id=p["id"], kind=kind, frm=frm, to=to, reason="rate")
            return
        self.emit("pulse_pending", now, id=p["id"], kind=kind, frm=frm, to=to, score=p["score"])
        if c["veto_s"] <= 0:
            self._send_pulse(p, now, "즉시")
        else:
            self.pending_pulses.append(p)

    def _send_pulse(self, p, now, why):
        if p in self.pending_pulses:
            self.pending_pulses.remove(p)
        pat = self.cfg["pattern_weak"] if p["kind"] == "weak" else self.cfg["pattern_strong"]
        self.pulse_sent.append(now)
        self.emit("pulse_sent", now, id=p["id"], kind=p["kind"], frm=p["frm"], to=p["to"], how=why)
        self.send_command("actor", pat, "pulse", p["id"], now)

    def _make_candidate(self, now):
        c = self.cfg
        self.cand_made = True
        self.n_cand += 1
        cid = "C%d" % self.n_cand
        why = self._blocked_reason()
        if why:
            self.emit("cand_suppressed", now, id=cid, reason=why, score=_r(self.ema["act"]))
            self.cooldown_until = now + c["cooldown_s"]
            return
        self.pending_cand = {"id": cid, "t": now, "expires": now + c["cand_expire_s"], "left_since": None,
                             "score": _r(self.ema["act"])}
        bi = self._beat_info(now)
        self.emit("cand_created", now, id=cid, score=_r(self.ema["act"]),
                  beat=None if not bi else bi.get("title"))

    def _close_cand(self, status, now, **kw):
        pc = self.pending_cand
        if not pc:
            return
        self.pending_cand = None
        self.cooldown_until = now + self.cfg["cooldown_s"]
        self.emit("cand_" + status, now, id=pc["id"], **kw)

    # -------- 명령 전송
    def send_command(self, target, pattern, cause, ref, now=None):
        now = self.clock.now() if now is None else now
        with self.lock:
            self.n_cmd += 1
            cid = "V%d" % self.n_cmd
            link = self.links.get(target)
            ok = bool(link and link.send(cid, pattern))
            self.cmds[cid] = {"id": cid, "target": target, "pattern": pattern, "real": time.monotonic(),
                              "rtt_ms": None, "sent": ok}
        self.emit("cmd", now, id=cid, target=target, pattern=pattern, cause=cause, ref=ref, delivered=ok)
        return cid, ok

    # -------- 스탭 조작
    def act_phase(self, action):
        with self.lock:
            now = self.clock.now()
            if action == "baseline_start":
                for d in self.pools.values():
                    d.clear()
                self.stats, self.levels = {}, {"aud": None, "act": None}
                self.ema = {"aud": None, "act": None}
                self.phase, self.baseline_t0 = "baseline", now
                self.perf_t0 = None
            elif action == "baseline_end":
                if self.phase != "baseline":
                    return {"ok": False, "msg": "기준선 중이 아닙니다"}
                self._compute_stats(False)
                self.phase = "ready"
            elif action == "perf_start":
                if self.phase == "performance":
                    return {"ok": False, "msg": "이미 공연 중입니다"}
                if not self.stats:
                    self._compute_stats(True)
                if not self.stats:
                    return {"ok": False, "msg": "기준선 값이 없습니다. 센서 신호부터 확인하세요"}
                self.phase, self.perf_t0 = "performance", now
                self.act_high_since, self.cand_made = None, False
                self.cooldown_until = 0.0
                self.cue_count, self.last_cue_t = 0, None
                self.pending_cand, self.pending_pulses = None, []
                self.pulse_sent.clear()
            elif action == "perf_end":
                self.phase = "ended"
                self.pending_cand, self.pending_pulses = None, []
                for l in set(self.links.values()):
                    l.stop_all()
            else:
                return {"ok": False, "msg": "알 수 없는 동작"}
            self.emit("phase", now, action=action, provisional=self.baseline_provisional,
                      stats={k: [round(v[0], 2), round(v[1], 2)] for k, v in self.stats.items()})
            return {"ok": True}

    def act_candidate(self, cid, action, reason=""):
        with self.lock:
            now = self.clock.now()
            pc = self.pending_cand
            if not pc or pc["id"] != cid:
                return {"ok": False, "msg": "이미 처리됐거나 만료된 후보입니다"}
            if action == "approve":
                if self.halted:
                    return {"ok": False, "msg": "전체 정지 상태입니다"}
                _, ok = self.send_command("seat", self.cfg["seat_pattern"], "approved", cid, now)
                self._close_cand("approved", now, delivered=ok, latency_from_created=round(now - pc["t"], 2))
                return {"ok": True, "delivered": ok}
            if action == "reject":
                self._close_cand("rejected", now, reason=reason or "기타")
                return {"ok": True}
        return {"ok": False, "msg": "알 수 없는 동작"}

    def act_pulse(self, pid, action, tag=""):
        with self.lock:
            now = self.clock.now()
            p = next((x for x in self.pending_pulses if x["id"] == pid), None)
            if not p:
                return {"ok": False, "msg": "이미 전송됐거나 처리된 펄스입니다"}
            if action == "block":
                self.pending_pulses.remove(p)
                self.emit("pulse_blocked", now, id=pid, kind=p["kind"], tag=tag or "기타")
                return {"ok": True}
            if action == "send_now":
                self._send_pulse(p, now, "스탭 즉시 전송")
                return {"ok": True}
        return {"ok": False, "msg": "알 수 없는 동작"}

    def act_cue(self, cue, reason):
        with self.lock:
            now = self.clock.now()
            c = self.cfg
            if cue not in ("up", "down", "hold"):
                return {"ok": False, "msg": "알 수 없는 큐"}
            if self.phase != "performance":
                return {"ok": False, "msg": "공연 중에만 큐를 보낼 수 있습니다"}
            if self.halted:
                return {"ok": False, "msg": "전체 정지 상태입니다"}
            if self.cue_count >= c["cue_max"]:
                return {"ok": False, "msg": "큐 최대 횟수(%d)에 도달했습니다" % c["cue_max"]}
            if self.last_cue_t is not None and now - self.last_cue_t < c["cue_gap_s"]:
                return {"ok": False, "msg": "큐 간격이 짧습니다 (%d초 남음)" % math.ceil(c["cue_gap_s"] - (now - self.last_cue_t))}
            self.cue_count += 1
            self.n_cue += 1
            self.last_cue_t = now
            qid = "Q%d" % self.n_cue
            lamp = self.links.get("lamp")
            ok = bool(lamp and lamp.send(qid, cue))
            self.emit("cue", now, id=qid, cue=cue, reason=reason or "장면 맥락", n=self.cue_count, delivered=ok)
            return {"ok": True, "delivered": ok, "n": self.cue_count}

    def act_marker(self, label=""):
        self.emit("marker", None, src="staff", label=label or "박수 마커")
        return {"ok": True}

    def act_halt(self, on):
        with self.lock:
            now = self.clock.now()
            self.halted = bool(on)
            if self.halted:
                self.pending_pulses = []
                if self.pending_cand:
                    self._close_cand("cancelled", now)
                for l in set(self.links.values()):
                    l.stop_all()
            self.emit("halt", now, on=self.halted)
            return {"ok": True}

    def act_condition(self, on):
        with self.lock:
            self.feedback_on = bool(on)
            if not self.feedback_on:
                self.pending_pulses = []
                if self.pending_cand:
                    self._close_cand("cancelled", self.clock.now())
            self.emit("condition", None, feedback="ON" if self.feedback_on else "OFF")
        return {"ok": True}

    def act_relay(self, on):
        with self.lock:
            self.relay_on = bool(on)
            if not self.relay_on:
                self.pending_pulses = []
            self.emit("relay", None, on=self.relay_on)
        return {"ok": True}

    def act_config(self, d):
        with self.lock:
            changed = {}
            old_sign = 1 if self.cfg["gsr_sign"] >= 0 else -1
            for k, v in d.items():
                if k not in DEFAULT_CFG:
                    continue
                try:
                    v = type(DEFAULT_CFG[k])(v)
                except (TypeError, ValueError):
                    continue
                if self.cfg[k] != v:
                    self.cfg[k] = v
                    changed[k] = v
            if self.cfg["t_low"] >= self.cfg["t_high"]:
                self.cfg["t_low"], self.cfg["t_high"] = DEFAULT_CFG["t_low"], DEFAULT_CFG["t_high"]
                changed["t_low"], changed["t_high"] = self.cfg["t_low"], self.cfg["t_high"]
            new_sign = 1 if self.cfg["gsr_sign"] >= 0 else -1
            if new_sign != old_sign:
                if "aud_gsr" in self.stats:
                    mu, sd = self.stats["aud_gsr"]
                    self.stats["aud_gsr"] = (-mu, sd)
                self.pools["aud_gsr"] = deque((-v for v in self.pools["aud_gsr"]), maxlen=2400)
            if changed:
                self.emit("config", None, changed=changed)
        return {"ok": True, "changed": changed}

    def act_script(self, name, text):
        parsed = parse_script(text)
        with self.lock:
            self.script = {"name": name or "대본", "blocks": parsed["blocks"], "beats": parsed["beats"]}
            self.script_manual = None
        self.log.text("script.md", text)
        self.emit("script", None, name=name, beats=len(parsed["beats"]), blocks=len(parsed["blocks"]))
        self.bus.publish("script", self.script)
        return {"ok": True, "beats": len(parsed["beats"])}

    def act_script_pos(self, beat):
        with self.lock:
            self.script_manual = None if beat is None else int(beat)
        return {"ok": True}

    # -------- 화면용
    def _cur_beat_idx(self, now):
        if not self.script or not self.script["beats"]:
            return None
        if self.script_manual is not None:
            return self.script_manual
        p = self.pt(now)
        if p is None:
            return None
        beats = self.script["beats"]
        for b in beats:
            if b["t0"] <= p < b["t1"]:
                return b["i"]
        return beats[-1]["i"] if p >= beats[-1]["t1"] else None

    def _beat_info(self, now):
        i = self._cur_beat_idx(now)
        if i is None:
            return None
        return self.script["beats"][i]

    def snapshot(self):
        with self.lock:
            return {"version": VERSION, "cfg": self.cfg, "notes": CFG_NOTES, "script": self.script,
                    "hist": list(self.hist)[-6000:], "events": list(self.events)[-600:],
                    "speed": self.clock.speed}

    def frame(self, now=None):
        with self.lock:
            now = self.clock.now() if now is None else now
            pts, self.new_pts = self.new_pts, []
            cand = None
            if self.pending_cand:
                pc = self.pending_cand
                cand = {"id": pc["id"], "score": pc["score"], "left": round(max(0.0, pc["expires"] - now), 1),
                        "total": self.cfg["cand_expire_s"]}
            pulses = [{"id": p["id"], "kind": p["kind"], "frm": p["frm"], "to": p["to"],
                       "left": round(max(0.0, p["due"] - now), 2), "total": self.cfg["veto_s"]}
                      for p in self.pending_pulses]
            links = {}
            for name in ("seat", "actor", "lamp"):
                l = self.links.get(name)
                rx = self.last_rx.get(name)
                links[name] = {"kind": l.kind if l else "none", "error": l.error if l else None,
                               "age": None if rx is None else round(now - rx, 1)}
            cue_wait = 0.0
            if self.last_cue_t is not None:
                cue_wait = max(0.0, self.cfg["cue_gap_s"] - (now - self.last_cue_t))
            lat = self.latencies[-30:]
            bi = self._cur_beat_idx(now)
            return {"t": round(now, 2), "speed": self.clock.speed, "phase": self.phase,
                    "pt": None if self.pt(now) is None else round(self.pt(now), 2),
                    "base_t": None if self.baseline_t0 is None or self.phase != "baseline" else round(now - self.baseline_t0, 1),
                    "provisional": self.baseline_provisional,
                    "pts": pts, "cand": cand, "pulses": pulses, "links": links,
                    "levels": dict(self.levels),
                    "scores": {"aud": _r(self.ema["aud"]), "act": _r(self.ema["act"])},
                    "raw": dict(self.last_raw),
                    "stats_ok": sorted(self.stats.keys()),
                    "cues": {"n": self.cue_count, "max": self.cfg["cue_max"], "wait": round(cue_wait, 1)},
                    "halted": self.halted, "feedback": self.feedback_on, "relay": self.relay_on,
                    "beat": bi, "manual": self.script_manual is not None,
                    "lat": {"n": len(self.latencies), "median": median(lat) if lat else None,
                            "max": max(lat) if lat else None},
                    "bad": self.bad_lines}


def _r(x):
    return None if x is None else round(x, 3)


# ---------------------------------------------------------------- 데모 신호
ACT_CURVE = [.55, .72, .55, .40, .35, .35, .38, .42, .50, .58, .66, .74, .82, .90, .95, .90, .85, .68, .62, .50,
             .42, .38, .40, .45, .42]
AUD_CURVE = [.30, .30, .32, .30, .28, .30, .30, .36, .38, .46, .50, .58, .66, .74, .80, .85, .80, .72, .62, .52,
             .42, .38, .40, .44, .38]  # 1:00의 좌석 튐(.62)은 아래에서 좌석에만 따로 넣는다


def curve(arr, t):
    x = clamp(t / 10.0, 0, len(arr) - 1)
    i = int(x)
    j = min(i + 1, len(arr) - 1)
    return arr[i] + (arr[j] - arr[i]) * (x - i)


class DemoSource(threading.Thread):
    """부록 B의 가상 각성 곡선으로 원신호(심박, GSR, 좌석 압력)를 만들어 같은 입력 줄 형식으로 넣는다."""

    def __init__(self, hub):
        threading.Thread.__init__(self, daemon=True)
        self.hub = hub
        self.alive = True
        self.mu = {"act": 72.0, "hr": 78.0, "gsr": 1800.0, "seat": 3.0}
        self.row = 0
        self.hr_a = self.mu["act"]
        self.hr_u = self.mu["hr"]
        self.last_hr_t = -9

    def run(self):
        h = self.hub
        while self.alive:
            now = h.clock.now()
            pt = h.pt(now) if h.phase == "performance" else None
            c = h.cfg
            span = c["z1"] - c["z0"]

            def z(s):
                return s * span + c["z0"]
            if pt is None:
                sa = sb = sg = sm = 0.25
            else:
                sa = curve(ACT_CURVE, pt)
                sb = curve(AUD_CURVE, pt)
                sg = curve(AUD_CURVE, max(0.0, pt - 5.0))
                sm = sb
                if 57 <= pt <= 66:
                    sm = 1.0
            ms = int(now * 1000)
            if now - self.last_hr_t >= 1.0:
                self.last_hr_t = now
                self.hr_a = self.mu["act"] + z(sa) * 3.0 + random.gauss(0, 0.8)
                self.hr_u = self.mu["hr"] + z(sb) * 3.0 + random.gauss(0, 0.8)
                h.ingest("actor", "H,%d,%d,1" % (ms, round(self.hr_a)), now)
                h.ingest("actor", "R,%d,%d" % (ms, 60000 / self.hr_a * random.uniform(0.97, 1.03)), now)
                h.ingest("seat", "R,%d,%d" % (ms, 60000 / self.hr_u * random.uniform(0.97, 1.03)), now)
            gsr = self.mu["gsr"] + z(sg) * 36.0 + random.gauss(0, 6)
            e = max(0.2, self.mu["seat"] + z(sm) * 1.5 + random.gauss(0, 0.3))
            sgn = 1 if self.row % 2 == 0 else -1
            self.row += 1
            fr = 2400 + sgn * e / 4 * random.uniform(0.7, 1.3)
            bk = 2000 + sgn * e / 4 * random.uniform(0.7, 1.3)
            h.ingest("seat", "S,%d,%d,%d,%d,%d,1" % (ms, round(fr), round(bk), round(gsr), round(self.hr_u)), now)
            time.sleep(0.1 / h.clock.speed)


# ---------------------------------------------------------------- 웹 서버
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    hub = None
    bus = None

    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            try:
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except OSError:
                self._send(500, "dashboard.html 파일이 없습니다")
        elif path == "/sample_script.md":
            try:
                with open(os.path.join(HERE, "sample_script.md"), "rb") as f:
                    self._send(200, f.read(), "text/plain; charset=utf-8")
            except OSError:
                self._send(404, "예시 대본이 없습니다")
        elif path == "/api/state":
            self._send(200, json.dumps(self.hub.frame(), ensure_ascii=False))
        elif path == "/events":
            self._sse()
        else:
            self._send(404, "not found", "text/plain; charset=utf-8")

    def _sse(self):
        q = self.bus.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            snap = "event: snap\ndata: %s\n\n" % json.dumps(self.hub.snapshot(), ensure_ascii=False)
            self.wfile.write(snap.encode("utf-8"))
            self.wfile.flush()
            while True:
                try:
                    msg = q.get(timeout=10)
                except queue.Empty:
                    msg = ": keepalive\n\n"
                self.wfile.write(msg.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            self.bus.unsubscribe(q)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        try:
            d = json.loads(self.rfile.read(n).decode("utf-8") or "{}")
        except ValueError:
            return self._send(400, json.dumps({"ok": False, "msg": "JSON 형식 오류"}))
        h = self.hub
        p = self.path
        try:
            if p == "/api/phase":
                r = h.act_phase(d.get("action"))
            elif p == "/api/candidate":
                r = h.act_candidate(d.get("id"), d.get("action"), d.get("reason", ""))
            elif p == "/api/pulse":
                r = h.act_pulse(d.get("id"), d.get("action"), d.get("tag", ""))
            elif p == "/api/cue":
                r = h.act_cue(d.get("cue"), d.get("reason", ""))
            elif p == "/api/marker":
                r = h.act_marker(d.get("label", ""))
            elif p == "/api/halt":
                r = h.act_halt(d.get("on"))
            elif p == "/api/condition":
                r = h.act_condition(d.get("on"))
            elif p == "/api/relay":
                r = h.act_relay(d.get("on"))
            elif p == "/api/config":
                r = h.act_config(d.get("cfg", {}))
            elif p == "/api/script":
                r = h.act_script(d.get("name", ""), d.get("text", ""))
            elif p == "/api/script_pos":
                r = h.act_script_pos(d.get("beat"))
            else:
                return self._send(404, json.dumps({"ok": False, "msg": "없는 주소"}))
        except Exception as e:  # 화면이 멈추지 않게 오류를 돌려준다
            r = {"ok": False, "msg": "내부 오류: %s" % e}
        self._send(200, json.dumps(r, ensure_ascii=False))


def engine(hub, bus, stop):
    """계산(허브 시간 0.25초마다)과 화면 전송(실시간 0.1초 이상 간격)."""
    last_pub = 0.0
    last_flush = time.monotonic()
    while not stop.is_set():
        hub.tick()
        r = time.monotonic()
        if r - last_pub >= 0.1:
            bus.publish("frame", hub.frame())
            last_pub = r
        if r - last_flush > 3:
            hub.log.flush()
            last_flush = r
        time.sleep(0.25 / hub.clock.speed)


def make_link(spec, hub, party, args):
    kind, _, arg = spec.partition(":")
    if kind == "none" or not spec:
        return None
    if kind == "serial":
        port, _, baud = arg.partition(",")
        return SerialLink(hub, party, port, baud or 115200)
    if kind == "udp":
        ip = args.actor_ip if party == "actor" else args.seat_ip
        return UdpLink(hub, party, int(arg or (5006 if party == "actor" else 5005)), ip, args.device_port)
    if kind == "ble":
        cmd = UdpLink(hub, party + "_cmd", 0, args.actor_ip, args.device_port) if args.actor_ip else None
        if cmd:
            cmd.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return BleHrLink(hub, party, arg or "Polar", cmd)
    raise SystemExit("알 수 없는 연결 방식: " + spec)


def main():
    ap = argparse.ArgumentParser(description="스탭 허브 — 배우·관객·스탭 3자 정서 피드백 루프")
    ap.add_argument("--demo", action="store_true", help="가짜 신호와 가짜 장치로 실행 (장비 없이 화면 확인)")
    ap.add_argument("--speed", type=float, default=1.0, help="데모 속도 배율 (예: 6)")
    ap.add_argument("--autoplay", action="store_true", help="데모에서 기준선과 공연 시작을 자동으로")
    ap.add_argument("--seat", default="none", help="serial:PORT[,BAUD] | udp[:PORT]")
    ap.add_argument("--actor", default="none", help="udp[:PORT] | serial:PORT[,BAUD] | ble:폴라이름")
    ap.add_argument("--seat-ip", default=None)
    ap.add_argument("--actor-ip", default=None, help="배우 보드 IP (없으면 처음 받은 패킷에서 알아냄)")
    ap.add_argument("--device-port", type=int, default=4210, help="장치가 명령을 받는 UDP 포트")
    ap.add_argument("--lamp-ip", default=None, help="큐 램프 장치 IP[:PORT] (미정이면 생략, 로그만 남김)")
    ap.add_argument("--script", default=None, help="시작할 때 불러올 대본 파일")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--logdir", default=os.path.join(HERE, "logs"))
    ap.add_argument("--no-log", action="store_true")
    ap.add_argument("--list-ports", action="store_true")
    args = ap.parse_args()

    if args.list_ports:
        try:
            from serial.tools import list_ports
            for p in list_ports.comports():
                print(p.device, "-", p.description)
        except ImportError:
            print("pyserial이 필요합니다: pip3 install pyserial")
        return

    clock = Clock(args.speed if args.demo else 1.0)
    log = SessionLog(args.logdir, not args.no_log)
    bus = Bus()
    hub = Hub(clock, {}, log, bus)
    stop = threading.Event()

    if args.demo:
        hub.links["seat"], hub.links["actor"] = DemoLink(hub, "seat"), DemoLink(hub, "actor")
        DemoSource(hub).start()
    else:
        for party, spec in (("seat", args.seat), ("actor", args.actor)):
            l = make_link(spec, hub, party, args)
            if l:
                hub.links[party] = l
                l.start()
    if args.lamp_ip:
        ip, _, port = args.lamp_ip.partition(":")
        lamp = LampLink(hub, ip, int(port or 4211))
        lamp.start()
        hub.links["lamp"] = lamp
    if args.script:
        with open(args.script, encoding="utf-8") as f:
            hub.act_script(os.path.basename(args.script), f.read())
    hub.emit("start", None, version=VERSION, demo=args.demo, speed=clock.speed,
             seat=args.seat if not args.demo else "demo", actor=args.actor if not args.demo else "demo")

    threading.Thread(target=engine, args=(hub, bus, stop), daemon=True).start()

    if args.autoplay and args.demo:
        def auto():
            time.sleep(1.5)
            hub.act_phase("baseline_start")
            time.sleep(hub.cfg["baseline_s"] / clock.speed)
            hub.act_phase("baseline_end")
            time.sleep(1.0)
            hub.act_phase("perf_start")
        threading.Thread(target=auto, daemon=True).start()

    Handler.hub, Handler.bus = hub, bus
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    srv.daemon_threads = True
    print("스탭 허브 %s  http://%s:%d  (기록: %s)" % (VERSION, "localhost" if args.host == "127.0.0.1" else args.host,
                                                   args.port, log.dir or "없음"))
    if args.host != "127.0.0.1":
        print("[주의] 이 주소는 같은 네트워크의 누구나 진동을 조작할 수 있습니다. 공연 장소의 공용 와이파이에서는 쓰지 마세요.")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        for l in set(hub.links.values()):
            try:
                l.stop_all()
                l.close()
            except Exception:
                pass
        hub.emit("stop", None)
        log.close()
        print("종료. 기록 폴더:", log.dir)


if __name__ == "__main__":
    main()
