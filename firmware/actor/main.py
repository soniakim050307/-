# 배우 보드 (ESP32 DevKitC V4, MicroPython + aioble) — USB 시리얼로 허브와 통신
# 보드 -> 허브: H,ms,hr,ok / R,ms,rr_ms / M / ack,id,pattern   (115200, 줄 단위)
# 허브 -> 보드: V,id,pattern (weak|strong) / X (전부 끄기)
# 주의: import network / WLAN 을 넣지 말 것(BLE 연결에서 멈춘 적 있음).
from machine import Pin
VIB = Pin(4, Pin.OUT, value=0)  # 가장 먼저 OFF (S-GND 10k 풀다운도 달 것)

import sys, select, time
import uasyncio as asyncio
import bluetooth
import aioble

TARGET_ID = "1E0A803C"      # Polar H9. 광고 이름에 이 ID가 들어 있으면 연결
HR_SVC, HR_CHR = bluetooth.UUID(0x180D), bluetooth.UUID(0x2A37)
MAX_ON_MS = 1500            # 진동 1회 최대 ON 시간(안전 상한)
PATTERNS = {                # (켬 ms, 끔 ms) 목록
    "weak": ((250, 0),),
    "strong": ((400, 150), (400, 150), (400, 0)),
}

btn = Pin(0, Pin.IN, Pin.PULL_UP)
vib_task = None


def send(line):
    print(line)


def ms():
    return time.ticks_ms()


def vib_off():
    VIB.value(0)


async def run_pattern(pat):
    try:
        for on, off in PATTERNS[pat]:
            VIB.value(1)
            await asyncio.sleep_ms(min(on, MAX_ON_MS))
            VIB.value(0)
            if off:
                await asyncio.sleep_ms(off)
    finally:
        VIB.value(0)


def stop_vib():
    global vib_task
    if vib_task is not None:
        vib_task.cancel()
        vib_task = None
    vib_off()


def start_vib(pat):
    global vib_task
    stop_vib()
    vib_task = asyncio.create_task(run_pattern(pat))


def handle(line):
    p = line.strip().split(",")
    if p[0] == "X":
        stop_vib()                      # 정지가 항상 먼저
        send("ack,X,off")
    elif p[0] == "V" and len(p) >= 3 and p[2] in PATTERNS:
        send("ack,%s,%s" % (p[1], p[2]))  # 확인을 먼저 보냄(왕복 지연 측정)
        start_vib(p[2])


async def cmd_task():
    poll = select.poll()
    poll.register(sys.stdin, select.POLLIN)
    buf = ""
    while True:
        while poll.poll(0):
            ch = sys.stdin.read(1)
            if ch in ("\n", "\r"):
                if buf:
                    handle(buf)
                buf = ""
            else:
                buf += ch
                if len(buf) > 64:
                    buf = ""
        await asyncio.sleep_ms(10)


async def button_task():
    prev = 1
    while True:
        v = btn.value()
        if prev == 1 and v == 0:
            send("M")
            await asyncio.sleep_ms(200)  # 디바운스
        prev = v
        await asyncio.sleep_ms(20)


def parse_hr(data):
    flags = data[0]
    if flags & 0x01:
        bpm, i = data[1] | (data[2] << 8), 3
    else:
        bpm, i = data[1], 2
    if flags & 0x08:
        i += 2
    rrs = []
    if flags & 0x10:
        while i + 1 < len(data):
            rrs.append((data[i] | (data[i + 1] << 8)) * 1000 // 1024)
            i += 2
    return bpm, rrs


async def find_polar():
    async with aioble.scan(5000, interval_us=30000, window_us=30000, active=True) as sc:
        async for r in sc:
            try:
                name = r.name() or ""
            except Exception:   # 이름이 UTF-8이 아닌 주변 기기는 건너뜀
                continue
            if TARGET_ID in name:
                return r.device
    return None


async def polar_task():
    while True:
        try:
            send("# scan")
            dev = await find_polar()
            if dev is None:
                continue
            conn = await dev.connect(timeout_ms=10000)
            async with conn:
                svc = await conn.service(HR_SVC)
                chr_ = await svc.characteristic(HR_CHR)
                await chr_.subscribe(notify=True)
                send("# polar connected")
                while True:
                    data = await chr_.notified(timeout_ms=10000)
                    bpm, rrs = parse_hr(data)
                    t = ms()
                    send("H,%d,%d,%d" % (t, bpm, 1 if bpm > 0 else 0))
                    for rr in rrs:
                        send("R,%d,%d" % (t, rr))
        except Exception as e:  # 끊김·타임아웃이면 다시 찾는다
            send("# polar lost: %r" % (e,))
            await asyncio.sleep_ms(1000)


async def main():
    send("# actor ready")
    await asyncio.gather(cmd_task(), button_task(), polar_task())


try:
    asyncio.run(main())
finally:
    VIB.value(0)  # 종료·예외 시 반드시 OFF
