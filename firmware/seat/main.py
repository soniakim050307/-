# 좌석 보드 (ESP32 DevKitC V4, MicroPython + aioble) — USB 시리얼로 허브와 통신
# 보드 -> 허브: S,ms,front,back,gsr,hr,hr_ok / R,ms,rr_ms / M / A,id,pattern  (115200, 줄 단위)
# 허브 -> 보드: V,id,pattern (weak|strong) / X (전부 끄기)
# 시작 시 자체 테스트(진동) 없음. 진동 핀은 켜자마자 OFF.
from machine import Pin, ADC
VIBS = [Pin(n, Pin.OUT, value=0) for n in (4, 16, 17)]  # 가장 먼저 OFF (S-GND 10k 풀다운도 달 것)

import sys, select, time
import uasyncio as asyncio
import bluetooth
import aioble

TARGET_ID = "19B7393F"      # Polar H10. 광고 이름에 이 ID가 들어 있으면 연결
HR_SVC, HR_CHR = bluetooth.UUID(0x180D), bluetooth.UUID(0x2A37)
SAMPLE_MS = 100             # S 줄 10Hz
HR_STALE_MS = 5000          # 이 시간 넘게 심박 없으면 hr_ok=0
MAX_ON_MS = 1500
PATTERNS = {
    "weak": ((250, 0),),
    "strong": ((400, 150), (400, 150), (400, 0)),
}

def make_adc(n):  # ADC1 핀(32~39)만 사용
    a = ADC(Pin(n))
    a.atten(ADC.ATTN_11DB)
    return a

adc_front, adc_back, adc_gsr = make_adc(32), make_adc(33), make_adc(36)
btn = Pin(0, Pin.IN, Pin.PULL_UP)
vib_task = None
hr_now, hr_t = 0, None


def send(line):
    print(line)


def ms():
    return time.ticks_ms()


def all_off():
    for v in VIBS:
        v.value(0)


async def run_pattern(pat):
    try:
        for on, off in PATTERNS[pat]:
            for v in VIBS:
                v.value(1)
            await asyncio.sleep_ms(min(on, MAX_ON_MS))
            all_off()
            if off:
                await asyncio.sleep_ms(off)
    finally:
        all_off()


def stop_vib():
    global vib_task
    if vib_task is not None:
        vib_task.cancel()
        vib_task = None
    all_off()


def start_vib(pat):
    global vib_task
    stop_vib()
    vib_task = asyncio.create_task(run_pattern(pat))


def handle(line):
    p = line.strip().split(",")
    if p[0] == "X":
        stop_vib()                      # 정지가 항상 먼저
        send("A,X,off")
    elif p[0] == "V" and len(p) >= 3 and p[2] in PATTERNS:
        send("A,%s,%s" % (p[1], p[2]))  # 확인을 먼저 보냄(왕복 지연 측정)
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
            await asyncio.sleep_ms(200)
        prev = v
        await asyncio.sleep_ms(20)


def avg(adc, n=8):
    return sum(adc.read() for _ in range(n)) // n


async def sample_task():
    while True:
        t = ms()
        ok = 1 if (hr_t is not None and time.ticks_diff(t, hr_t) < HR_STALE_MS and hr_now > 0) else 0
        send("S,%d,%d,%d,%d,%d,%d" % (t, avg(adc_front), avg(adc_back), avg(adc_gsr),
                                       hr_now if ok else 0, ok))
        await asyncio.sleep_ms(SAMPLE_MS)


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
            name = r.name() or ""
            if TARGET_ID in name:
                return r.device
    return None


async def polar_task():
    global hr_now, hr_t
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
                    hr_now, hr_t = bpm, ms()
                    for rr in rrs:
                        send("R,%d,%d" % (hr_t, rr))
        except Exception as e:
            send("# polar lost: %r" % (e,))
            await asyncio.sleep_ms(1000)


async def main():
    send("# seat ready")
    await asyncio.gather(cmd_task(), button_task(), sample_task(), polar_task())


try:
    asyncio.run(main())
finally:
    all_off()
