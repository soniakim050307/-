"""가상 시리얼(pty) 보드. 실제 보드 없이 허브의 시리얼 입출력을 시험한다.
python3 tools/fake_board.py seat|actor  -> 출력된 /dev/pts/N 을 허브에 넘긴다."""
import os, pty, sys, time, select, random
kind = sys.argv[1] if len(sys.argv) > 1 else "seat"
m, s = pty.openpty()
print(os.ttyname(s), flush=True)
t0 = time.time(); buf = b""; last = 0; last_status = 0
while True:
    r, _, _ = select.select([m], [], [], 0.05)
    if r:
        buf += os.read(m, 256)
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            p = line.decode().strip().split(",")
            print("<-", p, flush=True)
            if p[0] == "V":
                os.write(m, ((("A,%s,%s" if kind == "seat" else "ack,%s,%s") % (p[1], p[2])) + "\n").encode())
    if kind.startswith("actor") and time.time() - last_status >= 1:
        last_status = time.time(); os.write(m, b"# scan\n")
    if time.time() - last >= 0.1:
        last = time.time(); ms = int((last - t0) * 1000)
        if kind == "seat":
            os.write(m, ("S,%d,%d,%d,%d,%d,1\n" % (ms, 2000 + random.randint(-50, 50), 1800, 900 + random.randint(-20, 20), 72 + random.randint(-2, 2))).encode())
        elif kind == "actor":
            os.write(m, ("H,%d,%d,1\nR,%d,%d\n" % (ms, 68 + random.randint(-2, 2), ms, 880)).encode())
        else:  # actor-arousal: 30초 평온(심박 68, RR 변동 큼) 뒤 각성(심박 95, RR 변동 작음)
            if last - t0 < 30:
                hr, rr = 68 + random.randint(-2, 2), 880 + random.randint(-50, 50)
            else:
                hr, rr = 95 + random.randint(-1, 1), 630 + random.randint(-6, 6)
            if int(last * 10) % 10 == 0:  # 약 1초에 한 번
                os.write(m, ("H,%d,%d,1\nR,%d,%d\n" % (ms, hr, ms, rr)).encode())
