"""가상 시리얼(pty) 보드. 실제 보드 없이 허브의 시리얼 입출력을 시험한다.
python3 tools/fake_board.py seat|actor  -> 출력된 /dev/pts/N 을 허브에 넘긴다."""
import os, pty, sys, time, select, random
kind = sys.argv[1] if len(sys.argv) > 1 else "seat"
m, s = pty.openpty()
print(os.ttyname(s), flush=True)
t0 = time.time(); buf = b""; last = 0
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
    if time.time() - last >= 0.1:
        last = time.time(); ms = int((last - t0) * 1000)
        if kind == "seat":
            os.write(m, ("S,%d,%d,%d,%d,%d,1\n" % (ms, 2000 + random.randint(-50, 50), 1800, 900 + random.randint(-20, 20), 72 + random.randint(-2, 2))).encode())
        else:
            os.write(m, ("H,%d,%d,1\nR,%d,%d\n" % (ms, 68 + random.randint(-2, 2), ms, 880)).encode())
