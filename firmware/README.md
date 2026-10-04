# 보드 펌웨어 (USB 시리얼용, 와이파이 없음)

| 폴더 | 보드 | 폴라 | 진동 핀 | 센서 |
|---|---|---|---|---|
| `actor/main.py` | 배우 | H9 `1E0A803C` | IO4 | BOOT 버튼(마커) |
| `seat/main.py` | 좌석 | H10 `19B7393F` | IO4, IO16, IO17 | FSR IO32/IO33, GSR IO36, BOOT 버튼 |

폴라는 광고 이름에 ID(예: `1E0A803C`)가 들어 있으면 연결한다.

## 올리기 (Thonny는 끈다)
    pip install mpremote
    mpremote connect COM5 cp firmware/seat/main.py :main.py     # 좌석
    mpremote connect COM6 cp firmware/actor/main.py :main.py    # 배우
    mpremote connect COM6 reset
(보드에 aioble이 없으면 `mpremote connect COMx mip install aioble`)

## 보드 쪽만 확인
    mpremote connect COM6 repl      # 줄이 흐르는지 본다. Ctrl+] 로 나옴
배우: `# scan` → `# polar connected` → `H,...` / `R,...`. 좌석: `S,...` 줄이 0.1초마다.

## 허브 연결
    python3 hub.py --list-ports
    python3 hub.py --seat serial:COM5 --actor serial:COM6 --script sample_script.md

## 장비 없이 허브 입출력 시험
    python3 tools/fake_board.py seat    # /dev/pts/N 출력
    python3 tools/fake_board.py actor
    python3 hub.py --seat serial:/dev/pts/1 --actor serial:/dev/pts/0

## 안전
- 켜자마자 진동 핀 OFF, `X`는 확인보다 먼저 진동 정지, 1회 ON 최대 1.5초, 종료·예외 시 OFF.
- 부팅 직후~main 실행 전에는 핀이 떠 있다 → S–GND 10kΩ 풀다운을 달 것(CIRCUIT.md).
