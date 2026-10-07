데이터통신 HW2 - 9팀 좌석 예매 시스템
수정본: 프로토콜 v2 / Python 표준 라이브러리 구현
문서 작성일: 2026-10-07 KST

0. 제출 준비 상태
이 문서는 수정본의 구현/실행 설명이다. 아래 미작성 사항을 채워야 제출이 완성된다.
- 조원 1 이름/학번/역할: [작성 필요]
- 조원 2 이름/학번/역할: [작성 필요]
- 조원 3 이름/학번/역할: [작성 필요]
- 최종 원격 호스트 서비스/인스턴스 유형/OS: [실제 사용 환경 작성 필요]
- 실제 서버/클라이언트 실행 명령, 테스트 날짜 및 로그 폴더: [작성 필요]
- 수정본 원격 30 x 5000 정식 실행의 실측 결과표: [실행 후 작성 필요]
- download.txt의 G9HW2.mp4 다운로드 링크: [5분 이내 영상 촬영/공유 후 작성 필요]
공인 IP는 가려도 된다. 비밀번호/SSH 개인키/API키를 저장소나 제출물에 넣지 않는다.
logs/에 이미 있는 파일은 수정 전 실행이다. 새 소스와 섞어서 제출하지 않는다.

1. 실행 환경 및 프로그램 구성
Python 3.10 이상. 별도 pip 패키지 필요 없음. Windows 로컬 + Linux 원격 서버 사용 가능.
로그 시간대는 모든 노드에서 KST(+09:00)로 고정한다.
wall-clock 로그: [HH:MM:SS.mmm] NODE | EVENT | STATUS | JSON payload
기간 측정은 각 노드의 time.perf_counter()를 사용한다. 서로 다른 노드 시각을 빼지 않는다.

AWS 서버와 로컬 PC의 시간 처리 (HW2description 2/14쪽, HW2explanation 9쪽):
- AWS 운영체제의 시간대가 UTC여도 logger가 명시적으로 KST로 변환하므로 양쪽 로그는 KST다.
- 응답시간은 로컬 Client의 요청 전송 직전~첫 응답 수신까지 같은 Client 시계로 측정한다.
  여기에는 AWS까지의 네트워크 왕복 및 서버 처리 시간이 포함된다.
- Throughput 분모는 AWS Server의 첫 연결~마지막 첫 응답 전송 기간이다.
  두 끝점 모두 동일 Server 프로세스의 perf_counter를 사용한다.
- Waitlist 평균은 AWS Server의 대기 등록~NOTIFY 전송 완료 기간이며 미해결 대기는 제외한다.
- 서버 로그 시각에서 클라이언트 로그 시각을 빼거나 서로 다른 호스트의 perf_counter 값을
  빼지 않는다. 시계 원점이 다를 수 있으므로 각 Client에서 계산한 기간만 서버로 전달한다.
- NTP 동기화는 로그 비교를 위한 권장 사항이며 과제 필수 조건은 아니다.
  실행 전 Windows의 자동 시간 설정과 AWS의 시간 동기화 상태를 확인한다.
  chrony를 사용하는 EC2 Linux: chronyc sources -v / chronyc tracking
  로컬 Windows: w32tm /query /status (진단용; 서비스 설정은 자동 변경하지 않는다).
  EC2의 169.254.169.123은 인스턴스 전용 시간 서버 주소이므로 로컬 PC에 설정하지 않는다.
  OS별 AWS 안내: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configure-ec2-ntp.html
  설정을 바꾸거나 NTP가 시간을 보정해도 성능 측정에는 wall-clock을 사용하지 않는다.

src/server/main.py      실행, 5초 POOL 관찰, 완료/장애 감시, 종료 및 정합성 확인
src/server/listener.py  selectors 기반 단일 Listener, TCP 파싱, 큐 적재, 송신
src/server/worker.py    서버 시작 때 만든 Worker 10개, 예약/취소 처리
src/server/seat.py      좌석 100개, 좌석별 Lock/owner/version/FIFO 대기열
src/server/notifier.py  Notifier 1개, 블로킹 통지 큐 처리
src/server/runtime.py   Condition 기반 Queue 최고 길이 및 잠금으로 보호된 통계
src/server/verification.py 서버와 클라이언트 최종 보유/수지 대조
src/client/main.py      클라이언트 송신 루프와 독립 수신 스레드
src/client/state.py     응답 순서에 안전한 좌석 상태/요청 상태, 요청 생성
src/client/launcher.py  30개 독립 프로세스 실행/회수 및 실패 전파
src/common/protocol.py  JSON + LF 메시지 경계 및 상수
src/common/logger.py    스레드 안전 로그, 이전 실행 파일 덮어쓰기 방지
src/verify.py           노드 로그의 별도 이력 재생/최종 대조
tests/                 단위/통신 회귀 테스트 및 로컬 리허설 도구

2. 실행 방법
반드시 src 디렉터리가 있는 저장소 루트에서 아래 명령을 실행한다.
Python 실행 명령이 python3인 Linux 환경에서는 python을 python3로 바꾼다.
지원 버전 확인: python --version
테스트: python -m unittest discover -s tests -v

간단한 로컬 리허설(서버/30개 클라이언트 자동 실행, 각 30건):
python tests/run_local.py --log-dir runs/local-01 --requests 30

빠른 동시성 스트레스 검사(제출용 아님):
python tests/run_local.py --log-dir runs/stress-01 --requests 5000 --interval-min 0.001 --interval-max 0.005

원격 서버에서 정식 실행:
python -m src.server.main --host 0.0.0.0 --port 9000 --num-clients 30 --requests 5000 --log-dir runs/final-01 --quiet

로컬에서 정식 실행(SERVER_IP 교체):
python -m src.client.launcher --server-ip SERVER_IP --server-port 9000 --num-clients 30 --requests 5000 --log-dir runs/final-01 --quiet

실행 순서: 서버 실행 -> 로컬 클라이언트 실행 -> 자동 종료/exit code 0 확인.
방화벽/클라우드 보안그룹에서 지정 TCP 포트를 허용한다.
서버 주소/포트는 하드코딩하지 않고 명령행으로 지정한다.
정식 간격 기본값은 0.2~1.0초이고 클라이언트별 5000건이다. 약 50분을 확보한다.
--quiet는 콘솔 출력을 줄일 뿐 파일 로그는 모두 남긴다.
클라이언트 --seed 또는 launcher --seed로 재현용 난수 시드를 정할 수 있다.
서버 --connection-timeout 기본 120초: 모든 클라이언트의 최초 접속 허용 시간.
서버 --idle-timeout 기본 120초: 요청 완료 진전이 전혀 없을 때 실패 처리.
서버 --send-timeout 기본 10초: 소켓별 송신 잠금/부분 전송을 포함한 한 메시지 제한.
서버 --shutdown-timeout 기본 60초: 종료 단계별 최대 허용 시간.
정상 클라이언트는 BYE까지 기다린다. 60초가 지났다는 이유로 임의 종료하지 않는다.
실험을 마친 원격 인스턴스는 비용 정책에 맞게 중지/정리한다.

로그는 x 모드로 열어 이미 있는 동일 이름 파일을 덮어쓰지 않는다.
새 실행은 final-02처럼 별도 폴더를 사용한다. 서버와 클라이언트 로그는 같은 run_id여야 한다.
원격 서버의 Server.txt와 로컬 Client1.txt~Client30.txt를 한 폴더에 모은 뒤:
python -m src.verify --log-dir runs/final-01 --submission --output runs/final-01/verification.json
프로그램은 오류/정합성 실패 시 exit code 1을 반환한다.
과거 v1 로그는 새 검증 도구의 구조화된 v2 로그와 호환되지 않는다.

3. 서버 스레드 및 Request Queue
Worker 10개 + Listener 1개 + Notifier 1개만 새로 생성한다.
main 스레드가 모니터 역할을 하며 별도의 Monitor/Feeder 스레드를 만들지 않는다.
연결/요청 수가 늘어도 Worker 수를 바꾸지 않는다.

Request Queue와 Notify Queue는 queue.Queue(maxsize=0)이다.
Queue 내부 mutex와 not_empty/not_full/all_tasks_done은 threading.Condition 기반이다.
Worker와 Notifier는 get()으로 블로킹 대기하며 sleep 반복 조회를 하지 않는다.
큐 크기는 무제한이므로 '가득 참' 동작은 없다. 접속당 요청은 고유 ID 1..5000으로
제한하므로 정식 실행에서 수용할 업무 요청은 최대 150000건이다.
장점: Listener가 큐 포화로 멈추지 않으며 구현/종료가 단순하다.
단점: 처리보다 유입이 빠르면 메모리 사용이 증가한다.
현재 길이는 큐 mutex 아래에서 읽으며, 최대 길이는 _put에서 같은 mutex를 가진 채
갱신한다. 5초마다 표본을 보고 최대값을 추정하지 않는다.
종료 sentinel은 최대 업무 큐 길이 집계에서 제외한다.

4. 좌석별 임계구역과 Deadlock 회피
좌석마다 threading.Lock 한 개를 둔다. 전체 좌석 맵을 막는 전역 Lock은 없다.
owner, version, waitlist, 배정/해제 수, FIFO ticket은 해당 좌석 Lock 안에서만 읽고 쓴다.
상태 출력과 최종 snapshot도 좌석마다 잠금을 잡아 복사한 값만 사용한다.
POOL snapshot은 실행 중 좌석별 관찰 결과이고 전체의 단일 시점 원자적 snapshot은 아니다.
최종 snapshot은 모든 Worker가 종료한 뒤 생성하므로 일관된 최종 상태다.

단일 예약/취소: 좌석 Lock -> 판정 -> 상태 갱신/대기 등록 또는 handoff -> Lock 해제.
다중 예약: 입력 개수/중복/범위를 잠금 전에 확인 -> 번호 오름차순 Lock 획득 ->
전부 EMPTY인지 확인 -> 전부 배정 또는 전혀 변경 없음 -> 역순 해제.
예외나 실패에도 finally에서 이미 획득한 모든 Lock을 해제한다.
Lock 순서가 모든 Worker에서 같아 순환 대기를 방지한다. 타임아웃으로 풀어주는 방식이 아니다.
장점: 번호가 다른 단일 좌석은 병렬 처리하며, 다중 예약의 교착상태를 예방한다.
단점: 같은 인기 좌석/겹친 다중 요청은 순차 처리되며 큰 번호 Lock을 기다리는 동안
작은 번호 Lock을 보유할 수 있다.
소켓 전송 및 파일 로그는 좌석 Lock을 해제한 뒤 실행한다.
통계용 Condition이나 큐 mutex를 잡은 채 좌석 Lock을 획득하지 않는다.
Lock 경합은 Worker의 acquire(blocking=False)가 실패한 횟수로 센다.
서버 main은 큐에 미완료 요청이 있고 30초 동안 완료가 없으면 Deadlock/진전 중단으로
기록하고 실행을 실패 처리한다. 정상 실행의 Deadlock 지표는 0이어야 한다.

5. 요청 처리 규칙
RESERVE:
- 좌석 번호 범위/형식 오류: FAIL.
- EMPTY: SUCCESS, owner=요청자.
- 이미 본인 소유 또는 같은 좌석 대기열에 이미 본인 존재: FAIL.
- 다른 사람이 소유: WAITLISTED, FIFO 뒤에 등록.
RESERVE_MULTI:
- 2~4석이 아님, 번호 중복, 범위/형식 오류: FAIL.
- 하나라도 EMPTY가 아니면 아무 상태도 바꾸지 않고 FAIL. Waitlist에 들어가지 않는다.
- 전부 EMPTY면 하나의 임계구역에서 전부 배정하고 SUCCESS.
CANCEL:
- 잘못된 좌석/본인 소유 아님: FAIL.
- 본인 소유면 SUCCESS. 대기자 없으면 EMPTY, 있으면 head에게 바로 배정.
대기자가 있을 때 좌석을 잠시 EMPTY로 공개하지 않으므로 끼어들기가 없다.
handoff는 해제 1회 + 배정 1회로 집계한다.

6. Waitlist와 Notifier
좌석별 deque에 (client ID, request ID, 최초 등록 perf_counter 시각, FIFO ticket)을 저장한다.
등록 후 Worker는 WAITLISTED를 응답하고 다음 요청을 처리한다. 좌석 배정을 기다리지 않는다.
CANCEL의 동일 좌석 임계구역 안에서 popleft하여 바로 소유권을 넘긴다.
통지 작업은 잠금을 푼 뒤 Notify Queue에 넣는다. Notifier 한 개가 Queue의 Condition으로
깨어나 소켓별 송신 Lock 아래 NOTIFY를 전송한다.
FIFO 기준은 좌석 배정 순서이며 실제 알림 도착 순서가 아니다.
최초 waitlist 등록부터 NOTIFY 전송 완료까지 서버 내 시계로 측정한다.
종료 시 남아 있는 미해결 대기는 오류가 아니고 평균 대기시간에서는 제외한다.

7. 통신 형식 및 클라이언트 상태
UTF-8 JSON 객체 하나 + LF 하나. 이 수정은 v2이며 서버/클라이언트를 동시에 배포해야 한다.
수신은 bytearray에 누적하고 완전한 줄이 될 때 디코딩한다. 분할 수신/합쳐진 수신 및
UTF-8 문자 바이트 분할을 처리한다. 한 프레임은 최대 65536바이트다.

클라이언트 접속:
{"type":"HELLO","protocol":2,"cid":1,"requests":5000}
서버:
{"type":"WELCOME","protocol":2,"run_id":"서버가 생성한 UUID","requests":5000}
요청:
{"type":"REQUEST","rid":1,"cmd":"RESERVE","seats":[42]}
{"type":"REQUEST","rid":2,"cmd":"RESERVE_MULTI","seats":[5,3]}
{"type":"REQUEST","rid":3,"cmd":"CANCEL","seats":[42]}
응답:
{"type":"RESP","rid":1,"cmd":"RESERVE","status":"SUCCESS","states":[{"seat":42,"version":1,"owner":1}],"reason":""}
대기 통지:
{"type":"NOTIFY","rid":4,"state":{"seat":10,"version":7,"owner":1}}

모든 응답/NOTIFY는 원래 요청 rid로 매칭한다. WAITLISTED가 첫 응답이며 NOTIFY는 별도다.
좌석 owner가 바뀔 때마다 좌석별 version을 1 증가시킨다.
클라이언트는 더 높은 version만 반영하며 같은 version의 모순된 owner는 오류로 처리한다.
따라서 늦은 CANCEL 응답이나 늦은 NOTIFY가 새 배정을 지우거나 옛 배정을 부활시키지 않는다.
취소 중인 좌석은 pending_cancel로 별도 관리하며 성공 전에 owned 상태를 지우지 않는다.
NOTIFY가 WAITLISTED보다 먼저 왔더라도 요청별 notified 기록으로 대기열에 재등록하지 않는다.
중복 첫 응답/중복 NOTIFY/다른 요청의 좌석 상태는 오류다.

송신은 main, 수신은 별도 스레드이며 이전 응답이나 WAITLISTED 배정을 기다리지 않고
0.2~1.0초 무작위 간격으로 다음 요청을 보낸다.
요청 종류 비율은 보유 시 예약30/다중20/취소50, 미보유 시 예약60/다중40을 기본으로 한다.
취소 진행 중 좌석은 새 취소 대상으로 고르지 않는다.
인기 좌석은 1~10번이다. 일반 선택 확률 60%에 더해 누적 좌석 선택의 절반 이상이
인기 좌석이 되도록 보정한다. CANCEL도 이 수지에 포함한다.
보정이 필요하고 인기 보유 좌석이 없으면 CANCEL 대신 인기 좌석 RESERVE를 보낸다.
이 보정 때문에 최종 요청 종류 비율은 참고 비율과 다를 수 있다.
다중 요청의 좌석 순서는 섞어서 보내 서버의 정렬이 실제로 사용되게 한다.

8. 종료 및 장애 처리
각 Client ID의 고유 요청 5000건에 대해 실제 응답 전송 성공이 확인되어야 전체 완료다.
완료 절차:
1) Worker 큐 끝에 sentinel 10개를 넣고 모든 Worker를 join한다.
2) 이때 모든 통지 생산이 끝났다. Notify Queue 끝에 sentinel을 넣고 Notifier를 join한다.
3) 미완료 큐 작업이 0인지 확인하고 최종 snapshot을 얻는다.
4) FINISH를 모든 Client에게 보낸다.
5) Client는 요청 완료 여부를 확인하고 최종 보유/수지 REPORT를 TCP로 보낸다.
6) 서버가 클라이언트 보고와 최종 좌석/대기열 수지를 실제 대조한다.
7) 성공 시 BYE(PASS)를 보낸다. Client는 최종 로그를 남기고 ACK를 보낸 뒤 종료한다.
8) ACK 수신 후 서버는 Listener를 중지/join하고 소켓과 로그를 닫는다.

FINISH/REPORT/BYE/ACK는 제어 메시지이고 150000건 업무 요청 수에 포함하지 않는다.
등록된 클라이언트의 연결 단절/중복 요청/잘못된 프로토콜/송신 실패는 실행 전체 FAIL이다.
재접속이나 소유권 자동 회수는 구현하지 않는다. 새 로그 폴더로 전체 실행을 다시 한다.
소켓 전송 실패를 성공 응답/성공 NOTIFY로 세지 않는다.
미등록 연결의 잘못된 메시지/중복 ID 접속은 해당 연결만 거부하며 Listener는 계속 동작한다.
소켓별 송신 Lock, 부분 전송 반복, 송신 deadline을 사용해 메시지 혼합과 무한 대기를 막는다.
Graceful 정상 종료의 확인은 각 Client의 BYE 수신 로그 + 서버의 ACK 수/스레드 join 로그다.
타임아웃이나 예외가 났는데 정상 종료했다고 기록하지 않는다.

9. 이중예약 및 정합성 검증
각 좌석 상태 변경 시 기대한 이전 owner인지 검사한다. 다른 owner를 덮어쓰면 오류를
집계하고 실행을 실패시킨다. 최종 dict의 키 중복 여부를 검사하는 방식은 사용하지 않는다.
정상 종료 때 다음을 모두 확인한다.
- 배정 수 - 해제 수 = 최종 점유 좌석 수
- 서버 최종 좌석 owner = Client 30개의 final_held 합 (중복 없음)
- WAITLISTED 수 = NOTIFY 실제 수신 수 + 미해결 대기 수 (전체 및 Client별)
- Client별 요청/첫 응답 건수, 서버/Client 결과별 건수 일치
- 서버/Client 통지 건수, FIFO handoff 및 미해결 수지
- 인기 좌석 선택 절반 이상
- 이중예약 불변조건 위반 0건
검증 후 PASS/FAIL을 기록한다.

추가 src.verify 검증기는 로그의 run_id를 대조하고 좌석별 version 순서로 전이 이력을 재생한다.
로그 I/O가 임계구역 밖에 있어 줄 순서는 달라질 수 있으므로 version을 기준으로 한다.
실제 요청/응답 ID 집합, 원자적 다중 배정, 오름차순 LOCK, FIFO ticket/handoff,
서버/Client 통지 내용, 최종 snapshot 및 배정/해제 수지를 확인한다.
단일 RESERVE/CANCEL도 요청별 성공·실패와 실제 전이를 대조한다. SUCCESS는 해당 요청의
전이와 응답 좌석 상태가 일치해야 하고, FAIL에는 전이/대기 등록이 없어야 한다.
WAITLISTED는 대기 등록 1개와 대응하며, 응답의 좌석 버전·소유자도 이력과 대조한다.
--submission은 30x5000, 클라이언트 설정 간격 0.2~1.0도 확인한다.
원격 배포 사실은 이 도구만으로 증명하지 못하므로 배포 설명/영상으로 함께 확인한다.

10. 성능 지표
Throughput: 서버의 성공적으로 전송한 첫 응답 수 / (첫 Client 연결~마지막 첫 응답 전송).
평균 응답시간: 각 Client의 요청 준비/전송 직전~첫 응답 수신 기간. Client 내 perf_counter.
Queue 최대 길이: put 순간의 큐 크기 최고값, sentinel 제외.
이중예약: 상태 변경 불변조건 위반 및 별도 이력 재생 검사, 정상 0건.
Deadlock: 미완료 업무가 있는데 30초간 완료 진전이 없는 횟수, 정상 0건.
Waitlist 평균: 등록~NOTIFY 전송 완료, 서버 내 perf_counter, 미해결 제외.
Lock 경합: Worker의 좌석 try-acquire 실패 횟수.
최종 좌석 정합성: 모든 검증 PASS/FAIL.
5초마다 전체 100좌석 owner/대기 수, 현재 큐 길이/최대 길이/누적 응답 수를 POOL에 기록한다.
서버는 METRICS, Client는 TERMINATE에 최종 수치를 기록한다.

11. 수정본 실측 결과표
[정식 원격 실행 후 verification.json 및 로그에서 작성할 것]
서버: Throughput __ req/s / Queue 최대 __건 / 이중예약 __건 / Deadlock __건
Waitlist 평균 __초 / Lock 경합 __건 / 최종 정합성 PASS 또는 FAIL
Client 30개 합계: 요청 150000 / SUCCESS __ / FAIL __ / WAITLISTED __
NOTIFY 수신 __ / 미해결 __ / 평균 응답시간 __ms
로컬 개발/스트레스 실측은 별도 TEST_RESULTS.txt에 기록한다. 원격 실측으로 대체하지 않는다.

12. 제출 체크리스트
G9HW2.zip 한 개, 조원 1명이 제출. 마감 2026-10-12(월) 23:59.
- 전체 소스 (src/, 테스트 도구는 함께 포함 가능)
- Readme.txt: 이름/학번/역할, 배포 정보, 설계/실행, 원격 실측표 완료
- AllDefinedLogs.txt
- 수정본 같은 실행의 Server.txt + Client1.txt~Client30.txt
- download.txt: 5분 이내 G9HW2.mp4 다운로드 링크, 공유 권한/재생 확인
- ZIP 실제 압축 해제 및 깨끗한 폴더에서 실행 명령 확인
서버/로컬 로그 수집 과정에서 같은 이름을 덮어쓰거나 다른 실행 파일을 섞지 않는다.
영상 구성은 자유이며 원격 서버 실행, 30개 연결, 다중예약, 최종 검증 장면을 넣을 수 있다.
