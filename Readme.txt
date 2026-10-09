================================================================================
HW#2 - 멀티스레드 좌석 예약 시스템 (Multithreaded Seat Reservation System)
9조
================================================================================

문서 수정일: 2026-10-09 KST


1. 조원 정보
================================================================================

  김대영 (20223087)
    - 클라이언트 구현, 시연 영상, 문서 작성

  김민재 (20223089)
    - 소켓 통신, 프로토콜 설계, 로그 시스템

  황태웅 (20223152)
    - 서버 구현, 인프라 관리 (EC2)


2. 프로그램 개요
================================================================================

프로그램 설명

  - Python 3.10 이상에서 실행하는 TCP 기반 좌석 예약 시스템
  - Server: AWS EC2에서 100개 좌석의 예약 상태를 관리
  - Client: 로컬 PC에서 독립 프로세스 30개 실행
  - 각 Client는 0.2~1.0초 간격으로 5,000건씩 요청 (총 150,000건)
  - 요청 종류: RESERVE, RESERVE_MULTI, CANCEL
  - 좌석별 Lock, 고정 Worker Pool, FIFO Waitlist로 동시 요청 처리
  - 실제 요청/응답 및 좌석 이력을 로그로 남기고 종료 시 정합성 검증

프로그램 구성요소

  src/common/                    서버·클라이언트 공용 모듈
    ├── protocol.py              JSON + LF 프레이밍, 입력 검증, 프로토콜 상수
    └── logger.py                KST 시각 로그, 스레드 안전 파일 출력

  src/server/                    원격 좌석 예약 서버
    ├── main.py                  서버 실행, 5초 주기 관찰, 종료 및 검증
    ├── listener.py              TCP 연결, 메시지 수신, 요청 큐 적재, 송신
    ├── worker.py                Worker 10개의 예약·다중 예약·취소 처리
    ├── seat.py                  좌석별 Lock, owner, version, FIFO Waitlist
    ├── notifier.py              Notifier 1개의 비동기 배정 통지
    ├── runtime.py               Condition 기반 큐와 실행 통계
    └── verification.py          서버·클라이언트 최종 보유 상태 및 수지 검증

  src/client/                    로컬 클라이언트
    ├── main.py                  요청 송신 루프와 독립 수신 스레드
    ├── state.py                 요청 생성, 보유 좌석·버전·대기 상태 관리
    └── launcher.py              Client 30개 프로세스 실행 및 종료 확인

  src/verify.py                  수집된 로그의 이력 재생 및 독립 검증
  tests/                         단위·통신 회귀 테스트
  tests/run_local.py             서버·클라이언트 자동 로컬 리허설
  AllDefinedLogs.txt             로그 이벤트 정의
  TEST_RESULTS.txt               개발 테스트 및 EC2 실험 결과
  download.txt                   시연 영상 다운로드 링크


3. 실행 환경 및 필수 준비물
================================================================================

실행 환경

  Server:      AWS EC2 t3.small, eu-north-1, Ubuntu 26.04 LTS
  Client:      로컬 PC (Windows/macOS/Linux, Python 3.10 이상)
  네트워크:    TCP Socket, 서버 포트 9000
  라이브러리:  Python 표준 라이브러리만 사용 (별도 pip 설치 불필요)
  시간 기록:   모든 노드의 로그는 KST, 기간 측정은 노드별 perf_counter

  2026-10-07 정식 실험 기록의 Python 버전:
    - Server: Python 3.14.4
    - Client: Windows 11, Python 3.14.6

필수 준비물

  로컬 PC
    - Python 3.10 이상: python --version
    - OpenSSH의 ssh/scp 명령 및 tar 명령
    - src/가 포함된 프로젝트 폴더
    - EC2 접속 키 파일 (HaEeee.pem)

  AWS
    - 실행 중인 Ubuntu EC2 인스턴스
    - 공인 IP와 SSH 접속 권한
    - 클라이언트 PC에서 서버 TCP 9000번 포트로 연결 가능한 보안 그룹

  아래 명령은 src/가 보이는 프로젝트 루트에서 실행한다.
  EC2에서는 python3, Windows 예시에서는 python을 사용한다.


4. AWS EC2 서버 준비
================================================================================

4-1. 인스턴스 및 보안 그룹

  사용 인스턴스 유형: t3.small
  리전: eu-north-1
  OS: Ubuntu 26.04 LTS
  실험에 사용한 공인 IP: 13.61.180.113

  EC2 인스턴스가 실행 중인지 확인하고 현재 공인 IP를 확인한다.
  인스턴스를 중지 후 다시 시작하면 공인 IP가 바뀔 수 있다.

  인바운드 규칙:
    - SSH (TCP 22): SSH 접속 PC의 공인 IP 허용
    - 사용자 지정 TCP (TCP 9000): Client 실행 PC의 공인 IP 허용

  Client는 서버로 연결하는 방식이므로 Client마다 별도 수신 포트를 열지 않는다.

4-2. 로컬 PowerShell 공통 변수

  다음 값은 실제 키 위치와 현재 EC2 공인 IP에 맞춰 설정한다.
  새 PowerShell 창을 열었다면 변수를 다시 설정한다.

    $Ec2Key = "$env:USERPROFILE\Downloads\HaEeee.pem"
    $Ec2Ip = "13.61.180.113"
    $Ec2Dir = "hw2-final"

  개인키는 소스 폴더나 제출 ZIP에 넣지 않는다.
  Windows에서 키 권한 오류가 나면 파일 보안 설정에서 현재 사용자만
  개인키를 읽을 수 있도록 조정한다.
  macOS/Linux에서는 chmod 600 /path/to/HaEeee.pem 으로 권한을 설정한다.

4-3. SSH 접속 확인

  로컬 PowerShell:

    ssh -i "$Ec2Key" "ubuntu@$Ec2Ip"

  EC2 터미널:

    python3 --version
    ss -ltnp 'sport = :9000'

  9000번 포트에서 다른 실험이 실행 중이면 담당자와 실행 시간을 맞춘다.
  팀원의 서버를 일괄 종료하는 명령으로 포트를 비우지 않는다.
  접속 확인 후 exit를 입력하면 로컬 PowerShell로 돌아온다.


5. 소스코드 업로드 (EC2)
================================================================================

로컬 프로젝트 루트에서 전송 파일 생성

  작성 중인 Readme.txt까지 포함하도록 현재 파일을 직접 묶는다.
  과거 실험 로그나 개인키는 업로드 묶음에 포함하지 않는다.

    tar --exclude=__pycache__ --exclude='*.pyc' -cf hw2-source.tar src tests Readme.txt AllDefinedLogs.txt TEST_RESULTS.txt download.txt

EC2에 새 배포 폴더 생성 및 전송

    ssh -i "$Ec2Key" "ubuntu@$Ec2Ip" "mkdir ~/$Ec2Dir"
    scp -i "$Ec2Key" .\hw2-source.tar "ubuntu@${Ec2Ip}:~/${Ec2Dir}/"
    ssh -i "$Ec2Key" "ubuntu@$Ec2Ip" "tar -xf ~/$Ec2Dir/hw2-source.tar -C ~/$Ec2Dir"

  폴더가 이미 있으면 Ec2Dir을 hw2-final-02처럼 새 이름으로 바꾼다.
  이후 EC2의 cd 경로도 같은 이름으로 맞춘다.
  서버와 클라이언트는 같은 버전의 소스를 사용한다.

배포 결과

  ~/hw2-final/
    ├── src/
    ├── tests/
    ├── Readme.txt
    ├── AllDefinedLogs.txt
    ├── TEST_RESULTS.txt
    └── download.txt


6. 명령행 인자 참고
================================================================================

Server 인자 (python3 -m src.server.main)

  --host                 바인딩 주소 (기본: 0.0.0.0)
  --port                 서버 포트 (기본: 9000)
  --num-clients          접속할 Client 수 (기본: 30)
  --requests             Client당 요청 수 (기본: 5000)
  --log-dir              로그 폴더 (기본: runs/current)
  --connection-timeout   최초 전체 연결 제한 (기본: 120초)
  --idle-timeout         처리 진전이 없는 시간 제한 (기본: 120초)
  --send-timeout         메시지 송신 제한 (기본: 10초)
  --shutdown-timeout     종료 단계별 제한 (기본: 60초)
  --quiet                콘솔 로그 생략, 파일 로그는 유지

Client launcher 인자 (python -m src.client.launcher)

  --server-ip            Server IP (필수)
  --server-port          Server 포트 (필수)
  --num-clients          Client 프로세스 수 (기본: 30)
  --requests             Client당 요청 수 (기본: 5000)
  --interval-min         요청 간격 하한 (기본: 0.2초)
  --interval-max         요청 간격 상한 (기본: 1.0초)
  --seed                 난수 시드 기준값 (기본: 9000)
  --log-dir              로그 폴더 (기본: runs/current)
  --quiet                콘솔 로그 생략, 파일 로그는 유지

  launcher는 각 Client에 seed + Client ID를 전달한다.
  단일 Client를 직접 실행하는 src.client.main은 --id도 지정해야 한다.
  로그 검증 명령 src.verify는 --log-dir, --submission, --output을 지원한다.


7. 프로그램 실행
================================================================================

7-1. 실행 폴더 결정

  아래 예시는 새 로그 폴더 runs/final-02를 사용한다.
  이미 같은 폴더로 실행했다면 final-03 등 새 이름을 양쪽에 지정한다.
  프로그램은 기존 Server.txt 또는 ClientN.txt를 덮어쓰지 않는다.
  과거 정식 실험 결과 runs/final-01은 F항에 정리했다.

7-2. Server 실행 (EC2)

  SSH로 접속한 EC2 터미널:

    cd ~/hw2-final
    python3 -m src.server.main --host 0.0.0.0 --port 9000 --num-clients 30 --requests 5000 --log-dir runs/final-02 --quiet

  서버를 먼저 실행하고 기본 120초 안에 Client를 실행한다.
  --quiet 사용 시 콘솔이 조용해도 runs/final-02/Server.txt에는 로그가 남는다.
  진행 상황은 별도 SSH 창에서 다음 명령으로 확인할 수 있다.

    tail -f ~/hw2-final/runs/final-02/Server.txt

7-3. 연결 확인 (로컬 PowerShell)

    Test-NetConnection $Ec2Ip -Port 9000

  TcpTestSucceeded : True이면 TCP 연결이 가능하다.
  Linux/macOS에서는 nc -zv 13.61.180.113 9000으로 확인할 수 있다.

7-4. Client 30개 실행 (로컬 프로젝트 루트)

    python -m src.client.launcher --server-ip $Ec2Ip --server-port 9000 --num-clients 30 --requests 5000 --interval-min 0.2 --interval-max 1.0 --log-dir runs/final-02 --quiet

  launcher가 Client1~Client30을 독립 프로세스로 실행한다.
  전체 요청은 150,000건이며 정식 간격으로 약 50분 이상 소요된다.
  서버 SSH 세션과 로컬 실행 창을 유지하고 PC가 절전되지 않도록 한다.

7-5. 완료 및 종료 확인

  정상 완료 시 launcher 최종 콘솔:

    30 clients: PASS

  서버·클라이언트는 자동 종료한다. 종료 코드는 0이어야 한다.
  로그에서 다음 항목을 확인한다.

    Server.txt:
      TERMINATE | SUCCESS, phase="closed"
      acknowledged=30, all_threads_joined=true

    Client1.txt~Client30.txt:
      TERMINATE | SUCCESS, bye_received=true
      sent=5000, responded=5000

  로컬 PowerShell에서 종료 코드 확인: $LASTEXITCODE
  EC2 셸에서 종료 코드 확인: echo $?
  종료 직후 다른 명령을 실행하기 전에 확인한다.

  중간에 중단하려면 본인이 실행한 서버·클라이언트 창에서 Ctrl+C를 누른다.
  중단된 실행은 정식 완료 결과로 사용하지 않고 새 로그 폴더로 다시 실행한다.


8. 로컬 전용 테스트 (EC2 없이)
================================================================================

단위·통신 회귀 테스트

    python -m unittest discover -s tests -v

소규모 리허설 (서버 + Client 30개 자동 실행, 각 30건)

    python tests/run_local.py --log-dir runs/local-01 --clients 30 --requests 30

  요청 간격은 0.2~1.0초이며 verification.json을 자동 생성한다.
  서버·클라이언트 종료 코드와 로그 정합성 결과를 함께 확인한다.

빠른 동시성 스트레스 검사

    python tests/run_local.py --log-dir runs/stress-01 --clients 30 --requests 5000 --interval-min 0.001 --interval-max 0.005

  이 검사는 간격을 줄인 개발용 검사이며 정식 원격 실험을 대신하지 않는다.
  리허설 인자는 --clients이고 서버·launcher 인자는 --num-clients이다.
  재실행할 때는 local-02, stress-02처럼 새 로그 폴더를 사용한다.


9. 로그 회수 및 검증
================================================================================

로그 위치

  EC2:   ~/hw2-final/runs/final-02/Server.txt
  로컬:  runs/final-02/Client1.txt~Client30.txt

  같은 이름의 폴더라도 서로 다른 컴퓨터에 있으므로 한곳으로 모아야 한다.
  실행이 정상 종료된 뒤 로컬 프로젝트 루트에서 다음을 실행한다.

새 수집 폴더 생성 및 로그 복사 (PowerShell)

    New-Item -ItemType Directory -Path .\runs\collected-final-02 -ErrorAction Stop
    Copy-Item -Path .\runs\final-02\Client*.txt -Destination .\runs\collected-final-02\
    scp -i "$Ec2Key" "ubuntu@${Ec2Ip}:~/${Ec2Dir}/runs/final-02/Server.txt" .\runs\collected-final-02\

  수집 폴더가 이미 있으면 새 폴더 이름을 사용한다.
  Server.txt 1개 + Client1.txt~Client30.txt 30개, 총 31개를 확인한다.
  각 로그의 INIT에 기록된 run_id가 같은 실행인지 확인한다.

정식 실행 로그 검증

    python -m src.verify --log-dir runs/collected-final-02 --submission --output runs/collected-final-02/verification.json

  결과: status="PASS", errors=[], submission_mode=true
  서버와 Client의 요청·응답, 좌석 이력, Waitlist, 최종 수지를 대조한다.
  --submission은 30 x 5000 및 간격 설정 0.2~1.0초도 확인한다.
  실제 원격 실행 환경은 배포 기록과 시연 영상으로 함께 설명한다.

  저장소의 기존 logs/는 수정 전 실행 기록이다.
  새 실행의 원본 로그를 모으고 이전 로그와 섞지 않는다.


10. EC2 인스턴스 중지
================================================================================

  1) 서버와 Client의 정상 종료를 확인한다.
  2) Server.txt를 로컬로 회수하고 검증 결과를 보관한다.
  3) 다른 조원이 같은 인스턴스를 사용 중인지 확인한다.
  4) 모든 작업이 끝났다면 EC2 콘솔에서 해당 인스턴스를 중지한다.

  실험 프로그램의 자동 종료와 EC2 인스턴스 중지는 별개다.
  다시 시작할 때는 공인 IP를 확인하고 접속 주소를 갱신한다.


11. 문제 해결
================================================================================

Client가 Server에 연결되지 않음

  확인: Server 실행 여부, 현재 공인 IP, TCP 9000 보안 그룹, 로컬 네트워크.
  서버를 먼저 켠 뒤 연결 제한 시간 안에 Client를 실행한다.

Address already in use

  확인: EC2에서 ss -ltnp 'sport = :9000' 실행.
  다른 조원의 실험이면 실행 시간을 조정한다.
  본인의 이전 실행이면 해당 실행을 종료한 뒤 다시 시작한다.

FileExistsError 또는 로그 파일이 이미 존재함

  원인: 이전 실행과 동일한 로그 폴더를 사용함.
  해결: 기존 로그를 보관하고 --log-dir을 새 폴더로 지정한다.

server disconnected before BYE / 30 clients: FAIL

  확인: Server.txt의 ERROR·TERMINATE, 서버 프로세스 종료 상태, 연결 상태.
  중간 종료·통신 실패가 발생한 실행은 정상 종료 결과로 제출하지 않는다.
  원인을 확인한 뒤 서버·클라이언트를 함께 새 실행으로 시작한다.

콘솔 출력이 보이지 않음

  --quiet 옵션이면 정상 동작이다. 로그 파일의 INIT·POOL·응답 기록을 확인한다.

로그 검증이 FAIL

  Server.txt와 Client1.txt~Client30.txt의 run_id, 요청 수, 종료 기록을 확인한다.
  오래된 v1 로그와 현재 v2 로그는 호환되지 않는다.
  서로 다른 실행의 파일을 섞어 수치를 맞추지 않는다.

SSH 개인키 권한 오류

  키 위치와 현재 사용자에게 허용된 읽기 권한을 확인한다.
  개인키 자체를 소스 저장소에 업로드해서 해결하지 않는다.


12. 제출 파일 구성
================================================================================

  제출 파일명: G9HW2.zip (조원 1명이 제출)
  마감: 2026-10-12(월) 23:59

  포함 항목
    - src/ 및 필요한 실행·검증 도구
    - Readme.txt: 조원 정보, 실행 방법, 구현 설명, 실측 결과
    - AllDefinedLogs.txt
    - 동일한 정식 실행의 Server.txt + Client1.txt~Client30.txt
    - download.txt: 5분 이내 G9HW2.mp4 다운로드 링크

  영상에는 원격 Server 실행, Client 연결, 다중 예약, 최종 검증 장면을 담는다.
  링크의 공유 권한과 다운로드·재생 여부를 확인한다.
  최종 ZIP은 새 폴더에 압축을 풀어 소스·명령·로그 경로를 확인한다.

  현재 추가 확인 항목
    - 정식 실행 원본 로그 31개 확보 및 최종 ZIP에 포함
    - download.txt의 실제 영상 링크 입력

  저장소에 올라온 runs/final-01/verification.json은 검증 결과 요약이다.
  현재 저장소에는 그 실행의 원본 로그 31개가 포함되어 있지 않으므로,
  실행한 PC와 EC2에서 같은 run_id의 로그를 모아 최종 제출물을 구성한다.


A. 고정 Worker Pool과 Request Queue
================================================================================

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


B. 좌석별 Lock과 원자적 다중 예약
================================================================================

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

  요청별 처리 규칙

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


C. FIFO Waitlist와 비동기 Notifier
================================================================================

  좌석별 deque에 (client ID, request ID, 최초 등록 perf_counter 시각, FIFO ticket)을 저장한다.
  등록 후 Worker는 WAITLISTED를 응답하고 다음 요청을 처리한다. 좌석 배정을 기다리지 않는다.
  CANCEL의 동일 좌석 임계구역 안에서 popleft하여 바로 소유권을 넘긴다.
  통지 작업은 잠금을 푼 뒤 Notify Queue에 넣는다. Notifier 한 개가 Queue의 Condition으로
  깨어나 소켓별 송신 Lock 아래 NOTIFY를 전송한다.
  FIFO 기준은 좌석 배정 순서이며 실제 알림 도착 순서가 아니다.
  최초 waitlist 등록부터 NOTIFY 전송 완료까지 서버 내 시계로 측정한다.
  종료 시 남아 있는 미해결 대기는 오류가 아니고 평균 대기시간에서는 제외한다.


D. 통신 프로토콜과 클라이언트 상태 관리
================================================================================

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

  AWS 서버와 로컬 PC의 시간 처리(HW2description 2/14쪽, HW2explanation 9쪽):
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


E. 종료 절차와 정합성 검증
================================================================================

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

  이중예약 및 정합성 검증

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


F. 성능 지표 및 EC2 정식 실험 결과
================================================================================

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

  EC2 정식 실험 기록

  출처: runs/final-01/verification.json 및 TEST_RESULTS.txt
  정식 원격 실행의 조건과 실측 결과는 다음과 같다.

  테스트 날짜: 2026-10-07 KST
  원격 호스트: AWS EC2 t3.small, eu-north-1, Ubuntu 26.04 LTS, Python 3.14.4
  로컬 호스트: Windows 11, Python 3.14.6
  run_id: 1c2730b8-6008-43dd-92d0-3ba802a0399d
  로그 폴더: runs/final-01
  서버 실행: python3 -m src.server.main --host 0.0.0.0 --port 9000 --num-clients 30 --requests 5000 --log-dir runs/final-01 --quiet
  클라이언트 실행: python -m src.client.launcher --server-ip <EC2 IP> --server-port 9000 --num-clients 30 --requests 5000 --log-dir runs/final-01 --quiet

  서버: Throughput 48.76 req/s / Queue 최대 4건 / 이중예약 0건 / Deadlock 0건
  Waitlist 평균 31.828초 / Lock 경합 0건 / 최종 정합성 PASS
  Client 30개 합계: 요청 150000 / SUCCESS 60382 / FAIL 70681 / WAITLISTED 18937
  NOTIFY 수신 18683 / 미해결 254 / 평균 응답시간 291.58ms
  배정 40551 / 해제 40523 / 최종 점유 28석
  소요 시간: 3076초 (약 51분)
  실행 기록: --submission PASS, 오류 0건, ACK 30/30, 전 스레드 join 완료
  로컬 개발/스트레스 실측은 별도 TEST_RESULTS.txt에 기록한다. 원격 실측으로 대체하지 않는다.

  결과 수지 확인

  - SUCCESS 60,382 + FAIL 70,681 + WAITLISTED 18,937 = 150,000건
  - NOTIFY 18,683 + 미해결 대기 254 = WAITLISTED 18,937건
  - 배정 40,551 - 해제 40,523 = 최종 점유 28석
  - FAIL은 예약 불가·취소 불가 등의 업무 응답이며 프로세스 실패와 구분한다.
  - 원본 로그를 확보한 뒤 9항의 명령으로 최종 제출본을 다시 검증한다.

================================================================================
