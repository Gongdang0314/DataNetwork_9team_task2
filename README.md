# DataNetwork 9팀 HW2

Python 3.10 이상, 표준 라이브러리만 사용합니다. 모든 명령은 저장소 루트에서 실행합니다.

## 먼저 확인

- 상세 설계·실행·제출 체크리스트: [Readme.txt](Readme.txt)
- 로그 명세: [AllDefinedLogs.txt](AllDefinedLogs.txt)
- 영상 링크: [download.txt](download.txt) (아직 작성 필요)
- 기존 `logs/`는 수정 전 EC2 실행 기록입니다. 수정본의 제출 증거로 재사용하지 않습니다.
- 수정본은 JSON 기반 프로토콜 v2를 사용하므로 서버와 클라이언트를 함께 업데이트합니다.
- 로그 파일이 이미 있으면 덮어쓰지 않고 실패합니다. 실행마다 새 폴더를 지정하세요.

## 테스트

```powershell
python -m unittest discover -s tests -v
python tests/run_local.py --log-dir runs/local-check --requests 30
```

로컬 리허설은 기본 30명, 각 30건, 요청 간격 0.2~1.0초입니다.
결과는 해당 폴더의 `verification.json`에 저장됩니다.

## 원격 정식 실행

원격 서버:

```text
python -m src.server.main --host 0.0.0.0 --port 9000 --num-clients 30 --requests 5000 --log-dir runs/remote-final --quiet
```

로컬 PC (SERVER_IP를 실제 주소로 교체):

```text
python -m src.client.launcher --server-ip SERVER_IP --server-port 9000 --num-clients 30 --requests 5000 --log-dir runs/remote-final --quiet
```

원격의 `Server.txt`와 로컬의 `Client1.txt`~`Client30.txt`를 같은 폴더에 모은 뒤:

```text
python -m src.verify --log-dir runs/remote-final --submission --output runs/remote-final/verification.json
```

`PASS`와 모든 프로세스의 정상 종료를 확인합니다. 검증 도구는 로그의 실행 ID·처리 이력·수지를 검사합니다.
실제로 원격 서버에서 수행했는지는 배포 환경과 영상으로 별도 확인해야 합니다.
