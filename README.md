# GakuToolkit

GakuToolkit 은 외부 폴더의 엑셀 시트의 텍스트, JSON 변환을 중점으로 작동합니다

### 준비

본 도구는 Rclone을 사용하여 동기화를 진행해 사전 설정이 필요합니다.
대부분의 설정은 docker-compose.yaml 을 수정해 설정할 수 있으며
직접 사용할려면 docker-compose.yaml 및 output 폴더의 submodule 을 수정해야합니다.
- RCLONE_CONFIG 에 미리 구성을 완료한 rclone.conf 의 경로 등록
- Google drive에 대해 설정한 rclone remote 이름을 REMOTE_NAME 으로 설정
- output 폴더의 submodule을 업데이트할 깃으로 설정, GIT_EMAIL, GIT_NAME 환경변수 설정
- cache, drive 폴더 마운트 또는 생성


### 엑셀 시트 > TXT/Json 변환 (드라이브 > 로컬 동기화)

### TXT/Json > 엑셀 시트 변환 (로컬 > 드라이브 동기화)

### Todo

- [x] Adv 처리
- [x] MasterDB 처리
- [x] Generic 처리
- [x] Localization 처리
- [x] .gitignore 추가
- [x] MasterDB Convert 과정에서 미번역값 이 빈 문자열로 변환되어 포함됨
- [ ] Summary 에 업로드하거나 수정된 파일 링크 달기
- [x] Dockerfile 작성
    - [x] 실행 전 서브모듈 업데이트
    - [ ] 실행 로그 저장
    - [x] output 레포 업데이트
- [ ] 자동 시트 수정?(업데이트 된 파일 체크 해제 / 추가 된 파일 추가(추가 후 알림을 보내 url을 File Chip 으로 변환해야함))