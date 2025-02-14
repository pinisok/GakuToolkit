# GakuToolkit

GakuToolkit 은 외부 폴더의 엑셀 시트의 텍스트, JSON 변환을 중점으로 작동합니다

### 준비

본 도구는 Rclone을 사용하여 동기화를 진행해 사전 설정이 필요합니다.
- Rclone config를 설정한 RCLONE_CONFIG 를 docker secret 에 구성
- 원격 환경 이름을 RCLONE_NAME 환경 변수 에 설정 

- 업데이트시 네트워크 낭비를 방지하기 위해 res/drive 에 마운트해 캐쉬 구성 필요
- cache 폴더 역시 기록을 위해 마운트 필요함


### 엑셀 시트 > TXT/Json 변환 (드라이브 > 로컬 동기화)


### TXT/Json > 엑셀 시트 변환 (로컬 > 드라이브 동기화)


### Todo

- [x] Adv 처리
- [x] MasterDB 처리
- [x] Generic 처리
- [x] Localization 처리
- [x] .gitignore 추가
- [ ] Dockerfile 작성
    - [ ] 실행 전 서브모듈 업데이트
    - [ ] 실행 로그 저장
    - [ ] output 레포 업데이트
- [ ] 자동 시트 수정?(업데이트 된 파일 체크 해제 / 추가 된 파일 추가(추가 후 알림을 보내 url을 File Chip 으로 변환해야함))