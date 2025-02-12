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

