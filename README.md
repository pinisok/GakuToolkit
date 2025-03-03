# GakuToolkit

GakuToolkit 은 외부 폴더의 엑셀 시트의 텍스트, JSON 변환을 중점으로 작동합니다

### 지원 기능
    - Convert : Xlsx to json/txt to dest repository (엑셀 시트 > TXT/Json 변환 (드라이브 > 로컬 동기화))
        ADV
        MasterDB
        Generic
        Localization
    - Update : Json/txt to Xlxs at google shared drive (TXT/Json > 엑셀 시트 변환 (로컬 > 드라이브 동기화))
        ADV(https://github.com/DreamGallery/Campus-adv-txts/)
        MasterDB(https://github.com/pinisok/gakumas-master-translation/)


### 준비
    1. cache, drive 폴더 마운트 또는 생성
    2. rclone remote 설정
    3. main.py 실행해 작업
        - Support arguments
            --fullupdate : 엑셀 파일 > 번역 파일로 변환하는 과정에서 모든 파일을 강제로 변환합니다.
            --DEBUG : 디버깅 로그 출력을 활성화
            - 특정 작업만 수행
                '--convert' : 엑셀 파일 > 번역 파일 변환 기능만 사용
                '--update' : 원본 파일 > 엑셀 파일 업데이트 기능만 사용
            - 특정 종류 파일만 선택
                '--adv' : Adv(커뮤) 파일만 변환 또는 업데이트
                '--masterdb' : MasterDB(UI) 파일만 변환 또는 업데이트
                '--generic' : Generic(UI) 파일만 변환
                '--localization' : Localization(UI) 파일만 변환
### Todo

- [x] Adv 처리
- [x] MasterDB 처리
- [x] Generic 처리
- [x] Localization 처리
- [x] .gitignore 추가
- [x] MasterDB Convert 과정에서 미번역값 이 빈 문자열로 변환되어 포함됨
- [ ] Summary 에 업로드하거나 수정된 파일 링크 달기
- [ ] 자동 시트 수정?(업데이트 된 파일 체크 해제 / 추가 된 파일 추가(추가 후 알림을 보내 url을 File Chip 으로 변환해야함))
- [ ] ADV 파일 변환시 강조 표시 내에 공백이 있는 경우 분리