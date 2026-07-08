#!/bin/bash
# prune_campus_media.sh — campus cache/raw에서 번역 파이프라인이 사용하지 않는
# 미디어 파일(동영상 usm, 음성 acb/awb/mp3)을 삭제한다.
#
# 안전 근거: campus 도구는 octo revision 기반으로 다운로드를 결정하므로
# (octo_record.json의 해시 비교) 로컬 파일을 지워도 재다운로드하지 않는다.
# 2026-06-11 실측 검증: 파일 삭제 후 cron 틱에서 "revision is already up to
# date, skip downloading assets" 확인.
#
# campus-cron.sh 마지막 단계에서 호출된다. 1일 이상 지난 파일만 지워서
# 진행 중 fetch와 경합하지 않는다.
set -u
RAW_DIR="${CAMPUS_RAW_DIR:-/root/worker/campus/cache/raw}"
[ -d "$RAW_DIR" ] || exit 0

deleted=$(find "$RAW_DIR" -maxdepth 1 -type f -mtime +1 \
  \( -name "*.usm" -o -name "*.acb" -o -name "*.awb" -o -name "*.mp3" \) \
  -print -delete | wc -l)
[ "$deleted" -gt 0 ] && echo "[prune_campus_media] deleted $deleted media file(s)"
exit 0
