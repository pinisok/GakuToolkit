#!/bin/bash
set -e

echo $(date "+%Y-%m-%d %H:%M:%S")

# 잠금 파일 — 동시 실행 방지
LOCKFILE="/tmp/gakutoolkit.lock"
if [ -f "$LOCKFILE" ]; then
    echo "Already running (lockfile exists: $LOCKFILE)"
    exit 1
fi
trap 'rm -f "$LOCKFILE"' EXIT
touch "$LOCKFILE"

# 서브모듈 업데이트
git submodule update --init --remote --recursive

# masterdb 캐시 정리
rm -f ./res/masterdb/data/*
rm -rf ./res/masterdb/gakumasu-diff/json
rm -rf ./res/masterdb/pretranslate_todo/

# 7일 이상 된 로그 파일 정리
find . -maxdepth 1 -name "output_python_*.log" -mtime +7 -delete 2>/dev/null || true

# 메인 실행
python3 main.py

# 변경사항이 있을 때만 커밋/푸시
cd output
if [ -n "$(git status --porcelain)" ]; then
    git add --all
    git commit -m "Update translate $(date '+%Y-%m-%d %H:%M')"
    git push origin main
else
    echo "No changes to push"
fi
