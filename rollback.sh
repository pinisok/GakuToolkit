#!/bin/bash
# rollback.sh — backup.sh로 만든 백업에서 복원
# 사용법: bash rollback.sh backup_20260322_030000

set -e

BACKUP_DIR="${1:?사용법: bash rollback.sh <백업 디렉토리>}"

if [ ! -d "$BACKUP_DIR" ]; then
    echo "❌ 백업 디렉토리 없음: $BACKUP_DIR"
    exit 1
fi

echo "=== 복원 시작: $BACKUP_DIR ==="

# 1. DB.dat
if [ -f "$BACKUP_DIR/DB.dat" ]; then
    cp "$BACKUP_DIR/DB.dat" DB.dat
    echo "  ✓ DB.dat 복원"
fi

# 2. cache/
if [ -d "$BACKUP_DIR/cache" ]; then
    rm -rf cache
    cp -r "$BACKUP_DIR/cache" cache
    echo "  ✓ cache/ 복원"
fi

# 3. output 서브모듈 롤백
if [ -f "$BACKUP_DIR/output_commit_hash.txt" ]; then
    HASH=$(cat "$BACKUP_DIR/output_commit_hash.txt")
    echo ""
    echo "  ⚠ output 서브모듈 롤백이 필요하면 수동 실행:"
    echo "    cd output && git reset --hard $HASH && cd .."
    echo "    (원격에 이미 push된 경우 force push 필요: git push -f origin main)"
fi

# 4. 코드 롤백
if [ -f "$BACKUP_DIR/code_commit_hash.txt" ]; then
    HASH=$(cat "$BACKUP_DIR/code_commit_hash.txt")
    echo ""
    echo "  ⚠ 코드 롤백이 필요하면 수동 실행:"
    echo "    git reset --hard $HASH"
fi

# 5. masterdb json 재생성 (submodule update로 복원 가능)
echo ""
echo "  ⚠ masterdb json은 git submodule update로 복원 가능:"
echo "    git submodule update --init --remote --recursive"

echo ""
echo "=== 복원 완료 ==="
echo ""
echo "Google Drive 데이터는 자동 복원 불가."
echo "필요 시 rclone으로 수동 복원하거나, Drive 버전 기록에서 복원하세요."
