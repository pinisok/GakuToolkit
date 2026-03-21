#!/bin/bash
# backup.sh — run.sh 실행 전 데이터 백업
# 사용법: bash backup.sh

set -e

BACKUP_DIR="backup_$(date '+%Y%m%d_%H%M%S')"
echo "=== 백업 시작: $BACKUP_DIR ==="
mkdir -p "$BACKUP_DIR"

# 1. DB.dat (shelve 캐시)
if [ -f DB.dat ]; then
    cp DB.dat "$BACKUP_DIR/"
    echo "  ✓ DB.dat"
fi

# 2. cache/ (타임스탬프)
if [ -d cache ]; then
    cp -r cache "$BACKUP_DIR/"
    echo "  ✓ cache/"
fi

# 3. output 서브모듈 커밋 해시
if [ -d output/.git ]; then
    git -C output rev-parse HEAD > "$BACKUP_DIR/output_commit_hash.txt"
    echo "  ✓ output 커밋 해시: $(cat $BACKUP_DIR/output_commit_hash.txt)"
fi

# 4. 코드 커밋 해시
git rev-parse HEAD > "$BACKUP_DIR/code_commit_hash.txt"
echo "  ✓ 코드 커밋 해시: $(cat $BACKUP_DIR/code_commit_hash.txt)"

# 5. res/masterdb 변경 가능 데이터 (json은 용량이 크므로 해시만)
if [ -d res/masterdb/gakumasu-diff/json ]; then
    find res/masterdb/gakumasu-diff/json -name "*.json" -exec md5sum {} \; > "$BACKUP_DIR/masterdb_json_hashes.txt" 2>/dev/null
    echo "  ✓ masterdb json 해시 ($(wc -l < $BACKUP_DIR/masterdb_json_hashes.txt)개 파일)"
fi

echo ""
echo "=== 백업 완료: $BACKUP_DIR ==="
echo ""
echo "복원 방법:"
echo "  bash rollback.sh $BACKUP_DIR"
