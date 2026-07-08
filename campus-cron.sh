#!/bin/bash
# campus-cron — single owner of /root/worker/campus invocations.
#
# Pulls the latest assetbundles + master DB so both consumers can read a
# consistent snapshot of /root/worker/campus/cache/:
#   - GakuToolkit/scripts/campus_sync.py  — reads cache/raw, cache/masterYaml
#   - gkms-texture-tools/cronjob.sh       — reads cache/assets
#
# Neither consumer invokes campus on its own anymore: see GakuToolkit/run.sh
# (uses `sync … --skip-campus`) and gkms-texture-tools/cronjob.sh
# (RUN_CAMPUS_FETCH=0 / CAMPUS_AUTO_UPDATE=0 by default). That avoids a race
# where one consumer downloads a new revision into cache/, the other's cleanup
# fires before it's processed, and the revision is silently lost.
#
# Crontab example (every 30 minutes):
#   */30 * * * * /root/worker/GakuToolkit/campus-cron.sh >> /var/log/campus-cron.log 2>&1
#
# Shared lock is the same /tmp/campus.lock that GakuToolkit campus_sync and
# gkms-texture-tools cronjob both honour, so any ad-hoc invocation also
# queues behind this one.
set -euo pipefail

CAMPUS_DIR="${CAMPUS_DIR:-/root/worker/campus}"
CAMPUS_LOCK="${CAMPUS_LOCK:-/tmp/campus.lock}"
CAMPUS_LOCK_TIMEOUT="${CAMPUS_LOCK_TIMEOUT:-1800}"
CAMPUS_FLAGS="${CAMPUS_FLAGS:--ab --imgab -db}"
CAMPUS_AUTO_UPDATE="${CAMPUS_AUTO_UPDATE:-1}"
CAMPUS_BRANCH="${CAMPUS_BRANCH:-main}"
CAMPUS_RUN_CMD="${CAMPUS_RUN_CMD:-env CGO_ENABLED=1 /usr/local/go/bin/go run .}"

echo "[$(date '+%F %T')] campus-cron start"

# Self-update campus source (git pull only — `go run` recompiles on demand).
if [ "$CAMPUS_AUTO_UPDATE" = "1" ] && [ -d "$CAMPUS_DIR/.git" ]; then
  echo "  self-update (branch: $CAMPUS_BRANCH)"
  (
    cd "$CAMPUS_DIR"
    old=$(git rev-parse HEAD)
    git fetch --quiet origin "$CAMPUS_BRANCH"
    new=$(git rev-parse "origin/$CAMPUS_BRANCH")
    if [ "$old" != "$new" ]; then
      echo "  $old → $new (fast-forward)"
      git pull --ff-only --quiet origin "$CAMPUS_BRANCH"
      /usr/local/go/bin/go mod download
    else
      echo "  already at $new"
    fi
  ) || echo "  ⚠ self-update failed — continuing with existing source"
fi

# 갱신 트리거용 스냅숏 — campus 실행 전 버전 지문 (2026-06-11 추가)
# octo_record.json은 mtime이 아니라 내용 hash로 비교한다. campus가 같은
# revision 파일을 다시 쓰기만 한 경우 run.sh가 매 tick마다 재트리거되는 것을 막는다.
SNAP_BEFORE="$(cat "$CAMPUS_DIR/cache/master_version" 2>/dev/null || true)|$(sha256sum "$CAMPUS_DIR/cache/octo_record.json" 2>/dev/null | awk '{print $1}')"

# Run campus under shared lock. Blocking with hard timeout so a hung
# ad-hoc holder cannot silently strand the cron.
(
  flock -w "$CAMPUS_LOCK_TIMEOUT" 9 || {
    echo "  ⚠ campus shared lock timed out after ${CAMPUS_LOCK_TIMEOUT}s"
    exit 1
  }
  echo "$$" >&9
  cd "$CAMPUS_DIR" && eval "$CAMPUS_RUN_CMD $CAMPUS_FLAGS"
) 9>"$CAMPUS_LOCK"

echo "[$(date '+%F %T')] campus-cron done"

# 미디어 GC — 번역에 안 쓰는 usm/acb/awb/mp3 재축적 방지 (2026-06-11 추가)
bash "$(dirname "$0")/scripts/prune_campus_media.sh" || true

# 갱신 감지 시 run.sh 즉시 트리거 (2026-06-11 추가) — 기존엔 하루 2회(11:30/23:30)
# cron만으로 시트 반영이 지연됐다. run.sh는 중복 실행/락 경합 시 즉시 실패하지 않고
# 몇 분 뒤 한 번만 재시도하도록 예약한다.
SNAP_AFTER="$(cat "$CAMPUS_DIR/cache/master_version" 2>/dev/null || true)|$(sha256sum "$CAMPUS_DIR/cache/octo_record.json" 2>/dev/null | awk '{print $1}')"
if [ "$SNAP_BEFORE" != "$SNAP_AFTER" ]; then
  echo "  campus updated — triggering run.sh"
  GK_DIR="$(cd "$(dirname "$0")" && pwd)"
  nohup bash -c "cd '$GK_DIR' && ./run.sh > \"./output_auto_\$(date +%Y%m%d_%H%M).log\" 2>&1" >/dev/null 2>&1 &
fi
