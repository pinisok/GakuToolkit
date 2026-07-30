#!/bin/bash
set -Eeuo pipefail

# cron/systemd may run with Hermes/system Python first in PATH.  Pin this
# workflow to the project virtualenv when it exists so imports such as
# rclone_python resolve the same way as manual maintenance runs.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if [ -x "$SCRIPT_DIR/.venv/bin/python3" ]; then
    # Prefer the project venv, but do not use it for legacy GNU dbm-backed
    # MasterDB DB.dat when that Python lacks _gdbm. In that case shelve.open()
    # fails with: "db type is dbm.gnu, but the module is not available".
    if [ -f "$SCRIPT_DIR/DB.dat" ] && "$SCRIPT_DIR/.venv/bin/python3" - <<'PY' >/dev/null 2>&1
import dbm, importlib.util, sys
sys.exit(0 if dbm.whichdb('DB.dat') == 'dbm.gnu' and importlib.util.find_spec('_gdbm') is None else 1)
PY
    then
        export PATH="/usr/bin:$PATH"
        echo "$(date '+%Y-%m-%d %H:%M:%S') using /usr/bin/python3 for GNU dbm DB.dat compatibility"
    else
        export PATH="$SCRIPT_DIR/.venv/bin:$PATH"
    fi
fi

FAILURE_NOTIFIED=0
notify_failure() {
    local component="${1:-run.sh}"
    local exit_code="${2:-1}"
    local detail="${3:-}"
    local latest_log=""

    if [ "${FAILURE_NOTIFIED:-0}" = "1" ]; then
        return 0
    fi
    FAILURE_NOTIFIED=1

    set +e
    latest_log=$( { ls -t "$SCRIPT_DIR"/output_*.log "$SCRIPT_DIR"/logs/*.log 2>/dev/null || true; } | head -1 )
    bash "$SCRIPT_DIR/scripts/notify_run_failure.sh" "$component" "$exit_code" "$detail" "$latest_log"
    local notify_rc=$?
    set -e
    if [ "$notify_rc" -ne 0 ]; then
        echo "⚠ failure Discord notification skipped"
    fi
    return 0
}

RETRY_DELAY_SECONDS="${GAKUTOOLKIT_RETRY_DELAY_SECONDS:-300}"
RETRY_MAX_ATTEMPTS="${GAKUTOOLKIT_RETRY_MAX_ATTEMPTS:-12}"
RETRY_STATE_DIR="${GAKUTOOLKIT_RETRY_STATE_DIR:-/root/worker/.hermes-routing/state}"
RETRY_MARKER="${RETRY_STATE_DIR}/gakutoolkit-run.retry"
RETRY_GUARD="${RETRY_STATE_DIR}/gakutoolkit-run.retry.lock"

schedule_retry() {
    local reason="${1:-lock busy}"
    local attempt="${GAKUTOOLKIT_RETRY_ATTEMPT:-0}"
    if ! [[ "$attempt" =~ ^[0-9]+$ ]]; then
        attempt=0
    fi
    if [ "$attempt" -ge "$RETRY_MAX_ATTEMPTS" ]; then
        echo "❌ retry exhausted after ${attempt} attempt(s): ${reason}"
        return 1
    fi

    local next_attempt=$((attempt + 1))
    local now next_at existing_at
    now=$(date +%s)
    next_at=$((now + RETRY_DELAY_SECONDS))
    mkdir -p "$RETRY_STATE_DIR"

    (
        flock -n 9 || {
            echo "  retry scheduling already in progress — ${reason}"
            exit 0
        }
        existing_at=$(awk -F= '$1=="scheduled_epoch" {print $2}' "$RETRY_MARKER" 2>/dev/null | tail -1 || true)
        if [[ "$existing_at" =~ ^[0-9]+$ ]] && [ "$existing_at" -gt "$now" ]; then
            echo "  retry already scheduled for $(date -d "@${existing_at}" '+%F %T') — ${reason}"
            exit 0
        fi
        {
            echo "scheduled_epoch=${next_at}"
            echo "attempt=${next_attempt}"
            printf 'reason=%s\n' "$reason"
        } > "$RETRY_MARKER"
        echo "  scheduling retry in ${RETRY_DELAY_SECONDS}s (attempt ${next_attempt}/${RETRY_MAX_ATTEMPTS}): ${reason}"
        nohup env \
            GAKUTOOLKIT_RETRY_ATTEMPT="$next_attempt" \
            GAKUTOOLKIT_RETRY_REASON="$reason" \
            bash -c 'exec 9>&- 200>&- 201>&-; sleep "$1"; rm -f "$2"; cd "$3" && ./run.sh > "./output_retry_$(date +%Y%m%d_%H%M%S).log" 2>&1' \
            _ "$RETRY_DELAY_SECONDS" "$RETRY_MARKER" "$SCRIPT_DIR" >/dev/null 2>&1 &
    ) 9>"$RETRY_GUARD"
}

run_campus_sync_or_retry() {
    local target="$1"
    local tmp rc
    tmp=$(mktemp)
    set +e
    GAKUTOOLKIT_SUPPRESS_POST_SYNC_TRIGGER=1 \
        CAMPUS_LOCK_TIMEOUT="${GAKUTOOLKIT_CAMPUS_SYNC_LOCK_WAIT:-30}" \
        python3 scripts/campus_sync.py sync "$target" --skip-campus 2>&1 | tee "$tmp"
    rc=${PIPESTATUS[0]}
    set -e

    if [ "$rc" -eq 0 ]; then
        rm -f "$tmp"
        return 0
    fi

    if grep -Eq 'timed out after [0-9]+s waiting for .*/?campus\.lock|campus shared lock timed out|/tmp/campus\.lock.*abort' "$tmp"; then
        if schedule_retry "campus lock busy during ${target} sync"; then
            rm -f "$tmp"
            exit 0
        fi
    fi

    echo "❌ ${target} sync 실패"
    notify_failure "${target} sync" "$rc" "campus_sync.py sync ${target} --skip-campus failed"
    rm -f "$tmp"
    exit "$rc"
}

trap 'rc=$?; line=$LINENO; cmd=$BASH_COMMAND; notify_failure "run.sh" "$rc" "line ${line}: ${cmd}"; exit "$rc"' ERR

echo $(date "+%Y-%m-%d %H:%M:%S")

# 잠금 — 동시 실행 방지 (flock 기반: 프로세스가 죽으면 커널이 자동 해제)
# 파일 자체는 webhook_server.py / submodule_watcher.py 가 PID 검증용으로 읽으므로
# 정상 종료 시에만 trap 으로 정리. 비정상 종료 시에도 flock 은 자동 해제되며,
# webhook/watcher 가 dead PID 감지 후 stale 락 정리.
#
# SKIP_FLOCK=1 이면 자체 잠금을 건너뛴다 — webhook_server.py 의 worker 가 이미
# fcntl.flock 으로 같은 LOCKFILE 을 잡고 호출하기 때문. 중복 시도 시 "Already
# running" 으로 빠지면 trigger queue 가 매번 no-op 이 되어 의미가 사라짐.
LOCKFILE="/tmp/gakutoolkit.lock"
if [ "${SKIP_FLOCK:-0}" != "1" ]; then
    LOCK_FD=200
    eval "exec ${LOCK_FD}>\"\$LOCKFILE\""
    if ! flock -n "$LOCK_FD"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Already running (lock held by another process) — scheduling retry"
        if schedule_retry "gakutoolkit lock held by another run"; then
            exit 0
        fi
        notify_failure "run.sh lock" 75 "gakutoolkit lock stayed busy and retry attempts were exhausted"
        exit 75
    fi
    echo "$$" > "$LOCKFILE"
    trap 'rm -f "$LOCKFILE"' EXIT
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') SKIP_FLOCK=1 — lock managed by caller (webhook_server)"
fi

# repo-local 공유 락 (2026-06-11 추가) — NanoClaw 번역 에이전트 컨테이너는 호스트
# /tmp 를 볼 수 없으므로, res/drive 를 만지는 모든 주체(이 스크립트 + 컨테이너의
# rclone sync/apply/upload)는 이 파일로 직렬화한다. 컨테이너 쪽 사용법:
#   flock /workspace/extra/gakutoolkit/.shared.lock -c '<명령>'
SHARED_LOCK_FD=201
eval "exec ${SHARED_LOCK_FD}>\"./.shared.lock\""
SHARED_LOCK_TIMEOUT="${GAKUTOOLKIT_SHARED_LOCK_WAIT:-30}"
if ! flock -w "$SHARED_LOCK_TIMEOUT" "$SHARED_LOCK_FD"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') shared lock timeout after ${SHARED_LOCK_TIMEOUT}s — scheduling retry"
    if schedule_retry "GakuToolkit shared lock busy"; then
        exit 0
    fi
    notify_failure "shared lock" 75 "GakuToolkit shared lock stayed busy and retry attempts were exhausted"
    exit 75
fi

# 데이터 동기화 — campus 호출은 /root/worker/GakuToolkit/campus-cron.sh 가 단일
# 소유자. 여기서는 단순 reader (campus 미호출). cache 가 비어있거나 너무 옛 데이터면
# campus_sync 가 "source empty" 로 abort 하므로 다음 campus-cron tick 후 재시도.
# campus-cron 이 실패해 cache 가 stale 상태이면 fallback 으로 git submodule 사용 가능:
#   python3 scripts/campus_sync.py fallback masterdb
#   python3 scripts/campus_sync.py fallback adv
run_campus_sync_or_retry masterdb
run_campus_sync_or_retry adv

# campus diff 별도 알림은 중복/스팸 방지를 위해 여기서 보내지 않는다.
# run.sh 내부 campus_sync 호출은 GAKUTOOLKIT_SUPPRESS_POST_SYNC_TRIGGER=1 로
# webhook self-trigger를 막고, 실제 처리는 현재 run.sh 한 번에서 끝낸다.

# output 서브모듈은 여전히 git (push 대상이므로 git 워크플로우 유지).
# `.gitmodules`의 SSH URL을 매 실행마다 복원해 독립 HTTPS clone으로 바뀐
# 작업 복사본도 비대화형 서비스 환경에서 다시 정상 push할 수 있게 한다.
# 로컬 main이 origin/main보다 앞서 있으면 이전 push 실패 커밋을 먼저
# 재전송하며, dirty/diverged 상태는 자동 reset하지 않고 fail-closed 한다.
OUTPUT_ORIGIN_URL=$(git config -f "$SCRIPT_DIR/.gitmodules" --get submodule.output.url)
if [ -z "$OUTPUT_ORIGIN_URL" ]; then
    echo "❌ .gitmodules에서 output origin URL을 찾을 수 없음" >&2
    exit 72
fi
git submodule sync -- output
git submodule update --init -- output
bash "$SCRIPT_DIR/scripts/prepare_output_repo.sh" "$SCRIPT_DIR/output" "$OUTPUT_ORIGIN_URL"

# masterdb 변환 결과 정리 (campus의 orig 이외 산출물)
rm -f ./res/masterdb/data/*
rm -rf ./res/masterdb/gakumasu-diff/json
rm -rf ./res/masterdb/pretranslate_todo/

# 7일 이상 된 로그 파일 정리 (run.sh의 output_*, webhook_server 의 output_webhook_*,
# webhook 자체 로그 회전 별도). webhook.log 는 systemd journal 이 함께 잡고 있으므로
# 너무 커지지 않도록 30일 컷.
find . -maxdepth 1 -name "output_python_*.log" -mtime +7 -delete 2>/dev/null || true
find . -maxdepth 1 -name "output_webhook_*.log" -mtime +7 -delete 2>/dev/null || true
find . -maxdepth 1 -name "output_auto_*.log" -mtime +7 -delete 2>/dev/null || true
find . -maxdepth 1 -name "output_retry_*.log" -mtime +7 -delete 2>/dev/null || true
find ./logs -maxdepth 1 -name "*.log" -mtime +30 -delete 2>/dev/null || true

# output 서브모듈 현재 상태 기록 (Phase 1 실패 시 복구용)
OUTPUT_HEAD=""
if git -C output rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    OUTPUT_HEAD=$(git -C output rev-parse HEAD 2>/dev/null || echo "")
fi

# 메인 실행
if python3 main.py; then
    main_rc=0
else
    main_rc=$?
fi
if [ "$main_rc" -ne 0 ]; then
    echo "❌ main.py 실패 — output 복구 중"
    if [ -n "$OUTPUT_HEAD" ] && git -C output rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git -C output reset --hard "$OUTPUT_HEAD" >/dev/null 2>&1 || true
        git -C output clean -fd >/dev/null 2>&1 || true
        echo "  ✓ output 서브모듈을 실행 전 상태로 복원"
    fi
    notify_failure "main.py" "$main_rc" "main.py failed; output restored to ${OUTPUT_HEAD:-unknown}"
    exit "$main_rc"
fi

# 변경사항이 있을 때만 커밋/푸시 — version.txt 만 dirty 인 경우 (= Convert 가 돌았지만
# 출력 JSON 이 git HEAD 와 byte-identical 인 no-op run) 는 의미 없는 commit 생성하므로
# 차단. main.py 는 ADV_FILE 비어있지 않으면 무조건 version.txt 에 timestamp 적어서
# 이런 케이스가 정기적으로 발생함.
cd output
real_changes=$(git status --porcelain | grep -v '^.M version\.txt$' || true)
if [ -n "$real_changes" ]; then
    git add --all
    git commit -m "Update translate $(date '+%Y-%m-%d %H:%M')"
    git push origin main
elif [ -n "$(git status --porcelain)" ]; then
    echo "no-op run — only version.txt changed, skipping commit"
    git checkout -- version.txt 2>/dev/null || true
else
    echo "No changes to push"
fi
cd ..

# Discord 알림 — localization 새 release 가 RELEASE_NOTES.md 에 추가됐을 때만.
# 이미 보고한 tag 는 cache 로 추적해 중복 안 보냄. 실패시 next tick 에서 재시도.
./scripts/notify_localization_diff.sh || echo "⚠ localization Discord notification skipped"
