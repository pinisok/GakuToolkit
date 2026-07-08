#!/bin/bash
# notify_run_failure.sh — best-effort Discord alert for GakuToolkit fatal runs.
#
# Usage:
#   scripts/notify_run_failure.sh <component> <exit_code> <detail> [log_path]
#
# Configuration:
#   DISCORD_BOT_TOKEN                    — preferred from env; fallback reads /root/worker/nanoclaw-v2/.env
#   GAKUTOOLKIT_FAILURE_NOTIFY_CHANNEL_ID — destination channel; default matches existing localization notifier
#   GAKUTOOLKIT_NOTIFY_DRY_RUN=1          — print message instead of POSTing to Discord
#
# This script must never fail the caller's original error path. Network/token
# issues are printed and swallowed so run.sh can keep the original exit code.

set -euo pipefail
cd "$(dirname "$0")/.."

component="${1:-run.sh}"
exit_code="${2:-1}"
detail="${3:-}"
log_path="${4:-}"
channel_id="${GAKUTOOLKIT_FAILURE_NOTIFY_CHANNEL_ID:-${LOCALIZATION_NOTIFY_CHANNEL_ID:-1514549283666268300}}"

if [ -z "$log_path" ]; then
    log_path=$( { ls -t output_*.log logs/*.log 2>/dev/null || true; } | head -1 )
fi

now=$(date '+%Y-%m-%d %H:%M:%S %z')
head_sha=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
output_sha=$(git -C output rev-parse --short HEAD 2>/dev/null || echo "unknown")

detail_trimmed=$(printf '%s' "$detail" | head -c 700)
log_tail=""
log_label=""
if [ -n "$log_path" ] && [ -r "$log_path" ]; then
    log_label="$log_path"
    log_tail=$(tail -40 "$log_path" 2>/dev/null | head -c 1400 || true)
fi

content=$(LOG_TAIL="$log_tail" python3 - "$component" "$exit_code" "$detail_trimmed" "$now" "$head_sha" "$output_sha" "$log_label" <<'PY'
import os
import sys
component, exit_code, detail, now, head_sha, output_sha, log_label = sys.argv[1:]
log_tail = os.environ.get("LOG_TAIL", "")
parts = [
    "🚨 **GakuToolkit run failed**",
    f"- component: `{component}`",
    f"- exit_code: `{exit_code}`",
    f"- time: `{now}`",
    f"- GakuToolkit HEAD: `{head_sha}`",
    f"- output HEAD: `{output_sha}`",
]
if detail:
    parts.append(f"- detail: `{detail}`")
if log_label:
    parts.append(f"- log: `{log_label}`")
if log_tail:
    parts.append("```text\n" + log_tail[-1400:] + "\n```")
message = "\n".join(parts)
if len(message) > 1900:
    message = message[:1850] + "\n... (truncated)"
print(message)
PY
)

if [ "${GAKUTOOLKIT_NOTIFY_DRY_RUN:-0}" = "1" ]; then
    echo "[notify-failure] DRY RUN channel ${channel_id}"
    printf '%s\n' "$content"
    exit 0
fi

token="${DISCORD_BOT_TOKEN:-}"
if [ -z "$token" ] && [ -r "/root/worker/nanoclaw-v2/.env" ]; then
    token=$(grep -E '^DISCORD_BOT_TOKEN=' /root/worker/nanoclaw-v2/.env | head -1 | cut -d= -f2- || true)
fi
if [ -z "$token" ]; then
    echo "[notify-failure] DISCORD_BOT_TOKEN missing — skipping" >&2
    exit 0
fi

resp_file="/tmp/notify-gakutoolkit-failure.$$.resp"
trap 'rm -f "$resp_file"' EXIT
body_json=$(python3 -c "import json,sys; print(json.dumps({'content': sys.stdin.read()}))" <<< "$content")

http_status=$(curl -sS -o "$resp_file" -w '%{http_code}' \
    -X POST "https://discord.com/api/v10/channels/${channel_id}/messages" \
    -H "Authorization: Bot ${token}" \
    -H "Content-Type: application/json" \
    -d "$body_json" \
    --max-time 10 || echo "000")

if [ "$http_status" = "200" ] || [ "$http_status" = "201" ]; then
    echo "[notify-failure] posted to channel ${channel_id} for ${component}"
else
    echo "[notify-failure] Discord post FAILED — HTTP ${http_status}" >&2
    cat "$resp_file" >&2 || true
    echo >&2
fi

exit 0
