#!/bin/bash
# agent_upload.sh — atomic upload + verify + trigger for nanoclaw agents.
#
# Operationally we've observed:
#   - rclone copy returning success but the file never landing (network glitch,
#     Drive quota, token refresh racing the request)
#   - agents calling the webhook but not checking response (404/401/500 swallowed
#     by `|| true` style invocations) so the trigger gets dropped silently
#   - one operation succeeding and the other failing, leaving Drive ↔ output
#     out of sync until the next 12-hour cron tick
#
# This script wraps all three steps:
#   1. rclone copy with --checksum
#   2. rclone lsjson to verify the destination file exists + matches by sha
#   3. POST /internal/trigger and assert 202 response
#
# Any failure exits non-zero so the calling agent can surface it in their
# channel report.
#
# Usage from inside a NanoClaw agent container:
#   ./scripts/agent_upload.sh \
#       "res/drive/text assets/cidol/adv_X.xlsx" \
#       "gakumas:Gakumas_KR/text assets/cidol/adv_X.xlsx" \
#       --reason "adv batch translate" --pipeline adv
#
# Env:
#   RCLONE_CONFIG          path to agent rclone config (required)
#   WEBHOOK_HOST           default 172.17.0.1
#   WEBHOOK_PORT           default 9876
#   WEBHOOK_TOKEN_FILE     default /workspace/extra/gakutoolkit/.webhook-token

set -euo pipefail

SRC=""
DST=""
REASON="agent upload"
PIPELINE=""
CALLER="${HOSTNAME:-unknown}"

while [ $# -gt 0 ]; do
    case "$1" in
        --reason)   REASON="$2"; shift 2 ;;
        --pipeline) PIPELINE="$2"; shift 2 ;;
        --caller)   CALLER="$2"; shift 2 ;;
        --)         shift; break ;;
        -*)         echo "unknown flag: $1" >&2; exit 2 ;;
        *)
            if [ -z "$SRC" ]; then SRC="$1"
            elif [ -z "$DST" ]; then DST="$1"
            else echo "extra positional: $1" >&2; exit 2
            fi
            shift
            ;;
    esac
done

if [ -z "$SRC" ] || [ -z "$DST" ]; then
    echo "usage: agent_upload.sh <local-src> <rclone-dst> [--reason X] [--pipeline Y] [--caller Z]" >&2
    exit 2
fi
if [ ! -f "$SRC" ]; then
    echo "src not found: $SRC" >&2
    exit 1
fi

WEBHOOK_HOST="${WEBHOOK_HOST:-172.17.0.1}"
WEBHOOK_PORT="${WEBHOOK_PORT:-9876}"
WEBHOOK_TOKEN_FILE="${WEBHOOK_TOKEN_FILE:-/workspace/extra/gakutoolkit/.webhook-token}"

# ── 1. rclone copy ────────────────────────────────────────────────────────────
echo "[upload] copy $SRC → $DST"
rclone copyto "$SRC" "$DST" \
    --drive-shared-with-me --checksum --no-check-certificate \
    || { echo "[upload] rclone copy failed" >&2; exit 1; }

# ── 2. verify destination ─────────────────────────────────────────────────────
# rclone copyto's exit code only confirms it tried — silent drops still happen.
# lsjson the parent and look for the exact filename.
dst_parent="$(dirname "$DST")"
dst_name="$(basename "$DST")"
listing=$(rclone lsjson "$dst_parent" --drive-shared-with-me --no-check-certificate 2>/dev/null || true)
if ! echo "$listing" | grep -q "\"Name\":\"$dst_name\""; then
    echo "[upload] verify FAILED — $dst_name not found in $dst_parent after copy" >&2
    exit 1
fi

# Optional sha match (cheap sanity — protects against partial uploads)
if command -v sha1sum >/dev/null && command -v jq >/dev/null; then
    local_sha=$(sha1sum "$SRC" | awk '{print $1}')
    remote_sha=$(echo "$listing" | jq -r ".[] | select(.Name==\"$dst_name\") | .Hashes.sha1 // empty" 2>/dev/null || true)
    if [ -n "$remote_sha" ] && [ "$local_sha" != "$remote_sha" ]; then
        echo "[upload] verify FAILED — sha1 mismatch local=$local_sha remote=$remote_sha" >&2
        exit 1
    fi
fi
echo "[upload] verify ✓ $dst_name landed in $dst_parent"

# ── 3. fire webhook trigger ───────────────────────────────────────────────────
if [ ! -r "$WEBHOOK_TOKEN_FILE" ]; then
    echo "[trigger] token file unreadable ($WEBHOOK_TOKEN_FILE) — skipping" >&2
    exit 0   # upload itself succeeded; cron is the fallback
fi
token=$(cat "$WEBHOOK_TOKEN_FILE")
if [ -z "$token" ]; then
    echo "[trigger] token empty — skipping" >&2
    exit 0
fi

body=$(printf '{"reason":"%s","pipeline":"%s","files":["%s"]}' \
    "$REASON" "$PIPELINE" "$dst_name")
http_status=$(curl -sS -o /tmp/agent_upload.resp -w '%{http_code}' \
    -X POST "http://${WEBHOOK_HOST}:${WEBHOOK_PORT}/internal/trigger" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -H "X-Caller: $CALLER" \
    -d "$body" \
    --max-time 10 || echo "000")

if [ "$http_status" != "202" ]; then
    echo "[trigger] FAILED — HTTP $http_status" >&2
    cat /tmp/agent_upload.resp >&2 || true
    echo
    exit 1
fi
echo "[trigger] ✓ HTTP 202 — $(cat /tmp/agent_upload.resp)"
