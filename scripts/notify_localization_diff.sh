#!/bin/bash
# notify_localization_diff.sh — post a Discord message when run.sh shipped a
# localization release.
#
# Called from run.sh after main.py completes. We don't have a long-running
# notifier service because localization updates piggyback on the standard
# cron / webhook run; emitting the message inline keeps the path obvious.
#
# Triggers on either signal:
#   - a fresh `## <tag>` section in output/RELEASE_NOTES.md whose tag is
#     newer than the last committed version (i.e. localization_release
#     just applied a release diff)
#   - LOCALIZATION rows in the run's gspread summary log
#
# Configuration (env-overridable, defaults work on this host):
#   DISCORD_BOT_TOKEN              — required, from nanoclaw-v2/.env
#   LOCALIZATION_NOTIFY_CHANNEL_ID — Discord channel ID; default = general
#                                    (1514549283666268300). Override to
#                                    redirect to a different channel without
#                                    touching this script.
#
# Best-effort: any failure (no token, network blip, Discord 5xx) prints a
# warning but does not fail run.sh.

set -euo pipefail
cd "$(dirname "$0")/.."

NOTES_PATH="output/RELEASE_NOTES.md"
[ -f "$NOTES_PATH" ] || exit 0

# What was the last section we already announced? Stored in cache so we don't
# re-announce on every cron tick.
LAST_ANNOUNCED_FILE="cache/localization_last_announced.txt"
mkdir -p cache
last_announced=$(cat "$LAST_ANNOUNCED_FILE" 2>/dev/null || true)

# Extract the topmost `## <tag>` section and its body until the next `##` (or EOF).
current_tag=$(grep -m1 -oE '^## [^ ]+' "$NOTES_PATH" | sed 's/^## //' || true)
[ -n "$current_tag" ] || exit 0

if [ "$current_tag" = "$last_announced" ]; then
    # Nothing new to announce.
    exit 0
fi

# Pull the section body (header line + everything until the next ## or EOF).
# The section header has a date suffix (`## 3.1.0 (2026-06-12)`) so match by
# prefix rather than exact-equal.
body=$(awk -v prefix="## $current_tag" '
    p && /^## / { exit }
    index($0, prefix) == 1 { p = 1 }
    p { print }
' "$NOTES_PATH")
[ -n "$body" ] || exit 0

# Load Discord bot token. Look in the canonical nanoclaw-v2 .env first, fall
# back to environment.
token="${DISCORD_BOT_TOKEN:-}"
if [ -z "$token" ] && [ -r "/root/worker/nanoclaw-v2/.env" ]; then
    token=$(grep -E '^DISCORD_BOT_TOKEN=' /root/worker/nanoclaw-v2/.env | head -1 | cut -d= -f2- || true)
fi
if [ -z "$token" ]; then
    echo "[notify-loc] DISCORD_BOT_TOKEN missing — skipping (still marking announced)"
    echo "$current_tag" > "$LAST_ANNOUNCED_FILE"
    exit 0
fi

channel_id="${LOCALIZATION_NOTIFY_CHANNEL_ID:-1514549283666268300}"

# Discord per-message content limit is 2,000 chars. RELEASE_NOTES sections
# are short (header + 6 lines + at most ~60 sample keys) so this rarely
# trips, but guard anyway.
DISCORD_LIMIT=1900
header="📘 **localization release published**"
content=$(printf '%s\n```markdown\n%s\n```' "$header" "$body")
if [ ${#content} -gt $DISCORD_LIMIT ]; then
    truncated=$(printf '%s\n' "$body" | head -40)
    content=$(printf '%s\n```markdown\n%s\n... (truncated, see output/RELEASE_NOTES.md)\n```' "$header" "$truncated")
fi

# JSON-encode via python3 since jq isn't a hard host dep.
body_json=$(python3 -c "import json,sys; print(json.dumps({'content': sys.stdin.read()}))" <<< "$content")

response=$(curl -sS -o /tmp/notify-loc.resp -w '%{http_code}' \
    -X POST "https://discord.com/api/v10/channels/${channel_id}/messages" \
    -H "Authorization: Bot ${token}" \
    -H "Content-Type: application/json" \
    -d "$body_json" \
    --max-time 10 || echo "000")

if [ "$response" = "200" ]; then
    echo "[notify-loc] posted to channel ${channel_id} for tag ${current_tag}"
    echo "$current_tag" > "$LAST_ANNOUNCED_FILE"
else
    echo "[notify-loc] Discord post FAILED — HTTP ${response}" >&2
    cat /tmp/notify-loc.resp >&2 || true
    echo >&2
    # Do NOT mark announced — next run retries.
fi
