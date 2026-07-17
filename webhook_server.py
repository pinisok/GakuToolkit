"""GitHub Webhook + internal trigger server for run.sh.

Two trigger sources, one coalescing queue:

  POST /webhook           — GitHub push events (submodule repos)
  POST /internal/trigger  — Bearer-authenticated trigger from Hermes profiles /
                            scripts after a verified Drive upload

Both write into the same durable pending queue and return 202 only after the
queue file has been atomically persisted.
A single background worker drains the queue: while pending, take the
cross-process flock (`/tmp/gakutoolkit.lock`) and run `run.sh`. Multiple
triggers during a run collapse into one extra run. The flock is shared with
the cron-driven `run.sh`, so cron and trigger-driven runs serialize safely.

No request is ever rejected with a conflict — that was the previous design's
silent-loss bug. The worker is allowed to wait up to TRIGGER_FLOCK_TIMEOUT
seconds for the flock; if cron's run is long-running, the trigger queues
and fires as soon as it releases.

Config (env vars):
  WEBHOOK_PORT             — port to listen on (default: 9876)
  WEBHOOK_SECRET           — GitHub HMAC secret (optional, validates /webhook)
  INTERNAL_TRIGGER_TOKEN   — Bearer token for /internal/trigger (required to
                             enable that endpoint; if unset, returns 503)
  TRIGGER_FLOCK_TIMEOUT    — seconds to wait for /tmp/gakutoolkit.lock
                             (default: 1800 = 30 min)
"""

from __future__ import annotations

import datetime
import fcntl
import hashlib
import hmac
import json
import logging
import os
import secrets
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from flask import Flask, abort, jsonify, request

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PORT = int(os.environ.get("WEBHOOK_PORT", 9876))
SECRET = os.environ.get("WEBHOOK_SECRET", "")
INTERNAL_TRIGGER_TOKEN = os.environ.get("INTERNAL_TRIGGER_TOKEN", "").strip()
TRIGGER_FLOCK_TIMEOUT = int(os.environ.get("TRIGGER_FLOCK_TIMEOUT", "1800"))
TRIGGER_RETRY_DELAY = int(os.environ.get(
    "TRIGGER_RETRY_DELAY", os.environ.get("GAKUTOOLKIT_RETRY_DELAY_SECONDS", "300")
))
TRIGGER_RETRY_MAX_ATTEMPTS = int(os.environ.get(
    "TRIGGER_RETRY_MAX_ATTEMPTS", os.environ.get("GAKUTOOLKIT_RETRY_MAX_ATTEMPTS", "12")
))
ENABLE_GITHUB_MIRROR_WEBHOOK = os.environ.get(
    "ENABLE_GITHUB_MIRROR_WEBHOOK", "0"
).strip().lower() in {"1", "true", "yes", "on"}

WORKDIR = Path(__file__).resolve().parent
LOCKFILE = Path("/tmp/gakutoolkit.lock")
LOGFILE = WORKDIR / "webhook.log"
QUEUE_FILE = Path(os.environ.get(
    "WEBHOOK_QUEUE_FILE", str(WORKDIR / ".webhook_queue.json")
))

WATCHED_REPOS = {
    "DreamGallery/Campus-adv-txts",
    "pinisok/gakumas-master-translation",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("webhook")

# ---------------------------------------------------------------------------
# Coalescing trigger queue
# ---------------------------------------------------------------------------
#
# Two layers of synchronization:
#   * Process-local `_worker_lock` — only one drain thread runs at a time
#     inside this Flask process. Coalesces a flood of triggers into a single
#     extra run after the current one finishes.
#   * Cross-process `flock` on LOCKFILE — serializes against the cron-driven
#     run.sh. Worker waits (not non-blocking) so triggers never get dropped.
#
# `_pending` and `_inflight` are persisted atomically. On service restart an
# in-flight batch is moved back to pending; the shared flock prevents overlap
# with a surviving run.sh process, so replay is safe and idempotent.
#
# `_last_triggers` keeps a short tail of recent trigger reasons for the
# /status endpoint — debugging aid only.

_pending: list[dict] = []        # queue of trigger entries awaiting a drain
_inflight: list[dict] = []       # current batch; recovered to pending on restart
_dead_letter: list[dict] = []    # bounded durable record of attempted failures
_pending_lock = threading.Lock()
_worker_lock = threading.Lock()
_run_state = {
    "in_progress": False, "current_trigger_ids": [],
    "last_started_at": None, "last_finished_at": None,
    "last_exit_code": None, "last_log": None, "last_trigger_ids": [],
    "last_failure_at": None, "last_failure_detail": None, "last_failure_log": None,
    "last_retry_at": None, "last_retry_detail": None,
    "runs_total": 0, "runs_failed": 0, "runs_coalesced": 0, "runs_deferred": 0,
}
_run_state_lock = threading.Lock()
_last_triggers: list[dict] = []
_LAST_TRIGGERS_MAX = 20
_DEAD_LETTER_MAX = 100


def _fsync_parent(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    fd = os.open(str(path.parent), flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _persist_queue_locked() -> None:
    """Persist queue state atomically. Caller must hold `_pending_lock`."""
    QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{QUEUE_FILE.name}.", dir=str(QUEUE_FILE.parent)
    )
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "pending": _pending,
                    "inflight": _inflight,
                    "dead_letter": _dead_letter[-_DEAD_LETTER_MAX:],
                },
                fh,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, QUEUE_FILE)
        _fsync_parent(QUEUE_FILE)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def _restore_queue_state() -> int:
    """Load durable state and recover an interrupted batch to pending."""
    if not QUEUE_FILE.exists():
        return 0
    data = json.loads(QUEUE_FILE.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"invalid webhook queue state: {QUEUE_FILE}")

    with _pending_lock:
        pending = list(data.get("pending") or [])
        recovered = list(data.get("inflight") or [])
        dead = list(data.get("dead_letter") or [])[-_DEAD_LETTER_MAX:]

        # Deduplicate by trigger id while placing interrupted work first.
        seen: set[str] = set()
        restored: list[dict] = []
        for entry in recovered + pending:
            tid = str(entry.get("tid", ""))
            if tid and tid in seen:
                continue
            if tid:
                seen.add(tid)
            restored.append(entry)
        _pending[:] = restored
        _inflight.clear()
        _dead_letter[:] = dead
        _last_triggers[:] = (restored + dead)[-_LAST_TRIGGERS_MAX:]
        if recovered:
            _persist_queue_locked()

    if recovered:
        log.warning(
            "recovered %d in-flight webhook trigger(s) after restart",
            len(recovered),
        )
    return len(_pending)


def _new_trigger_id() -> str:
    """Short, log-friendly id. urlsafe base64 of 6 random bytes."""
    return secrets.token_urlsafe(6)


def _enqueue_trigger(reason: dict) -> str:
    """Mark work pending, capture the trigger entry, and ensure a drain
    thread is running. Returns the trigger_id (caller logs it + echoes to
    the HTTP response so callers can grep follow-on log lines)."""
    tid = _new_trigger_id()
    entry = {
        "tid": tid,
        "at": datetime.datetime.now().isoformat(timespec="seconds"),
        **reason,
    }
    with _pending_lock:
        _pending.append(entry)
        _last_triggers.append(entry)
        del _last_triggers[:-_LAST_TRIGGERS_MAX]
        # HTTP 202 is returned only after this fsync+replace succeeds.
        _persist_queue_locked()
    log.info(f"[tid={tid}] enqueued: {reason}")
    threading.Thread(target=_drain, daemon=True, name="trigger-drain").start()
    return tid


def _drain() -> None:
    """Run pending work; coalesce concurrent triggers into at most one
    additional run after the current one. Only one drain thread executes;
    others return immediately because the queue contents are preserved and
    the running worker will see them next iteration.
    """
    if not _worker_lock.acquire(blocking=False):
        log.debug("drain: another worker is active — pending stays queued")
        return
    try:
        rounds = 0
        while True:
            with _pending_lock:
                if not _pending:
                    if rounds > 1:
                        with _run_state_lock:
                            _run_state["runs_coalesced"] += rounds - 1
                        log.info(f"drain: completed {rounds} rounds (coalesced extras)")
                    return
                # Move everything currently pending into a durable in-flight
                # batch. A crash from here through subprocess completion is
                # recovered on service startup.
                batch = list(_pending)
                _pending.clear()
                _inflight[:] = batch
                _persist_queue_locked()
            tids = [e["tid"] for e in batch]
            retry_attempt = max(int(e.get("retry_attempt", 0) or 0) for e in batch)
            log.info(f"drain: starting round={rounds + 1} batch_size={len(batch)} tids={tids}")
            outcome = _run_pipeline(tids, retry_attempt=retry_attempt)

            with _pending_lock:
                _inflight.clear()
                if outcome == "deferred":
                    for entry in batch:
                        entry["retry_attempt"] = retry_attempt + 1
                    _pending[0:0] = batch
                elif outcome in {"failed", "exhausted"}:
                    failed_at = datetime.datetime.now().isoformat(timespec="seconds")
                    _dead_letter.extend(
                        {**entry, "failed_at": failed_at, "outcome": outcome}
                        for entry in batch
                    )
                    del _dead_letter[:-_DEAD_LETTER_MAX]
                _persist_queue_locked()

            if outcome == "deferred":
                timer = threading.Timer(TRIGGER_RETRY_DELAY, _drain)
                timer.daemon = True
                timer.start()
                return
            rounds += 1
    finally:
        _worker_lock.release()


def _acquire_flock_with_wait(timeout: float):
    """Acquire LOCKFILE flock with timeout. Returns the open fd on success,
    None on timeout. The webhook worker holds this for the entire run.sh
    invocation, then releases — matching cron's behavior of using the same
    `/tmp/gakutoolkit.lock` for cross-process serialization.

    Implementation note: we use fcntl directly (not subprocess flock(1)) so
    we own the fd in this process. run.sh is invoked with SKIP_FLOCK=1 so
    its internal `flock -n` block is bypassed — it would otherwise try to
    re-acquire the same lock and refuse with "Already running".
    """
    LOCKFILE.touch(exist_ok=True)
    fd = os.open(str(LOCKFILE), os.O_RDWR)
    deadline = time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except BlockingIOError:
            if time.monotonic() >= deadline:
                os.close(fd)
                return None
            time.sleep(1.0)


def _tail_file(path: Path, lines: int = 20) -> str:
    """Best-effort tail for failure-detail capture."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = min(size, 8192)
            f.seek(size - block)
            data = f.read().decode("utf-8", errors="replace")
        return "\n".join(data.splitlines()[-lines:])
    except OSError as e:
        return f"<could not read {path.name}: {e}>"


def _run_pipeline(trigger_ids: list[str], retry_attempt: int = 0) -> str:
    """Invoke run.sh under the cross-process flock. Blocks up to
    TRIGGER_FLOCK_TIMEOUT for the cron-side runner to release.

    `trigger_ids` is the batch of caller-visible ids — included in log
    lines so an operator can grep `tid=...` across webhook.log and the
    per-run output log to follow one trigger end-to-end.
    """
    started_at = datetime.datetime.now()
    logfile = WORKDIR / f"output_webhook_{started_at:%Y%m%d_%H%M%S}.log"
    tag = f"tids={trigger_ids}" if trigger_ids else "tids=[]"
    log.info(
        f"[{tag}] _run_pipeline start — waiting for flock "
        f"(timeout={TRIGGER_FLOCK_TIMEOUT}s), log={logfile.name}"
    )

    with _run_state_lock:
        _run_state.update({
            "in_progress": True,
            "current_trigger_ids": list(trigger_ids),
            "last_started_at": started_at.isoformat(timespec="seconds"),
            "last_log": logfile.name,
            "last_trigger_ids": list(trigger_ids),
        })

    fd = _acquire_flock_with_wait(TRIGGER_FLOCK_TIMEOUT)
    if fd is None:
        detail = (
            f"timed out waiting {TRIGGER_FLOCK_TIMEOUT}s for {LOCKFILE}. "
            "Another run is still active; scheduling a retry instead of dropping the trigger."
        )
        exhausted = retry_attempt >= TRIGGER_RETRY_MAX_ATTEMPTS
        retry_at = None if exhausted else (
            datetime.datetime.now()
            + datetime.timedelta(seconds=TRIGGER_RETRY_DELAY)
        ).isoformat(timespec="seconds")
        if exhausted:
            log.error(f"[{tag}] _run_pipeline retry exhausted — {detail}")
        else:
            log.warning(f"[{tag}] _run_pipeline deferred — {detail} retry_at={retry_at}")
        finished_at = datetime.datetime.now()
        with _run_state_lock:
            _run_state.update({
                "in_progress": False,
                "current_trigger_ids": [],
                "last_finished_at": finished_at.isoformat(timespec="seconds"),
                "last_retry_at": retry_at,
                "last_retry_detail": detail,
            })
            _run_state["runs_deferred"] += 1
            if exhausted:
                _run_state["runs_failed"] += 1
                _run_state["last_failure_at"] = finished_at.isoformat(timespec="seconds")
                _run_state["last_failure_detail"] = detail
        return "exhausted" if exhausted else "deferred"

    proc = None
    exception_detail = None
    try:
        LOCKFILE.write_text(f"{os.getpid()}\n")
        env = {**os.environ, "SKIP_FLOCK": "1"}
        with open(logfile, "w") as out:
            out.write(
                f"=== webhook-triggered run.sh ===\n"
                f"started_at: {started_at.isoformat(timespec='seconds')}\n"
                f"trigger_ids: {trigger_ids}\n"
                f"================================\n\n"
            )
            out.flush()
            proc = subprocess.run(
                ["bash", "run.sh"],
                cwd=str(WORKDIR),
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
            )
    except Exception as e:
        exception_detail = f"subprocess raised {type(e).__name__}: {e}"
        log.exception(f"[{tag}] _run_pipeline subprocess failure")
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    finished_at = datetime.datetime.now()
    duration = (finished_at - started_at).total_seconds()
    exit_code = proc.returncode if proc is not None else -1
    failed = exception_detail is not None or exit_code != 0

    with _run_state_lock:
        _run_state.update({
            "in_progress": False,
            "current_trigger_ids": [],
            "last_finished_at": finished_at.isoformat(timespec="seconds"),
            "last_exit_code": exit_code,
        })
        _run_state["runs_total"] += 1
        if failed:
            _run_state["runs_failed"] += 1
            _run_state["last_failure_at"] = finished_at.isoformat(timespec="seconds")
            _run_state["last_failure_log"] = logfile.name
            _run_state["last_failure_detail"] = (
                exception_detail
                or f"run.sh exited with code {exit_code}. Tail of {logfile.name}:\n"
                f"{_tail_file(logfile, lines=20)}"
            )

    level = log.error if failed else log.info
    level(
        f"[{tag}] _run_pipeline done — exit={exit_code} "
        f"duration={duration:.1f}s log={logfile.name}"
        + (" (FAILED — see /status for detail)" if failed else "")
    )
    return "failed" if failed else "succeeded"


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _verify_signature(payload: bytes, signature: str) -> bool:
    """GitHub webhook HMAC-SHA256 signature. Skips when SECRET is unset."""
    if not SECRET:
        return True
    if not signature or not signature.startswith("sha256="):
        return False
    expected = hmac.new(SECRET.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)


def _verify_bearer(header_value: str) -> bool:
    if not INTERNAL_TRIGGER_TOKEN:
        return False
    if not header_value or not header_value.startswith("Bearer "):
        return False
    presented = header_value[len("Bearer "):].strip()
    return hmac.compare_digest(presented, INTERNAL_TRIGGER_TOKEN)


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/webhook", methods=["POST"])
def github_webhook():
    """Handle GitHub push events. Enqueues via the coalescing queue."""
    signature = request.headers.get("X-Hub-Signature-256", "")
    if not _verify_signature(request.data, signature):
        log.warning("Invalid GitHub signature")
        abort(403, "Invalid signature")

    event = request.headers.get("X-GitHub-Event", "")
    if event == "ping":
        log.info("Ping received")
        return jsonify({"status": "pong"})

    if event != "push":
        log.info(f"Ignored event: {event}")
        return jsonify({"status": "ignored", "event": event})

    if not ENABLE_GITHUB_MIRROR_WEBHOOK:
        log.info(
            "Ignored GitHub mirror push webhook because campus-primary mode "
            "owns original-data updates"
        )
        return jsonify({
            "status": "ignored",
            "reason": "github_mirror_webhook_disabled_campus_primary_mode",
        })

    payload = request.get_json(silent=True)
    if not payload:
        abort(400, "Invalid JSON")

    repo = payload.get("repository", {}).get("full_name", "")
    ref = payload.get("ref", "")
    log.info(f"Push event: {repo} ref={ref}")

    if repo not in WATCHED_REPOS:
        return jsonify({"status": "ignored", "reason": "repo_not_watched"})
    if ref not in ("refs/heads/main", "refs/heads/master"):
        return jsonify({"status": "ignored", "reason": "not_main_branch"})

    tid = _enqueue_trigger({"source": "github", "repo": repo, "ref": ref})
    return jsonify({"status": "queued", "tid": tid, "source": "github", "repo": repo}), 202


@app.route("/internal/trigger", methods=["POST"])
def internal_trigger():
    """Trigger a run.sh execution from a trusted internal Hermes caller after
    a verified Drive upload. Request is always queued — never
    rejected with a conflict — so concurrent cron runs do not cause loss.

    Body (optional JSON): { "reason": "...", "pipeline": "...", "files": [...] }
    """
    if not INTERNAL_TRIGGER_TOKEN:
        log.warning("Internal trigger called but INTERNAL_TRIGGER_TOKEN unset")
        abort(503, "Internal trigger not configured")

    if not _verify_bearer(request.headers.get("Authorization", "")):
        log.warning("Internal trigger: invalid bearer")
        abort(401, "Invalid token")

    body = request.get_json(silent=True) or {}
    reason = {
        "source": "internal",
        "reason": body.get("reason", ""),
        "pipeline": body.get("pipeline", ""),
        "files": body.get("files", []),
        "caller": request.headers.get("X-Caller", ""),
    }
    tid = _enqueue_trigger(reason)
    return jsonify({"status": "queued", "tid": tid, **reason}), 202


@app.route("/status", methods=["GET"])
def status():
    """Compact diagnostic snapshot. Look here first when 'did my trigger run'
    is the question; cross-reference `tid` against webhook.log / the named
    last_log file for full detail.
    """
    with _pending_lock:
        pending_count = len(_pending)
        pending_tids = [e.get("tid") for e in _pending]
        inflight_tids = [e.get("tid") for e in _inflight]
        dead_letter_count = len(_dead_letter)
        recent = list(_last_triggers)
    with _run_state_lock:
        run_state = dict(_run_state)
    return jsonify({
        "pending_count": pending_count,
        "pending_tids": pending_tids,
        "inflight_tids": inflight_tids,
        "dead_letter_count": dead_letter_count,
        "queue_file": QUEUE_FILE.name,
        "run_state": run_state,
        "recent_triggers": recent[-10:],
        "lock_exists": LOCKFILE.exists(),
        "webhook_log": str(LOGFILE.name),
        "github_mirror_webhook_enabled": ENABLE_GITHUB_MIRROR_WEBHOOK,
        "watched_repos": sorted(WATCHED_REPOS) if ENABLE_GITHUB_MIRROR_WEBHOOK else [],
    })


@app.route("/logs/recent", methods=["GET"])
def logs_recent():
    """Return the last N lines of webhook.log and the most recent run output
    log (when present). Lets an operator diagnose without ssh-ing in. No
    auth — content is internal-only since the host bind is LAN/bridge only.
    Override count via ?lines=N (default 50, max 500).
    """
    try:
        lines = max(1, min(int(request.args.get("lines", "50")), 500))
    except ValueError:
        lines = 50

    last_run = None
    with _run_state_lock:
        last_log_name = _run_state.get("last_log")
    if last_log_name:
        last_run_path = WORKDIR / last_log_name
        if last_run_path.exists():
            last_run = {
                "name": last_log_name,
                "tail": _tail_file(last_run_path, lines=lines),
            }

    return jsonify({
        "webhook_log": {
            "name": LOGFILE.name,
            "tail": _tail_file(LOGFILE, lines=lines),
        },
        "last_run_log": last_run,
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    restored_pending = _restore_queue_state()
    log.info(f"Starting webhook server on port {PORT}")
    log.info(
        "GitHub mirror webhook: %s%s",
        "enabled" if ENABLE_GITHUB_MIRROR_WEBHOOK else "disabled (campus-primary mode)",
        f"; watched repos: {WATCHED_REPOS}" if ENABLE_GITHUB_MIRROR_WEBHOOK else "",
    )
    log.info(f"Work directory: {WORKDIR}")
    log.info(f"Internal trigger: {'enabled' if INTERNAL_TRIGGER_TOKEN else 'DISABLED (set INTERNAL_TRIGGER_TOKEN)'}")
    log.info(
        f"flock timeout: {TRIGGER_FLOCK_TIMEOUT}s; retry delay: {TRIGGER_RETRY_DELAY}s; "
        f"retry max attempts: {TRIGGER_RETRY_MAX_ATTEMPTS}"
    )
    if restored_pending:
        log.warning("starting drain for %d restored trigger(s)", restored_pending)
        threading.Thread(target=_drain, daemon=True, name="startup-drain").start()
    app.run(host="0.0.0.0", port=PORT, debug=False)
