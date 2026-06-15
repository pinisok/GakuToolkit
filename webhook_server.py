"""GitHub Webhook + internal trigger server for run.sh.

Two trigger sources, one coalescing queue:

  POST /webhook           — GitHub push events (submodule repos)
  POST /internal/trigger  — Bearer-authenticated trigger from agents /
                            scripts (e.g. nanoclaw agent after Drive upload)

Both write into the same in-process pending flag and return 202 immediately.
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
import logging
import os
import subprocess
import sys
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

WORKDIR = Path(__file__).resolve().parent
LOCKFILE = Path("/tmp/gakutoolkit.lock")
LOGFILE = WORKDIR / "webhook.log"

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
# `_pending` is the single bit of state: "there is work to do." Triggers set
# it; the worker reads-and-clears it each iteration. If a trigger arrives
# during a run, _pending is set again → worker loops once more.
#
# `_last_triggers` keeps a short tail of recent trigger reasons for the
# /status endpoint — debugging aid only.

_pending = False
_pending_lock = threading.Lock()
_worker_lock = threading.Lock()
_run_state = {"in_progress": False, "last_started_at": None, "last_finished_at": None,
              "last_exit_code": None, "last_log": None, "runs_total": 0,
              "runs_failed": 0}
_run_state_lock = threading.Lock()
_last_triggers: list[dict] = []
_LAST_TRIGGERS_MAX = 20


def _enqueue_trigger(reason: dict) -> None:
    """Mark work pending and ensure a drain thread is running."""
    global _pending
    with _pending_lock:
        _pending = True
    with _pending_lock:
        _last_triggers.append({
            "at": datetime.datetime.now().isoformat(timespec="seconds"),
            **reason,
        })
        del _last_triggers[:-_LAST_TRIGGERS_MAX]
    threading.Thread(target=_drain, daemon=True, name="trigger-drain").start()


def _drain() -> None:
    """Run pending work; coalesce concurrent triggers into at most one
    additional run after the current one. Only one drain thread executes;
    others return immediately because `_pending` is preserved and the running
    worker will see it next iteration.
    """
    global _pending
    if not _worker_lock.acquire(blocking=False):
        return
    try:
        while True:
            with _pending_lock:
                if not _pending:
                    return
                _pending = False
            _run_pipeline()
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


def _run_pipeline() -> None:
    """Invoke run.sh under the cross-process flock. Blocks up to
    TRIGGER_FLOCK_TIMEOUT for the cron-side runner to release."""
    started_at = datetime.datetime.now()
    logfile = WORKDIR / f"output_webhook_{started_at:%Y%m%d_%H%M%S}.log"
    log.info(
        f"_run_pipeline start — waiting for flock (timeout={TRIGGER_FLOCK_TIMEOUT}s), log={logfile.name}"
    )

    with _run_state_lock:
        _run_state.update({
            "in_progress": True,
            "last_started_at": started_at.isoformat(timespec="seconds"),
            "last_log": logfile.name,
        })

    fd = _acquire_flock_with_wait(TRIGGER_FLOCK_TIMEOUT)
    if fd is None:
        log.error(
            f"_run_pipeline aborted — timed out waiting {TRIGGER_FLOCK_TIMEOUT}s for {LOCKFILE}"
        )
        with _run_state_lock:
            _run_state["in_progress"] = False
            _run_state["last_finished_at"] = datetime.datetime.now().isoformat(timespec="seconds")
            _run_state["last_exit_code"] = -1
            _run_state["runs_total"] += 1
            _run_state["runs_failed"] += 1
        return

    try:
        LOCKFILE.write_text(f"{os.getpid()}\n")
        env = {**os.environ, "SKIP_FLOCK": "1"}
        with open(logfile, "w") as out:
            proc = subprocess.run(
                ["bash", "run.sh"],
                cwd=str(WORKDIR),
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
            )
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    finished_at = datetime.datetime.now()
    duration = (finished_at - started_at).total_seconds()
    with _run_state_lock:
        _run_state["in_progress"] = False
        _run_state["last_finished_at"] = finished_at.isoformat(timespec="seconds")
        _run_state["last_exit_code"] = proc.returncode
        _run_state["runs_total"] += 1
        if proc.returncode != 0:
            _run_state["runs_failed"] += 1

    log.info(
        f"_run_pipeline done — exit={proc.returncode} duration={duration:.1f}s log={logfile.name}"
    )


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

    _enqueue_trigger({"source": "github", "repo": repo, "ref": ref})
    return jsonify({"status": "queued", "source": "github", "repo": repo}), 202


@app.route("/internal/trigger", methods=["POST"])
def internal_trigger():
    """Trigger a run.sh execution from a trusted internal caller (e.g.
    nanoclaw agent after Drive upload). Request is always queued — never
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
    log.info(f"Internal trigger queued: {reason}")
    _enqueue_trigger(reason)
    return jsonify({"status": "queued", **reason}), 202


@app.route("/status", methods=["GET"])
def status():
    with _pending_lock:
        pending = _pending
        recent = list(_last_triggers)
    with _run_state_lock:
        run_state = dict(_run_state)
    return jsonify({
        "pending": pending,
        "run_state": run_state,
        "recent_triggers": recent[-5:],
        "lock_exists": LOCKFILE.exists(),
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info(f"Starting webhook server on port {PORT}")
    log.info(f"Watching repos: {WATCHED_REPOS}")
    log.info(f"Work directory: {WORKDIR}")
    log.info(f"Internal trigger: {'enabled' if INTERNAL_TRIGGER_TOKEN else 'DISABLED (set INTERNAL_TRIGGER_TOKEN)'}")
    log.info(f"flock timeout: {TRIGGER_FLOCK_TIMEOUT}s")
    app.run(host="0.0.0.0", port=PORT, debug=False)
