from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import webhook_server as webhook


def _reset_queue(path: Path) -> None:
    webhook.QUEUE_FILE = path
    with webhook._pending_lock:
        webhook._pending.clear()
        webhook._inflight.clear()
        webhook._dead_letter.clear()
        webhook._last_triggers.clear()


def test_inflight_batch_is_recovered_after_restart(tmp_path):
    queue_file = tmp_path / "queue.json"
    _reset_queue(queue_file)
    queue_file.write_text(
        json.dumps(
            {
                "pending": [{"tid": "new", "at": "now"}],
                "inflight": [{"tid": "interrupted", "at": "before"}],
                "dead_letter": [],
            }
        ),
        encoding="utf-8",
    )

    restored = webhook._restore_queue_state()

    assert restored == 2
    assert [entry["tid"] for entry in webhook._pending] == [
        "interrupted",
        "new",
    ]
    persisted = json.loads(queue_file.read_text(encoding="utf-8"))
    assert persisted["inflight"] == []
    assert [entry["tid"] for entry in persisted["pending"]] == [
        "interrupted",
        "new",
    ]


def test_deferred_batch_remains_durable_pending(tmp_path):
    queue_file = tmp_path / "queue.json"
    _reset_queue(queue_file)
    with webhook._pending_lock:
        webhook._pending.append({"tid": "retry-me", "at": "now"})
        webhook._persist_queue_locked()

    timer = mock.Mock()
    with mock.patch.object(webhook, "_run_pipeline", return_value="deferred"), \
         mock.patch.object(webhook.threading, "Timer", return_value=timer):
        webhook._drain()

    assert [entry["tid"] for entry in webhook._pending] == ["retry-me"]
    assert webhook._pending[0]["retry_attempt"] == 1
    persisted = json.loads(queue_file.read_text(encoding="utf-8"))
    assert persisted["inflight"] == []
    assert persisted["pending"][0]["tid"] == "retry-me"
    timer.start.assert_called_once()


def test_failed_attempt_moves_to_durable_dead_letter(tmp_path):
    queue_file = tmp_path / "queue.json"
    _reset_queue(queue_file)
    with webhook._pending_lock:
        webhook._pending.append({"tid": "failed", "at": "now"})
        webhook._persist_queue_locked()

    with mock.patch.object(webhook, "_run_pipeline", return_value="failed"):
        webhook._drain()

    assert webhook._pending == []
    assert webhook._inflight == []
    assert webhook._dead_letter[-1]["tid"] == "failed"
    persisted = json.loads(queue_file.read_text(encoding="utf-8"))
    assert persisted["dead_letter"][-1]["outcome"] == "failed"
