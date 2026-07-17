import json

import submodule_watcher as watcher


def test_busy_run_does_not_acknowledge_new_campus_state(tmp_path, monkeypatch):
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"octoCacheRevision": 1}), encoding="utf-8")
    monkeypatch.setattr(watcher, "STATEFILE", state_file)
    monkeypatch.setattr(
        watcher,
        "_current_state",
        lambda: {"octoCacheRevision": 2},
    )
    monkeypatch.setattr(watcher, "_trigger_run", lambda changed: False)

    watcher.check_and_trigger()

    assert json.loads(state_file.read_text(encoding="utf-8")) == {
        "octoCacheRevision": 1
    }


def test_successful_run_acknowledges_new_campus_state(tmp_path, monkeypatch):
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"octoCacheRevision": 1}), encoding="utf-8")
    monkeypatch.setattr(watcher, "STATEFILE", state_file)
    monkeypatch.setattr(
        watcher,
        "_current_state",
        lambda: {"octoCacheRevision": 2},
    )
    monkeypatch.setattr(watcher, "_trigger_run", lambda changed: True)

    watcher.check_and_trigger()

    assert json.loads(state_file.read_text(encoding="utf-8")) == {
        "octoCacheRevision": 2
    }
