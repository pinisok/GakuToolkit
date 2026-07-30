import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUN_SH = ROOT / "run.sh"
PREPARE = ROOT / "scripts" / "prepare_output_repo.sh"


def _git(*args, cwd: Path) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def _init_remote_fixture(tmp_path: Path) -> tuple[Path, Path]:
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    output = tmp_path / "output"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "init", "-b", "main", str(seed)], check=True, capture_output=True)
    _git("config", "user.name", "test", cwd=seed)
    _git("config", "user.email", "test@example.invalid", cwd=seed)
    (seed / "payload.txt").write_text("base\n", encoding="utf-8")
    _git("add", "payload.txt", cwd=seed)
    _git("commit", "-m", "base", cwd=seed)
    _git("remote", "add", "origin", str(remote), cwd=seed)
    _git("push", "-u", "origin", "main", cwd=seed)
    subprocess.run(["git", "clone", "-b", "main", str(remote), str(output)], check=True, capture_output=True)
    _git("config", "user.name", "test", cwd=output)
    _git("config", "user.email", "test@example.invalid", cwd=output)
    return remote, output


def test_run_sh_failure_notifier_is_root_anchored():
    text = RUN_SH.read_text(encoding="utf-8")
    assert 'bash "$SCRIPT_DIR/scripts/notify_run_failure.sh"' in text
    assert "bash ./scripts/notify_run_failure.sh" not in text


def test_prepare_output_repo_repairs_origin_and_pushes_local_ahead_commit(tmp_path):
    remote, output = _init_remote_fixture(tmp_path)
    (output / "payload.txt").write_text("base\nlocal ahead\n", encoding="utf-8")
    _git("add", "payload.txt", cwd=output)
    _git("commit", "-m", "local ahead", cwd=output)
    local_head = _git("rev-parse", "HEAD", cwd=output)
    _git("remote", "set-url", "origin", str(tmp_path / "broken-https-origin"), cwd=output)

    result = subprocess.run(
        ["bash", str(PREPARE), str(output), str(remote)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert _git("remote", "get-url", "origin", cwd=output) == str(remote)
    remote_head = subprocess.check_output(
        ["git", "--git-dir", str(remote), "rev-parse", "refs/heads/main"], text=True
    ).strip()
    assert remote_head == local_head
    assert _git("status", "--porcelain", cwd=output) == ""


def test_prepare_output_repo_refuses_dirty_tree_without_deleting_changes(tmp_path):
    remote, output = _init_remote_fixture(tmp_path)
    payload = output / "payload.txt"
    payload.write_text("base\nuncommitted\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", str(PREPARE), str(output), str(remote)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "uncommitted" in payload.read_text(encoding="utf-8")
    assert _git("status", "--porcelain", cwd=output)
