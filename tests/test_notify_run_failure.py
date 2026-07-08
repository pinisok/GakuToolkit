import os
import subprocess


def test_notify_run_failure_dry_run_includes_failure_context(tmp_path):
    """Failure notifier should be testable without Discord/network access."""
    run_log = tmp_path / "output_20260704_1130.log"
    run_log.write_text("line 1\nline 2\n❌ main.py 실패 — output 복구 중\n", encoding="utf-8")

    env = os.environ.copy()
    env["GAKUTOOLKIT_NOTIFY_DRY_RUN"] = "1"
    env["GAKUTOOLKIT_FAILURE_NOTIFY_CHANNEL_ID"] = "1234567890"

    result = subprocess.run(
        [
            "bash",
            "scripts/notify_run_failure.sh",
            "main.py",
            "1",
            "Convert phase had 1 error(s)",
            str(run_log),
        ],
        cwd=os.path.dirname(os.path.dirname(__file__)),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "DRY RUN" in result.stdout
    assert "main.py" in result.stdout
    assert "Convert phase had 1 error(s)" in result.stdout
    assert "output_20260704_1130.log" in result.stdout
    assert "DISCORD_BOT_TOKEN" not in result.stdout
