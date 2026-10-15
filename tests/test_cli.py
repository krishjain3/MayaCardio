import json
import os
import subprocess
import sys
import tempfile
import pytest

from pipeline.storage import Storage
from pipeline.processing import compute_window_result


@pytest.fixture
def cli_env():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "cli_test.db")
        storage = Storage(db_path)
        session_id = "cli_session_99"

        # Populate samples
        s1 = [[100, 200, 300], [101, 201, 301]]
        s2 = [[400000, 150000], [400001, 150001]]
        storage.insert_samples_batch(session_id, 1, 0, s1)
        storage.insert_samples_batch(session_id, 2, 0, s2)
        storage.insert_loss_records(session_id, 1, [{"from_index": 10, "to_index": 20, "reason": "test_loss"}])

        # Compute a window result
        res = compute_window_result(storage, session_id, 0, mode="offline")
        storage.save_window_result(res)

        yield db_path, session_id


def test_cli_status(cli_env):
    db_path, session_id = cli_env
    cmd = [sys.executable, "-m", "pipeline.cli", "status", "--db", db_path, "--session", session_id]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)

    output_lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
    assert len(output_lines) == 1, f"Status must be exactly one JSON line, got: {output_lines}"

    status = json.loads(output_lines[0])
    assert "mode" in status
    assert "samples_saved" in status
    assert "samples_cloud" in status
    assert "backlog" in status
    assert status["samples_saved"] == 4


def test_cli_export(cli_env):
    db_path, session_id = cli_env
    cmd = [sys.executable, "-m", "pipeline.cli", "export", "--session", session_id, "--from", "device", "--db", db_path]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)

    lines = [json.loads(l) for l in result.stdout.splitlines() if l.strip()]
    # 2 s1 samples + 2 s2 samples + 1 loss record = 5 lines
    assert len(lines) == 5

    samples = [r for r in lines if "values" in r]
    losses = [r for r in lines if "from_index" in r]

    assert len(samples) == 4
    assert len(losses) == 1
    assert losses[0]["reason"] == "test_loss"


def test_cli_results(cli_env):
    db_path, session_id = cli_env
    cmd = [sys.executable, "-m", "pipeline.cli", "results", "--session", session_id, "--db", db_path]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)

    lines = [json.loads(l) for l in result.stdout.splitlines() if l.strip()]
    assert len(lines) >= 1
    res = lines[0]

    assert res["session_id"] == session_id
    assert res["window"] == 0
    assert "missing_1" in res
    assert "missing_2" in res
    assert "complete" in res
    assert "mean" in res
    assert "min" in res
    assert "max" in res
    assert "magnitude" in res
    assert "heart_rate" in res
    assert "mode" in res
