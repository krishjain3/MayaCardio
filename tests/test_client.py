import os
import tempfile
import threading
import time
import pytest

from mock.biosignal_mock import MockServer
from pipeline.storage import Storage
from pipeline.client import SensorClient


@pytest.fixture
def mock_and_client():
    port = 7188
    server = MockServer(mode="signal", seed=7, duration=60, port=port, corrupt_crc_rate=0.0)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.15)  # wait for server to bind

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "client_test.db")
        storage = Storage(db_path)
        client = SensorClient(port=port, storage=storage, timeout=2.0)
        yield server, client, storage

    server.stop()
    client.disconnect()


def test_client_ingests_both_sensors(mock_and_client):
    server, client, storage = mock_and_client

    # Run client polling for both sensors
    # Need to wait until mock clock produces at least a few samples
    time.sleep(0.1)
    samples_read = client.run_once()
    assert samples_read > 0

    session_id = client.current_session_id
    assert session_id is not None
    assert len(session_id) > 0

    saved, _, _ = storage.get_counts(session_id)
    assert saved == samples_read


def test_client_handles_corrupt_crc(mock_and_client):
    server, client, storage = mock_and_client

    # Force corrupt CRC on server
    server.corrupt_crc_rate = 1.0
    reply = client.read_sensor_once(1)
    # Because CRC is corrupt, client must reject and return None
    assert reply is None

    # Restore server CRC
    server.corrupt_crc_rate = 0.0
    reply2 = client.read_sensor_once(1)
    assert reply2 is not None
    assert reply2["sensor"] == 1


def test_client_handles_mock_restart(mock_and_client):
    server, client, storage = mock_and_client

    time.sleep(0.05)
    client.run_once()
    first_session = client.current_session_id
    assert first_session is not None

    # Simulate mock restart with new session ID
    new_session_id = "restart_sess_99"
    fake_reply = {
        "session_id": new_session_id,
        "sensor": 1,
        "first_index": 0,
        "count": 2,
        "lost": [],
        "session_end": False,
        "crc32": "f9a5bf62",
        "data": [[100, 200, 300], [101, 201, 301]]
    }
    client.process_reply(fake_reply)

    assert client.current_session_id == new_session_id

    # Check that previous session has loss recorded with to_index: None
    losses = list(storage.get_unuploaded_losses(first_session))
    assert len(losses) > 0
    # At least one loss record for old session must have to_index is None and reason session_restart
    restart_loss = [l for l in losses if l["reason"] == "session_restart"]
    assert len(restart_loss) > 0
    assert restart_loss[0]["to_index"] is None
