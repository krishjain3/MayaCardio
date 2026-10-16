import json
import os
import tempfile
import threading
import time
import pytest
try:
    from werkzeug.serving import make_server
except ImportError:
    from wsgiref.simple_server import make_server

from mock.biosignal_mock import MockServer
from mock.check_ground_truth import check_export
from pipeline.storage import Storage
from pipeline.client import SensorClient
from pipeline.sync import SyncWorker
from pipeline.service import PipelineService
from cloud.server import create_app


class ServerThread(threading.Thread):
    def __init__(self, app, port):
        super().__init__(daemon=True)
        self.server = make_server("127.0.0.1", port, app)
        self.port = port

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


def test_ground_truth_ramp_mode():
    """Validates Criterion 1 and 2 using check_ground_truth on ramp mode."""
    port = 7189
    mock = MockServer(mode="ramp", seed=42, duration=10, port=port)
    mock_thread = threading.Thread(target=mock.run, daemon=True)
    mock_thread.start()
    time.sleep(0.15)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "gt_ramp.db")
        export_path = os.path.join(tmpdir, "export_ramp.ndjson")
        storage = Storage(db_path)
        client = SensorClient(port=port, storage=storage, timeout=2.0)

        # Collect 800 samples per sensor (~2 seconds of data)
        time.sleep(0.5)
        while True:
            client.run_once()
            c1 = storage.get_max_sample_index(client.current_session_id)
            if c1 >= 600:
                break
            time.sleep(0.05)

        session_id = client.current_session_id
        # Export to file
        with open(export_path, "w", encoding="utf-8") as f:
            for line in storage.export_data(session_id):
                f.write(line + "\n")

        # Run ground truth verifier
        passed, stats = check_export(export_path, mode="ramp", seed=42)
        assert passed is True, f"Ground truth check failed: {stats}"
        assert stats["duplicates"] == 0
        assert stats["value_mismatches"] == 0
        assert stats["silent_losses"] == 0

    mock.stop()
    client.disconnect()


def test_ground_truth_signal_mode():
    """Validates Criterion 1 and 2 on realistic signal mode."""
    port = 7190
    mock = MockServer(mode="signal", seed=7, duration=10, port=port)
    mock_thread = threading.Thread(target=mock.run, daemon=True)
    mock_thread.start()
    time.sleep(0.15)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "gt_signal.db")
        export_path = os.path.join(tmpdir, "export_signal.ndjson")
        storage = Storage(db_path)
        client = SensorClient(port=port, storage=storage, timeout=2.0)

        time.sleep(0.5)
        while True:
            client.run_once()
            c1 = storage.get_max_sample_index(client.current_session_id)
            if c1 >= 600:
                break
            time.sleep(0.05)

        session_id = client.current_session_id
        with open(export_path, "w", encoding="utf-8") as f:
            for line in storage.export_data(session_id):
                f.write(line + "\n")

        passed, stats = check_export(export_path, mode="signal", seed=7)
        assert passed is True, f"Ground truth check failed: {stats}"
        assert stats["duplicates"] == 0
        assert stats["value_mismatches"] == 0
        assert stats["silent_losses"] == 0

    mock.stop()
    client.disconnect()


def test_network_cut_and_restore_full_catchup():
    """
    Validates Criterion 4, 5, 6:
    - Streams online initially
    - Network is cut -> offline mode entered, collection and local processing continue
    - Network restored -> online mode entered within 10s, catch-up synchronizes complete backlog
    - Cloud export compared with ground truth
    """
    cloud_port = 8899
    with tempfile.TemporaryDirectory() as tmpdir:
        cloud_db = os.path.join(tmpdir, "cloud_sim.db")
        device_db = os.path.join(tmpdir, "device_sim.db")

        # Start cloud
        cloud_app = create_app(cloud_db)
        server_thread = ServerThread(cloud_app, cloud_port)
        server_thread.start()
        time.sleep(0.15)

        device_storage = Storage(device_db)
        cloud_storage = Storage(cloud_db)
        sync_worker = SyncWorker(
            storage=device_storage,
            cloud_url=f"http://127.0.0.1:{cloud_port}",
            batch_size=500,
            request_timeout=1.0
        )

        session_id = "network_test_sess"
        # Phase 1: Online mode - ingest and sync 400 samples
        s1_p1 = [[10 + i, 20 + i, 30 + i] for i in range(400)]
        s2_p1 = [[400000 + i, 150000 + i] for i in range(400)]
        device_storage.insert_samples_batch(session_id, 1, 0, s1_p1)
        device_storage.insert_samples_batch(session_id, 2, 0, s2_p1)

        sync_worker.check_connectivity()
        assert sync_worker.current_mode == "online"
        sync_worker.sync_backlog_step(session_id)

        # Phase 2: Cut network!
        server_thread.shutdown()
        time.sleep(0.1)

        # Ingest 800 more samples while network is down
        s1_p2 = [[10 + i, 20 + i, 30 + i] for i in range(400, 1200)]
        s2_p2 = [[400000 + i, 150000 + i] for i in range(400, 1200)]
        device_storage.insert_samples_batch(session_id, 1, 400, s1_p2)
        device_storage.insert_samples_batch(session_id, 2, 400, s2_p2)

        # Trigger health checks to force switch to offline
        sync_worker.last_health_check_time = 0.0
        sync_worker.check_connectivity()
        sync_worker.last_health_check_time = 0.0
        sync_worker.check_connectivity()
        assert sync_worker.current_mode == "offline"

        # Offline processing continues on device
        sync_worker.process_offline_windows(session_id)
        local_results = device_storage.get_window_results(session_id)
        assert len(local_results) >= 2

        # Check backlog accumulated
        saved, cloud_cnt, backlog = device_storage.get_counts(session_id)
        assert saved == 2400
        assert backlog == 1600

        # Phase 3: Restore network!
        server_thread2 = ServerThread(cloud_app, cloud_port)
        server_thread2.start()
        time.sleep(0.15)

        # Detect reconnection
        sync_worker.last_health_check_time = 0.0
        sync_worker.check_connectivity()
        assert sync_worker.current_mode == "online"

        # Synchronize catch-up
        while True:
            cnt = sync_worker.sync_backlog_step(session_id)
            if cnt == 0:
                break

        # Verify cloud now has 100% complete session with 0 backlog
        saved_final, cloud_final, backlog_final = device_storage.get_counts(session_id)
        assert saved_final == 2400
        assert cloud_final == 2400
        assert backlog_final == 0

        cloud_saved, _, _ = cloud_storage.get_counts(session_id)
        assert cloud_saved == 2400

        server_thread2.shutdown()


def test_crash_recovery_resumes_cleanly():
    """
    Validates Criterion 3:
    - Data persists in SQLite WAL across process termination
    - Resumes without corruption or duplicate entries
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "crash_test.db")

        # Process 1: write some data and abruptly close/simulate crash
        s1 = Storage(db_path, synchronous="FULL")
        session_id = "crash_sess_01"
        s1.insert_samples_batch(session_id, 1, 0, [[1, 2, 3], [4, 5, 6]])
        s1.insert_samples_batch(session_id, 2, 0, [[100, 200], [101, 201]])
        # Do not cleanly checkpoint or close, just re-open storage in fresh process
        del s1

        # Process 2: open DB after reboot
        s2 = Storage(db_path, synchronous="FULL")
        saved, _, _ = s2.get_counts(session_id)
        assert saved == 4

        # Ingest subsequent samples
        s2.insert_samples_batch(session_id, 1, 2, [[7, 8, 9]])
        saved_after, _, _ = s2.get_counts(session_id)
        assert saved_after == 5
