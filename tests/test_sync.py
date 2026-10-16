import os
import tempfile
import threading
import time
import pytest
try:
    from werkzeug.serving import make_server
except ImportError:
    from wsgiref.simple_server import make_server

from cloud.server import app, create_app
from pipeline.storage import Storage
from pipeline.sync import SyncWorker
from pipeline.processing import compute_window_result


class ServerThread(threading.Thread):
    def __init__(self, app, port):
        super().__init__(daemon=True)
        self.server = make_server("127.0.0.1", port, app)
        self.port = port

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


@pytest.fixture
def cloud_and_device():
    port = 8877
    with tempfile.TemporaryDirectory() as tmpdir:
        cloud_db = os.path.join(tmpdir, "cloud_test.db")
        device_db = os.path.join(tmpdir, "device_test.db")

        cloud_app = create_app(cloud_db)
        server_thread = ServerThread(cloud_app, port)
        server_thread.start()
        time.sleep(0.15)

        device_storage = Storage(device_db)
        cloud_storage = Storage(cloud_db)
        sync_worker = SyncWorker(
            storage=device_storage,
            cloud_url=f"http://127.0.0.1:{port}",
            batch_size=500
        )

        yield server_thread, sync_worker, device_storage, cloud_storage

        server_thread.shutdown()


def test_mode_switching_and_connectivity(cloud_and_device):
    server_thread, sync_worker, device_storage, cloud_storage = cloud_and_device

    # Initial check: cloud server is running, should detect online
    assert sync_worker.check_connectivity() is True
    assert sync_worker.current_mode == "online"

    # Now shut down cloud server to simulate network outage
    server_thread.shutdown()
    time.sleep(0.1)

    # Re-check connectivity: should fail and switch to offline
    sync_worker.last_health_check_time = 0.0  # force recheck
    sync_worker.check_connectivity()
    sync_worker.last_health_check_time = 0.0
    sync_worker.check_connectivity()
    assert sync_worker.current_mode == "offline"


def test_catch_up_synchronization(cloud_and_device):
    server_thread, sync_worker, device_storage, cloud_storage = cloud_and_device

    session_id = "test_sync_sess_01"
    # Populate device with 1200 samples (3 windows) for both sensors
    s1_samples = [[10 + i, 20 + i, 30 + i] for i in range(1200)]
    s2_samples = [[400000 + i, 150000 + i] for i in range(1200)]

    device_storage.insert_samples_batch(session_id, 1, 0, s1_samples)
    device_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    saved, cloud, backlog = device_storage.get_counts(session_id)
    assert saved == 2400
    assert cloud == 0
    assert backlog == 2400

    # Ensure online mode and sync
    sync_worker.current_mode = "online"

    # Step through sync
    uploaded = 0
    while True:
        cnt = sync_worker.sync_backlog_step(session_id)
        if cnt == 0:
            break
        uploaded += cnt

    # Check that device now reports 0 backlog
    saved2, cloud2, backlog2 = device_storage.get_counts(session_id)
    assert saved2 == 2400
    assert cloud2 == 2400
    assert backlog2 == 0

    # Check that cloud storage now has all 2400 samples
    cloud_saved, _, _ = cloud_storage.get_counts(session_id)
    assert cloud_saved == 2400


def test_offline_processing_on_device(cloud_and_device):
    server_thread, sync_worker, device_storage, cloud_storage = cloud_and_device

    session_id = "test_offline_sess_01"
    # Insert 400 samples (1 window)
    s1_samples = [[10, 20, 30] for _ in range(400)]
    s2_samples = [[3, 4] for _ in range(400)]

    device_storage.insert_samples_batch(session_id, 1, 0, s1_samples)
    device_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    # Process offline
    sync_worker.process_offline_windows(session_id)

    # Results should be stored locally
    results = device_storage.get_window_results(session_id)
    assert len(results) == 1
    assert results[0].window == 0
    assert results[0].mode == "offline"
    assert results[0].complete is True
    assert results[0].magnitude == 5.0


def test_cloud_ingest_sensors_arrive_different_times(cloud_and_device):
    """
    Validates Requirement 3:
    In cloud online mode, if Sensor 1 arrives first, Window 0 is initially incomplete.
    When Sensor 2 arrives in a subsequent request, Window 0 is updated to complete.
    """
    server_thread, sync_worker, device_storage, cloud_storage = cloud_and_device
    import requests

    session_id = "test_cloud_diff_arrival"
    cloud_url = sync_worker.cloud_url

    # 1. Ingest Sensor 1 only for Window 0
    s1_samples = [[100, 200, 300] for _ in range(400)]
    resp1 = requests.post(
        f"{cloud_url}/api/v1/ingest",
        json={
            "session_id": session_id,
            "sensor": 1,
            "first_index": 0,
            "samples": s1_samples,
            "mode": "online"
        }
    )
    assert resp1.status_code == 200

    # Verify Window 0 is present in cloud but incomplete
    results_incomplete = cloud_storage.get_window_results(session_id)
    assert len(results_incomplete) >= 1
    w0_inc = [r for r in results_incomplete if r.window == 0 and r.mode == "online"][0]
    assert w0_inc.missing_1 == 0
    assert w0_inc.missing_2 == 400
    assert w0_inc.complete is False
    assert w0_inc.mean["sensor_2"] is None

    # 2. Ingest Sensor 2 for Window 0
    s2_samples = [[3, 4] for _ in range(400)]
    resp2 = requests.post(
        f"{cloud_url}/api/v1/ingest",
        json={
            "session_id": session_id,
            "sensor": 2,
            "first_index": 0,
            "samples": s2_samples,
            "mode": "online"
        }
    )
    assert resp2.status_code == 200

    # Verify Window 0 is updated in cloud to complete
    results_complete = cloud_storage.get_window_results(session_id)
    w0_comp = [r for r in results_complete if r.window == 0 and r.mode == "online"][0]
    assert w0_comp.missing_1 == 0
    assert w0_comp.missing_2 == 0
    assert w0_comp.complete is True
    assert w0_comp.magnitude == 5.0
    assert w0_comp.mean["sensor_1"] == [100.0, 200.0, 300.0]
    assert w0_comp.mean["sensor_2"] == [3.0, 4.0]


def test_production_auth_rejection(cloud_and_device):
    """
    Validates Requirement 6:
    In production deployment, missing credentials must NEVER be accepted silently.
    """
    server_thread, sync_worker, device_storage, cloud_storage = cloud_and_device
    import requests

    cloud_url = sync_worker.cloud_url

    # Set environment to production temporarily
    old_env = os.environ.get("ENVIRONMENT")
    old_key = os.environ.get("CLOUD_API_KEY")
    try:
        os.environ["ENVIRONMENT"] = "production"
        os.environ["CLOUD_API_KEY"] = "super-secret-prod-token"

        # Request with no credentials should be rejected with 401
        r_unauth = requests.get(f"{cloud_url}/api/v1/status")
        assert r_unauth.status_code == 401

        # Request with wrong credentials should be rejected with 401
        r_wrong = requests.get(
            f"{cloud_url}/api/v1/status",
            headers={"X-API-Key": "wrong-token"}
        )
        assert r_wrong.status_code == 401

        # Request with valid credentials should succeed
        r_auth = requests.get(
            f"{cloud_url}/api/v1/status",
            headers={"X-API-Key": "super-secret-prod-token"}
        )
        assert r_auth.status_code == 200

    finally:
        if old_env is not None:
            os.environ["ENVIRONMENT"] = old_env
        else:
            os.environ.pop("ENVIRONMENT", None)

        if old_key is not None:
            os.environ["CLOUD_API_KEY"] = old_key
        else:
            os.environ.pop("CLOUD_API_KEY", None)

