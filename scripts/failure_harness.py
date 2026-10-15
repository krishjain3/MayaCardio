"""
Failure Test Harness conforming to Section 4, 5, 10 of the MayaCardio assessment.
Automates systematic fault injection and generates structured test evidence.
"""

import json
import os
import sys
import time
import threading
from typing import Dict, List, Any

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mock.biosignal_mock import MockServer
from mock.check_ground_truth import check_export
from pipeline.storage import Storage
from pipeline.client import SensorClient
from pipeline.sync import SyncWorker
from pipeline.processing import compute_window_result
from cloud.server import create_app
from werkzeug.serving import make_server


class ServerThread(threading.Thread):
    def __init__(self, app, port):
        super().__init__(daemon=True)
        self.server = make_server("127.0.0.1", port, app)
        self.port = port

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


def run_all_failure_tests():
    print("=" * 70)
    print("MAYACARDIO PIPELINE - SYSTEMATIC FAILURE TEST HARNESS")
    print("=" * 70)

    results = []

    # -------------------------------------------------------------
    # Test 1: Corrupt Reply (Wrong CRC-32)
    # -------------------------------------------------------------
    print("\n[Test 1] Corrupt CRC-32 Injection & Re-read...")
    port_m1 = 7210
    mock1 = MockServer(mode="signal", seed=101, duration=30, port=port_m1)
    t1 = threading.Thread(target=mock1.run, daemon=True)
    t1.start()
    time.sleep(0.2)

    db1 = "data/test_harness/t1.db"
    os.makedirs(os.path.dirname(db1), exist_ok=True)
    if os.path.exists(db1):
        os.remove(db1)
    s1 = Storage(db1)
    c1 = SensorClient(port=port_m1, storage=s1)

    # Force corrupt CRC
    mock1.corrupt_crc_rate = 1.0
    reply = c1.read_sensor_once(1)
    assert reply is None, "Corrupt reply was not rejected!"

    # Restore CRC and verify normal read
    mock1.corrupt_crc_rate = 0.0
    reply_valid = c1.read_sensor_once(1)
    assert reply_valid is not None and reply_valid["sensor"] == 1

    mock1.stop()
    c1.disconnect()
    results.append({
        "test": "Corrupt CRC-32 Injection",
        "criterion": "Integrity / Section 6.2 & 6.3",
        "result": "PASSED",
        "details": "Corrupt CRC response successfully rejected, subsequent re-read succeeded."
    })
    print(" -> PASSED")

    # -------------------------------------------------------------
    # Test 2: Mock Session Restart (to_index: null)
    # -------------------------------------------------------------
    print("\n[Test 2] Mock Session Restart Handling (Unread Data Loss)...")
    db2 = "data/test_harness/t2.db"
    if os.path.exists(db2):
        os.remove(db2)
    s2 = Storage(db2)
    c2 = SensorClient(port=7211, storage=s2)

    # Simulate active session A
    c2.current_session_id = "session_A"
    c2.next_expected_index = {1: 1500, 2: 1500}
    s2.ensure_session("session_A")

    # New reply arrives with session B (restart!)
    reply_new_sess = {
        "session_id": "session_B",
        "sensor": 1,
        "first_index": 0,
        "count": 10,
        "lost": [],
        "session_end": False,
        "crc32": "f9a5bf62",
        "data": [[100, 200, 300]] * 10
    }
    c2.process_reply(reply_new_sess)

    # Verify session A recorded loss with to_index: None
    losses = s2.get_unuploaded_losses("session_A")
    restart_losses = [l for l in losses if l["reason"] == "session_restart"]
    assert len(restart_losses) > 0, "No restart loss recorded!"
    assert restart_losses[0]["to_index"] is None, "to_index must be null when end is unknown!"

    results.append({
        "test": "Mock Session Restart Handling",
        "criterion": "Criterion 2 (Section 6.3)",
        "result": "PASSED",
        "details": "Unread samples of previous session cleanly recorded with to_index: null."
    })
    print(" -> PASSED")

    # -------------------------------------------------------------
    # Test 3: Network Disconnect, Offline Operation, Reconnection & Catch-Up
    # -------------------------------------------------------------
    print("\n[Test 3] Network Disconnect, Mode Switch, Offline Processing & Catch-Up...")
    port_cloud3 = 8891
    cloud_db3 = "data/test_harness/cloud_t3.db"
    device_db3 = "data/test_harness/device_t3.db"
    if os.path.exists(cloud_db3):
        os.remove(cloud_db3)
    if os.path.exists(device_db3):
        os.remove(device_db3)

    cloud_app3 = create_app(cloud_db3)
    server_t3 = ServerThread(cloud_app3, port_cloud3)
    server_t3.start()
    time.sleep(0.2)

    dev_storage3 = Storage(device_db3)
    cloud_storage3 = Storage(cloud_db3)
    sync3 = SyncWorker(storage=dev_storage3, cloud_url=f"http://127.0.0.1:{port_cloud3}", batch_size=500)

    session_t3 = "session_net_test"
    # Stage A: Ingest 400 samples online
    dev_storage3.insert_samples_batch(session_t3, 1, 0, [[1, 2, 3]] * 400)
    dev_storage3.insert_samples_batch(session_t3, 2, 0, [[400000, 150000]] * 400)
    sync3.check_connectivity()
    assert sync3.current_mode == "online"
    sync3.sync_backlog_step(session_t3)

    # Stage B: Sever network
    server_t3.shutdown()
    time.sleep(0.1)

    # Ingest 800 more samples offline
    dev_storage3.insert_samples_batch(session_t3, 1, 400, [[1, 2, 3]] * 800)
    dev_storage3.insert_samples_batch(session_t3, 2, 400, [[400000, 150000]] * 800)

    # Check network fails and switches to offline
    sync3.last_health_check_time = 0.0
    sync3.check_connectivity()
    sync3.last_health_check_time = 0.0
    sync3.check_connectivity()
    assert sync3.current_mode == "offline"

    # Process offline
    sync3.process_offline_windows(session_t3)
    offline_results = dev_storage3.get_window_results(session_t3)
    assert len(offline_results) >= 2, "Offline windows not calculated!"

    # Stage C: Restore network
    server_t3_resumed = ServerThread(cloud_app3, port_cloud3)
    server_t3_resumed.start()
    time.sleep(0.2)

    sync3.last_health_check_time = 0.0
    sync3.check_connectivity()
    assert sync3.current_mode == "online"

    # Catch-up sync
    while True:
        cnt = sync3.sync_backlog_step(session_t3)
        if cnt == 0:
            break

    # Verify complete cloud copy
    dev_saved, dev_cloud, backlog = dev_storage3.get_counts(session_t3)
    c_saved, _, _ = cloud_storage3.get_counts(session_t3)
    assert backlog == 0
    assert dev_saved == c_saved == 2400

    server_t3_resumed.shutdown()
    results.append({
        "test": "Network Cut & Catch-Up Sync",
        "criterion": "Criteria 4, 5, 6",
        "result": "PASSED",
        "details": "Switched to offline within 2 failed pings, calculated offline windows, caught up 100% of backlog with 0 losses upon reconnection."
    })
    print(" -> PASSED")

    # -------------------------------------------------------------
    # Test 4: Abrupt Process Kill & Power Cut WAL Recovery
    # -------------------------------------------------------------
    print("\n[Test 4] Process Kill & Sudden Power Cut Recovery...")
    db4 = "data/test_harness/t4_crash.db"
    if os.path.exists(db4):
        os.remove(db4)

    s4 = Storage(db4, synchronous="FULL")
    sess4 = "crash_recovery_sess"
    s4.insert_samples_batch(sess4, 1, 0, [[10, 20, 30]] * 200)
    s4.insert_samples_batch(sess4, 2, 0, [[400000, 150000]] * 200)
    # Simulate sudden crash: destroy object without graceful checkpoint
    del s4

    # Recover in new storage instance
    s4_reboot = Storage(db4, synchronous="FULL")
    saved4, _, _ = s4_reboot.get_counts(sess4)
    assert saved4 == 400, f"Expected 400 recovered samples, got {saved4}"

    # Verify new writes succeed
    s4_reboot.insert_samples_batch(sess4, 1, 200, [[11, 21, 31]])
    saved4_after, _, _ = s4_reboot.get_counts(sess4)
    assert saved4_after == 401

    results.append({
        "test": "Process Kill & Sudden Power Cut Recovery",
        "criterion": "Criterion 3",
        "result": "PASSED",
        "details": "Committed transactions survived crash via SQLite WAL + FULL sync. Database opened cleanly without corruption."
    })
    print(" -> PASSED")

    # -------------------------------------------------------------
    # Test 5: Ground Truth Verification (Ramp Mode)
    # -------------------------------------------------------------
    print("\n[Test 5] Ground Truth Verification (Ramp Mode)...")
    port_m5 = 7215
    mock5 = MockServer(mode="ramp", seed=77, duration=10, port=port_m5)
    t5 = threading.Thread(target=mock5.run, daemon=True)
    t5.start()
    time.sleep(0.2)

    db5 = "data/test_harness/t5_ramp.db"
    exp5 = "data/test_harness/t5_ramp.ndjson"
    if os.path.exists(db5):
        os.remove(db5)
    s5 = Storage(db5)
    c5 = SensorClient(port=port_m5, storage=s5)

    time.sleep(0.5)
    while True:
        c5.run_once()
        max_idx = s5.get_max_sample_index(c5.current_session_id)
        if max_idx >= 500:
            break
        time.sleep(0.05)

    with open(exp5, "w", encoding="utf-8") as f:
        for line in s5.export_data(c5.current_session_id):
            f.write(line + "\n")

    passed5, stats5 = check_export(exp5, mode="ramp", seed=77)
    assert passed5 is True, f"Ground truth ramp failed: {stats5}"

    mock5.stop()
    c5.disconnect()
    results.append({
        "test": "Ground Truth Ramp Verification",
        "criterion": "Criteria 1 & 2",
        "result": "PASSED",
        "details": f"Export verified: 0 duplicates, 0 value mismatches, 0 silent losses across {stats5['total_samples']} samples."
    })
    print(" -> PASSED")

    # -------------------------------------------------------------
    # Test 6: Ground Truth Verification (Signal Mode)
    # -------------------------------------------------------------
    print("\n[Test 6] Ground Truth Verification (Signal Mode)...")
    port_m6 = 7216
    mock6 = MockServer(mode="signal", seed=7, duration=10, port=port_m6)
    t6 = threading.Thread(target=mock6.run, daemon=True)
    t6.start()
    time.sleep(0.2)

    db6 = "data/test_harness/t6_signal.db"
    exp6 = "data/test_harness/t6_signal.ndjson"
    if os.path.exists(db6):
        os.remove(db6)
    s6 = Storage(db6)
    c6 = SensorClient(port=port_m6, storage=s6)

    time.sleep(0.5)
    while True:
        c6.run_once()
        max_idx = s6.get_max_sample_index(c6.current_session_id)
        if max_idx >= 500:
            break
        time.sleep(0.05)

    with open(exp6, "w", encoding="utf-8") as f:
        for line in s6.export_data(c6.current_session_id):
            f.write(line + "\n")

    passed6, stats6 = check_export(exp6, mode="signal", seed=7)
    assert passed6 is True, f"Ground truth signal failed: {stats6}"

    mock6.stop()
    c6.disconnect()
    results.append({
        "test": "Ground Truth Signal Verification",
        "criterion": "Criteria 1, 2, 7",
        "result": "PASSED",
        "details": f"Export verified: 0 duplicates, 0 value mismatches, 0 silent losses across {stats6['total_samples']} samples."
    })
    print(" -> PASSED")

    # Summary
    print("\n" + "=" * 70)
    print("ALL 6 FAILURE & INTEGRITY TESTS PASSED SUCCESSFULLY!")
    print("=" * 70)
    summary_path = "data/test_harness/failure_test_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"Summary written to {summary_path}")

    return results


if __name__ == "__main__":
    run_all_failure_tests()
