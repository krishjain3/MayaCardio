# MayaCardio Data Pipeline: Failure Test Evidence Report

**Test Suite:** Systematic Fault Injection and Recovery Harness  
**Environment:** Python 3.14 / SQLite 3.45 / Windows & Linux ARM64 Compatible  
**Date of Execution:** October 2026  
**Status:** **ALL TESTS PASSED (100% Pass Rate)**  

---

## 1. Executive Summary

This report documents empirical validation of all failure modes specified in Sections 4, 6.2, and 6.3 of the MayaCardio assessment. The automated failure test harness (`scripts/failure_harness.py`) and pytest suite (`tests/`) executed simulated network cuts, corrupt packets, mock restarts, process terminations, and ground truth validations.

| Test # | Test Description | Target Criterion | Injected Fault | Observed Behavior | Status |
| :---: | :--- | :--- | :--- | :--- | :---: |
| **1** | Corrupt CRC-32 Packet | Section 6.2, 6.3 | Injected bad CRC `00000000` | Packet rejected, re-read requested with `from_index` | **PASSED** |
| **2** | Mock Session Restart | Criterion 2, Sec 6.3 | New `session_id` sent | Old session unread data flagged `to_index: null` | **PASSED** |
| **3** | Network Disconnection | Criterion 4, 5 | Cloud API killed | Switched to `offline` in < 4s, collection continued | **PASSED** |
| **4** | Offline Local Processing | Criterion 3, 7 | 800 samples ingested offline | 1-sec windows computed on device with identical math | **PASSED** |
| **5** | Network Restoration & Catch-Up | Criterion 5, 6 | Cloud API restored | Switched to `online` in < 2s, backlog uploaded 100% | **PASSED** |
| **6** | Sudden Process Termination | Criterion 3 | Hard termination without close | SQLite WAL cleanly recovered all committed samples | **PASSED** |
| **7** | Ramp Mode Ground Truth | Criteria 1, 2, 7 | Predictable ramp signal | 0 duplicates, 0 mismatches, 0 silent losses | **PASSED** |
| **8** | Signal Mode Ground Truth | Criteria 1, 2, 7 | Synthetic ECG signal | 0 duplicates, 0 mismatches, 0 silent losses | **PASSED** |

---

## 2. Detailed Test Logs and Evidence

### Test 1: Corrupt CRC-32 Injection & Re-read
- **Mechanism:** Mock server configured with `corrupt_crc_rate = 1.0` to inject `crc32: "00000000"`.
- **Validation Log:**
  ```text
  Corrupt reply for sensor 1! CRC mismatch: got 67cdd1f1, expected 00000000. Rejecting reply and re-reading from index 0.
  ```
- **Result:** Reply was safely rejected before database ingestion. Client issued re-read request `{"cmd": "read", "sensor": 1, "from_index": 0}`, successfully receiving valid data once the fault was cleared.

### Test 2: Mock Session Restart (Unread Data Loss)
- **Mechanism:** Active session `session_A` interrupted; mock server sends new packet with `session_B` and reset index 0.
- **Validation Log:**
  ```text
  Mock restart detected! Previous session: session_A, new session: session_B. 
  Recording unread data of previous session as lost with to_index: null.
  ```
- **Database Query Result:**
  ```json
  {"sensor": 1, "from_index": 1500, "to_index": null, "reason": "session_restart", "session_id": "session_A"}
  {"sensor": 2, "from_index": 1500, "to_index": null, "reason": "session_restart", "session_id": "session_A"}
  ```
- **Result:** Meets Criterion 2 and Section 6.3 without creating silent gaps or mixing session sequences.

### Test 3 & 4: Network Disconnect & Offline Processing
- **Mechanism:** Ingested 400 samples online. Cloud service was then terminated. Ingested 800 additional samples while disconnected.
- **Validation Log:**
  ```text
  Cloud connectivity lost (2 failed attempts). Switching to OFFLINE mode.
  [Offline Processing] Computed Window 0: complete=True, magnitude=5.0, mode=offline
  [Offline Processing] Computed Window 1: complete=True, magnitude=5.0, mode=offline
  [Offline Processing] Computed Window 2: complete=True, magnitude=5.0, mode=offline
  ```
- **State Check:** Device database accumulated 2,400 samples with backlog = 1,600. Zero samples lost during disconnection. Mode switched in 3.8 seconds (< 10-second threshold).

### Test 5: Reconnection & Backlog Synchronization
- **Mechanism:** Cloud service restarted on port 8891.
- **Validation Log:**
  ```text
  Cloud connectivity restored. Switching to ONLINE mode.
  POST /api/v1/ingest HTTP/1.1 200 - Uploaded batch (500 samples)
  POST /api/v1/ingest HTTP/1.1 200 - Uploaded batch (500 samples)
  POST /api/v1/ingest HTTP/1.1 200 - Uploaded batch (500 samples)
  POST /api/v1/ingest HTTP/1.1 200 - Uploaded batch (100 samples)
  ```
- **Result:** Cloud sample count reached 2,400. Device backlog dropped to 0. Cloud export matched device export with 0 discrepancies.

### Test 6: Abrupt Process Kill (Power Cut Simulation)
- **Mechanism:** Storage instance written with 400 samples under `PRAGMA synchronous = FULL` and destroyed immediately without closing connections or checkpointing.
- **Reboot Query:** Fresh storage opened on same file:
  ```python
  saved = storage.get_counts("crash_recovery_sess")[0]
  assert saved == 400
  ```
- **Result:** SQLite WAL automatic recovery cleanly replayed log. No database corruption, zero duplicates.

### Tests 7 & 8: Ground Truth Verification Tool (`check_ground_truth.py`)
- **Ramp Mode Result:**
  ```json
  {
    "passed": true,
    "total_samples": 1204,
    "sensor_1_samples": 602,
    "sensor_2_samples": 602,
    "loss_records": 0,
    "duplicates": 0,
    "value_mismatches": 0,
    "silent_losses": 0
  }
  ```
- **Signal Mode Result:**
  ```json
  {
    "passed": true,
    "total_samples": 1204,
    "sensor_1_samples": 602,
    "sensor_2_samples": 602,
    "loss_records": 0,
    "duplicates": 0,
    "value_mismatches": 0,
    "silent_losses": 0
  }
  ```

---

## 3. Disclosures & Reproducibility Notice

1. **Mock Service Binary:** The official assessment document omitted the URL (`Download: __ (ARM64 and x86-64)`). The tests were executed against a high-fidelity implementation of `biosignal-mock` conforming to all parameters in Section 6.
2. **Physical Power-Cut Verification:** Sudden power cuts were simulated programmatically via OS process termination and uncheckpointed memory drop. To perform a physical power cut test on hardware, deploy `deploy/systemd/mayacardio-device.service` on a physical Raspberry Pi 4, connect to `biosignal-mock`, and disconnect the USB-C power lead during continuous sampling.
