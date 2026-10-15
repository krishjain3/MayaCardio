# MayaCardio Data Pipeline: Demo Video Guide & Recording Script

**Assessment Deliverable 5:** Demonstration Video  
**Maximum Duration:** 5 minutes  
**Required Demonstrations:** Network cut, process kill, sudden power cut  

---

## 1. Video Structure & Timeline (Total: 4 min 30 sec)

| Section | Timestamp | Action / Demonstration | Key Talking Point |
| :--- | :---: | :--- | :--- |
| **1. Introduction & Setup** | 0:00 – 0:45 | Show Raspberry Pi environment, mock running, pipeline starting. | Introduce architecture, small-device resource compliance (<= 4 cores, 2 GB RAM). |
| **2. Online Streaming** | 0:45 – 1:30 | Run `pipeline status` showing `mode: online`, 0 backlog. Run `pipeline results`. | Demonstrate live cloud ingestion and 1-second window results within 5 seconds. |
| **3. Network Cut & Recovery** | 1:30 – 3:00 | Cut network connection. Show switch to `offline` (< 10s). Local results continue. Reconnect and catch up. | Show zero data loss, durable backlog accumulation, and rapid catch-up sync. |
| **4. Process Kill & Power Cut** | 3:00 – 4:15 | Kill process (`kill -9`). Restart. Show WAL recovery. Pull power/kill VM. Verify <= 0.5s loss limit. | Explain SQLite WAL + `synchronous=FULL` guarantee and lack of database corruption. |
| **5. Conclusion & Verification**| 4:15 – 4:45 | Run `check_ground_truth.py` on exported data. | Show 0 duplicates, 0 value mismatches, 0 silent losses. |

---

## 2. Recommended Screen Layout

- **Left Terminal (Device / Raspberry Pi):** Running `pipeline run` and operational CLI commands (`pipeline status`, `pipeline export`).
- **Right Terminal (Cloud Service / Network Controller):** Running `cloud.server` and network disconnect script (`iptables` / stopping cloud process).
- **Bottom Panel (System Metrics):** `htop` or `scripts/measure_resources.py` showing CPU usage < 25% and RAM < 60 MB.

---

## 3. Step-by-Step Recording Script

### Scene 1: Baseline Online Ingestion (0:00 – 1:30)
1. **Action:** Start `biosignal-mock` in terminal 1:
   ```bash
   python3 -m mock.biosignal_mock --mode signal --seed 7 --port 7100
   ```
2. **Action:** Start `cloud.server` in terminal 2:
   ```bash
   python3 -m cloud.server
   ```
3. **Action:** Start `pipeline run` in terminal 3:
   ```bash
   pipeline run
   ```
4. **Action:** Check status in terminal 4:
   ```bash
   pipeline status
   ```
   *Expected Output:*
   ```json
   {"mode": "online", "samples_saved": 4800, "samples_cloud": 4800, "backlog": 0, "session_id": "..."}
   ```
5. **Action:** Display computed 1-second results:
   ```bash
   pipeline results --session <session_id>
   ```

### Scene 2: Network Cut & Catch-Up Synchronization (1:30 – 3:00)
1. **Action:** Terminate the cloud server or block port 8000 using `iptables` / task manager.
2. **Narration:** *"We now cut the network connection to simulate a hospital Wi-Fi drop."*
3. **Action:** Run `pipeline status` repeatedly:
   - Within 4–6 seconds (well under the 10-second limit), output shifts to:
     ```json
     {"mode": "offline", "samples_saved": 7200, "samples_cloud": 4800, "backlog": 2400}
     ```
4. **Narration:** *"The device seamlessly continues collecting and saving every sample locally. It also processes 1-second windows offline."*
5. **Action:** Run `pipeline results --session <session_id>`: Show new windows computed with `"mode": "offline"`.
6. **Action:** Restore cloud server / unblock network.
7. **Action:** Observe automatic reconnection within 2 seconds:
   - Backlog immediately starts draining.
   - Within a few seconds, `pipeline status` confirms `backlog: 0` and `mode: online`.

### Scene 3: Process Kill & Sudden Power Cut (3:00 – 4:15)
1. **Action:** Send a hard kill signal to the pipeline service process:
   ```bash
   kill -9 <pipeline_pid>
   ```
2. **Narration:** *"We abruptly terminate the pipeline process with SIGKILL to simulate a sudden crash."*
3. **Action:** Restart `pipeline run`.
4. **Action:** Run `pipeline status`:
   - Storage recovers immediately from SQLite WAL.
   - All previously committed samples are intact.
5. **Narration:** *"Because we use SQLite with synchronous=FULL and immediate transaction commits, all committed data survives power drops. The maximum possible loss is strictly bounded to at most 0.5 seconds of uncommitted volatile RAM buffer."*

### Scene 4: Ground Truth Verification (4:15 – 4:45)
1. **Action:** Export the session from device:
   ```bash
   pipeline export --session <session_id> --from device > export.ndjson
   ```
2. **Action:** Run the ground truth checker:
   ```bash
   python3 -m mock.check_ground_truth export.ndjson --mode signal --seed 7
   ```
3. **Action:** Highlight results:
   ```json
   {
     "passed": true,
     "duplicates": 0,
     "value_mismatches": 0,
     "silent_losses": 0
   }
   ```
4. **Closing Statement:** *"Every sample received was safely saved on disk, zero duplicates were created, results match clinical reference rules, and resources remained well within Raspberry Pi 4 limits."*

---

## 4. Pre-Recording Checklist

- [ ] Clean database files (`rm -f data/*.db data/*.db-wal data/*.db-shm`).
- [ ] Confirm Python packages installed (`pytest`, `requests`, `flask`, `psutil`, `numpy`).
- [ ] Verify mock server port (7100) and cloud port (8000) are free.
- [ ] Check terminal font sizes (minimum 16pt for clear video resolution).
- [ ] Set video recording resolution to 1080p (1920x1080) at 30 or 60 fps.
- [ ] Ensure total runtime is strictly under 5 minutes.
