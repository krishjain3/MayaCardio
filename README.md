# MayaCardio Bedside Biosignal Data Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests Passing](https://img.shields.io/badge/tests-33%20passed-brightgreen.svg)]()

> High-reliability dual-sensor biosignal pipeline designed for Raspberry Pi-class ARM64 embedded Linux devices operating in unmonitored hospital environments with intermittent Wi-Fi and power instability.

---

## Table of Contents
1. [Overview](#overview)
2. [Key Guarantees & Durability Trade-offs](#key-guarantees--durability-trade-offs)
3. [Architecture](#architecture)
4. [Installation & Setup](#installation--setup)
5. [Operational CLI](#operational-cli)
6. [Cloud Service & Free-Tier Deployment](#cloud-service--free-tier-deployment)
7. [Running the Automated Test Suite](#running-the-automated-test-suite)
8. [Failure Test Harness & Resource Benchmark](#failure-test-harness--resource-benchmark)
9. [Submission Deliverables & Reports](#submission-deliverables--reports)
10. [Submission Checklist](#submission-checklist)
11. [AI-Tool Disclosure](#ai-tool-disclosure)

---

## 1. Overview

The pipeline continuously records two synthetic biosignal sensors (Sensor 1 at 400 Hz with 3 channels, Sensor 2 at 400 Hz with 2 channels) over TCP from `biosignal-mock`:
- **Collect and Save:** Persists every sample durably to local SQLite with WAL and `synchronous=FULL`.
- **Online Mode:** Streams samples to the cloud continuously and computes 1-second statistical and ECG heart rate windows within 5 seconds.
- **Offline Mode:** Seamlessly continues collecting, persisting, and calculating identical window results locally when network connectivity drops.
- **Automatic Switching:** Transitions between online and offline modes in under 5 seconds (well within the 10-second requirement).
- **Catch-up Sync:** Reconciles all pending backlog samples and loss records upon reconnection, ensuring the cloud copy becomes 100% complete with zero duplicate records.
- **Honest Loss Handling:** Unnotified gaps, mock buffer overflows, or session restarts are explicitly recorded in durable loss tables (`to_index: null` when end is unknown). Values are never invented.

---

## 2. Key Guarantees & Durability Trade-offs

- **Zero Silent Loss:** Every received sample is safely written to disk. Any dropped samples are logged in the `losses` table with exact sensor, range, and reason.
- **Power Cut Loss Bound:** **At most 0.5 seconds of data (200 samples per sensor)**. Sub-second SQLite WAL transactions coupled with `PRAGMA synchronous = FULL` ensure operating system `fsync()` calls occur at least every 200 ms. Only uncommitted volatile RAM receive buffers can be lost during an abrupt power cut.
- **Exact Algorithm Parity:** Both on-device offline processing and cloud-side online processing share the same mathematical engine (`pipeline/processing.py`).

---

## 3. Architecture

```
MayaCardio-Software-Systems-Engineer/
├── pipeline/                 # Core device pipeline
│   ├── client.py             # TCP NDJSON client, CRC-32 extraction & retry
│   ├── storage.py            # SQLite engine (WAL, crash-safety, conflict checks)
│   ├── processing.py         # Section 3 window math, ECG peak detection, heart rate
│   ├── sync.py               # Connectivity monitor, cloud streamer & offline runner
│   ├── service.py            # Central background orchestrator
│   ├── models.py             # Data classes & NDJSON serializers
│   ├── crc.py                # Wire-level CRC-32 extractor and validator
│   └── cli.py                # Command-line interface
├── cloud/                    # Cloud ingestion and processing
│   ├── server.py             # Flask API endpoints (/health, /ingest, /export, /results)
│   └── deploy.sh             # Free-tier cloud deployment script
├── mock/                     # Biosignal mock service & verifier
│   ├── biosignal_mock.py     # Section 6 emulator (ramp & signal modes, fault injection)
│   └── check_ground_truth.py # Export verification tool
├── scripts/                  # Operational and benchmarking scripts
│   ├── run_device.py         # Launcher for device daemon
│   ├── run_cloud.py          # Launcher for cloud API
│   ├── measure_resources.py  # psutil 1-hour resource benchmark
│   └── failure_harness.py    # Automated failure test harness
├── deploy/systemd/           # Systemd units with 4-core & 2GB RAM limits
│   ├── mayacardio-device.service
│   └── mayacardio-cloud.service
├── tests/                    # 33 automated unit and integration tests
├── docs/                     # Required submission deliverables
│   ├── design_note.md        # Architecture & power-cut analysis (<= 2 pages)
│   ├── failure_test_report.md# Empirical failure test results and logs
│   ├── resource_report.md    # ARM64 CPU, RAM, disk, and network report
│   └── demo_video_guide.md   # <= 5 min demo recording script and checklist
├── pipeline                  # Linux executable wrapper
├── pipeline.bat              # Windows executable wrapper
├── pyproject.toml            # Packaging & dependencies
├── .env.example              # Configuration template
└── README.md
```

---

## 4. Installation & Setup

### Requirements:
- Python 3.10+ (tested on Python 3.14)
- Linux (ARM64 Raspberry Pi OS or Ubuntu) or Windows 10/11
- Recommended hardware: Raspberry Pi 4 (4 cores, 2 GB RAM)

### Fresh Board Installation:

```bash
# 1. Clone the repository
git clone https://github.com/krishjain3/MayaCardio.git
cd MayaCardio-Software-Systems-Engineer

# 2. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies in editable mode
pip install -e ".[test]"

# 4. Configure environment
cp .env.example .env
```

---

## 5. Operational CLI

The `pipeline` command conforms strictly to Section 4:

### A. Status
Prints exactly one JSON line showing current mode, samples saved, samples in the cloud, and backlog:
```bash
python3 -m pipeline.cli status
# Or using the wrapper:
./pipeline status
```
*Output:*
```json
{"mode": "online", "samples_saved": 48000, "samples_cloud": 48000, "backlog": 0, "session_id": "5f0c2a9e"}
```

### B. Export
Exports session data as newline-delimited JSON (samples followed by losses):
```bash
# Export from local device database:
./pipeline export --session 5f0c2a9e --from device

# Export from cloud service:
./pipeline export --session 5f0c2a9e --from cloud
```
*Output Format:*
```json
{"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
{"sensor": 1, "index": 120001, "values": [708, 1180, 1534]}
{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
```

### C. Results
Outputs one JSON line per 1-second window and mode with Section 3 outputs:
```bash
./pipeline results --session 5f0c2a9e
```
*Output Format:*
```json
{
  "session_id": "5f0c2a9e",
  "window": 4,
  "missing_1": 0,
  "missing_2": 0,
  "complete": true,
  "mean": {"sensor_1": [601.0, 1001.5, 1302.0], "sensor_2": [404917.9, 151981.5]},
  "min": {"sensor_1": [601, 1001, 1301], "sensor_2": [404755, 151764]},
  "max": {"sensor_1": [601, 1002, 1303], "sensor_2": [405000, 152187]},
  "magnitude": 432500.7,
  "heart_rate": 60.0,
  "mode": "online"
}
```

---

## 6. Cloud Service & Free-Tier Deployment

The cloud service (`cloud/server.py`) can be deployed to any genuinely free cloud tier (such as Render, Railway, Fly.io, or an Oracle Cloud Always Free ARM64 instance).

### Free-Tier Recommendations:
- **Render / Railway Free Tier:** Deploy as a Python web service using `gunicorn cloud.server:app` or `python3 -m cloud.server`.
- **Fly.io Free Tier:** Deploy via Dockerfile using standard 256MB micro-VM.
- **Oracle Cloud Always Free ARM64:** Deploy using the provided `deploy/systemd/mayacardio-cloud.service`.

### Running Cloud Locally:
```bash
python3 scripts/run_cloud.py --port 8000
```

---

## 7. Running the Automated Test Suite

Execute the complete 33-test pytest suite:
```bash
python3 -m pytest tests/ -v
```
All 33 tests pass with 100% success rate:
- `tests/test_crc.py` (4 tests): CRC-32 calculation & wire-level slice matching.
- `tests/test_storage.py` (4 tests): Transactions, idempotency, conflict detection.
- `tests/test_processing.py` (10 tests): Statistical window formulas, peak detection, 60 bpm, missing data boundaries, 800-sample history boundary, 100-sample boundary suppression, different sensor arrival times.
- `tests/test_client.py` (3 tests): TCP polling, corrupt CRC rejection, mock restarts.
- `tests/test_sync.py` (5 tests): Health monitoring, mode transitions, catch-up backlog, cloud ingest on different sensor arrival times, production auth rejection.
- `tests/test_cli.py` (3 tests): Status, export, and results CLI commands.
- `tests/test_fault_injection.py` (4 tests): Network cuts, crash recovery, ground truth checks.

---

## 8. Failure Test Harness & Resource Benchmark

### Run the Systematic Failure Test Harness:
```bash
python3 scripts/failure_harness.py
```
Executes all 6 major fault injection scenarios and writes evidence to `data/test_harness/failure_test_summary.json`.

### Run the Resource Utilization Benchmark:
```bash
# 60-second verification benchmark:
python3 scripts/measure_resources.py --duration 60

# Full 1-hour official benchmark:
python3 scripts/measure_resources.py --duration 3600
```
Reports average CPU, peak RAM, disk bytes written, network bytes sent, and system specs.

---

## 9. Submission Deliverables & Reports

All five required submission deliverables are complete and available in `docs/`:

1. **Design Note (at most 2 pages):** [`docs/design_note.md`](file:///docs/design_note.md)
   - Complete system architecture.
   - Guarantees and non-guarantees.
   - Maximum data loss on sudden power cut (<= 0.5s) and engineering justification.
2. **Failure Test Evidence Report:** [`docs/failure_test_report.md`](file:///docs/failure_test_report.md)
   - Detailed logs and outcomes for corrupt CRC, session restart, network cuts, and process kills.
3. **Resource Utilization Report:** [`docs/resource_report.md`](file:///docs/resource_report.md)
   - Measured CPU, peak RAM (52.3 MB), disk written, and network transfer.
4. **Demo Video Script & Checklist:** [`docs/demo_video_guide.md`](file:///docs/demo_video_guide.md)
   - Step-by-step 5-minute video recording guide for network cut, process kill, and power cut.
5. **Install & Run Instructions:** Contained directly in this `README.md`.

---

## 10. Submission Checklist

- [x] Every sample received by device is saved on disk unchanged (Criterion 1).
- [x] Missing samples listed in `losses` with reason and sensor (Criterion 2).
- [x] Survives crash, reboot, power cut with stated loss <= 0.5s (Criterion 3).
- [x] In online mode, 1-sec windows produced within 5 seconds (Criterion 4).
- [x] Mode switches within 10s of network changing, without loss or duplication (Criterion 5).
- [x] Cloud copy complete within 15 minutes after reconnection (Criterion 6).
- [x] Results follow Section 3 exactly, verified against 60/90/72 bpm (Criterion 7).
- [x] Runs within Raspberry Pi limits (4 cores, 2 GB RAM; peak RSS 52 MB) (Criterion 8).
- [x] Required CLI commands implemented: `pipeline status`, `pipeline export`, `pipeline results`.
- [x] Genuinely free cloud deployment path documented.
- [x] Complete automated test suite passing (33/33 passed).

---

## 11. AI-Tool Disclosure

In compliance with the candidate assessment instructions and job description:
- **AI Tool Used:** Google Antigravity IDE (Gemini 3.8 Flash model).
- **Scope of AI Assistance:** Used for code structuring, drafting test fixtures, mathematical formula verification, and documentation formatting.
- **Verification Notice:** All algorithms, mathematical logic, CRC extraction rules, database concurrency behaviors, and failure scenarios were independently inspected, executed, and verified through empirical test runs on the host system.
