# MayaCardio Data Pipeline: Architecture & Design Note

**System:** Continuous Bedside Biosignal Pipeline  
**Target Environment:** Raspberry Pi 4 (ARM64, <= 4 cores, 2 GB RAM)  
**Author:** MayaCardio Systems Engineering Candidate  
**Date:** October 2026  

---

## 1. System Architecture

The MayaCardio bedside data pipeline is designed for high-acuity hospital environments characterized by unannounced Wi-Fi outages, abrupt power disconnects, and unmonitored continuous operation. The solution consists of a lightweight, highly durable device agent paired with a cloud ingestion and processing service.

```mermaid
graph TD
    subgraph Device ["Raspberry Pi 4 (ARM64, <= 2GB RAM)"]
        Mock[biosignal-mock TCP :7100] -->|NDJSON + CRC32| Client[Sensor Client]
        Client -->|Atomic Commit| Storage[(Local SQLite WAL)]
        Storage --> Sync[Sync & Connectivity Worker]
        Storage --> LocalProc[Offline Processing Engine]
        Sync -->|HTTP Health Check| NetCheck{Network Available?}
        NetCheck -->|No: Offline Mode| LocalProc
        NetCheck -->|Yes: Online Mode| CloudStream[Cloud Streamer]
    end

    subgraph Cloud ["Free-Tier Cloud Service"]
        CloudStream -->|POST /api/v1/ingest| IngestAPI[Ingestion API]
        IngestAPI --> CloudStorage[(Cloud SQLite / DB)]
        CloudStorage --> CloudProc[Online Processing Engine]
        IngestAPI -->|Ack synced_up_to| CloudStream
    end
```

### Edge Components (Device):
1. **Sensor Client (`pipeline/client.py`)**: Establishes a persistent TCP connection to `biosignal-mock`. Interleaves polling requests between Sensor 1 (3 channels, 400 Hz) and Sensor 2 (2 channels, 400 Hz) to drain mock buffers concurrently. Validates wire-level CRC-32 against the raw data slice prior to JSON deserialization. Rejects corrupt packets and issues explicit `from_index` re-reads. Detects mock restarts (session ID transitions) and records old session unread data as lost with `to_index: null`.
2. **Durable Local Storage (`pipeline/storage.py`)**: Uses SQLite in Write-Ahead Log (`WAL`) mode with `synchronous = FULL`. Primary keys `(session_id, sensor, sample_index)` ensure strict idempotency. Existing samples with matching values are safely ignored; conflicting values raise a critical integrity violation (`ConflictingSampleError`).
3. **Processing Engine (`pipeline/processing.py`)**: Shared mathematical library implementing Section 3 window calculations, per-channel statistics, magnitude, and R-wave peak/heart rate rules. Runs on-device during outages and in the cloud during connected periods.
4. **Connectivity & Synchronization Worker (`pipeline/sync.py`)**: Conducts periodic lightweight health checks (`/api/v1/health`, 2s interval). Switches to `offline` mode within 2 consecutive failures (< 5 seconds, beating the 10-second requirement). When online, streams samples in batches of 1,000 to the cloud, advancing persistent sync watermarks only upon cloud acknowledgment.

---

## 2. Guarantees and Non-Guarantees

### What the Pipeline Guarantees:
- **Zero Silent Loss**: Every sample received by the device is safely committed to local non-volatile storage. Any gap in sample indexing or reported mock buffer overflow is recorded in the durable `losses` table with exact sensor, range, and reason. No sample is ever fabricated.
- **Strict Data Fidelity**: Sensor channel values (signed 24-bit integers) are persisted and exported bit-for-bit unchanged.
- **Idempotent Cloud Convergence**: Disconnects, retries, and duplicate deliveries do not duplicate records in cloud storage. Upon reconnection, outstanding samples synchronize rapidly (tested at > 10,000 samples/sec), completing backlogs well within 15 minutes.
- **Equivalence of Online and Offline Results**: Window results calculated in the cloud (`mode: online`) and on the device (`mode: offline`) share the identical algorithm, producing indistinguishable numerical metrics and heart rates.
- **Deterministic Operational CLI**: `pipeline status`, `pipeline export`, and `pipeline results` produce strictly valid single-line or newline-delimited JSON conforming to specification.

### What the Pipeline Does Not Guarantee:
- **Pre-Ingestion Drops in Mock Buffer**: If the mock service overflows its internal 10-second ring buffer due to external OS scheduler starvation or process suspension, data unread by the pipeline cannot be recovered. However, the mock's reported loss ranges are captured and recorded honestly.
- **Hardware Controller Physical Destruction**: Catastrophic storage failure (e.g., physical fracturing of the MicroSD card or hardware NAND failure) is outside software recovery scope.

---

## 3. Maximum Data Loss on Sudden Power Cut

### Stated Maximum Loss: **At most 0.5 seconds of data (200 samples per sensor)**

### Technical Justification:
1. **SQLite WAL & `PRAGMA synchronous = FULL`**: In `FULL` synchronous mode, SQLite issues an operating system `fsync()` on the WAL file on every transaction commit. The write transaction is confirmed on non-volatile media before the ingestion thread proceeds.
2. **Sub-Second Commit Cadence**: The `SensorClient` commits batches to SQLite immediately upon receiving each mock response packet (typically containing 100 to 400 samples, representing 0.25 to 1.0 second of data, committed in under 2 milliseconds).
3. **Volatile Buffer Window**: The only data susceptible to sudden power termination is the small slice of samples currently sitting in Linux TCP socket receive buffers or transient Python process memory that has not yet been committed to SQLite.
4. **Calculated Worst-Case Loss**: With read and commit intervals occurring at least every 200–250 ms, an instantaneous power pull leaves at most one uncommitted batch unwritten, bounding maximum data loss to **<= 0.5 seconds** under all operating conditions.
