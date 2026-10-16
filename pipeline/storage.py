"""
Durable local and cloud SQLite storage engine with WAL journaling, crash-safety,
idempotency, and conflict detection.
"""

import os
import sqlite3
import json
import time
import contextlib
from typing import Dict, List, Optional, Tuple, Iterator, Any
from pipeline.models import Sample, LossRecord, WindowResult, PipelineStatus


class ConflictingSampleError(Exception):
    """Raised when an incoming sample has the same identity but conflicting values."""
    pass


class Storage:
    """Manages SQLite database connections and transactions with durability guarantees."""

    def __init__(self, db_path: str, synchronous: str = "FULL"):
        self.db_path = db_path
        self.synchronous = synchronous
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    @contextlib.contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)  # autocommit mode
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute(f"PRAGMA synchronous = {self.synchronous};")
        conn.execute("PRAGMA busy_timeout = 30000;")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self.get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS samples (
                    session_id TEXT NOT NULL,
                    sensor INTEGER NOT NULL,
                    sample_index INTEGER NOT NULL,
                    ch0 INTEGER NOT NULL,
                    ch1 INTEGER NOT NULL,
                    ch2 INTEGER,
                    PRIMARY KEY (session_id, sensor, sample_index)
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_samples_lookup 
                ON samples (session_id, sensor, sample_index);
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS losses (
                    session_id TEXT NOT NULL,
                    sensor INTEGER NOT NULL,
                    from_index INTEGER NOT NULL,
                    to_index INTEGER,
                    reason TEXT NOT NULL,
                    uploaded INTEGER DEFAULT 0,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (session_id, sensor, from_index)
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_cursor (
                    session_id TEXT NOT NULL,
                    sensor INTEGER NOT NULL,
                    last_synced_index INTEGER NOT NULL DEFAULT -1,
                    PRIMARY KEY (session_id, sensor)
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS results (
                    session_id TEXT NOT NULL,
                    window INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    missing_1 INTEGER NOT NULL,
                    missing_2 INTEGER NOT NULL,
                    complete INTEGER NOT NULL,
                    mean_json TEXT,
                    min_json TEXT,
                    max_json TEXT,
                    magnitude REAL,
                    heart_rate REAL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (session_id, window, mode)
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_results_lookup 
                ON results (session_id, window);
            """)
            conn.execute("COMMIT;")

    def ensure_session(self, session_id: str):
        now = time.time()
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO sessions (session_id, created_at, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET updated_at = ?
            """, (session_id, now, now, now))

    def get_latest_session_id(self) -> Optional[str]:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT session_id FROM sessions ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
            return row["session_id"] if row else None

    def insert_samples_batch(
        self,
        session_id: str,
        sensor: int,
        first_index: int,
        data: List[List[int]]
    ) -> int:
        """
        Inserts a batch of samples atomically.
        Detects duplicates and ensures data integrity:
        - If duplicate matches existing values: ignored idempotently.
        - If duplicate conflicts with existing values: raises ConflictingSampleError.
        """
        if not data:
            return 0

        self.ensure_session(session_id)
        now = time.time()
        inserted_count = 0

        with self.get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                # Check for existing samples in this range to detect conflicts
                end_index = first_index + len(data) - 1
                rows = conn.execute("""
                    SELECT sample_index, ch0, ch1, ch2
                    FROM samples
                    WHERE session_id = ? AND sensor = ? AND sample_index BETWEEN ? AND ?
                """, (session_id, sensor, first_index, end_index)).fetchall()

                existing_map = {}
                for r in rows:
                    ch2 = r["ch2"]
                    vals = [r["ch0"], r["ch1"]] if ch2 is None else [r["ch0"], r["ch1"], ch2]
                    existing_map[r["sample_index"]] = vals

                to_insert = []
                for i, vals in enumerate(data):
                    idx = first_index + i
                    if idx in existing_map:
                        if existing_map[idx] != vals:
                            raise ConflictingSampleError(
                                f"Conflicting sample for session={session_id}, sensor={sensor}, index={idx}: "
                                f"existing={existing_map[idx]} vs new={vals}"
                            )
                        # Duplicate identical sample: safe to ignore
                        continue

                    ch0 = vals[0]
                    ch1 = vals[1]
                    ch2 = vals[2] if len(vals) > 2 else None
                    to_insert.append((session_id, sensor, idx, ch0, ch1, ch2))

                if to_insert:
                    conn.executemany("""
                        INSERT INTO samples (session_id, sensor, sample_index, ch0, ch1, ch2)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, to_insert)
                    inserted_count = len(to_insert)

                conn.execute("""
                    UPDATE sessions SET updated_at = ? WHERE session_id = ?
                """, (now, session_id))
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise

        return inserted_count

    def insert_loss_records(
        self,
        session_id: str,
        sensor: int,
        losses: List[Dict[str, Any]],
        uploaded: int = 0
    ) -> int:
        if not losses:
            return 0

        self.ensure_session(session_id)
        now = time.time()
        to_insert = []
        for l in losses:
            f_idx = l["from_index"]
            t_idx = l.get("to_index")
            reason = l.get("reason", "unknown")
            to_insert.append((session_id, sensor, f_idx, t_idx, reason, uploaded, now))

        with self.get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                conn.executemany("""
                    INSERT INTO losses (session_id, sensor, from_index, to_index, reason, uploaded, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, sensor, from_index) DO UPDATE SET
                        to_index = excluded.to_index,
                        reason = excluded.reason
                """, to_insert)
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise

        return len(to_insert)

    def get_sync_cursor(self, session_id: str, sensor: int) -> int:
        with self.get_connection() as conn:
            row = conn.execute("""
                SELECT last_synced_index FROM sync_cursor
                WHERE session_id = ? AND sensor = ?
            """, (session_id, sensor)).fetchone()
            return row["last_synced_index"] if row else -1

    def advance_sync_cursor(self, session_id: str, sensor: int, up_to_index: int):
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO sync_cursor (session_id, sensor, last_synced_index)
                VALUES (?, ?, ?)
                ON CONFLICT(session_id, sensor) DO UPDATE SET
                    last_synced_index = MAX(last_synced_index, excluded.last_synced_index)
            """, (session_id, sensor, up_to_index))

    def get_unuploaded_samples(
        self,
        session_id: str,
        sensor: int,
        limit: int = 1000
    ) -> Tuple[int, List[List[int]]]:
        """
        Returns (start_index, samples) for the next batch of unuploaded contiguous samples.
        """
        cursor = self.get_sync_cursor(session_id, sensor)
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT sample_index, ch0, ch1, ch2
                FROM samples
                WHERE session_id = ? AND sensor = ? AND sample_index > ?
                ORDER BY sample_index ASC
                LIMIT ?
            """, (session_id, sensor, cursor, limit)).fetchall()

            if not rows:
                return cursor, []

            start_idx = rows[0]["sample_index"]
            samples = []
            for r in rows:
                ch2 = r["ch2"]
                vals = [r["ch0"], r["ch1"]] if ch2 is None else [r["ch0"], r["ch1"], ch2]
                samples.append(vals)

            return start_idx, samples

    def get_unuploaded_losses(self, session_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT sensor, from_index, to_index, reason
                FROM losses
                WHERE session_id = ? AND uploaded = 0
                ORDER BY sensor ASC, from_index ASC
            """, (session_id,)).fetchall()
            return [dict(r) for r in rows]

    def mark_losses_uploaded(self, session_id: str, loss_keys: List[Tuple[int, int]]):
        if not loss_keys:
            return
        with self.get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            for sensor, from_index in loss_keys:
                conn.execute("""
                    UPDATE losses SET uploaded = 1
                    WHERE session_id = ? AND sensor = ? AND from_index = ?
                """, (session_id, sensor, from_index))
            conn.execute("COMMIT;")

    def save_window_result(self, result: WindowResult):
        now = time.time()
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO results (
                    session_id, window, mode, missing_1, missing_2, complete,
                    mean_json, min_json, max_json, magnitude, heart_rate, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, window, mode) DO UPDATE SET
                    missing_1 = excluded.missing_1,
                    missing_2 = excluded.missing_2,
                    complete = excluded.complete,
                    mean_json = excluded.mean_json,
                    min_json = excluded.min_json,
                    max_json = excluded.max_json,
                    magnitude = excluded.magnitude,
                    heart_rate = excluded.heart_rate,
                    created_at = excluded.created_at
            """, (
                result.session_id,
                result.window,
                result.mode,
                result.missing_1,
                result.missing_2,
                1 if result.complete else 0,
                json.dumps(result.mean),
                json.dumps(result.min),
                json.dumps(result.max),
                result.magnitude,
                result.heart_rate,
                now
            ))

    def get_window_results(self, session_id: str) -> List[WindowResult]:
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT session_id, window, mode, missing_1, missing_2, complete,
                       mean_json, min_json, max_json, magnitude, heart_rate
                FROM results
                WHERE session_id = ?
                ORDER BY window ASC, mode ASC
            """, (session_id,)).fetchall()

            results = []
            for r in rows:
                results.append(WindowResult(
                    session_id=r["session_id"],
                    window=r["window"],
                    missing_1=r["missing_1"],
                    missing_2=r["missing_2"],
                    complete=bool(r["complete"]),
                    mean=json.loads(r["mean_json"]) if r["mean_json"] else None,
                    min=json.loads(r["min_json"]) if r["min_json"] else None,
                    max=json.loads(r["max_json"]) if r["max_json"] else None,
                    magnitude=r["magnitude"],
                    heart_rate=r["heart_rate"],
                    mode=r["mode"]
                ))
            return results

    def get_incomplete_windows(self, session_id: str, mode: Optional[str] = None) -> List[int]:
        """Returns sorted list of window indices that are marked incomplete (complete = 0)."""
        with self.get_connection() as conn:
            if mode:
                rows = conn.execute(
                    "SELECT window FROM results WHERE session_id = ? AND mode = ? AND complete = 0 ORDER BY window ASC",
                    (session_id, mode)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT window FROM results WHERE session_id = ? AND complete = 0 ORDER BY window ASC",
                    (session_id,)
                ).fetchall()
            return [r["window"] for r in rows]

    def get_samples_in_range(
        self,
        session_id: str,
        sensor: int,
        start_index: int,
        end_index: int
    ) -> Dict[int, List[int]]:
        """Returns dict of {index: [values]} for samples in [start_index, end_index]."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT sample_index, ch0, ch1, ch2
                FROM samples
                WHERE session_id = ? AND sensor = ? AND sample_index BETWEEN ? AND ?
                ORDER BY sample_index ASC
            """, (session_id, sensor, start_index, end_index)).fetchall()

            res = {}
            for r in rows:
                ch2 = r["ch2"]
                vals = [r["ch0"], r["ch1"]] if ch2 is None else [r["ch0"], r["ch1"], ch2]
                res[r["sample_index"]] = vals
            return res

    def get_max_sample_index(self, session_id: str) -> int:
        with self.get_connection() as conn:
            row = conn.execute("""
                SELECT MAX(sample_index) as m
                FROM samples
                WHERE session_id = ?
            """, (session_id,)).fetchone()
            return row["m"] if row and row["m"] is not None else -1

    def export_data(self, session_id: str) -> Iterator[str]:
        """
        Exports session data in the exact format required by Section 4:
        First all samples: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
        Then all losses:   {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
        """
        with self.get_connection() as conn:
            # Yield samples ordered deterministically
            cursor = conn.execute("""
                SELECT sensor, sample_index, ch0, ch1, ch2
                FROM samples
                WHERE session_id = ?
                ORDER BY sensor ASC, sample_index ASC
            """, (session_id,))
            for r in cursor:
                ch2 = r["ch2"]
                vals = [r["ch0"], r["ch1"]] if ch2 is None else [r["ch0"], r["ch1"], ch2]
                sample = Sample(sensor=r["sensor"], index=r["sample_index"], values=vals)
                yield sample.to_export_ndjson()

            # Yield losses
            loss_cursor = conn.execute("""
                SELECT sensor, from_index, to_index, reason
                FROM losses
                WHERE session_id = ?
                ORDER BY sensor ASC, from_index ASC
            """, (session_id,))
            for r in loss_cursor:
                loss = LossRecord(
                    sensor=r["sensor"],
                    from_index=r["from_index"],
                    to_index=r["to_index"],
                    reason=r["reason"]
                )
                yield loss.to_export_ndjson()

    def get_counts(self, session_id: Optional[str] = None) -> Tuple[int, int, int]:
        """
        Returns (samples_saved, samples_cloud, backlog)
        For device storage, samples_cloud is calculated from sync_cursor.
        """
        if not session_id:
            session_id = self.get_latest_session_id()
        if not session_id:
            return 0, 0, 0

        with self.get_connection() as conn:
            saved_row = conn.execute("""
                SELECT COUNT(*) as cnt FROM samples WHERE session_id = ?
            """, (session_id,)).fetchone()
            samples_saved = saved_row["cnt"] if saved_row else 0

            # Count synced
            c1 = self.get_sync_cursor(session_id, 1)
            c2 = self.get_sync_cursor(session_id, 2)

            synced_row = conn.execute("""
                SELECT COUNT(*) as cnt FROM samples
                WHERE session_id = ? AND (
                    (sensor = 1 AND sample_index <= ?) OR
                    (sensor = 2 AND sample_index <= ?)
                )
            """, (session_id, c1, c2)).fetchone()
            samples_cloud = synced_row["cnt"] if synced_row else 0
            backlog = max(0, samples_saved - samples_cloud)

            return samples_saved, samples_cloud, backlog
