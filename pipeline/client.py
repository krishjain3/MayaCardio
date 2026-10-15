"""
TCP Sensor Client conforming strictly to Section 6 of the MayaCardio assessment.
Reads Sensor 1 and Sensor 2 sequentially/interleaved over a single TCP connection.
Validates wire-level CRC-32, detects corruptions, duplicate replies, reported losses,
session restarts, and unnotified index gaps.
"""

import json
import logging
import socket
import time
from typing import Dict, List, Optional, Tuple, Any

from pipeline.crc import extract_data_slice_and_validate_crc
from pipeline.storage import Storage, ConflictingSampleError

logger = logging.getLogger("mayacardio.client")


class SensorClient:
    """Connects to biosignal-mock TCP server and ingests samples safely to local storage."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7100,
        storage: Optional[Storage] = None,
        timeout: float = 3.0,
        max_reconnect_delay: float = 2.0
    ):
        self.host = host
        self.port = port
        self.storage = storage
        self.timeout = timeout
        self.max_reconnect_delay = max_reconnect_delay

        self.sock: Optional[socket.socket] = None
        self.sock_file = None
        self.running = False

        self.current_session_id: Optional[str] = None
        self.next_expected_index = {1: 0, 2: 0}
        self.session_end_reached = {1: False, 2: False}
        self.last_sensor_polled = 2  # start with 1 next

    def connect(self) -> bool:
        """Establishes TCP connection to biosignal-mock server with retry."""
        self.disconnect()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect((self.host, self.port))
            self.sock = s
            self.sock_file = s.makefile("r", encoding="utf-8")
            logger.info(f"Connected to biosignal-mock at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.warning(f"Connection to biosignal-mock failed: {e}")
            self.disconnect()
            return False

    def disconnect(self):
        if self.sock_file:
            try:
                self.sock_file.close()
            except Exception:
                pass
            self.sock_file = None
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def read_sensor_once(self, sensor: int, force_from_index: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Sends a single read command for sensor, validates reply and CRC-32.
        Returns parsed reply dict if valid, or None if corrupt/error.
        """
        if not self.sock:
            if not self.connect():
                return None

        from_idx = force_from_index if force_from_index is not None else self.next_expected_index.get(sensor, 0)
        req = {"cmd": "read", "sensor": sensor}
        if from_idx > 0:
            req["from_index"] = from_idx

        req_line = json.dumps(req) + "\n"

        try:
            self.sock.sendall(req_line.encode("utf-8"))
            raw_line = self.sock_file.readline()
            if not raw_line:
                logger.warning(f"Mock server closed connection for sensor {sensor}")
                self.disconnect()
                return None

            raw_line = raw_line.strip()
            if not raw_line:
                return None

            # Validate wire-level CRC-32 strictly
            is_crc_valid, actual_crc, expected_crc = extract_data_slice_and_validate_crc(raw_line)
            if not is_crc_valid:
                logger.error(
                    f"Corrupt reply for sensor {sensor}! CRC mismatch: got {actual_crc}, expected {expected_crc}. "
                    f"Rejecting reply and re-reading from index {from_idx}."
                )
                return None

            reply = json.loads(raw_line)
            return reply

        except (socket.timeout, socket.error, ConnectionResetError, BrokenPipeError) as e:
            logger.warning(f"Socket error reading sensor {sensor}: {e}")
            self.disconnect()
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing reply for sensor {sensor}: {e}")
            return None

    def process_reply(self, reply: Dict[str, Any]) -> bool:
        """
        Ingests a validated reply into durable storage, handling session restarts,
        losses, gaps, and sample insertion.
        """
        sensor = reply["sensor"]
        session_id = reply["session_id"]
        first_index = reply["first_index"]
        count = reply["count"]
        lost_ranges = reply.get("lost", [])
        session_end = reply.get("session_end", False)
        data = reply.get("data", [])

        # 1. Detect mock restart / new session
        if self.current_session_id is not None and session_id != self.current_session_id:
            logger.warning(
                f"Mock restart detected! Previous session: {self.current_session_id}, new session: {session_id}. "
                f"Recording unread data of previous session as lost with to_index: null."
            )
            if self.storage:
                for s in [1, 2]:
                    self.storage.insert_loss_records(
                        self.current_session_id,
                        s,
                        [{
                            "from_index": self.next_expected_index[s],
                            "to_index": None,
                            "reason": "session_restart"
                        }]
                    )
            # Reset counters for the new session
            self.next_expected_index = {1: 0, 2: 0}
            self.session_end_reached = {1: False, 2: False}

        self.current_session_id = session_id
        if self.storage:
            self.storage.ensure_session(session_id)

        # 2. Record explicit loss records reported by mock
        if lost_ranges and self.storage:
            self.storage.insert_loss_records(session_id, sensor, lost_ranges)
            for l in lost_ranges:
                t_idx = l.get("to_index")
                if t_idx is not None and t_idx >= self.next_expected_index[sensor]:
                    self.next_expected_index[sensor] = t_idx + 1

        # 3. Detect unnotified gap between next_expected_index and first_index
        exp_idx = self.next_expected_index[sensor]
        if first_index > exp_idx:
            # Check if this range was already covered by lost_ranges
            gap_covered = False
            for l in lost_ranges:
                if l["from_index"] <= exp_idx and (l.get("to_index") or 0) >= (first_index - 1):
                    gap_covered = True
                    break

            if not gap_covered and self.storage:
                logger.warning(f"Unnotified gap detected for sensor {sensor}: [{exp_idx}, {first_index - 1}]")
                self.storage.insert_loss_records(
                    session_id,
                    sensor,
                    [{
                        "from_index": exp_idx,
                        "to_index": first_index - 1,
                        "reason": "gap_detected"
                    }]
                )
            self.next_expected_index[sensor] = first_index

        # 4. Insert samples into storage
        if count > 0 and data and self.storage:
            try:
                self.storage.insert_samples_batch(session_id, sensor, first_index, data)
            except ConflictingSampleError as e:
                logger.critical(f"Data integrity violation: {e}")
                raise

        # 5. Advance expected index
        if count > 0:
            self.next_expected_index[sensor] = max(self.next_expected_index[sensor], first_index + count)

        if session_end:
            self.session_end_reached[sensor] = True

        return True

    def poll_cycle(self) -> Tuple[bool, int]:
        """
        Polls the next sensor in round-robin order.
        Returns (success, samples_read_count).
        """
        # Alternate between sensor 1 and 2
        sensor = 1 if self.last_sensor_polled == 2 else 2
        self.last_sensor_polled = sensor

        # If previous read failed with CRC error, we re-read from next_expected_index
        reply = self.read_sensor_once(sensor, force_from_index=self.next_expected_index[sensor])
        if reply is None:
            return False, 0

        self.process_reply(reply)
        return True, reply.get("count", 0)

    def run_once(self) -> int:
        """Polls both sensors once, returning total samples read."""
        total_samples = 0
        for _ in range(2):
            ok, cnt = self.poll_cycle()
            if ok:
                total_samples += cnt
        return total_samples
