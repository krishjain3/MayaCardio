"""
Connectivity monitor, cloud synchronizer, and offline/online orchestrator conforming
to Section 2, 4, 7 of the MayaCardio assessment.
"""

import logging
import time
import requests
from typing import Dict, List, Optional, Tuple, Any

from pipeline.storage import Storage
from pipeline.processing import compute_window_result

logger = logging.getLogger("mayacardio.sync")


class SyncWorker:
    """Manages cloud synchronization, health checks, automatic mode switching, and catch-up."""

    def __init__(
        self,
        storage: Storage,
        cloud_url: str = "http://127.0.0.1:8000",
        api_key: str = "",
        batch_size: int = 1000,
        health_check_interval: float = 2.0,
        request_timeout: float = 2.0
    ):
        self.storage = storage
        self.cloud_url = cloud_url.rstrip("/")
        self.api_key = api_key
        self.batch_size = batch_size
        self.health_check_interval = health_check_interval
        self.request_timeout = request_timeout

        self.current_mode: str = "offline"  # starts offline until health check passes
        self.last_health_check_time: float = 0.0
        self.consecutive_failures: int = 0
        self.consecutive_successes: int = 0

        # Offline processing cursor
        self.local_processed_window: Dict[str, int] = {}

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def check_connectivity(self) -> bool:
        """Pings cloud health endpoint. Switches mode within 10 seconds of change."""
        now = time.time()
        if now - self.last_health_check_time < self.health_check_interval:
            return (self.current_mode == "online")

        self.last_health_check_time = now
        try:
            resp = requests.get(
                f"{self.cloud_url}/api/v1/health",
                headers=self._headers(),
                timeout=self.request_timeout
            )
            if resp.status_code == 200:
                self.consecutive_successes += 1
                self.consecutive_failures = 0
                if self.current_mode != "online":
                    logger.info("Cloud connectivity restored. Switching to ONLINE mode.")
                    self.current_mode = "online"
                return True
            else:
                self._handle_network_failure()
                return False
        except Exception as e:
            self._handle_network_failure()
            return False

    def _handle_network_failure(self):
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        if self.consecutive_failures >= 2 and self.current_mode != "offline":
            logger.warning(
                f"Cloud connectivity lost ({self.consecutive_failures} failed attempts). "
                f"Switching to OFFLINE mode."
            )
            self.current_mode = "offline"

    def sync_backlog_step(self, session_id: str) -> int:
        """
        Synchronizes one batch of samples and unuploaded losses to the cloud.
        Returns the number of samples uploaded in this step.
        """
        if not self.check_connectivity():
            return 0

        total_uploaded = 0
        # 1. Upload unuploaded losses
        unuploaded_losses = self.storage.get_unuploaded_losses(session_id)
        loss_keys = [(l["sensor"], l["from_index"]) for l in unuploaded_losses]

        # 2. Upload samples for sensor 1 and sensor 2
        for sensor in (1, 2):
            start_idx, samples = self.storage.get_unuploaded_samples(
                session_id, sensor, limit=self.batch_size
            )
            sensor_losses = [l for l in unuploaded_losses if l["sensor"] == sensor]

            if not samples and not sensor_losses:
                continue

            payload = {
                "session_id": session_id,
                "sensor": sensor,
                "first_index": start_idx,
                "samples": samples,
                "losses": sensor_losses,
                "mode": self.current_mode
            }

            try:
                resp = requests.post(
                    f"{self.cloud_url}/api/v1/ingest",
                    json=payload,
                    headers=self._headers(),
                    timeout=self.request_timeout * 3
                )
                if resp.status_code == 200:
                    data = resp.json()
                    synced_up_to = data.get("synced_up_to", -1)
                    if synced_up_to >= 0:
                        self.storage.advance_sync_cursor(session_id, sensor, synced_up_to)
                        total_uploaded += len(samples)

                    # Mark uploaded losses
                    if sensor_losses:
                        acked_keys = [(l["sensor"], l["from_index"]) for l in sensor_losses]
                        self.storage.mark_losses_uploaded(session_id, acked_keys)
                else:
                    logger.warning(f"Cloud ingest returned status {resp.status_code}: {resp.text}")
                    self._handle_network_failure()
                    break
            except Exception as e:
                logger.warning(f"Error posting samples to cloud: {e}")
                self._handle_network_failure()
                break

        return total_uploaded

    def process_offline_windows(self, session_id: str):
        """
        Calculates window results locally on device when offline (Section 3).
        """
        max_idx = self.storage.get_max_sample_index(session_id)
        if max_idx < 0:
            return

        last_win = max_idx // 400
        current_win = self.local_processed_window.get(session_id, 0)

        # Check and update any previously incomplete offline windows
        incomplete_wins = self.storage.get_incomplete_windows(session_id, mode="offline")
        for inc_w in incomplete_wins:
            if inc_w <= last_win:
                res = compute_window_result(self.storage, session_id, inc_w, mode="offline")
                self.storage.save_window_result(res)

        while current_win <= last_win:
            res = compute_window_result(self.storage, session_id, current_win, mode="offline")
            self.storage.save_window_result(res)
            current_win += 1

        self.local_processed_window[session_id] = current_win
