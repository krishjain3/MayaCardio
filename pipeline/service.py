"""
Central pipeline orchestrator running on the device.
Coordinates biosignal-mock ingestion, durable local storage, automatic connectivity detection,
cloud streaming, and offline window processing.
"""

import logging
import signal
import sys
import time
import threading
from typing import Optional

from pipeline.storage import Storage
from pipeline.client import SensorClient
from pipeline.sync import SyncWorker

logger = logging.getLogger("mayacardio.service")


class PipelineService:
    def __init__(
        self,
        db_path: str = "data/device.db",
        mock_host: str = "127.0.0.1",
        mock_port: int = 7100,
        cloud_url: str = "http://127.0.0.1:8000",
        api_key: str = "",
        batch_size: int = 1000
    ):
        self.storage = Storage(db_path, synchronous="FULL")
        self.client = SensorClient(
            host=mock_host,
            port=mock_port,
            storage=self.storage
        )
        self.sync_worker = SyncWorker(
            storage=self.storage,
            cloud_url=cloud_url,
            api_key=api_key,
            batch_size=batch_size
        )
        self.running = False
        self._stop_event = threading.Event()

    def run(self, max_iterations: Optional[int] = None):
        """Main service loop."""
        self.running = True
        logger.info("Starting MayaCardio Pipeline Service...")

        # Initial connectivity check
        self.sync_worker.check_connectivity()
        logger.info(f"Initial operating mode: {self.sync_worker.current_mode}")

        iterations = 0
        last_sync_time = 0.0

        try:
            while not self._stop_event.is_set():
                iterations += 1
                if max_iterations and iterations > max_iterations:
                    break

                # 1. Ingest samples from mock service
                samples_read = self.client.run_once()

                session_id = self.client.current_session_id or self.storage.get_latest_session_id()

                # 2. Synchronize / process periodically
                now = time.time()
                if session_id and (now - last_sync_time >= 0.2):
                    last_sync_time = now

                    # Check connectivity & mode
                    self.sync_worker.check_connectivity()

                    if self.sync_worker.current_mode == "online":
                        # Upload backlog to cloud
                        self.sync_worker.sync_backlog_step(session_id)
                    else:
                        # Process windows locally
                        self.sync_worker.process_offline_windows(session_id)

                # Backpressure: if no samples were available from mock, sleep briefly
                if samples_read == 0:
                    time.sleep(0.02)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Stopping pipeline service...")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        self._stop_event.set()
        self.client.disconnect()
        logger.info("Pipeline service stopped gracefully.")
