"""
biosignal-mock emulator conforming strictly to Section 6 of the MayaCardio assessment.
Generates synthetic sensor data (ramp and signal modes) over a newline-delimited JSON TCP socket.
"""

import argparse
import json
import math
import random
import socket
import sys
import threading
import time
import zlib
from typing import Dict, List, Optional, Tuple

MOD_24BIT = 8_388_608  # 2^23
SAMPLE_RATE = 400  # 400 samples per second
BUFFER_SECONDS = 10  # Mock retains last 10 seconds per sensor
BUFFER_SAMPLES = BUFFER_SECONDS * SAMPLE_RATE  # 4000 samples


def compute_crc32_str(data_str: str) -> str:
    """Computes CRC-32 (zlib) formatted as 8-character lowercase hex."""
    return f"{zlib.crc32(data_str.encode('utf-8')):08x}"


class BiosignalGenerator:
    """Generates deterministic sensor samples according to Section 6.1."""

    def __init__(self, mode: str = "signal", seed: int = 7):
        self.mode = mode
        self.seed = seed
        self.rng = random.Random(seed)

    def get_sample(self, sensor: int, index: int) -> List[int]:
        if self.mode == "ramp":
            if sensor == 1:
                return [(3 * index + c) % MOD_24BIT for c in range(3)]
            elif sensor == 2:
                return [(2 * index + c) % MOD_24BIT for c in range(2)]
            else:
                raise ValueError(f"Unknown sensor: {sensor}")

        elif self.mode == "signal":
            if sensor == 1:
                # ECG synthetic signal with sharp R-peak
                # Rate cycle: 0-2m -> 60 bpm, 2-4m -> 90 bpm, 4-6m -> 72 bpm (repeating every 6m = 360s = 144,000 samples)
                cycle_samples = 360 * SAMPLE_RATE
                t_in_cycle = (index % cycle_samples) / SAMPLE_RATE  # seconds in 6-minute cycle

                if t_in_cycle < 120:
                    bpm = 60.0
                elif t_in_cycle < 240:
                    bpm = 90.0
                else:
                    bpm = 72.0

                interval_sec = 60.0 / bpm
                interval_samples = interval_sec * SAMPLE_RATE  # 400 for 60bpm, 266.67 for 90bpm, 333.33 for 72bpm

                # Phase within beat:
                offset_in_beat = math.fmod(index, interval_samples)

                # ECG waveform components: baseline 1000 + P wave + QRS complex + T wave
                # Sharp peak at center of QRS:
                baseline = 1000.0
                qrs_pos = 50.0  # peak occurs 50 samples into beat
                dist = offset_in_beat - qrs_pos

                if abs(dist) < 1.0:
                    # Sharp R peak: maximum value
                    r_peak = 5000.0 * (1.0 - abs(dist))
                elif abs(dist) < 5.0:
                    r_peak = 2000.0 * math.exp(-0.5 * (dist / 1.5) ** 2)
                else:
                    # P wave (~sample 20) and T wave (~sample 120)
                    p_wave = 200.0 * math.exp(-0.5 * ((offset_in_beat - 20) / 6.0) ** 2)
                    t_wave = 400.0 * math.exp(-0.5 * ((offset_in_beat - 110) / 15.0) ** 2)
                    r_peak = p_wave + t_wave

                ecg_val = int(baseline + r_peak)
                # Keep within signed 24-bit range
                ch2 = max(-MOD_24BIT, min(MOD_24BIT - 1, ecg_val))
                # Sensor 1 channels 1 and 3 are channel 2 x 0.6 and x 1.3
                ch1 = max(-MOD_24BIT, min(MOD_24BIT - 1, int(round(ch2 * 0.6))))
                ch3 = max(-MOD_24BIT, min(MOD_24BIT - 1, int(round(ch2 * 1.3))))
                return [ch1, ch2, ch3]

            elif sensor == 2:
                # Sensor 2 has channel 1 near 400,000 and channel 2 near 150,000, varying slowly
                t_sec = index / SAMPLE_RATE
                slow_var_1 = math.sin(2 * math.pi * 0.05 * t_sec) * 5000
                slow_var_2 = math.cos(2 * math.pi * 0.03 * t_sec) * 3000
                ch1 = int(round(400_000 + slow_var_1))
                ch2 = int(round(150_000 + slow_var_2))
                return [ch1, ch2]
            else:
                raise ValueError(f"Unknown sensor: {sensor}")
        else:
            raise ValueError(f"Unknown mode: {self.mode}")


class MockServer:
    """Mock TCP Server providing biosignal stream with configurable faults."""

    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        port: int = 7100,
        host: str = "0.0.0.0",
        session_id: Optional[str] = None,
        corrupt_crc_rate: float = 0.0,
        duplicate_rate: float = 0.0,
    ):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.host = host
        self.session_id = session_id or f"{random.Random(seed).randint(0x10000000, 0xFFFFFFFF):08x}"
        self.generator = BiosignalGenerator(mode=mode, seed=seed)
        self.corrupt_crc_rate = corrupt_crc_rate
        self.duplicate_rate = duplicate_rate

        self.start_time = None
        self.running = False
        self.server_socket = None

        # Per-sensor read cursor and buffer state
        self.client_cursor = {1: 0, 2: 0}
        self.lock = threading.Lock()

    def get_current_max_index(self) -> int:
        if self.start_time is None:
            return 0
        elapsed = time.time() - self.start_time
        max_idx = int(elapsed * SAMPLE_RATE)
        total_duration_samples = self.duration * SAMPLE_RATE
        return min(max_idx, total_duration_samples)

    def handle_client(self, conn: socket.socket, addr):
        conn_file = conn.makefile("r", encoding="utf-8")
        try:
            for raw_line in conn_file:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    req = json.loads(line)
                except Exception:
                    continue

                if req.get("cmd") != "read":
                    continue

                sensor = req.get("sensor", 1)
                from_index = req.get("from_index")

                with self.lock:
                    current_available = self.get_current_max_index()
                    total_samples = self.duration * SAMPLE_RATE

                    # Determine read start index
                    if from_index is not None:
                        start_idx = from_index
                    else:
                        start_idx = self.client_cursor.get(sensor, 0)

                    # Mock buffer retains only the last 10 seconds (BUFFER_SAMPLES)
                    earliest_available = max(0, current_available - BUFFER_SAMPLES)
                    lost = []

                    if start_idx < earliest_available:
                        # Data dropped due to buffer overflow
                        lost.append({
                            "from_index": start_idx,
                            "to_index": earliest_available - 1,
                            "reason": "buffer_overflow"
                        })
                        start_idx = earliest_available

                    # At most 1 second of data per reply (400 samples)
                    count = min(SAMPLE_RATE, max(0, current_available - start_idx))
                    samples = [self.generator.get_sample(sensor, start_idx + i) for i in range(count)]

                    self.client_cursor[sensor] = start_idx + count
                    session_end = (start_idx + count >= total_samples)

                    # Serialize data text exactly
                    data_str = json.dumps(samples)
                    crc32_val = compute_crc32_str(data_str)

                    # Optional fault injection: corrupt CRC
                    if self.corrupt_crc_rate > 0 and random.random() < self.corrupt_crc_rate:
                        crc32_val = "00000000"

                    reply = {
                        "session_id": self.session_id,
                        "sensor": sensor,
                        "first_index": start_idx,
                        "count": count,
                        "lost": lost,
                        "session_end": session_end,
                        "crc32": crc32_val,
                        "data": samples
                    }

                    # We format with default separator ", " matching prompt
                    reply_json = json.dumps(reply) + "\n"
                    conn.sendall(reply_json.encode("utf-8"))

                    # Optional fault injection: send duplicate reply
                    if self.duplicate_rate > 0 and random.random() < self.duplicate_rate:
                        conn.sendall(reply_json.encode("utf-8"))

        except (socket.error, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def run(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)  # Section 6.2: "one client at a time"
        self.running = True
        self.start_time = time.time()

        print(f"[biosignal-mock] Serving {self.mode} mode on {self.host}:{self.port} (session_id={self.session_id})")
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                self.handle_client(conn, addr)
            except Exception:
                break

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="biosignal-mock emulator")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Mode: signal or ramp")
    parser.add_argument("--seed", type=int, default=7, help="Random seed")
    parser.add_argument("--duration", type=int, default=3600, help="Duration in seconds")
    parser.add_argument("--port", type=int, default=7100, help="Port to listen on")
    parser.add_argument("--host", default="0.0.0.0", help="Host interface")
    parser.add_argument("--corrupt-crc-rate", type=float, default=0.0, help="Probability of corrupt CRC")
    parser.add_argument("--duplicate-rate", type=float, default=0.0, help="Probability of duplicate reply")

    args = parser.parse_args()
    server = MockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        host=args.host,
        corrupt_crc_rate=args.corrupt_crc_rate,
        duplicate_rate=args.duplicate_rate,
    )
    try:
        server.run()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
