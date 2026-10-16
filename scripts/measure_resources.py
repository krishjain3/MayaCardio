"""
Resource measurement script conforming to Section 5, Deliverable 4 and Section 11.
Measures:
- Average CPU utilization (%)
- Peak memory usage (MB RSS)
- Megabytes written to disk (MB)
- Megabytes sent over the network (MB)
- Hardware and environment details (OS, architecture, CPU count, RAM)
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import threading
import time
import psutil

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.service import PipelineService
from mock.biosignal_mock import MockServer
from cloud.server import create_app
try:
    from werkzeug.serving import make_server
except ImportError:
    from wsgiref.simple_server import make_server


class ServerThread(threading.Thread):
    def __init__(self, app, port):
        super().__init__(daemon=True)
        self.server = make_server("127.0.0.1", port, app)
        self.port = port

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


def get_system_environment_info():
    uname = platform.uname()
    vm = psutil.virtual_memory()
    return {
        "os": uname.system,
        "os_release": uname.release,
        "os_version": uname.version,
        "architecture": uname.machine,
        "processor": uname.processor,
        "cpu_physical_cores": psutil.cpu_count(logical=False),
        "cpu_total_cores": psutil.cpu_count(logical=True),
        "ram_total_bytes": vm.total,
        "ram_total_gb": round(vm.total / (1024 ** 3), 2),
        "python_version": platform.python_version()
    }


def measure_resources(duration_seconds: int = 60, port_mock: int = 7199, port_cloud: int = 8898):
    env_info = get_system_environment_info()
    print("=" * 60)
    print("MayaCardio Pipeline - Resource Measurement Benchmark")
    print(f"Target Duration: {duration_seconds} seconds ({duration_seconds / 3600:.2f} hours)")
    print(f"Environment: {env_info['architecture']} - {env_info['os']} ({env_info['cpu_total_cores']} cores, {env_info['ram_total_gb']} GB RAM)")
    print("=" * 60)

    # Temporary directories for benchmarking
    benchmark_dir = os.path.abspath("data/benchmark")
    os.makedirs(benchmark_dir, exist_ok=True)
    device_db = os.path.join(benchmark_dir, "benchmark_device.db")
    cloud_db = os.path.join(benchmark_dir, "benchmark_cloud.db")

    if os.path.exists(device_db):
        os.remove(device_db)
    if os.path.exists(cloud_db):
        os.remove(cloud_db)

    # Start mock service
    mock = MockServer(mode="signal", seed=7, duration=duration_seconds + 300, port=port_mock)
    mock_thread = threading.Thread(target=mock.run, daemon=True)
    mock_thread.start()
    time.sleep(0.2)

    # Start cloud service
    cloud_app = create_app(cloud_db)
    cloud_server = ServerThread(cloud_app, port_cloud)
    cloud_server.start()
    time.sleep(0.2)

    # Start pipeline service in background thread
    pipeline = PipelineService(
        db_path=device_db,
        mock_host="127.0.0.1",
        mock_port=port_mock,
        cloud_url=f"http://127.0.0.1:{port_cloud}",
        batch_size=1000
    )
    pipeline_thread = threading.Thread(target=pipeline.run, daemon=True)
    pipeline_thread.start()

    # Track current process and system I/O
    current_proc = psutil.Process()
    initial_disk_io = psutil.disk_io_counters()
    initial_net_io = psutil.net_io_counters()

    cpu_samples = []
    peak_memory_rss_bytes = 0

    start_time = time.time()
    next_print_time = start_time + 5.0

    print("Measuring continuous online pipeline execution...")
    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= duration_seconds:
                break

            # Collect CPU & memory
            cpu = current_proc.cpu_percent(interval=0.5)
            cpu_samples.append(cpu)

            rss = current_proc.memory_info().rss
            if rss > peak_memory_rss_bytes:
                peak_memory_rss_bytes = rss

            if time.time() >= next_print_time:
                next_print_time = time.time() + 5.0
                saved, cloud_cnt, backlog = pipeline.storage.get_counts()
                print(f"[{elapsed:5.1f}s / {duration_seconds}s] CPU: {cpu:4.1f}% | RAM Peak: {peak_memory_rss_bytes / (1024**2):5.1f} MB | Samples Saved: {saved} | Cloud: {cloud_cnt} | Backlog: {backlog}")

    finally:
        pipeline.stop()
        cloud_server.shutdown()
        mock.stop()

    actual_duration = time.time() - start_time
    final_disk_io = psutil.disk_io_counters()
    final_net_io = psutil.net_io_counters()

    disk_write_bytes = (final_disk_io.write_bytes - initial_disk_io.write_bytes) if initial_disk_io and final_disk_io else 0
    net_sent_bytes = (final_net_io.bytes_sent - initial_net_io.bytes_sent) if initial_net_io and final_net_io else 0

    # Also compute device database file size
    device_db_size = os.path.getsize(device_db) if os.path.exists(device_db) else 0

    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    peak_mem_mb = peak_memory_rss_bytes / (1024 ** 2)
    disk_written_mb = disk_write_bytes / (1024 ** 2)
    net_sent_mb = net_sent_bytes / (1024 ** 2)

    report = {
        "environment": env_info,
        "duration_seconds": round(actual_duration, 1),
        "duration_hours": round(actual_duration / 3600, 3),
        "average_cpu_percent": round(avg_cpu, 2),
        "peak_memory_mb": round(peak_mem_mb, 2),
        "disk_written_mb": round(disk_written_mb, 2),
        "network_sent_mb": round(net_sent_mb, 2),
        "device_db_size_mb": round(device_db_size / (1024 ** 2), 2),
        "samples_saved": pipeline.storage.get_counts()[0],
        "samples_in_cloud": pipeline.storage.get_counts()[1],
        "backlog": pipeline.storage.get_counts()[2]
    }

    print("\n" + "=" * 60)
    print("RESOURCE MEASUREMENT REPORT")
    print("=" * 60)
    print(f"Duration:               {report['duration_seconds']} seconds ({report['duration_hours']} hours)")
    print(f"Average CPU:            {report['average_cpu_percent']} %")
    print(f"Peak Memory (RSS):      {report['peak_memory_mb']} MB")
    print(f"Disk Written:           {report['disk_written_mb']} MB (DB size: {report['device_db_size_mb']} MB)")
    print(f"Network Sent:           {report['network_sent_mb']} MB")
    print(f"Total Samples Saved:    {report['samples_saved']}")
    print(f"Total Samples in Cloud: {report['samples_in_cloud']}")
    print(f"Backlog:                {report['backlog']}")
    print("=" * 60)

    # Save to file
    out_file = "data/benchmark/resource_measurement.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"Saved benchmark data to {out_file}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Measure pipeline resource utilization")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds to run benchmark (default: 60)")
    args = parser.parse_args()
    measure_resources(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
