"""
Operational CLI conforming strictly to Section 4 of the MayaCardio assessment.
Commands:
- pipeline status: current mode, samples saved, samples in the cloud, backlog. Printed as one JSON line.
- pipeline export --session ID --from device (or --from cloud): data as JSON lines, one per sample, then one line per loss.
- pipeline results --session ID: one JSON line per window and mode, with section 3 outputs.
- pipeline run: starts the data pipeline daemon.
"""

import argparse
import json
import os
import sys
import requests

from pipeline.storage import Storage
from pipeline.service import PipelineService
from pipeline.models import PipelineStatus

DEFAULT_DEVICE_DB = os.environ.get("DEVICE_DB_PATH", "data/device.db")
DEFAULT_CLOUD_URL = os.environ.get("CLOUD_URL", "http://127.0.0.1:8000")
DEFAULT_API_KEY = os.environ.get("CLOUD_API_KEY", "")


def cmd_status(args):
    db_path = getattr(args, "db", DEFAULT_DEVICE_DB)
    cloud_url = getattr(args, "cloud_url", DEFAULT_CLOUD_URL).rstrip("/")
    api_key = getattr(args, "api_key", DEFAULT_API_KEY)

    storage = Storage(db_path)
    session_id = getattr(args, "session", None) or storage.get_latest_session_id()

    saved, cloud, backlog = storage.get_counts(session_id)

    # Determine mode via health check
    mode = "offline"
    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key

    try:
        r = requests.get(f"{cloud_url}/api/v1/health", headers=headers, timeout=1.5)
        if r.status_code == 200:
            mode = "online"
    except Exception:
        mode = "offline"

    status = PipelineStatus(
        mode=mode,
        samples_saved=saved,
        samples_cloud=cloud,
        backlog=backlog,
        session_id=session_id
    )
    # Printed as exactly one JSON line
    print(status.to_ndjson())


def cmd_export(args):
    session_id = args.session
    from_source = args.from_source
    db_path = getattr(args, "db", DEFAULT_DEVICE_DB)
    cloud_url = getattr(args, "cloud_url", DEFAULT_CLOUD_URL).rstrip("/")
    api_key = getattr(args, "api_key", DEFAULT_API_KEY)

    if from_source == "device":
        storage = Storage(db_path)
        for line in storage.export_data(session_id):
            print(line)
    elif from_source == "cloud":
        headers = {}
        if api_key:
            headers["X-API-Key"] = api_key
        try:
            r = requests.get(
                f"{cloud_url}/api/v1/export",
                params={"session_id": session_id},
                headers=headers,
                stream=True,
                timeout=10.0
            )
            if r.status_code != 200:
                sys.stderr.write(f"Cloud export error {r.status_code}: {r.text}\n")
                sys.exit(1)
            for line in r.iter_lines(decode_unicode=True):
                if line:
                    print(line)
        except Exception as e:
            sys.stderr.write(f"Error connecting to cloud export endpoint: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Unknown source: {from_source}. Use 'device' or 'cloud'.\n")
        sys.exit(1)


def cmd_results(args):
    session_id = args.session
    db_path = getattr(args, "db", DEFAULT_DEVICE_DB)
    cloud_url = getattr(args, "cloud_url", DEFAULT_CLOUD_URL).rstrip("/")
    api_key = getattr(args, "api_key", DEFAULT_API_KEY)

    storage = Storage(db_path)
    results = storage.get_window_results(session_id)

    if not results:
        # Check cloud if not found locally
        headers = {}
        if api_key:
            headers["X-API-Key"] = api_key
        try:
            r = requests.get(
                f"{cloud_url}/api/v1/results",
                params={"session_id": session_id},
                headers=headers,
                stream=True,
                timeout=5.0
            )
            if r.status_code == 200:
                for line in r.iter_lines(decode_unicode=True):
                    if line:
                        print(line)
                return
        except Exception:
            pass

    for r in results:
        print(r.to_ndjson())


def cmd_run(args):
    db_path = getattr(args, "db", DEFAULT_DEVICE_DB)
    cloud_url = getattr(args, "cloud_url", DEFAULT_CLOUD_URL)
    api_key = getattr(args, "api_key", DEFAULT_API_KEY)
    mock_host = getattr(args, "mock_host", "127.0.0.1")
    mock_port = getattr(args, "mock_port", 7100)
    batch_size = getattr(args, "batch_size", 1000)

    service = PipelineService(
        db_path=db_path,
        mock_host=mock_host,
        mock_port=mock_port,
        cloud_url=cloud_url,
        api_key=api_key,
        batch_size=batch_size
    )
    service.run()


def main():
    parser = argparse.ArgumentParser(prog="pipeline", description="MayaCardio Biosignal Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. pipeline status
    p_status = subparsers.add_parser("status", help="Show current pipeline status as a single JSON line")
    p_status.add_argument("--session", help="Session ID to inspect (defaults to latest)")
    p_status.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Path to device SQLite database")
    p_status.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud API base URL")
    p_status.add_argument("--api-key", default=DEFAULT_API_KEY, help="Cloud API key")
    p_status.set_defaults(func=cmd_status)

    # 2. pipeline export --session ID --from device/cloud
    p_export = subparsers.add_parser("export", help="Export samples and loss records as NDJSON")
    p_export.add_argument("--session", required=True, help="Session ID")
    p_export.add_argument("--from", dest="from_source", choices=["device", "cloud"], required=True, help="Export source: device or cloud")
    p_export.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Path to device SQLite database")
    p_export.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud API base URL")
    p_export.add_argument("--api-key", default=DEFAULT_API_KEY, help="Cloud API key")
    p_export.set_defaults(func=cmd_export)

    # 3. pipeline results --session ID
    p_results = subparsers.add_parser("results", help="Output one JSON line per window and mode")
    p_results.add_argument("--session", required=True, help="Session ID")
    p_results.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Path to device SQLite database")
    p_results.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud API base URL")
    p_results.add_argument("--api-key", default=DEFAULT_API_KEY, help="Cloud API key")
    p_results.set_defaults(func=cmd_results)

    # 4. pipeline run
    p_run = subparsers.add_parser("run", help="Run the continuous pipeline service")
    p_run.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Path to device SQLite database")
    p_run.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud API base URL")
    p_run.add_argument("--api-key", default=DEFAULT_API_KEY, help="Cloud API key")
    p_run.add_argument("--mock-host", default="127.0.0.1", help="biosignal-mock host")
    p_run.add_argument("--mock-port", type=int, default=7100, help="biosignal-mock port")
    p_run.add_argument("--batch-size", type=int, default=1000, help="Upload batch size")
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
