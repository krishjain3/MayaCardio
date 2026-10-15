"""
Device service launcher.
Starts the MayaCardio pipeline daemon connecting to local biosignal-mock.
"""

import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.service import PipelineService


def main():
    parser = argparse.ArgumentParser(description="Run MayaCardio Device Pipeline Service")
    parser.add_argument("--db", default=os.environ.get("DEVICE_DB_PATH", "data/device.db"), help="SQLite database path")
    parser.add_argument("--mock-host", default=os.environ.get("MOCK_HOST", "127.0.0.1"), help="biosignal-mock host")
    parser.add_argument("--mock-port", type=int, default=int(os.environ.get("MOCK_PORT", 7100)), help="biosignal-mock port")
    parser.add_argument("--cloud-url", default=os.environ.get("CLOUD_URL", "http://127.0.0.1:8000"), help="Cloud server URL")
    parser.add_argument("--api-key", default=os.environ.get("CLOUD_API_KEY", ""), help="Cloud API Key")
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("SYNC_BATCH_SIZE", 1000)), help="Sync batch size")

    args = parser.parse_args()

    print(f"Starting MayaCardio Device Pipeline...")
    print(f"DB Path:    {args.db}")
    print(f"Mock Host:  {args.mock_host}:{args.mock_port}")
    print(f"Cloud URL:  {args.cloud_url}")

    service = PipelineService(
        db_path=args.db,
        mock_host=args.mock_host,
        mock_port=args.mock_port,
        cloud_url=args.cloud_url,
        api_key=args.api_key,
        batch_size=args.batch_size
    )
    service.run()


if __name__ == "__main__":
    main()
