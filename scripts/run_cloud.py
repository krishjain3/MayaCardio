"""
Cloud service launcher.
Starts the MayaCardio Cloud ingestion and processing server.
"""

import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from cloud.server import create_app


def main():
    parser = argparse.ArgumentParser(description="Run MayaCardio Cloud Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)), help="Port to listen on")
    parser.add_argument("--db", default=os.environ.get("CLOUD_DB_PATH", "data/cloud.db"), help="Path to cloud SQLite database")

    args = parser.parse_args()

    print(f"Starting MayaCardio Cloud Service...")
    print(f"Host:    {args.host}")
    print(f"Port:    {args.port}")
    print(f"DB Path: {args.db}")

    app = create_app(args.db)
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
