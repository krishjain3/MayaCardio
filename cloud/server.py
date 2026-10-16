"""
Cloud service ingestion API and processing engine conforming to Section 2, 3, 4 of the MayaCardio assessment.
Can be deployed to any free cloud provider (e.g., Render, Railway, Fly.io, or Linux VM).
"""

import os
import time
import json
import logging
from typing import Optional
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS

from pipeline.storage import Storage, ConflictingSampleError
from pipeline.processing import compute_window_result
from pipeline.models import Sample, LossRecord, WindowResult

logger = logging.getLogger("mayacardio.cloud")

app = Flask(__name__)
CORS(app)

CLOUD_DB_PATH = os.environ.get("CLOUD_DB_PATH", "data/cloud.db")
CLOUD_API_KEY = os.environ.get("CLOUD_API_KEY", "")
storage = Storage(CLOUD_DB_PATH, synchronous="NORMAL")


def is_production() -> bool:
    env = os.environ.get("ENVIRONMENT", "").lower()
    flask_env = os.environ.get("FLASK_ENV", "").lower()
    return env in ("production", "prod") or flask_env == "production" or os.environ.get("REQUIRE_AUTH", "").lower() in ("true", "1")


def check_auth() -> bool:
    api_key = os.environ.get("CLOUD_API_KEY", CLOUD_API_KEY)
    if is_production():
        if not api_key:
            logger.error("Authentication rejected: CLOUD_API_KEY must be configured in production deployments.")
            return False
        if api_key == "test-secret-key-change-in-production":
            logger.error("Authentication rejected: Insecure placeholder CLOUD_API_KEY detected in production deployment.")
            return False

    if not api_key:
        return True

    auth_header = request.headers.get("Authorization", "")
    api_key_header = request.headers.get("X-API-Key", "")
    if api_key_header == api_key:
        return True
    if auth_header.startswith("Bearer ") and auth_header[7:].strip() == api_key:
        return True
    return False


@app.route("/api/v1/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "mayacardio-cloud",
        "timestamp": time.time()
    }), 200


@app.route("/api/v1/status", methods=["GET"])
def status():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    session_id = request.args.get("session_id")
    if not session_id:
        session_id = storage.get_latest_session_id()

    if not session_id:
        return jsonify({
            "status": "ok",
            "session_id": None,
            "samples_saved": 0,
            "latest_window": None
        }), 200

    saved, _, _ = storage.get_counts(session_id)
    max_idx = storage.get_max_sample_index(session_id)
    latest_win = (max_idx // 400) if max_idx >= 0 else None

    return jsonify({
        "status": "ok",
        "session_id": session_id,
        "samples_saved": saved,
        "max_sample_index": max_idx,
        "latest_window": latest_win
    }), 200


@app.route("/api/v1/ingest", methods=["POST"])
def ingest():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "Invalid JSON body"}), 400

    session_id = payload.get("session_id")
    sensor = payload.get("sensor")
    first_index = payload.get("first_index", 0)
    samples = payload.get("samples", [])
    losses = payload.get("losses", [])
    mode = payload.get("mode", "online")

    if not session_id or sensor not in (1, 2):
        return jsonify({"error": "Missing or invalid session_id or sensor"}), 400

    # 1. Insert losses if present
    if losses:
        storage.insert_loss_records(session_id, sensor, losses, uploaded=1)

    # 2. Insert samples if present
    inserted_cnt = 0
    synced_up_to = first_index - 1
    if samples:
        try:
            inserted_cnt = storage.insert_samples_batch(session_id, sensor, first_index, samples)
            synced_up_to = first_index + len(samples) - 1
        except ConflictingSampleError as e:
            logger.error(f"Integrity conflict: {e}")
            return jsonify({"error": "Integrity conflict", "details": str(e)}), 409

    # 3. If online mode: trigger online processing for completed/updated windows
    calculated_windows = []
    if mode == "online" and samples:
        max_idx = storage.get_max_sample_index(session_id)
        max_known_window = (max_idx // 400) if max_idx >= 0 else 0

        # Affected windows: from preceding window (which may have needed lookahead sample)
        # up to forward windows affected by 5-second heart rate lookback
        start_win = max(0, (first_index // 400) - 1)
        end_win = min(max_known_window, (synced_up_to // 400) + 4)

        windows_to_process = set(range(start_win, end_win + 1))

        # Recompute any previously incomplete online windows for this session
        incomplete_wins = storage.get_incomplete_windows(session_id, mode="online")
        for inc_w in incomplete_wins:
            if inc_w <= max_known_window:
                windows_to_process.add(inc_w)

        for win in sorted(windows_to_process):
            # Process window using exact same logic
            res = compute_window_result(storage, session_id, win, mode="online")
            storage.save_window_result(res)
            calculated_windows.append(win)

    return jsonify({
        "status": "ok",
        "session_id": session_id,
        "sensor": sensor,
        "inserted": inserted_cnt,
        "synced_up_to": synced_up_to,
        "calculated_windows": calculated_windows
    }), 200


@app.route("/api/v1/export", methods=["GET"])
def export():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    session_id = request.args.get("session_id")
    if not session_id:
        session_id = storage.get_latest_session_id()

    if not session_id:
        return Response("", mimetype="application/x-ndjson")

    def generate():
        for line in storage.export_data(session_id):
            yield line + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson")


@app.route("/api/v1/results", methods=["GET"])
def results():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    session_id = request.args.get("session_id")
    if not session_id:
        session_id = storage.get_latest_session_id()

    if not session_id:
        return Response("", mimetype="application/x-ndjson")

    res_list = storage.get_window_results(session_id)

    def generate():
        for r in res_list:
            yield r.to_ndjson() + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson")


def create_app(db_path: Optional[str] = None):
    global storage
    if db_path:
        storage = Storage(db_path, synchronous="NORMAL")
    return app


if __name__ == "__main__":
    if is_production():
        api_key = os.environ.get("CLOUD_API_KEY", "")
        if not api_key:
            raise RuntimeError("FATAL: CLOUD_API_KEY environment variable MUST be set for production deployment!")
        if api_key == "test-secret-key-change-in-production":
            raise RuntimeError("FATAL: Insecure default CLOUD_API_KEY cannot be used in production deployment!")

    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
