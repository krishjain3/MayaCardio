#!/usr/bin/env bash
# ==============================================================================
# MayaCardio Cloud Deployment Script for Free Tier Linux / Container Services
# ==============================================================================
set -euo pipefail

echo "=========================================================="
echo "MayaCardio Cloud Service Deployment"
echo "=========================================================="

APP_DIR="${APP_DIR:-/opt/mayacardio}"
PORT="${PORT:-8000}"
CLOUD_DB_PATH="${CLOUD_DB_PATH:-$APP_DIR/data/cloud.db}"

echo "[1/4] Ensuring Python and dependencies..."
python3 -m pip install --upgrade pip
python3 -m pip install -e .

echo "[2/4] Setting up storage directory..."
mkdir -p "$(dirname "$CLOUD_DB_PATH")"

echo "[3/4] Initializing cloud database schema..."
python3 -c "from pipeline.storage import Storage; Storage('$CLOUD_DB_PATH')"

echo "[4/4] Starting MayaCardio Cloud API on port $PORT..."
exec python3 -m cloud.server
