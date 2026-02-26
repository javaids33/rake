#!/usr/bin/env bash
# =============================================================================
# docker-run.sh — Build and run RustLake with Docker Compose
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "============================================"
echo "  RustLake — Docker Build & Run"
echo "============================================"
echo ""

# ── Build ────────────────────────────────────────────
echo "[1/3] Building RustLake Docker image..."
docker compose build rustlake-api
echo ""

# ── Start ────────────────────────────────────────────
echo "[2/3] Starting RustLake API server..."
docker compose up -d rustlake-api
echo ""

# ── Health Check ─────────────────────────────────────
echo "[3/3] Waiting for health check..."
MAX_RETRIES=20
RETRY_INTERVAL=3

for i in $(seq 1 $MAX_RETRIES); do
    if curl -sf http://localhost:3000/health > /dev/null 2>&1; then
        echo "  Health check passed."
        echo ""
        echo "============================================"
        echo "  RustLake is running!"
        echo ""
        echo "  API Server:  http://localhost:3000"
        echo "  Health:      http://localhost:3000/health"
        echo "  SQL API:     http://localhost:3000/api/v1/sql"
        echo "  Dashboard:   http://localhost:3000/dashboard"
        echo ""
        echo "  Example query:"
        echo "    curl -X POST http://localhost:3000/api/v1/sql \\"
        echo "      -H 'Content-Type: application/json' \\"
        echo "      -d '{\"sql\": \"SELECT * FROM '\\''sample-data/sales.csv'\\'' LIMIT 5\"}'"
        echo ""
        echo "  Run benchmarks:"
        echo "    docker compose run --rm rustlake-bench query \"SELECT count(*) FROM 'sample-data/sales.csv'\""
        echo ""
        echo "  Stop:"
        echo "    docker compose down"
        echo "============================================"
        exit 0
    fi
    echo "  Attempt $i/$MAX_RETRIES — waiting ${RETRY_INTERVAL}s..."
    sleep "$RETRY_INTERVAL"
done

echo ""
echo "ERROR: Health check did not pass after $((MAX_RETRIES * RETRY_INTERVAL))s."
echo "Check logs with: docker compose logs rustlake-api"
exit 1
