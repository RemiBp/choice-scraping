#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# sync_mongo_to_pg.sh
# Run on your Mac to pull all scraped MongoDB data into local PostgreSQL.
#
# What it does:
#   1. Opens an SSH tunnel → Hetzner server (MongoDB port 27017 → local 27018)
#   2. Waits for the backend to be running on localhost:6543
#   3. Runs migrate_to_backend.py to push all MongoDB venues to PostgreSQL
#   4. Closes the tunnel
#
# Usage: ./sync_mongo_to_pg.sh
# ─────────────────────────────────────────────────────────────────────────────
set -e

HETZNER_IP="46.225.142.238"
BACKEND_URL="http://localhost:6543"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "▶ Opening SSH tunnel to Hetzner (MongoDB 27017 → local 27018)..."
ssh -o StrictHostKeyChecking=no -N -L 27018:localhost:27017 root@${HETZNER_IP} &
TUNNEL_PID=$!
trap "kill $TUNNEL_PID 2>/dev/null; echo 'Tunnel closed.'" EXIT

sleep 3
echo "▶ Tunnel open (PID $TUNNEL_PID)"

# Check backend is running
if ! curl -s "${BACKEND_URL}/api/health" > /dev/null 2>&1; then
  echo "⚠️  Backend not reachable at ${BACKEND_URL}."
  echo "   Start it with: cd choice-backend && npm run dev"
  exit 1
fi
echo "▶ Backend reachable at ${BACKEND_URL}"

cd "$SCRIPT_DIR"

# Activate venv if present
[ -f .venv/bin/activate ] && source .venv/bin/activate

# Run migration using the tunneled MongoDB
MONGO_URI="mongodb://localhost:27018/" \
  python3 migrate_to_backend.py \
    --backend "${BACKEND_URL}" \
    "$@"

echo "✅ Sync complete."
