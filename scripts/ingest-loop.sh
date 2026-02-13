#!/bin/sh
set -e

INTERVAL="${INGEST_INTERVAL:-300}"

while true; do
    echo "$(date): Converting replay files..."
    rrrocket -m replays/ || true

    echo "$(date): Running ingestion..."
    python ingest.py

    echo "$(date): Done. Sleeping ${INTERVAL}s..."
    sleep "$INTERVAL"
done
