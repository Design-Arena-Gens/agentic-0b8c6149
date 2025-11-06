#!/usr/bin/env bash
set -euo pipefail

# Example: ethically crawl your OWN site or one you have permission for.
# Adjust BASE, URLS_FILE, and settings as needed. Keep rates low.

BASE="https://example.com"           # Replace with a domain you control
URLS_FILE="./scripts/example-paths.txt"
DB_PATH="./data.sqlite"

python -m app.collector \
  --base-url "$BASE" \
  --paths-file "$URLS_FILE" \
  --db-path "$DB_PATH" \
  --concurrency 10 \
  --requests-per-second 5 \
  --timeout-seconds 15 \
  --max-retries 2 \
  --respect-robots true
