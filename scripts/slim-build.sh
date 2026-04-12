#!/bin/bash
# Build only modified models compared to production
#
# Usage:
#   ./scripts/slim-build.sh
#   ./scripts/slim-build.sh --full-refresh
#
# Requires: ./scripts/dev-setup.sh to be run first

set -e

MANIFEST_DIR="./prod-manifest"

if [ ! -f "$MANIFEST_DIR/manifest.json" ]; then
    echo "Error: Production manifest not found. Run ./scripts/dev-setup.sh first."
    exit 1
fi

echo "Checking for modified models..."
dbt ls --select state:modified+ --state "$MANIFEST_DIR" 2>/dev/null || echo "No modified models found"

echo ""
echo "Building modified models..."
dbt build --select state:modified+ --defer --state "$MANIFEST_DIR" "$@"
