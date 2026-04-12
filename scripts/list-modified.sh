#!/bin/bash
# List models that have changed compared to production
#
# Usage: ./scripts/list-modified.sh
#
# Requires: ./scripts/dev-setup.sh to be run first

set -e

MANIFEST_DIR="./prod-manifest"

if [ ! -f "$MANIFEST_DIR/manifest.json" ]; then
    echo "Error: Production manifest not found. Run ./scripts/dev-setup.sh first."
    exit 1
fi

echo "Modified models:"
dbt ls --select state:modified --state "$MANIFEST_DIR" 2>/dev/null || echo "None"

echo ""
echo "Modified + downstream dependencies:"
dbt ls --select state:modified+ --state "$MANIFEST_DIR" 2>/dev/null || echo "None"
