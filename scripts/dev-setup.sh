#!/bin/bash
# Download production manifest for local Slim CI builds
#
# Usage: ./scripts/dev-setup.sh
#
# After running this, use:
#   ./scripts/slim-build.sh
#   dbt build --select state:modified+ --defer --state ./prod-manifest

set -e

GCS_BUCKET="gs://data-products-441119-dbt-artifacts/manifests"
LOCAL_DIR="./prod-manifest"

echo "Downloading production manifest..."

if ! command -v gsutil &> /dev/null; then
    echo "Error: gsutil is not installed. Install Google Cloud SDK first."
    exit 1
fi

mkdir -p "$LOCAL_DIR"

if gsutil cp "$GCS_BUCKET/prod/manifest.json" "$LOCAL_DIR/manifest.json" 2>/dev/null; then
    echo "Manifest downloaded to $LOCAL_DIR/manifest.json"
    echo ""
    echo "You can now run:"
    echo "  ./scripts/slim-build.sh"
    echo "  ./scripts/list-modified.sh"
else
    echo "Error: Could not download manifest."
    echo "Either no manifest exists yet (merge to main first) or you lack GCS access."
    exit 1
fi
