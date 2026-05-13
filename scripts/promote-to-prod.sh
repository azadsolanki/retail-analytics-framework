#!/bin/bash
# Promote models from QA to Production
# Usage: ./scripts/promote-to-prod.sh [model_selector]
#
# WARNING: This deploys to production. Use with caution.

set -e

MODEL_SELECTOR=${1:-"+"}  # Default to all models

echo "=== Promoting to PRODUCTION ==="
echo "Models: $MODEL_SELECTOR"
echo ""
echo "WARNING: This will modify production data!"
read -p "Are you sure? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

# Verify QA tests pass
echo ""
echo "Step 1: Verifying QA tests pass..."
dbt test --target qa --select $MODEL_SELECTOR

# Build in prod
echo ""
echo "Step 2: Building in production..."
dbt run --target prod --select $MODEL_SELECTOR

# Run tests in prod
echo ""
echo "Step 3: Running tests in production..."
dbt test --target prod --select $MODEL_SELECTOR

# Generate docs
echo ""
echo "Step 4: Generating documentation..."
dbt docs generate --target prod

echo ""
echo "=== Promotion to PRODUCTION complete ==="
