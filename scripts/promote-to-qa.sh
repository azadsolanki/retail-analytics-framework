#!/bin/bash
# Promote models from dev to QA environment
# Usage: ./scripts/promote-to-qa.sh [model_selector]

set -e

MODEL_SELECTOR=${1:-"+"}  # Default to all models

echo "=== Promoting to QA ==="
echo "Models: $MODEL_SELECTOR"
echo ""

# Run tests in dev first
echo "Step 1: Running tests in dev..."
dbt test --target dev --select $MODEL_SELECTOR

# Build in QA
echo ""
echo "Step 2: Building in QA..."
dbt run --target qa --select $MODEL_SELECTOR

# Run tests in QA
echo ""
echo "Step 3: Running tests in QA..."
dbt test --target qa --select $MODEL_SELECTOR

echo ""
echo "=== Promotion to QA complete ==="
