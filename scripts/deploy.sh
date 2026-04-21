#!/usr/bin/env bash
# Deploy the Databricks Asset Bundle to a target environment.
# Wraps `databricks bundle deploy` with safety prompts for prod.

set -euo pipefail

TARGET="${1:-dev}"

if [[ ! "$TARGET" =~ ^(dev|staging|prod)$ ]]; then
    echo "Usage: $0 <dev|staging|prod>" >&2
    exit 1
fi

if [[ -z "${DATABRICKS_HOST:-}" || -z "${DATABRICKS_TOKEN:-}" ]]; then
    echo "[error] DATABRICKS_HOST and DATABRICKS_TOKEN must be set" >&2
    exit 1
fi

echo "[info] Target: $TARGET"
echo "[info] Host:   $DATABRICKS_HOST"

if [[ "$TARGET" == "prod" ]]; then
    echo ""
    echo "⚠️  You are deploying to PRODUCTION."
    read -r -p "Type the word 'PRODUCTION' to confirm: " confirm
    if [[ "$confirm" != "PRODUCTION" ]]; then
        echo "[abort] Confirmation failed"
        exit 1
    fi
fi

echo "[1/3] Validating bundle..."
databricks bundle validate --target "$TARGET"

echo "[2/3] Deploying bundle..."
databricks bundle deploy --target "$TARGET"

echo "[3/3] Running smoke test..."
if [[ "$TARGET" != "prod" ]]; then
    databricks bundle run smoke_test --target "$TARGET"
else
    echo "[skip] Smoke test skipped in prod — run manually after review"
fi

echo "[done] Deployment to $TARGET complete"
