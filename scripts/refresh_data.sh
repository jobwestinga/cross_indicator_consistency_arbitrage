#!/usr/bin/env bash
# One-command local data refresh (F2):
#   1. generate a fresh export on the VPS (reads live Postgres -> zip bundle)
#   2. download the newest bundle to the repo root
#   3. refresh the FRED macro ground truth
#   4. run the readiness audit
#
# Host details are PRIVATE and come from .env (gitignored) or the environment:
#   PROD_HOST, PROD_USER, PROD_REPO_DIR, PROD_SSH_KEY, PROD_SSH_PORT (optional)
# See RUNBOOK.local.md for values and gotchas.
#
# Usage:  scripts/refresh_data.sh [--skip-export]
#   --skip-export : only download the newest existing bundle (no new export)

set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi
: "${PROD_HOST:?PROD_HOST not set (add it to .env, see RUNBOOK.local.md)}"
: "${PROD_USER:?PROD_USER not set}"
: "${PROD_REPO_DIR:?PROD_REPO_DIR not set}"
: "${PROD_SSH_KEY:?PROD_SSH_KEY not set (path to the deploy key)}"
PORT="${PROD_SSH_PORT:-22}"
SSH=(ssh -i "$PROD_SSH_KEY" -p "$PORT" "$PROD_USER@$PROD_HOST")

if [ "${1:-}" != "--skip-export" ]; then
  echo "==> generating export on $PROD_HOST (takes a few minutes, 12M+ rows) ..."
  "${SSH[@]}" "cd $PROD_REPO_DIR && docker compose run --rm collector \
    export-analysis-dataset --output-dir exports"
fi

echo "==> locating newest bundle ..."
latest=$("${SSH[@]}" "ls -t $PROD_REPO_DIR/exports/forecast_analysis_dataset_*.zip | head -1")
echo "    $latest"

echo "==> downloading ..."
scp -i "$PROD_SSH_KEY" -P "$PORT" "$PROD_USER@$PROD_HOST:$latest" .

echo "==> refreshing FRED ..."
python3 analysis/collect_fred.py

echo "==> readiness audit ..."
python3 analysis/check_readiness.py
