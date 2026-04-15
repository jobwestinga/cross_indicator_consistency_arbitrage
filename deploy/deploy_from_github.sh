#!/usr/bin/env bash

set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/cross_indicator_consistency_arbitrage}"
BRANCH="${BRANCH:-main}"

log() {
  printf '[deploy] %s\n' "$1"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log "Missing required command: $1"
    exit 1
  fi
}

require_command git
require_command docker

cd "$REPO_DIR"

if ! git diff --quiet || ! git diff --cached --quiet; then
  log "Refusing to deploy because the server worktree has tracked changes."
  git status --short
  exit 1
fi

log "Deploying branch '$BRANCH' in $REPO_DIR"
git fetch origin
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

log "Ensuring postgres is running"
docker compose up -d postgres

log "Rebuilding collector images"
docker compose build collector tester

log "Running migrations"
docker compose run --rm collector migrate

log "Deployment complete at commit $(git rev-parse --short HEAD)"
