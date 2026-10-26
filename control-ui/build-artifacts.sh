#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v bun >/dev/null 2>&1; then
  echo "bun is required to build RelayWay control UI assets" >&2
  exit 1
fi

cd "$SCRIPT_DIR"
bun install --frozen-lockfile
bun run build
