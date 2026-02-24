#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT"
exec uv run --with pytest pytest -q tests/gateway/test_gateway_cli.py "$@"
