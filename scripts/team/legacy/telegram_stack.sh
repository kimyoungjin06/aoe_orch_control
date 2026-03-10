#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
PACKAGE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STACK_SCRIPT="$PACKAGE_ROOT/scripts/team/aoe-team-stack.sh"
PROJECT_ROOT="${AOE_PROJECT_ROOT:-$PACKAGE_ROOT}"

exec "$STACK_SCRIPT" --project-root "$PROJECT_ROOT" "$@"
