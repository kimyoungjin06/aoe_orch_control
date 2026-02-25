#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
DEFAULT_PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${AOE_PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"

usage() {
  cat <<'EOF'
usage: aoe-team-stack.sh [--project-root PATH] <command> [args...]

commands are forwarded to:
  .aoe-team/telegram_tmux.sh

examples:
  aoe-team-stack.sh start
  aoe-team-stack.sh ui
  aoe-team-stack.sh switch 2
  aoe-team-stack.sh --project-root /path/to/project start
EOF
}

if [[ "${1:-}" == "--project-root" ]]; then
  shift
  PROJECT_ROOT="$(cd "${1:?missing path}" && pwd)"
  shift
fi

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

TEAM_SCRIPT="$PROJECT_ROOT/.aoe-team/telegram_tmux.sh"
if [[ ! -x "$TEAM_SCRIPT" ]]; then
  echo "[ERROR] stack script not found/executable: $TEAM_SCRIPT" >&2
  echo "[HINT] run from a valid project root or pass --project-root." >&2
  exit 1
fi

exec "$TEAM_SCRIPT" "$@"
