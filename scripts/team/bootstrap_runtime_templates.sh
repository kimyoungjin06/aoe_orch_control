#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEAM_DIR=""
OVERVIEW=""
FORCE=0

usage() {
  cat <<'EOF'
usage: bootstrap_runtime_templates.sh [--project-root PATH] [--team-dir PATH] [--overview TEXT] [--force]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root)
      shift
      PROJECT_ROOT="$(cd "${1:?missing path}" && pwd)"
      shift
      ;;
    --team-dir)
      shift
      TEAM_DIR="$1"
      shift
      ;;
    --overview)
      shift
      OVERVIEW="$1"
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$TEAM_DIR" ]]; then
  TEAM_DIR="$PROJECT_ROOT/.aoe-team"
fi

TEMPLATE_DIR="$PROJECT_ROOT/templates/aoe-team"
if [[ ! -d "$TEMPLATE_DIR" ]]; then
  echo "[ERROR] template directory not found: $TEMPLATE_DIR" >&2
  exit 1
fi

mkdir -p "$TEAM_DIR"

copy_file_if_needed() {
  local src="$1"
  local dst="$2"
  if [[ -f "$dst" && "$FORCE" -ne 1 ]]; then
    echo "[SKIP] $dst (exists)"
    return
  fi
  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
  echo "[COPY] $dst"
}

escape_sed() {
  printf '%s' "$1" | sed 's/[&|]/\\&/g'
}

render_json_template() {
  local src="$1"
  local dst="$2"
  if [[ -f "$dst" && "$FORCE" -ne 1 ]]; then
    echo "[SKIP] $dst (exists)"
    return
  fi
  local created_at project_name overview
  created_at="$(date -Iseconds)"
  project_name="$(basename "$PROJECT_ROOT")"
  overview="${OVERVIEW:-$project_name project orchestration}"
  mkdir -p "$(dirname "$dst")"
  sed \
    -e "s|__PROJECT_ROOT__|$(escape_sed "$PROJECT_ROOT")|g" \
    -e "s|__PROJECT_NAME__|$(escape_sed "$project_name")|g" \
    -e "s|__TEAM_DIR__|$(escape_sed "$TEAM_DIR")|g" \
    -e "s|__CREATED_AT__|$(escape_sed "$created_at")|g" \
    -e "s|__OVERVIEW__|$(escape_sed "$overview")|g" \
    "$src" > "$dst"
  echo "[COPY] $dst"
}

render_json_template "$TEMPLATE_DIR/team.sample.json" "$TEAM_DIR/team.json"
render_json_template "$TEMPLATE_DIR/orchestrator.sample.json" "$TEAM_DIR/orchestrator.json"

for f in "$TEMPLATE_DIR"/workers/*.json; do
  copy_file_if_needed "$f" "$TEAM_DIR/workers/$(basename "$f")"
done

for role in Orchestrator DataEngineer Reviewer; do
  src="$TEMPLATE_DIR/agents/$role/AGENTS.md"
  dst="$TEAM_DIR/agents/$role/AGENTS.md"
  if [[ -f "$src" ]]; then
    copy_file_if_needed "$src" "$dst"
  fi
done

echo "[OK] runtime template bootstrap complete"
echo " - project_root: $PROJECT_ROOT"
echo " - team_dir: $TEAM_DIR"
