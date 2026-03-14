#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
PACKAGE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${AOE_PROJECT_ROOT:-$PACKAGE_ROOT}"
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

TEMPLATE_DIR="$PACKAGE_ROOT/templates/aoe-team"
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

write_runtime_wrapper() {
  local dst="$1"
  local target="$2"
  mkdir -p "$(dirname "$dst")"
  cat >"$dst" <<EOF
#!/usr/bin/env bash
set -euo pipefail
exec env AOE_PROJECT_ROOT="$(printf '%s' "$PROJECT_ROOT")" AOE_TEAM_DIR="$(printf '%s' "$TEAM_DIR")" "$target" "\$@"
EOF
  chmod 755 "$dst"
  echo "[WRITE] $dst"
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

if [[ -f "$TEMPLATE_DIR/AOE_TODO.md" ]]; then
  copy_file_if_needed "$TEMPLATE_DIR/AOE_TODO.md" "$TEAM_DIR/AOE_TODO.md"
fi

if [[ -f "$TEMPLATE_DIR/telegram.env.sample" ]]; then
  copy_file_if_needed "$TEMPLATE_DIR/telegram.env.sample" "$TEAM_DIR/telegram.env.sample"
fi

if [[ -f "$TEMPLATE_DIR/sync_policy.sample.json" ]]; then
  copy_file_if_needed "$TEMPLATE_DIR/sync_policy.sample.json" "$TEAM_DIR/sync_policy.sample.json"
fi

if [[ -f "$TEMPLATE_DIR/tf_backend.sample.json" ]]; then
  copy_file_if_needed "$TEMPLATE_DIR/tf_backend.sample.json" "$TEAM_DIR/tf_backend.sample.json"
fi

for f in "$TEMPLATE_DIR"/workers/*.json; do
  copy_file_if_needed "$f" "$TEAM_DIR/workers/$(basename "$f")"
done

for role in Orchestrator DataEngineer Codex-Reviewer Claude-Reviewer Codex-Dev Codex-Writer Claude-Writer Codex-Analyst Claude-Analyst; do
  src="$TEMPLATE_DIR/agents/$role/AGENTS.md"
  dst="$TEAM_DIR/agents/$role/AGENTS.md"
  if [[ -f "$src" ]]; then
    copy_file_if_needed "$src" "$dst"
  fi
done

write_runtime_wrapper "$TEAM_DIR/telegram_tmux.sh" "$PACKAGE_ROOT/scripts/team/runtime/telegram_tmux.sh"
write_runtime_wrapper "$TEAM_DIR/worker_codex_handler.sh" "$PACKAGE_ROOT/scripts/team/runtime/worker_codex_handler.sh"
write_runtime_wrapper "$TEAM_DIR/telegram_stack.sh" "$PACKAGE_ROOT/scripts/team/legacy/telegram_stack.sh"

echo "[OK] runtime template bootstrap complete"
echo " - package_root: $PACKAGE_ROOT"
echo " - project_root: $PROJECT_ROOT"
echo " - team_dir: $TEAM_DIR"
