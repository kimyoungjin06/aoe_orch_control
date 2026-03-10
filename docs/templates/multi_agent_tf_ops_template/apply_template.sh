#!/usr/bin/env bash
set -euo pipefail

FORCE=0
if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
  shift
fi

if [[ $# -lt 5 ]]; then
  cat <<'USAGE'
Usage:
  ./apply_template.sh [--force] <target_project_root> <module_name> <active_project> <active_tf> <tag>

Example:
  ./apply_template.sh ~/Desktop/Workspace/aoe_orch_control aoe_orch_control O1 TF-001 2026-02-26
USAGE
  exit 1
fi

TARGET_ROOT="$1"
MODULE_NAME="$2"
ACTIVE_PROJECT="$3"
ACTIVE_TF="$4"
TAG="$5"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/template/docs/investigations_mo"
DST_DIR="$TARGET_ROOT/docs/investigations_mo"

escape_sed() {
  printf '%s' "$1" | sed -e 's/[\/&]/\\&/g'
}

MODULE_ESC="$(escape_sed "$MODULE_NAME")"
PROJECT_ESC="$(escape_sed "$ACTIVE_PROJECT")"
TF_ESC="$(escape_sed "$ACTIVE_TF")"
TAG_ESC="$(escape_sed "$TAG")"

mkdir -p "$DST_DIR"

copied=0
skipped=0

while IFS= read -r -d '' src; do
  rel="${src#"$SRC_DIR"/}"
  rel="${rel//__MODULE_NAME__/$MODULE_NAME}"
  rel="${rel//__ACTIVE_PROJECT__/$ACTIVE_PROJECT}"
  rel="${rel//__ACTIVE_TF__/$ACTIVE_TF}"

  dst="$DST_DIR/$rel"
  mkdir -p "$(dirname "$dst")"

  if [[ -e "$dst" && "$FORCE" -ne 1 ]]; then
    skipped=$((skipped + 1))
    continue
  fi

  sed \
    -e "s/__MODULE_NAME__/$MODULE_ESC/g" \
    -e "s/__ACTIVE_PROJECT__/$PROJECT_ESC/g" \
    -e "s/__ACTIVE_TF__/$TF_ESC/g" \
    -e "s/__TAG__/$TAG_ESC/g" \
    "$src" > "$dst"
  copied=$((copied + 1))
done < <(find "$SRC_DIR" -type f -print0)

echo "Template applied"
echo "  target_root: $TARGET_ROOT"
echo "  docs_root:   $DST_DIR"
echo "  copied:      $copied"
echo "  skipped:     $skipped"
echo "  force:       $FORCE"
