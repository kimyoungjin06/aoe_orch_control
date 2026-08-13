#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT

ARTIFACTS="$TEST_ROOT/artifacts"
mkdir -p "$ARTIFACTS"
printf 'forager release archive\n' > "$ARTIFACTS/forager-linux-amd64.tar.gz"
sha256sum "$ARTIFACTS/forager-linux-amd64.tar.gz" \
    > "$ARTIFACTS/forager-linux-amd64.tar.gz.sha256"

python3 "$REPO_ROOT/scripts/build_release_provenance.py" \
    --version 0.14.0 \
    --tag v0.14.0 \
    --source-sha 0123456789abcdef0123456789abcdef01234567 \
    --repository kimyoungjin06/forager-cli \
    --artifacts-dir "$ARTIFACTS" \
    --release-notes "$REPO_ROOT/docs/releases/v0.14.0.md" \
    --out "$ARTIFACTS/release-provenance.json" \
    --generated-at 2026-08-13T00:00:00+00:00

python3 - "$ARTIFACTS/release-provenance.json" <<'PY'
import json
import pathlib
import sys

manifest = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
assert manifest["schema"] == "forager_release_provenance.v1"
assert manifest["tag"] == "v0.14.0"
assert manifest["source_sha"] == "0123456789abcdef0123456789abcdef01234567"
assert len(manifest["assets"]) == 1
assert manifest["assets"][0]["name"] == "forager-linux-amd64.tar.gz"
assert len(manifest["assets"][0]["sha256"]) == 64
PY

printf 'changed after checksum\n' >> "$ARTIFACTS/forager-linux-amd64.tar.gz"
if python3 "$REPO_ROOT/scripts/build_release_provenance.py" \
    --version 0.14.0 \
    --tag v0.14.0 \
    --source-sha 0123456789abcdef0123456789abcdef01234567 \
    --repository kimyoungjin06/forager-cli \
    --artifacts-dir "$ARTIFACTS" \
    --release-notes "$REPO_ROOT/docs/releases/v0.14.0.md" \
    --out "$ARTIFACTS/should-not-exist.json" >/dev/null 2>&1; then
    echo "changed archive unexpectedly passed provenance validation" >&2
    exit 1
fi

echo "release provenance checks passed"
