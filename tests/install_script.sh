#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export FORAGER_INSTALL_LIBRARY_ONLY=1
source "$REPO_ROOT/scripts/install.sh"

TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT
MEMBER="forager-linux-amd64"
printf 'forager test binary\n' > "$TEST_ROOT/$MEMBER"
tar -C "$TEST_ROOT" -czf "$TEST_ROOT/good.tar.gz" "$MEMBER"
printf '%s  %s\n' "$(sha256_file "$TEST_ROOT/good.tar.gz")" "$MEMBER.tar.gz" \
    > "$TEST_ROOT/good.tar.gz.sha256"

verify_archive "$TEST_ROOT/good.tar.gz" "$TEST_ROOT/good.tar.gz.sha256"
mkdir "$TEST_ROOT/extracted"
extract_archive "$TEST_ROOT/good.tar.gz" "$MEMBER" "$TEST_ROOT/extracted"

printf 'not-a-checksum\n' > "$TEST_ROOT/malformed.sha256"
if (verify_archive "$TEST_ROOT/good.tar.gz" "$TEST_ROOT/malformed.sha256") >/dev/null 2>&1; then
    echo "malformed checksum unexpectedly passed" >&2
    exit 1
fi

printf '%064d  %s\n' 0 "$MEMBER.tar.gz" > "$TEST_ROOT/wrong.sha256"
if (verify_archive "$TEST_ROOT/good.tar.gz" "$TEST_ROOT/wrong.sha256") >/dev/null 2>&1; then
    echo "incorrect checksum unexpectedly passed" >&2
    exit 1
fi

printf 'unexpected\n' > "$TEST_ROOT/extra"
tar -C "$TEST_ROOT" -czf "$TEST_ROOT/extra.tar.gz" "$MEMBER" extra
if (extract_archive "$TEST_ROOT/extra.tar.gz" "$MEMBER" "$TEST_ROOT/extracted") >/dev/null 2>&1; then
    echo "archive with extra members unexpectedly passed" >&2
    exit 1
fi

echo "install script integrity checks passed"
