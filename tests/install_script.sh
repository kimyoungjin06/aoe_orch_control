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

if ! validate_version "v0.14.0"; then
    echo "valid release version was rejected" >&2
    exit 1
fi
if validate_version "0.14.0" || validate_version "v0.14"; then
    echo "invalid release version unexpectedly passed" >&2
    exit 1
fi

printf '%s\n' \
    '{' \
    '  "schema": "forager_release_provenance.v1",' \
    '  "source_sha": "0123456789abcdef0123456789abcdef01234567",' \
    '  "assets": [' \
    '    {' \
    '      "name": "forager-linux-amd64.tar.gz",' \
    "      \"sha256\": \"$(sha256_file "$TEST_ROOT/good.tar.gz")\"" \
    '    }' \
    '  ]' \
    '}' > "$TEST_ROOT/provenance.json"
if [ "$(read_release_source_sha "$TEST_ROOT/provenance.json")" != \
    "0123456789abcdef0123456789abcdef01234567" ]; then
    echo "release source SHA was not parsed" >&2
    exit 1
fi
if ! release_manifest_matches_archive \
    "$TEST_ROOT/provenance.json" \
    "forager-linux-amd64.tar.gz" \
    "$(sha256_file "$TEST_ROOT/good.tar.gz")"; then
    echo "matching release archive was not found in provenance" >&2
    exit 1
fi
if release_manifest_matches_archive \
    "$TEST_ROOT/provenance.json" \
    "forager-linux-arm64.tar.gz" \
    "$(sha256_file "$TEST_ROOT/good.tar.gz")"; then
    echo "wrong release archive unexpectedly matched provenance" >&2
    exit 1
fi

receipt="$TEST_ROOT/state/forager/install-receipt.txt"
write_install_receipt \
    "$receipt" \
    "v0.14.0" \
    "linux-amd64" \
    "$TEST_ROOT/bin/forager" \
    "$(printf '%064d' 1)" \
    "$(printf '%064d' 2)" \
    "https://example.invalid/forager-linux-amd64.tar.gz" \
    "0123456789abcdef0123456789abcdef01234567"
grep -qx 'schema=forager_install_receipt.v1' "$receipt"
grep -qx 'version=v0.14.0' "$receipt"
grep -qx 'source_sha=0123456789abcdef0123456789abcdef01234567' "$receipt"
grep -qx "binary_path=$TEST_ROOT/bin/forager" "$receipt"
if [ "$(stat -c '%a' "$receipt")" != "600" ]; then
    echo "install receipt permissions are not private" >&2
    exit 1
fi

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
