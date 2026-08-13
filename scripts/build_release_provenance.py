#!/usr/bin/env python3
"""Build a source-bound manifest for published Forager release archives."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import re
import sys


SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?$")
SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
GIT_SHA = re.compile(r"^[0-9a-fA-F]{40}$")


class ProvenanceError(RuntimeError):
    """Raised when release inputs cannot produce a trusted manifest."""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expected_checksum(path: pathlib.Path) -> str:
    try:
        checksum = path.read_text(encoding="utf-8").split()[0]
    except (OSError, IndexError) as error:
        raise ProvenanceError(f"cannot read checksum file: {path}") from error
    if not SHA256.fullmatch(checksum):
        raise ProvenanceError(f"malformed checksum file: {path}")
    return checksum.lower()


def build_manifest(args: argparse.Namespace) -> dict[str, object]:
    if not SEMVER.fullmatch(args.version):
        raise ProvenanceError(f"invalid release version: {args.version}")
    if args.tag != f"v{args.version}":
        raise ProvenanceError(
            f"tag {args.tag!r} does not match version {args.version!r}"
        )
    if not GIT_SHA.fullmatch(args.source_sha):
        raise ProvenanceError("source SHA must be a full 40-character Git SHA")

    artifacts_dir = args.artifacts_dir.resolve()
    if not artifacts_dir.is_dir():
        raise ProvenanceError(f"artifacts directory does not exist: {artifacts_dir}")

    assets: list[dict[str, object]] = []
    for archive in sorted(artifacts_dir.glob("*.tar.gz")):
        checksum_path = archive.with_name(f"{archive.name}.sha256")
        if not checksum_path.is_file():
            raise ProvenanceError(f"missing checksum file for {archive.name}")
        expected = expected_checksum(checksum_path)
        actual = sha256_file(archive)
        if actual != expected:
            raise ProvenanceError(
                f"checksum mismatch for {archive.name}: expected {expected}, got {actual}"
            )
        assets.append(
            {
                "name": archive.name,
                "size_bytes": archive.stat().st_size,
                "sha256": actual,
                "checksum_file": checksum_path.name,
            }
        )
    if not assets:
        raise ProvenanceError("no release archives were found")

    release_notes = args.release_notes.resolve()
    if not release_notes.is_file():
        raise ProvenanceError(f"release notes do not exist: {release_notes}")

    generated_at = args.generated_at or dt.datetime.now(dt.UTC).isoformat()
    return {
        "schema": "forager_release_provenance.v1",
        "version": args.version,
        "tag": args.tag,
        "source_sha": args.source_sha.lower(),
        "repository": args.repository,
        "generated_at": generated_at,
        "release_notes": {
            "path": args.release_notes.as_posix(),
            "sha256": sha256_file(release_notes),
        },
        "assets": assets,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--artifacts-dir", type=pathlib.Path, required=True)
    parser.add_argument("--release-notes", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--generated-at", help="Fixed timestamp for deterministic tests")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = build_manifest(args)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (OSError, ProvenanceError) as error:
        print(f"release provenance error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
