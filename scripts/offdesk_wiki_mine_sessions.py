#!/usr/bin/env python3
"""Batch-mine a directory of session logs into wiki candidates (local LLM).

Walks Codex/Claude session transcripts, keeps only sessions whose raw text
matches a failure signature or has enough operator messages (cheap grep, no
LLM), routes each session to a profile by its working directory, and runs
offdesk_wiki_session_distiller.py per surviving session on the local model.

This is deliberately a LOCAL-model batch: at roughly 5-25k local tokens and
4-16s per session it costs nothing but GPU time, where an agent-API sweep of
thousands of sessions would be absurd. Promotion stays operator-gated; with
--record the yield lands as unpromoted candidates for the tier-1/2 review.

Usage (dry-run by default):
  OFFDESK_LLM_BASE_URL=http://<gpu>:11434 OFFDESK_LLM_MODEL=qwen3-coder:30b \
  scripts/offdesk_wiki_mine_sessions.py \
    --sessions-dir ~/.codex/sessions \
    --project-map 1.4.5.Local_Map_Analysis=lrnm \
    --project-map 1.2.8.TwinPaper=twinpaper-review:twinpaper \
    [--max-sessions 20] [--record]
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from offdesk_wiki_session_distiller import INCIDENT_PATTERN, detect_format  # noqa: E402

DISTILLER = pathlib.Path(__file__).resolve().parent / "offdesk_wiki_session_distiller.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions-dir", action="append", required=True, type=pathlib.Path)
    parser.add_argument("--project-map", action="append", default=[],
                        help="cwd-substring=profile[:scope_ref]; sessions whose cwd matches route there.")
    parser.add_argument("--use-project-registry", action="store_true",
                        help="Load cwd->profile mappings from the forager project registry "
                        "(~/.config/forager/projects.toml) in addition to --project-map.")
    parser.add_argument("--default-profile", default="", help="Profile for unmapped sessions; unmapped are skipped if empty.")
    parser.add_argument("--min-operator-messages", type=int, default=5,
                        help="Keep signature-free sessions only if they have at least this many operator messages.")
    parser.add_argument("--max-sessions", type=int, default=200)
    parser.add_argument("--max-candidates", type=int, default=6, help="Per session.")
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--out-dir", type=pathlib.Path, help="Per-session reports + summary land here.")
    parser.add_argument("--state-file", type=pathlib.Path,
                        help="Durable cursor of already-mined transcripts (path -> mtime). "
                        "Unchanged transcripts are skipped on later runs, so nightly batches "
                        "do not re-mine the same sessions and resurrect rejected candidates.")
    return parser.parse_args()


def session_cwd(path: pathlib.Path, fmt: str) -> str:
    try:
        with path.open() as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except ValueError:
                    return ""
                if fmt == "codex":
                    return str((record.get("payload") or {}).get("cwd") or "")
                return str(record.get("cwd") or "")
    except OSError:
        return ""
    return ""


def cheap_scan(path: pathlib.Path) -> tuple[bool, int]:
    """(has_incident_signature, operator_message_count) without any LLM."""
    has_signature = False
    operators = 0
    try:
        with path.open(errors="replace") as handle:
            for line in handle:
                if not has_signature and INCIDENT_PATTERN.search(line):
                    has_signature = True
                if '"user_message"' in line or '"type":"user"' in line or '"type": "user"' in line:
                    operators += 1
    except OSError:
        return False, 0
    return has_signature, operators


def route(cwd: str, mappings: list[tuple[str, str, str]], default_profile: str) -> tuple[str, str]:
    for needle, profile, scope_ref in mappings:
        if needle in cwd:
            return profile, scope_ref
    return (default_profile, "") if default_profile else ("", "")


def main() -> int:
    args = parse_args()
    mappings = []
    for item in args.project_map:
        needle, _, target = item.partition("=")
        profile, _, scope_ref = target.partition(":")
        if needle and profile:
            mappings.append((needle, profile, scope_ref or profile))
    if args.use_project_registry:
        sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
        from telegram_operator.projects import load_registry  # noqa: PLC0415

        for entry in load_registry().values():
            profile = entry.get("wiki_profile")
            if not profile:
                continue
            for pattern in entry.get("workspace_patterns") or []:
                mappings.append((pattern, profile, entry["key"]))
        print(f"project registry loaded: {len(mappings)} total mappings")

    mined_state: dict[str, float] = {}
    if args.state_file and args.state_file.exists():
        try:
            loaded = json.loads(args.state_file.read_text())
            if isinstance(loaded, dict):
                mined_state = {str(k): float(v) for k, v in loaded.items()}
        except (ValueError, OSError):
            mined_state = {}

    transcripts = sorted(
        {p for d in args.sessions_dir for p in d.rglob("*.jsonl")},
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    print(f"found {len(transcripts)} transcripts; prefiltering (no LLM)...")
    selected: list[tuple[pathlib.Path, str, str, float]] = []
    skipped = {"quiet": 0, "unmapped": 0, "already_mined": 0}
    for path in transcripts:
        if len(selected) >= args.max_sessions:
            break
        mtime = path.stat().st_mtime
        if mined_state.get(str(path)) == mtime:
            skipped["already_mined"] += 1
            continue
        has_signature, operators = cheap_scan(path)
        if not has_signature and operators < args.min_operator_messages:
            skipped["quiet"] += 1
            continue
        fmt = detect_format(path)
        profile, scope_ref = route(session_cwd(path, fmt), mappings, args.default_profile)
        if not profile:
            skipped["unmapped"] += 1
            continue
        selected.append((path, profile, scope_ref, mtime))
    print(
        f"selected {len(selected)} sessions (skipped quiet={skipped['quiet']}, "
        f"unmapped={skipped['unmapped']}, already_mined={skipped['already_mined']})"
    )

    totals = {"accepted": 0, "rejected": 0, "sessions": 0, "failures": 0}
    by_profile: dict[str, int] = {}
    for path, profile, scope_ref, mtime in selected:
        command = [
            sys.executable, str(DISTILLER),
            "--transcript", str(path),
            "--profile", profile, "--scope", "project", "--scope-ref", scope_ref,
            "--domain-tag", scope_ref, "--max-candidates", str(args.max_candidates),
        ]
        if args.record:
            command.append("--record")
        if args.out_dir:
            args.out_dir.mkdir(parents=True, exist_ok=True)
            command += ["--out", str(args.out_dir / f"{path.stem[:12]}.json")]
        result = subprocess.run(command, capture_output=True, text=True)
        totals["sessions"] += 1
        if result.returncode != 0:
            totals["failures"] += 1
            print(f"  ! {path.name[:40]}: distiller failed: {(result.stderr or '')[:120]}")
            continue
        match = re.search(r"accepted (\d+), rejected (\d+)", result.stdout)
        accepted = int(match.group(1)) if match else 0
        rejected = int(match.group(2)) if match else 0
        totals["accepted"] += accepted
        totals["rejected"] += rejected
        by_profile[profile] = by_profile.get(profile, 0) + accepted
        mined_state[str(path)] = mtime
        print(f"  {path.name[:44]:46} -> {profile}: +{accepted} / -{rejected}")

    if args.state_file:
        args.state_file.parent.mkdir(parents=True, exist_ok=True)
        args.state_file.write_text(json.dumps(mined_state, indent=1))

    print(f"\nbatch done: {totals['sessions']} sessions, {totals['accepted']} candidates accepted, "
          f"{totals['rejected']} rejected, {totals['failures']} failures")
    for profile, count in sorted(by_profile.items()):
        print(f"  {profile}: {count}")
    if args.record and totals["accepted"]:
        print("\nnext: per profile ->")
        for profile in by_profile:
            print(f"  scripts/offdesk_wiki_prereview.py --profile {profile} --packet <out>/{profile}-packet.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
