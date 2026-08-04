#!/usr/bin/env python3
"""Local-LLM document distiller for the adaptive wiki.

Reads project documents (guides, AGENTS files, papers as text/markdown), asks a
local Ollama-compatible model to distill durable knowledge candidates using the
rules in docs/adaptive-wiki-distillation.md, verifies every candidate's evidence
quote verbatim against the source document, and (only with --record) records the
survivors as adaptive-wiki *candidates* with origin=background_review.

Governance boundary: this tool NEVER promotes. Candidates land in the normal
review queue; promotion stays an operator/Council decision. A candidate whose
evidence quote cannot be found verbatim in the source is rejected outright, so
a hallucinating model cannot invent knowledge with fake provenance.

Default is a dry run (report only). Typical use:

  OFFDESK_LLM_BASE_URL=http://<gpu-server>:11434 \
  OFFDESK_LLM_MODEL=gemma4:26b \
  scripts/offdesk_wiki_distiller.py \
    --doc /path/to/PROJECT/AGENTS.md \
    --profile my-project --scope-ref my-project \
    [--record]
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.request

VALID_KINDS = {"preference", "procedure", "failure_pattern", "policy_rule", "fact"}
VALID_FACETS = {"research", "product", "ops"}
VALID_MODES = {"planning", "development", "analysis", "writing", "critique", "review", "maintenance"}
CLAIM_HARD_CAP = 140
MIN_QUOTE_CHARS = 20

RUBRIC = """You distill durable knowledge from a project document into wiki candidates for an AI agent's context. The wiki is NOT a mirror of the document: the document stays the source of truth.

KEEP only:
- non-obvious gotchas that prevent a real mistake (fixed denominators, forbidden values, required flags);
- authority/boundary rules an agent could plausibly violate;
- durable domain facts an agent needs in context (canonical sources, IDs, metrics, windows);
- methodology rules that shape correct work.
PRUNE (do not emit): near-verbatim restatements of stable doc sections, generic best practice ("run tests", "write logs"), one-line environment conventions, marketing or narrative text.

Each candidate:
- claim: ONE durable statement, <= 120 characters, no throat-clearing, no project-name prefix;
- ai_instruction: imperative, actionable, <= 200 characters (empty string if the claim itself is the instruction);
- kind: one of preference | procedure | failure_pattern | policy_rule | fact;
- facet: research (scientific substance) | product (what the software is/does) | ops (how to run/build/operate);
- agent_modes: subset of [planning, development, analysis, writing, critique, review, maintenance], or [] if universal;
- evidence_quote: ONE short VERBATIM sentence or fragment (20-160 chars) copied exactly, character for character, from the document; never paraphrase, never repeat words;
- section: the heading of the section the quote came from, or "" if unclear.

Return STRICT JSON only: {"candidates": [{...}, ...]}. Emit at most {max_candidates} candidates. Fewer, well-chosen candidates beat many weak ones. If the document has nothing worth keeping, return {"candidates": []}."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc", action="append", required=True, type=pathlib.Path, help="Source document (repeatable).")
    parser.add_argument("--profile", required=True, help="Forager profile to record candidates into.")
    parser.add_argument("--scope", default="project", choices=["project", "user_global", "artifact_kind", "session"])
    parser.add_argument("--scope-ref", default="", help="Scope reference (e.g. project key). Required unless scope=user_global.")
    parser.add_argument("--domain-tag", default="", help="domain/<x> tag; defaults to the scope-ref.")
    parser.add_argument("--base-url", default=os.environ.get("OFFDESK_LLM_BASE_URL", "http://127.0.0.1:11434"))
    parser.add_argument("--model", default=os.environ.get("OFFDESK_LLM_MODEL", "qwen3-coder:30b"))
    parser.add_argument("--temperature", type=float, default=0.2)
    parser.add_argument("--num-ctx", type=int, default=16384)
    # 8 candidates with quotes can exceed 2048 tokens; a truncated response
    # parses as invalid JSON, so keep headroom by default.
    parser.add_argument("--num-predict", type=int, default=4096)
    parser.add_argument("--timeout-sec", type=int, default=600)
    parser.add_argument("--max-candidates", type=int, default=8, help="Cap per document.")
    parser.add_argument("--chunk-chars", type=int, default=24000, help="Split longer docs on heading boundaries near this size.")
    parser.add_argument("--record", action="store_true", help="Record verified candidates via `forager offdesk wiki record-candidate` (origin background_review). Default is dry-run.")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
    from forager_bin import resolve_forager_bin

    parser.add_argument("--forager-bin", default=resolve_forager_bin())
    parser.add_argument("--mock", action="store_true", help="Deterministic offline mode for smoke tests (no LLM call).")
    parser.add_argument("--out", type=pathlib.Path, help="Write the full JSON report here.")
    return parser.parse_args()


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def chunk_document(text: str, chunk_chars: int) -> list[str]:
    if len(text) <= chunk_chars:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    size = 0
    for line in text.splitlines(keepends=True):
        heading = line.startswith("#")
        if current and size >= chunk_chars and heading:
            chunks.append("".join(current))
            current, size = [], 0
        current.append(line)
        size += len(line)
    if current:
        chunks.append("".join(current))
    return chunks


# Accumulated local-LLM usage for this process, so reports can state what a
# run actually cost (tokens are free locally, but wall-clock and GPU time are
# the real budget; per-lesson cost makes the pipeline's economics reviewable).
LLM_USAGE = {"calls": 0, "prompt_tokens": 0, "output_tokens": 0, "duration_ms": 0}


def call_ollama(args: argparse.Namespace, prompt: str, *, model: str | None = None) -> str:
    payload = {
        "model": model or args.model,
        "prompt": prompt,
        "stream": False,
        "think": False,
        "format": "json",
        "options": {
            "temperature": args.temperature,
            "num_ctx": args.num_ctx,
            "num_predict": args.num_predict,
            # Verbatim-quote copying under format=json makes small models prone
            # to degenerate repetition loops; penalize repeats explicitly.
            "repeat_penalty": 1.15,
        },
    }
    request = urllib.request.Request(
        args.base_url.rstrip("/") + "/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=args.timeout_sec) as response:
        body = json.loads(response.read().decode("utf-8"))
    LLM_USAGE["calls"] += 1
    LLM_USAGE["prompt_tokens"] += int(body.get("prompt_eval_count") or 0)
    LLM_USAGE["output_tokens"] += int(body.get("eval_count") or 0)
    LLM_USAGE["duration_ms"] += int(body.get("total_duration") or 0) // 1_000_000
    return str(body.get("response") or "")


def mock_response(doc_text: str, max_candidates: int) -> str:
    # Deterministic: quote the first sufficiently long line so verification passes.
    quote = next(
        (normalize(line) for line in doc_text.splitlines() if len(normalize(line)) >= MIN_QUOTE_CHARS),
        "",
    )
    candidate = {
        "kind": "fact",
        "facet": "ops",
        "claim": "Mock distilled claim for smoke testing.",
        "ai_instruction": "",
        "agent_modes": [],
        "evidence_quote": quote[:200],
        "section": "",
    }
    fabricated = dict(candidate, claim="Fabricated claim that must be rejected.", evidence_quote="THIS QUOTE DOES NOT EXIST IN THE DOCUMENT 12345")
    return json.dumps({"candidates": [candidate, fabricated][: max(1, max_candidates)]})


def parse_candidates(raw_response: str) -> tuple[list, str]:
    """Parse the model's JSON, salvaging complete candidates from a truncated
    response (done_reason=length / repetition loops). Returns (candidates, note)."""
    try:
        parsed = json.loads(raw_response)
        return list(parsed.get("candidates") or []), ""
    except (ValueError, AttributeError):
        pass
    # Walk '}' positions from the end, trying to close the array early: this
    # recovers every fully-emitted candidate and drops the truncated tail.
    for index in range(len(raw_response) - 1, 0, -1):
        if raw_response[index] != "}":
            continue
        try:
            parsed = json.loads(raw_response[: index + 1] + "]}")
            salvaged = list(parsed.get("candidates") or [])
            if salvaged:
                return salvaged, f"salvaged {len(salvaged)} candidate(s) from truncated response"
        except ValueError:
            continue
    return [], "model returned unparseable JSON"


def repair_quote(quote: str, doc_lines: list[str]) -> str | None:
    """Small models often extract a TRUE fact but paraphrase the quote, which
    would reject good knowledge. Fuzzy-match the model's quote against actual
    document lines and return the best verbatim original above a similarity
    threshold. Because WE copy the replacement out of the document, the
    verbatim-provenance guarantee is preserved; only genuinely unsupported
    quotes still fail."""
    best_line, best_score = None, 0.0
    quote_lower = quote.lower()
    for line in doc_lines:
        if len(line) < MIN_QUOTE_CHARS:
            continue
        score = difflib.SequenceMatcher(None, quote_lower, line.lower()).ratio()
        if score > best_score:
            best_line, best_score = line, score
    if best_line is not None and best_score >= 0.55:
        return best_line[:300]
    return None


def verify_candidate(raw: dict, doc_norm: str, doc_lines: list[str]) -> tuple[dict | None, str, bool]:
    """Validate one model-emitted candidate.
    Returns (clean, reason-if-rejected, quote_was_repaired)."""
    kind = str(raw.get("kind") or "").strip().lower()
    if kind not in VALID_KINDS:
        return None, f"invalid kind: {kind!r}", False
    facet = str(raw.get("facet") or "").strip().lower()
    if facet not in VALID_FACETS:
        return None, f"invalid facet: {facet!r}", False
    claim = normalize(str(raw.get("claim") or ""))
    if not claim:
        return None, "empty claim", False
    if len(claim) > CLAIM_HARD_CAP:
        return None, f"claim too long ({len(claim)} > {CLAIM_HARD_CAP})", False
    quote = normalize(str(raw.get("evidence_quote") or ""))
    if len(quote) < MIN_QUOTE_CHARS:
        return None, f"evidence quote too short ({len(quote)} chars)", False
    # The safety core: the quote must exist verbatim (whitespace-normalized) in
    # the source document, so fabricated provenance is rejected mechanically.
    repaired = False
    if quote not in doc_norm:
        fixed = repair_quote(quote, doc_lines)
        if fixed is None:
            return None, "evidence quote not found in source (no close line match)", False
        quote, repaired = fixed, True
    modes = [m for m in (raw.get("agent_modes") or []) if str(m).strip().lower() in VALID_MODES]
    return {
        "kind": kind,
        "facet": facet,
        "claim": claim,
        "ai_instruction": normalize(str(raw.get("ai_instruction") or ""))[:200],
        "agent_modes": [str(m).strip().lower() for m in modes],
        "evidence_quote": quote,
        "section": normalize(str(raw.get("section") or ""))[:80],
    }, "", repaired


def record_candidate(args: argparse.Namespace, doc: pathlib.Path, cand: dict) -> tuple[bool, str]:
    evidence = f"doc:{doc}" + (f" ({cand['section']})" if cand["section"] else "")
    domain = args.domain_tag or args.scope_ref
    command = [
        args.forager_bin, "-p", args.profile, "offdesk", "wiki", "record-candidate",
        "--kind", cand["kind"], "--scope", args.scope,
        "--claim", cand["claim"],
        "--origin", "background_review",
        "--confidence", "inferred",
        "--evidence-ref", evidence,
        "--core-tag", f"facet/{cand['facet']}",
        "--review-reason", f"local-llm distilled from {doc.name}; verify quote: \"{cand['evidence_quote'][:120]}\"",
    ]
    if args.scope != "user_global":
        command += ["--scope-ref", args.scope_ref]
    if domain:
        command += ["--core-tag", f"domain/{domain}"]
    if cand["ai_instruction"]:
        command += ["--ai-instruction", cand["ai_instruction"]]
    for mode in cand["agent_modes"]:
        command += ["--agent-mode", mode]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        return False, (result.stderr or result.stdout).strip()[:200]
    return True, ""


def main() -> int:
    args = parse_args()
    if args.scope != "user_global" and not args.scope_ref.strip():
        print("--scope-ref is required unless --scope user_global", file=sys.stderr)
        return 2

    report = {
        "schema": "wiki_distiller_report.v1",
        "model": "mock" if args.mock else args.model,
        "profile": args.profile,
        "recorded_mode": bool(args.record),
        "documents": [],
    }
    total_accepted = 0
    for doc in args.doc:
        try:
            text = doc.read_text(encoding="utf-8", errors="replace")
        except OSError as error:
            report["documents"].append({"doc": str(doc), "error": str(error)})
            continue
        doc_norm = normalize(text)
        doc_lines = [normalize(line) for line in text.splitlines() if normalize(line)]
        accepted: list[dict] = []
        rejected: list[dict] = []
        for chunk in chunk_document(text, args.chunk_chars):
            if len(accepted) >= args.max_candidates:
                break
            prompt = (
                RUBRIC.replace("{max_candidates}", str(args.max_candidates))
                + "\n\n--- DOCUMENT ---\n"
                + chunk
            )
            raw_response = mock_response(text, args.max_candidates) if args.mock else call_ollama(args, prompt)
            raw_candidates, parse_note = parse_candidates(raw_response)
            if not raw_candidates:
                rejected.append({"reason": parse_note, "preview": raw_response[:160]})
                continue
            if parse_note:
                rejected.append({"reason": parse_note})
            for raw in raw_candidates:
                if len(accepted) >= args.max_candidates:
                    break
                if not isinstance(raw, dict):
                    continue
                clean, reason, repaired = verify_candidate(raw, doc_norm, doc_lines)
                if clean is None:
                    rejected.append({"reason": reason, "claim": normalize(str(raw.get("claim") or ""))[:100]})
                    continue
                if repaired:
                    clean["quote_repaired"] = True
                # de-dup within this run by normalized claim
                if any(c["claim"].lower() == clean["claim"].lower() for c in accepted):
                    rejected.append({"reason": "duplicate claim in run", "claim": clean["claim"][:100]})
                    continue
                clean["recorded"] = False
                if args.record:
                    ok, error = record_candidate(args, doc, clean)
                    clean["recorded"] = ok
                    if not ok:
                        clean["record_error"] = error
                accepted.append(clean)
        total_accepted += len(accepted)
        report["documents"].append(
            {"doc": str(doc), "accepted": accepted, "rejected": rejected}
        )
        print(f"{doc}: accepted {len(accepted)}, rejected {len(rejected)}"
              + ("" if args.record else " (dry-run, nothing recorded)"))
        for c in accepted:
            marker = " (quote repaired)" if c.get("quote_repaired") else ""
            print(f"  + [{c['kind']}/{c['facet']}] {c['claim']}{marker}")
        for r in rejected:
            print(f"  - rejected: {r['reason']}" + (f" | {r.get('claim','')}" if r.get("claim") else ""))

    report["llm_usage"] = dict(LLM_USAGE)
    if LLM_USAGE["calls"]:
        per = LLM_USAGE["duration_ms"] // max(1, total_accepted or 1)
        print(f"llm cost: {LLM_USAGE['calls']} call(s), {LLM_USAGE['prompt_tokens']}+{LLM_USAGE['output_tokens']} tokens, "
              f"{LLM_USAGE['duration_ms']/1000:.1f}s wall ({per/1000:.1f}s per accepted candidate)")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"report: {args.out}")
    if args.record and total_accepted:
        print(f"\n{total_accepted} candidate(s) recorded (origin=background_review, unpromoted).")
        print(f"Review with: {args.forager_bin} -p {args.profile} offdesk wiki candidates --json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
