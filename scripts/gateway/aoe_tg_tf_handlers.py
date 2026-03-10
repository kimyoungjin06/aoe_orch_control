#!/usr/bin/env python3
"""TF recipe helpers for Telegram gateway.

Design goals:
- Keep operator input minimal (slash-first).
- Provide "proof" style checks that are more than smoke/triage:
  Prefer deterministic local validators for known artifacts (JSON/YAML), so "proof"
  is reproducible and does not depend on an LLM run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class TFRecipe:
    id: str
    title: str
    prompt_template: str  # legacy: for future LLM-backed recipes
    default_tag: str = "phase-1"
    roles_csv: str = "Reviewer"
    priority: str = "P2"


_RECIPES: Dict[str, TFRecipe] = {
    "mod2-proof": TFRecipe(
        id="mod2-proof",
        title="TwinPaper Module02 pipeline proof (read-only, evidence-based)",
        default_tag="phase-1",
        roles_csv="Reviewer",
        prompt_template=(
            "목표: TwinPaper Module02(02_golden_set) 파이프라인 상태를 '증거 기반으로' 점검하고, "
            "contract-ci 및 phase1-dedup 산출물의 존재/정상 여부를 요약한다. "
            "파일 수정/파이프라인 실행은 금지하고(읽기 전용 확인만), 실제로 확인한 파일만 Evidence로 적어라.\n\n"
            "절차(필수):\n"
            "1) `modules/02_golden_set/docs/investigations/registry/active_stream_lock.yaml`를 읽고 아래만 추출:\n"
            "   - active_stream, effective_date\n"
            "   - bootstrap.must_run (명령 문자열 목록)\n"
            "   - bootstrap.must_check (경로 목록)\n"
            "2) must_check에 나온 파일들이 실제로 존재하는지 OK/MISSING로 표기.\n"
            "3) TAG={tag} 기준으로 아래 산출물을 읽고 핵심 숫자/게이트 결과를 요약:\n"
            "   - `data/metadata/phase1_overton_doc_family_summary_{tag}.json`\n"
            "   - `data/metadata/phase1_overton_doc_family_review_queue_autolabel_summary_{tag}.json` (counts: n_rows/n_auto_merge/n_auto_split/n_auto_pending)\n"
            "   - `data/metadata/module02_contract_ci_summary_{tag}.json` (status.overall_ok 및 주요 플래그)\n"
            "   - (선택) `data/metadata/module02_migration_smoke_summary_{tag}.json` (counts.pass/fail/total)\n"
            "   - (선택) `data/metadata/module02_investigations_registry_ci_summary_{tag}.json` (counts.errors/warnings)\n"
            "4) 결론: 아래 proof 기준으로 verdict를 내려라.\n"
            "   - proof_success 조건: must_check 모두 OK + contract_ci overall_ok=true + (있으면) migration fail=0 + registry errors=0\n"
            "   - 아니면 proof_retry(누락/불일치) 또는 proof_fail(명확한 깨짐/개입 필요)\n\n"
            "출력 형식(간결):\n"
            "- Evidence: 실제로 확인한 파일 경로 8~15개\n"
            "- Summary: key=value 형태 10~25줄(숫자/플래그 위주)\n"
            "- Verdict: proof_success|proof_retry|proof_fail + 1문장 이유\n"
            "- 다음 행동: 1줄\n"
        ),
    ),
}


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sanitize_tf_token(raw: str, max_len: int = 28) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(raw or "").strip()).strip("._-")
    if not token:
        return "TAG"
    if len(token) > int(max_len):
        token = token[: int(max_len)].rstrip("._-")
    return token or "TAG"


def _rel_paths_ok_missing(pairs: List[Tuple[str, bool]]) -> str:
    ok = sum(1 for _p, ex in pairs if ex)
    return f"{ok}/{len(pairs)}"


def _read_json(path: Path) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return (data if isinstance(data, dict) else {}), ""
    except Exception as e:
        return None, str(e)


def _read_yaml(path: Path) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        import yaml  # lazy import; only needed for proof recipes

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return (data if isinstance(data, dict) else {}), ""
    except Exception as e:
        return None, str(e)


def _nested_get(obj: Any, keys: List[str]) -> Any:
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _parse_iso_dt(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    # Support common "Z" suffix form.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_dt(dt: Optional[datetime]) -> str:
    if not isinstance(dt, datetime):
        return "-"
    # Use Z to keep it compact/human-friendly.
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _discover_mod2_tags(*, project_root: Path, limit: int = 20) -> List[Dict[str, Any]]:
    """Return newest-first tags that have required Module02 proof artifacts."""

    base = project_root / "data" / "metadata"
    if not base.exists():
        return []

    prefix = "module02_contract_ci_summary_"
    items: List[Dict[str, Any]] = []
    # Use contract-ci summary as the anchor; filter by presence of other required files.
    for p in sorted(base.glob(f"{prefix}*.json")):
        tag = p.stem[len(prefix) :] if p.stem.startswith(prefix) else ""
        if not tag:
            continue
        doc = base / f"phase1_overton_doc_family_summary_{tag}.json"
        auto = base / f"phase1_overton_doc_family_review_queue_autolabel_summary_{tag}.json"
        if not doc.exists() or not auto.exists():
            continue

        contract_data, _ = _read_json(p)
        auto_data, _ = _read_json(auto)

        generated = _parse_iso_dt((contract_data or {}).get("generated_at_utc")) if isinstance(contract_data, dict) else None
        if generated is None and p.exists():
            generated = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)

        overall_ok = _nested_get(contract_data or {}, ["status", "overall_ok"])
        pending = _nested_get(auto_data or {}, ["counts", "n_auto_pending"])

        items.append(
            {
                "tag": tag,
                "generated_at": generated,
                "generated_at_utc": _format_dt(generated),
                "overall_ok": (bool(overall_ok) if overall_ok is not None else None),
                "pending": (int(pending) if isinstance(pending, int) else pending),
            }
        )

    items.sort(
        key=lambda row: (row.get("generated_at") or datetime.fromtimestamp(0, tz=timezone.utc), str(row.get("tag", ""))),
        reverse=True,
    )
    return items[: max(1, int(limit))]


def _run_mod2_proof_local(*, project_root: Path, tag: str) -> Dict[str, Any]:
    """Deterministic validator for Module02 proof artifacts (read-only)."""

    evidence: List[str] = []
    summary: List[str] = []
    errors: List[str] = []
    missing: List[str] = []

    rel_yaml = "modules/02_golden_set/docs/investigations/registry/active_stream_lock.yaml"
    yaml_path = (project_root / rel_yaml).resolve()
    evidence.append(rel_yaml)
    ydata, yerr = _read_yaml(yaml_path) if yaml_path.exists() else (None, "missing")
    if ydata is None:
        if yerr == "missing":
            missing.append(rel_yaml)
            errors.append(f"required missing: {rel_yaml}")
        else:
            errors.append(f"yaml parse error: {rel_yaml}: {yerr}")

    active_stream = str((ydata or {}).get("active_stream", "")).strip() if isinstance(ydata, dict) else ""
    effective_date = str((ydata or {}).get("effective_date", "")).strip() if isinstance(ydata, dict) else ""
    summary.append(f"active_stream={active_stream or '-'}")
    summary.append(f"effective_date={effective_date or '-'}")

    bootstrap = (ydata or {}).get("bootstrap") if isinstance(ydata, dict) else {}
    if not isinstance(bootstrap, dict):
        bootstrap = {}
    must_run = bootstrap.get("must_run") or []
    must_check = bootstrap.get("must_check") or []
    must_run = [str(x).strip() for x in must_run if str(x).strip()] if isinstance(must_run, list) else []
    must_check = [str(x).strip() for x in must_check if str(x).strip()] if isinstance(must_check, list) else []
    summary.append(f"must_run.n={len(must_run)}")
    summary.append(f"must_check.n={len(must_check)}")

    must_check_pairs: List[Tuple[str, bool]] = []
    for rel in must_check:
        p = (project_root / rel).resolve()
        ex = p.exists()
        must_check_pairs.append((rel, ex))
        evidence.append(rel)
        if not ex:
            missing.append(rel)
    summary.append(f"must_check.ok={_rel_paths_ok_missing(must_check_pairs)}")

    req = [
        f"data/metadata/phase1_overton_doc_family_summary_{tag}.json",
        f"data/metadata/phase1_overton_doc_family_review_queue_autolabel_summary_{tag}.json",
        f"data/metadata/module02_contract_ci_summary_{tag}.json",
    ]
    opt = [
        f"data/metadata/module02_migration_smoke_summary_{tag}.json",
        f"data/metadata/module02_investigations_registry_ci_summary_{tag}.json",
    ]

    parsed: Dict[str, Dict[str, Any]] = {}
    parse_errors: Dict[str, str] = {}

    def ingest(rel: str, required: bool) -> None:
        p = (project_root / rel).resolve()
        evidence.append(rel)
        if not p.exists():
            summary.append(f"{Path(rel).name}.exists=false")
            if required:
                missing.append(rel)
            return
        data, err = _read_json(p)
        if data is None:
            parse_errors[rel] = err or "parse_error"
            summary.append(f"{Path(rel).name}.parse=ERROR")
        else:
            parsed[rel] = data
            summary.append(f"{Path(rel).name}.parse=OK")

    for rel in req:
        ingest(rel, required=True)
    for rel in opt:
        ingest(rel, required=False)

    doc_path, auto_path, ci_path = req
    mig_path, reg_path = opt

    if doc_path in parsed:
        counts = parsed[doc_path].get("counts") if isinstance(parsed[doc_path].get("counts"), dict) else {}
        if isinstance(counts, dict):
            for k in (
                "n_policy_doc_rows_input",
                "n_families",
                "n_merged_families_auto_high",
                "n_review_queue_pairs_medium",
                "n_medium_blocks_considered",
            ):
                if k in counts:
                    summary.append(f"doc_family.{k}={counts.get(k)}")

    if auto_path in parsed:
        counts = parsed[auto_path].get("counts") if isinstance(parsed[auto_path].get("counts"), dict) else {}
        if isinstance(counts, dict):
            for k in ("n_rows", "n_auto_merge", "n_auto_split", "n_auto_pending", "n_missing_source_record_lookup"):
                if k in counts:
                    summary.append(f"autolabel.{k}={counts.get(k)}")
            try:
                n_rows = int(counts.get("n_rows") or 0)
                n_merge = int(counts.get("n_auto_merge") or 0)
                n_split = int(counts.get("n_auto_split") or 0)
                n_pending = int(counts.get("n_auto_pending") or 0)
                n_missing = int(counts.get("n_missing_source_record_lookup") or 0)
                summary.append(f"autolabel.sum_ok={(n_merge + n_split + n_pending + n_missing) == n_rows}")
            except Exception:
                pass

    contract_overall_ok: Optional[bool] = None
    if ci_path in parsed:
        status = parsed[ci_path].get("status") if isinstance(parsed[ci_path].get("status"), dict) else {}
        if isinstance(status, dict):
            if "overall_ok" in status:
                contract_overall_ok = bool(status.get("overall_ok"))
            for k in ("contract_assets_ok", "required_inputs_ok", "wrapper_gate_all_ready", "overall_ok"):
                if k in status:
                    summary.append(f"contract_ci.status.{k}={status.get(k)}")
        for sec, label in (("required_assets", "contract_ci.required_assets"), ("required_inputs", "contract_ci.required_inputs")):
            block = parsed[ci_path].get(sec) if isinstance(parsed[ci_path].get(sec), dict) else {}
            if isinstance(block, dict):
                if block.get("n_ok") is not None and block.get("n_total") is not None:
                    summary.append(f"{label}.n_ok={block.get('n_ok')}")
                    summary.append(f"{label}.n_total={block.get('n_total')}")

    if mig_path in parsed:
        counts = parsed[mig_path].get("counts") if isinstance(parsed[mig_path].get("counts"), dict) else {}
        if isinstance(counts, dict):
            for k in ("pass", "fail", "total"):
                if k in counts:
                    summary.append(f"migration.counts.{k}={counts.get(k)}")

    if reg_path in parsed:
        counts = parsed[reg_path].get("counts") if isinstance(parsed[reg_path].get("counts"), dict) else {}
        if isinstance(counts, dict):
            for k in ("errors", "warnings"):
                if k in counts:
                    summary.append(f"registry_ci.counts.{k}={counts.get(k)}")

    verdict = "proof_success"
    reason = "all required artifacts present and gates OK"

    # Required parse errors => fail
    for rel, err in parse_errors.items():
        if rel in req:
            verdict = "proof_fail"
            reason = f"required parse error: {rel}"
            errors.append(f"parse_error: {rel}: {err}")
            break

    # Missing required artifacts => retry
    if verdict == "proof_success":
        required_missing = [p for p in (must_check + req) if p in missing]
        if required_missing:
            verdict = "proof_retry"
            reason = "missing required artifacts"

    # Contract gate
    if verdict == "proof_success":
        if contract_overall_ok is False:
            verdict = "proof_fail"
            reason = "contract_ci overall_ok=false"
        elif contract_overall_ok is None and (ci_path in parsed):
            verdict = "proof_retry"
            reason = "contract_ci missing status.overall_ok"

    # Optional gates when present
    if verdict == "proof_success" and mig_path in parsed:
        fail = _nested_get(parsed[mig_path], ["counts", "fail"])
        try:
            if fail is not None and int(fail) > 0:
                verdict = "proof_fail"
                reason = f"migration fail>0 (fail={fail})"
        except Exception:
            verdict = "proof_retry"
            reason = "migration counts.fail not int"

    if verdict == "proof_success" and reg_path in parsed:
        errs = _nested_get(parsed[reg_path], ["counts", "errors"])
        try:
            if errs is not None and int(errs) > 0:
                verdict = "proof_fail"
                reason = f"registry errors>0 (errors={errs})"
        except Exception:
            verdict = "proof_retry"
            reason = "registry counts.errors not int"

    if verdict == "proof_success":
        next_action = "(OK) no action; proceed to next stream or todo"
    elif verdict == "proof_retry":
        next_action = "check must_run in active_stream_lock.yaml; re-generate missing artifacts then re-run /tf"
    else:
        next_action = "investigate failing gate (contract-ci/migration/registry) before proceeding"

    return {
        "tag": tag,
        "active_stream": active_stream,
        "effective_date": effective_date,
        "must_run": must_run,
        "must_check_pairs": must_check_pairs,
        "evidence": evidence,
        "summary": summary,
        "verdict": verdict,
        "reason": reason,
        "next_action": next_action,
        "errors": errors,
        "missing": missing,
    }


def _write_mod2_proof_report(
    *,
    control_root: Path,
    project_alias: str,
    tf_id: str,
    result: Dict[str, Any],
) -> Path:
    docs_root = (control_root / "docs" / "investigations_mo").resolve()
    tf_dir = docs_root / "projects" / project_alias / "tfs" / tf_id
    tf_dir.mkdir(parents=True, exist_ok=True)
    report = tf_dir / "report.md"

    lines: List[str] = []
    lines.append(f"# {tf_id} Report")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- project_alias: {project_alias}")
    lines.append(f"- tag: {result.get('tag','-')}")
    lines.append(f"- generated_at_utc: {_now_iso_utc()}")
    if result.get("active_stream"):
        lines.append(f"- active_stream: {result.get('active_stream')}")
    if result.get("effective_date"):
        lines.append(f"- effective_date: {result.get('effective_date')}")
    lines.append("")
    lines.append("## Objective")
    lines.append("TwinPaper Module02(02_golden_set) proof check (read-only).")
    lines.append("")
    lines.append("## Verdict")
    lines.append(f"- {result.get('verdict','-')}: {result.get('reason','-')}")
    lines.append("")
    lines.append("## Must Run (Reference Only)")
    must_run = result.get("must_run") or []
    if isinstance(must_run, list) and must_run:
        for cmd in must_run[:12]:
            lines.append(f"- `{cmd}`")
    else:
        lines.append("- -")
    lines.append("")
    lines.append("## Evidence")
    for rel in (result.get("evidence") or [])[:80]:
        lines.append(f"- {rel}")
    lines.append("")
    lines.append("## Summary")
    for row in (result.get("summary") or [])[:120]:
        lines.append(f"- {row}")
    if result.get("errors"):
        lines.append("")
        lines.append("## Errors")
        for row in (result.get("errors") or [])[:60]:
            lines.append(f"- {row}")
    if result.get("missing"):
        lines.append("")
        lines.append("## Missing")
        for row in (result.get("missing") or [])[:120]:
            lines.append(f"- {row}")
    lines.append("")
    lines.append("## Next Action")
    lines.append(f"- {result.get('next_action','-')}")

    payload = "\n".join(lines).rstrip() + "\n"
    report.write_text(payload, encoding="utf-8")
    return report


def _tf_usage() -> str:
    ids = sorted(_RECIPES.keys())
    lines = [
        "tf recipes",
        "usage:",
        "- /tf                 # list recipes",
        "- /tf list            # list recipes",
        "- /tf <recipe> [tag]  # run a proof check (default tag depends on recipe)",
        "- /tf mod2-proof tags   # preview available tags",
        "- /tf mod2-proof latest # pick latest tag automatically",
        "",
        "recipes:",
    ]
    for rid in ids:
        r = _RECIPES[rid]
        lines.append(f"- {r.id}: {r.title} (default tag={r.default_tag})")
    return "\n".join(lines).strip()


def handle_tf_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    rest: str,
    send: Callable[..., bool],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
) -> Optional[Dict[str, Any]]:
    if cmd != "tf":
        return None

    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].strip().lower() if tokens else "").strip()
    arg1 = (tokens[1].strip() if len(tokens) >= 2 else "")
    arg2 = (tokens[2].strip() if len(tokens) >= 3 else "")

    # readonly chats can always list recipes.
    if sub in {"", "list", "ls", "help", "h", "?"}:
        key, entry, _p_args = get_context(None)
        alias = str((entry or {}).get("project_alias", "")).strip() or "-"
        send(f"{_tf_usage()}\n\nactive orch: {key} ({alias})", context="tf-usage", with_menu=True)
        return {"terminal": True}

    recipe_id = sub

    # Convenience aliases.
    if recipe_id in {"mod2", "m2", "module2", "module02"}:
        recipe_id = "mod2-proof"
        if arg1.strip().lower() in {"proof", "check"}:
            arg1 = arg2
            arg2 = ""

    recipe = _RECIPES.get(recipe_id)
    if recipe is None:
        send(f"unknown tf recipe: {recipe_id}\n\n{_tf_usage()}", context="tf-unknown", with_menu=True)
        return {"terminal": True}

    # Use current active orch unless user already switched via /use.
    key, entry, _p_args = get_context(None)
    alias = str((entry or {}).get("project_alias", "")).strip() or "-"
    project_root_raw = str((entry or {}).get("project_root", "")).strip()
    if not project_root_raw:
        send("tf error: missing project_root for active orch", context="tf-error", with_menu=True)
        return {"terminal": True}

    if recipe.id == "mod2-proof":
        # mod2-proof is a deterministic local check. It is safe for readonly chats.
        project_root = Path(project_root_raw).expanduser().resolve()

        action = arg1.strip().lower()
        if action in {"tags", "tag", "list-tags", "ls-tags"}:
            tags = _discover_mod2_tags(project_root=project_root, limit=12)
            if not tags:
                send(
                    "mod2-proof tags: (empty)\n"
                    f"- orch: {key} ({alias})\n"
                    "hint: no matching contract-ci summary files found under data/metadata.",
                    context="tf-mod2-tags-empty",
                    with_menu=True,
                )
                return {"terminal": True}

            lines = [
                "mod2-proof tags",
                f"- orch: {key} ({alias})",
                "format: idx) tag | overall_ok | pending | generated_at_utc",
            ]
            for idx, row in enumerate(tags, start=1):
                ok = row.get("overall_ok")
                ok_text = "-" if ok is None else str(ok)
                pending = row.get("pending")
                pending_text = "-" if pending is None else str(pending)
                ts = str(row.get("generated_at_utc") or "-")
                lines.append(f"- {idx}) {row.get('tag')} | overall_ok={ok_text} | pending={pending_text} | {ts}")
            lines.append("run: /tf mod2-proof <tag>  (or /tf mod2-proof latest)")
            send("\n".join(lines), context="tf-mod2-tags", with_menu=True)
            return {"terminal": True}

        if action in {"latest", "newest"}:
            tags = _discover_mod2_tags(project_root=project_root, limit=1)
            tag = str((tags[0] or {}).get("tag", "")).strip() if tags else ""
            tag = tag or recipe.default_tag
        else:
            tag = (arg1 or recipe.default_tag).strip()

        result = _run_mod2_proof_local(project_root=project_root, tag=tag)

        # Anchor docs under control workspace (same as investigations sync).
        try:
            team_dir = Path(str(getattr(args, "team_dir", "") or "")).expanduser().resolve()
            control_root = team_dir.parent.resolve() if str(team_dir) else Path(str(getattr(args, "project_root", ".") or ".")).expanduser().resolve()
        except Exception:
            control_root = Path(str(getattr(args, "project_root", ".") or ".")).expanduser().resolve()

        tf_id = f"TF-M2PROOF-{_sanitize_tf_token(tag)}"
        report_path = _write_mod2_proof_report(control_root=control_root, project_alias=alias, tf_id=tf_id, result=result)
        try:
            rel_report = str(report_path.relative_to(control_root))
        except Exception:
            rel_report = str(report_path)

        verdict = str(result.get("verdict", "")).strip() or "-"
        reason = str(result.get("reason", "")).strip() or "-"
        pending = "-"
        overall_ok = "-"
        must_check_ok = "-"
        for row in result.get("summary") or []:
            s = str(row)
            if s.startswith("autolabel.n_auto_pending="):
                pending = s.split("=", 1)[-1]
            elif s.startswith("contract_ci.status.overall_ok="):
                overall_ok = s.split("=", 1)[-1]
            elif s.startswith("must_check.ok="):
                must_check_ok = s.split("=", 1)[-1]

        send(
            "tf proof (local)\n"
            f"- recipe: {recipe.id}\n"
            f"- tag: {tag}\n"
            f"- orch: {key} ({alias})\n"
            f"- verdict: {verdict} ({reason})\n"
            f"- contract_ci.overall_ok: {overall_ok}\n"
            f"- autolabel.n_auto_pending: {pending}\n"
            f"- must_check.ok: {must_check_ok}\n"
            f"- report: {rel_report}",
            context="tf-local-proof",
            with_menu=True,
        )
        return {"terminal": True}

    if chat_role == "readonly":
        send("permission denied: readonly chat cannot start TF runs. (/tf list is allowed)", context="tf-deny", with_menu=True)
        return {"terminal": True}

    tag = (arg1 or recipe.default_tag).strip()

    # Fallback (future): LLM-backed recipe dispatch.
    prompt = recipe.prompt_template.format(tag=tag)
    send(
        "tf dispatch starting\n"
        f"- recipe: {recipe.id}\n"
        f"- tag: {tag}\n"
        f"- orch: {key} ({alias})\n"
        "note: proof recipes are read-only (they validate existing artifacts).",
        context="tf-start",
        with_menu=True,
    )

    return {
        "terminal": False,
        "cmd": "run",
        "orch_target": key,
        "run_prompt": prompt,
        "run_roles_override": recipe.roles_csv,
        "run_priority_override": recipe.priority,
        "run_force_mode": "dispatch",
        "run_auto_source": f"tf:{recipe.id}",
    }
