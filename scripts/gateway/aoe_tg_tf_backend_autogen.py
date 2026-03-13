#!/usr/bin/env python3
"""Read-only AutoGen Core TF backend.

This backend is intentionally conservative:

- experiment/sandbox use only
- read-only source inspection
- no queue, proposal, or syncback mutation
- output normalized to the current gateway result contract
"""

from __future__ import annotations

import asyncio
import importlib.util
import re
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from aoe_tg_tf_backend import TFBackendAdapter, TFBackendAvailability, TFBackendDeps, TFBackendRequest
from aoe_tg_tf_event_schema import normalize_followup_proposals, normalize_runtime_events
from aoe_tg_tf_exec import create_request_id, parse_roles_csv


READONLY_ANALYST_ROLE = "Codex-Analyst"
READONLY_WRITER_ROLE = "Codex-Writer"
READONLY_REVIEWER_ROLE = "Codex-Reviewer"


def autogen_core_installed() -> bool:
    return importlib.util.find_spec("autogen_core") is not None


def autogen_core_version() -> str:
    for name in ("autogen-core", "autogen_core"):
        try:
            return str(metadata.version(name))
        except Exception:
            continue
    return ""


def _load_autogen_core() -> Tuple[Any, Any, Any, Any, Any]:
    from autogen_core import AgentId, MessageContext, RoutedAgent, SingleThreadedAgentRuntime, rpc

    return AgentId, MessageContext, RoutedAgent, SingleThreadedAgentRuntime, rpc


def _priority_rank(priority: str) -> int:
    token = str(priority or "").strip().upper()
    if token == "P1":
        return 1
    if token == "P2":
        return 2
    if token == "P3":
        return 3
    return 9


def _resolve_readonly_roles(raw: str) -> Dict[str, List[str]]:
    requested = parse_roles_csv(raw)
    primary_role = READONLY_WRITER_ROLE if READONLY_WRITER_ROLE in requested else READONLY_ANALYST_ROLE
    executed: List[str] = [primary_role, READONLY_REVIEWER_ROLE]
    dropped = [role for role in requested if role not in executed]
    return {
        "requested": requested,
        "executed": executed,
        "dropped": dropped,
    }


def _extract_include_path(path: Path) -> Optional[Path]:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped.startswith("@include "):
                continue
            target = stripped.split(None, 1)[1].strip()
            if not target:
                continue
            resolved = (path.parent / target).expanduser().resolve()
            return resolved
    except Exception:
        return None
    return None


def _candidate_source_paths(project_root: Path, team_dir: Path) -> List[Path]:
    candidates: List[Path] = []
    team_todo = (team_dir / "AOE_TODO.md").resolve()
    include_target = _extract_include_path(team_todo) if team_todo.exists() else None
    for item in (
        include_target,
        (project_root / "TODO.md").resolve(),
        team_todo if team_todo.exists() else None,
    ):
        if item is None:
            continue
        if not item.exists():
            continue
        if item in candidates:
            continue
        candidates.append(item)
    return candidates


def _extract_open_checkbox_items(text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        match = re.match(r"^\s*[-*]\s+\[\s\]\s+(?:(P[123])\s*:\s*)?(.+?)\s*$", line)
        if not match:
            continue
        priority = (match.group(1) or "P2").strip().upper()
        body = match.group(2).strip()
        if not body:
            continue
        rows.append(
            {
                "priority": priority,
                "body": body,
                "line": lineno,
                "section": "Tasks",
                "source_reason": "open_checkbox",
            }
        )
    return rows


def _extract_section_items(text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    current_section = ""
    capture = False
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        heading = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading:
            current_section = heading.group(2).strip()
            token = current_section.lower()
            capture = any(key in token for key in ("task", "todo", "next", "follow", "action"))
            continue
        if not capture:
            continue
        bullet = re.match(r"^\s*(?:[-*]|\d+\.)\s+(?:(P[123])\s*:\s*)?(.+?)\s*$", line)
        if not bullet:
            continue
        body = bullet.group(2).strip()
        if not body:
            continue
        if body.lower().startswith(("purpose:", "update:", "audit memo:", "closed:", "canonical sync")):
            continue
        rows.append(
            {
                "priority": (bullet.group(1) or "P2").strip().upper(),
                "body": body,
                "line": lineno,
                "section": current_section or "Notes",
                "source_reason": "task_section_bullet",
            }
        )
    return rows


def _extract_fallback_note_items(text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("@include "):
            continue
        if len(stripped) < 16:
            continue
        rows.append(
            {
                "priority": "P3",
                "body": stripped[:220],
                "line": lineno,
                "section": "Document",
                "source_reason": "fallback_note",
            }
        )
        if len(rows) >= 5:
            break
    return rows


def _load_actionable_items(project_root: Path, team_dir: Path) -> Dict[str, Any]:
    sources = _candidate_source_paths(project_root, team_dir)
    if not sources:
        return {
            "source_path": "",
            "source_kind": "missing",
            "items": [],
        }

    for source_path in sources:
        try:
            text = source_path.read_text(encoding="utf-8")
        except Exception:
            continue
        items = _extract_open_checkbox_items(text)
        source_kind = "todo_checkboxes"
        if not items:
            items = _extract_section_items(text)
            source_kind = "task_sections"
        if not items:
            items = _extract_fallback_note_items(text)
            source_kind = "document_notes"
        if items:
            items = sorted(items, key=lambda row: (_priority_rank(row.get("priority", "")), int(row.get("line", 0))))
            return {
                "source_path": str(source_path),
                "source_kind": source_kind,
                "items": items[:10],
            }

    return {
        "source_path": str(sources[0]),
        "source_kind": "empty",
        "items": [],
    }


def _format_item_lines(items: Sequence[Dict[str, Any]], *, limit: int = 3) -> List[str]:
    lines: List[str] = []
    for index, row in enumerate(items[:limit], start=1):
        priority = str(row.get("priority", "P2")).strip().upper() or "P2"
        body = str(row.get("body", "")).strip()
        if not body:
            continue
        lines.append(f"{index}. {priority}: {body}")
    return lines


@dataclass
class TFRunMessage:
    request_id: str
    project_key: str
    project_root: str
    prompt: str
    source_path: str
    source_kind: str
    requested_roles: List[str]
    executed_roles: List[str]
    dropped_roles: List[str]
    todo_items: List[Dict[str, Any]]


@dataclass
class AnalysisRequest:
    request_id: str
    project_key: str
    prompt: str
    source_path: str
    source_kind: str
    requested_roles: List[str]
    executed_roles: List[str]
    dropped_roles: List[str]
    todo_items: List[Dict[str, Any]]


@dataclass
class AnalysisResponse:
    role: str
    body: str
    top_items: List[str]
    item_count: int


@dataclass
class ReviewRequest:
    request_id: str
    project_key: str
    source_path: str
    source_kind: str
    requested_roles: List[str]
    executed_roles: List[str]
    analysis_body: str
    top_items: List[str]
    item_count: int


@dataclass
class ReviewResponse:
    role: str
    verdict: str
    body: str
    success: bool


@dataclass
class OrchestratorResponse:
    request_id: str
    status: str
    complete: bool
    verdict: str
    replies: List[Dict[str, Any]]
    role_states: List[Dict[str, Any]]
    counts: Dict[str, int]
    done_roles: List[str]
    failed_roles: List[str]
    pending_roles: List[str]
    followup_proposals: List[Dict[str, Any]]


class _RuntimeEventRecorder:
    def __init__(self, now_iso: Any) -> None:
        self._now_iso = now_iso
        self.rows: List[Dict[str, Any]] = []

    def add(
        self,
        *,
        source: str,
        stage: str,
        kind: str,
        status: str,
        summary: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.rows.append(
            {
                "ts": self._now_iso(),
                "backend": "autogen_core",
                "source": source,
                "stage": stage,
                "kind": kind,
                "status": status,
                "summary": summary,
                "payload": dict(payload or {}),
            }
        )


def _build_runtime_events(recorder: _RuntimeEventRecorder, *, now_iso: Any) -> List[Dict[str, Any]]:
    return normalize_runtime_events(
        recorder.rows,
        default_backend="autogen_core",
        default_source="tf_orchestrator",
        now_iso=now_iso,
    )


def _build_followup_proposals(
    *,
    request_id: str,
    source_todo_id: str,
    primary_role: str,
    top_items: Sequence[str],
    source_path: str,
    source_kind: str,
) -> List[Dict[str, Any]]:
    if not top_items:
        return []
    kind = "handoff" if primary_role == READONLY_WRITER_ROLE else "followup"
    reason_prefix = "writer handoff surfaced" if primary_role == READONLY_WRITER_ROLE else "read-only analysis surfaced"
    raw_rows: List[Dict[str, Any]] = []
    for line in list(top_items)[:2]:
        raw_rows.append(
            {
                "summary": line,
                "priority": "P1" if line.startswith("1.") else "P2",
                "kind": kind,
                "reason": f"{reason_prefix} this canonical backlog item from {source_kind or 'todo source'}",
                "source_request_id": request_id,
                "source_todo_id": source_todo_id,
                "confidence": 0.78 if kind == "handoff" else 0.74,
                "source_file": source_path,
                "source_section": "Tasks",
                "source_reason": "autogen_readonly_runtime",
            }
        )
    return normalize_followup_proposals(
        raw_rows,
        default_source_request_id=request_id,
        default_source_todo_id=source_todo_id,
    )


def _build_request_id(request: TFBackendRequest) -> str:
    raw = str(request.metadata.get("request_id", "") or "").strip()
    return raw or create_request_id()


async def _run_autogen_runtime(request: TFBackendRequest, deps: TFBackendDeps) -> Dict[str, Any]:
    AgentId, MessageContext, RoutedAgent, SingleThreadedAgentRuntime, rpc = _load_autogen_core()
    globals()["MessageContext"] = MessageContext

    project_root = Path(str(request.args.project_root)).expanduser().resolve()
    team_dir = Path(str(request.args.team_dir)).expanduser().resolve()
    project_key = str(getattr(request.args, "_aoe_project_key", "") or "").strip() or project_root.name
    request_id = _build_request_id(request)
    roles = _resolve_readonly_roles(request.roles_override)
    load_result = _load_actionable_items(project_root, team_dir)
    source_path = str(load_result.get("source_path", "") or "")
    source_kind = str(load_result.get("source_kind", "") or "missing")
    todo_items = list(load_result.get("items") or [])
    source_todo_id = str(request.metadata.get("source_todo_id", "") or "").strip()
    recorder = _RuntimeEventRecorder(deps.now_iso)
    recorder.add(
        source="tf_orchestrator",
        stage="request.accepted",
        kind="lifecycle",
        status="info",
        summary="accepted sandbox read-only request",
        payload={"project_key": project_key, "source_path": source_path, "source_kind": source_kind},
    )
    recorder.add(
        source="tf_orchestrator",
        stage="roles.resolved",
        kind="dispatch",
        status="success",
        summary="resolved sandbox role subset",
        payload={
            "requested_roles": roles["requested"],
            "executed_roles": roles["executed"],
            "dropped_roles": roles["dropped"],
        },
    )

    primary_role = roles["executed"][0] if roles["executed"] else READONLY_ANALYST_ROLE
    primary_agent_type = "writer" if primary_role == READONLY_WRITER_ROLE else "analyst"

    class AnalystAgent(RoutedAgent):
        def __init__(self) -> None:
            super().__init__("sandbox analyst")

        @rpc
        async def handle_analysis(self, message: AnalysisRequest, ctx: MessageContext) -> AnalysisResponse:
            top_items = _format_item_lines(message.todo_items)
            if top_items:
                body = "\n".join(
                    [
                        "Codex-Analyst summary",
                        f"- source: {message.source_path or '(missing source)'} ({message.source_kind})",
                        f"- extracted actionable items: {len(message.todo_items)}",
                        f"- user request: {message.prompt}",
                        "- top backlog candidates:",
                        *[f"  {line}" for line in top_items],
                    ]
                )
            else:
                body = "\n".join(
                    [
                        "Codex-Analyst summary",
                        f"- source: {message.source_path or '(missing source)'} ({message.source_kind})",
                        "- extracted actionable items: 0",
                        f"- user request: {message.prompt}",
                        "- no actionable backlog items were extracted from the current canonical source.",
                    ]
                )
            recorder.add(
                source="analyst",
                stage="dispatch.submitted",
                kind="dispatch",
                status="success",
                summary="analyst summarized read-only backlog source",
                payload={"item_count": len(message.todo_items), "source_kind": message.source_kind},
            )
            return AnalysisResponse(
                role=READONLY_ANALYST_ROLE,
                body=body,
                top_items=top_items,
                item_count=len(message.todo_items),
            )

    class WriterAgent(RoutedAgent):
        def __init__(self) -> None:
            super().__init__("sandbox writer")

        @rpc
        async def handle_analysis(self, message: AnalysisRequest, ctx: MessageContext) -> AnalysisResponse:
            top_items = _format_item_lines(message.todo_items)
            intro = "Codex-Writer handoff"
            if top_items:
                body = "\n".join(
                    [
                        intro,
                        f"- source: {message.source_path or '(missing source)'} ({message.source_kind})",
                        f"- operator request: {message.prompt}",
                        f"- actionable backlog items extracted: {len(message.todo_items)}",
                        "- proposed handoff summary:",
                        f"  Focus first on {top_items[0]}",
                        *[f"  backlog: {line}" for line in top_items[1:3]],
                    ]
                )
            else:
                body = "\n".join(
                    [
                        intro,
                        f"- source: {message.source_path or '(missing source)'} ({message.source_kind})",
                        f"- operator request: {message.prompt}",
                        "- no actionable backlog items were extracted from the current canonical source.",
                    ]
                )
            recorder.add(
                source="writer",
                stage="dispatch.submitted",
                kind="dispatch",
                status="success",
                summary="writer produced read-only handoff summary from canonical backlog",
                payload={"item_count": len(message.todo_items), "source_kind": message.source_kind},
            )
            return AnalysisResponse(
                role=READONLY_WRITER_ROLE,
                body=body,
                top_items=top_items,
                item_count=len(message.todo_items),
            )

    class CodexReviewerAgent(RoutedAgent):
        def __init__(self) -> None:
            super().__init__("sandbox reviewer")

        @rpc
        async def handle_review(self, message: ReviewRequest, ctx: MessageContext) -> ReviewResponse:
            success = bool(message.item_count > 0 and message.top_items)
            verdict = "success" if success else "fail"
            lines = [
                f"{READONLY_REVIEWER_ROLE} verdict: {verdict}",
                f"- source: {message.source_path or '(missing source)'} ({message.source_kind})",
                f"- requested roles: {', '.join(message.requested_roles) if message.requested_roles else '(default)'}",
                f"- executed sandbox roles: {', '.join(message.executed_roles)}",
            ]
            if message.top_items:
                lines.append(f"- usable backlog candidates: {len(message.top_items)}")
                lines.append(f"- recommended first focus: {message.top_items[0]}")
            else:
                lines.append("- operator follow-up: inspect canonical TODO source before enabling broader pilot scope.")
            recorder.add(
                source="reviewer",
                stage="verdict.emitted",
                kind="verdict",
                status="success" if success else "warning",
                summary="reviewer emitted sandbox verdict",
                payload={"verdict": verdict, "item_count": message.item_count},
            )
            return ReviewResponse(
                role=READONLY_REVIEWER_ROLE,
                verdict=verdict,
                body="\n".join(lines),
                success=success,
            )

    class OrchestratorAgent(RoutedAgent):
        def __init__(self) -> None:
            super().__init__("sandbox tf orchestrator")

        @rpc
        async def handle_run(self, message: TFRunMessage, ctx: MessageContext) -> OrchestratorResponse:
            analysis = await self.send_message(
                AnalysisRequest(
                    request_id=message.request_id,
                    project_key=message.project_key,
                    prompt=message.prompt,
                    source_path=message.source_path,
                    source_kind=message.source_kind,
                    requested_roles=message.requested_roles,
                    executed_roles=message.executed_roles,
                    dropped_roles=message.dropped_roles,
                    todo_items=message.todo_items,
                ),
                AgentId(primary_agent_type, "default"),
            )
            review = await self.send_message(
                ReviewRequest(
                    request_id=message.request_id,
                    project_key=message.project_key,
                    source_path=message.source_path,
                    source_kind=message.source_kind,
                    requested_roles=message.requested_roles,
                    executed_roles=message.executed_roles,
                    analysis_body=analysis.body,
                    top_items=analysis.top_items,
                    item_count=analysis.item_count,
                ),
                AgentId("reviewer", "default"),
            )
            followup_proposals = _build_followup_proposals(
                request_id=message.request_id,
                source_todo_id=source_todo_id,
                primary_role=analysis.role,
                top_items=analysis.top_items,
                source_path=message.source_path,
                source_kind=message.source_kind,
            )
            if followup_proposals:
                recorder.add(
                    source="tf_orchestrator",
                    stage="proposals.emitted",
                    kind="proposal",
                    status="info",
                    summary="emitted backend-native follow-up proposals",
                    payload={"proposal_count": len(followup_proposals), "kind": followup_proposals[0].get("kind", "followup")},
                )
            recorder.add(
                source="tf_orchestrator",
                stage="runtime.completed",
                kind="lifecycle",
                status="success" if review.success else "warning",
                summary="completed sandbox read-only TF run",
                payload={
                    "reply_count": 2,
                    "verdict": review.verdict,
                    "item_count": analysis.item_count,
                },
            )
            replies = [
                {"role": analysis.role, "body": analysis.body},
                {"role": review.role, "body": review.body},
            ]
            role_states = [
                {"role": analysis.role, "status": "done", "summary": analysis.body[:240]},
                {"role": review.role, "status": "done", "summary": review.body[:240], "verdict": review.verdict},
            ]
            return OrchestratorResponse(
                request_id=message.request_id,
                status="completed",
                complete=True,
                verdict=review.verdict,
                replies=replies,
                role_states=role_states,
                counts={"assignments": len(message.executed_roles), "replies": len(replies)},
                done_roles=[analysis.role, review.role],
                failed_roles=[],
                pending_roles=[],
                followup_proposals=followup_proposals,
            )

    runtime = SingleThreadedAgentRuntime()
    await AnalystAgent.register(runtime, "analyst", lambda: AnalystAgent())
    await WriterAgent.register(runtime, "writer", lambda: WriterAgent())
    await CodexReviewerAgent.register(runtime, "reviewer", lambda: CodexReviewerAgent())
    await OrchestratorAgent.register(runtime, "tf_orchestrator", lambda: OrchestratorAgent())
    runtime.start()
    recorder.add(
        source="autogen_runtime",
        stage="runtime.started",
        kind="lifecycle",
        status="info",
        summary="bootstrapped sandbox AutoGen runtime",
        payload={"runtime": "SingleThreadedAgentRuntime"},
    )
    try:
        response = await runtime.send_message(
            TFRunMessage(
                request_id=request_id,
                project_key=project_key,
                project_root=str(project_root),
                prompt=request.prompt,
                source_path=source_path,
                source_kind=source_kind,
                requested_roles=roles["requested"],
                executed_roles=roles["executed"],
                dropped_roles=roles["dropped"],
                todo_items=todo_items,
            ),
            AgentId("tf_orchestrator", "default"),
        )
    finally:
        await runtime.stop_when_idle()

    runtime_events = _build_runtime_events(recorder, now_iso=deps.now_iso)
    return {
        "request_id": response.request_id,
        "status": response.status,
        "complete": response.complete,
        "verdict": response.verdict,
        "counts": dict(response.counts),
        "role_states": list(response.role_states),
        "replies": list(response.replies),
        "done_roles": list(response.done_roles),
        "failed_roles": list(response.failed_roles),
        "pending_roles": list(response.pending_roles),
        "requested_roles": list(roles["requested"]),
        "executed_roles": list(roles["executed"]),
        "dropped_roles": list(roles["dropped"]),
        "runtime_events": runtime_events,
        "followup_proposals": list(response.followup_proposals),
        "artifacts": [
            {
                "kind": "backlog_snapshot",
                "source_path": source_path,
                "source_kind": source_kind,
                "item_count": len(todo_items),
            }
        ],
    }


class AutoGenCoreTFBackend(TFBackendAdapter):
    backend_name = "autogen_core"

    def availability(self) -> TFBackendAvailability:
        if autogen_core_installed():
            version = autogen_core_version()
            detail = f"installed{f' ({version})' if version else ''}"
            return TFBackendAvailability(True, detail)
        return TFBackendAvailability(
            False,
            "autogen_core is not installed; sandbox backend cannot run",
        )

    def run(self, request: TFBackendRequest, deps: TFBackendDeps) -> Dict[str, Any]:
        return asyncio.run(_run_autogen_runtime(request, deps))


def autogen_core_backend() -> AutoGenCoreTFBackend:
    return AutoGenCoreTFBackend()
