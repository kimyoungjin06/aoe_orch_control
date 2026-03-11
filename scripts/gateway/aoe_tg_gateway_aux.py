#!/usr/bin/env python3
"""Gateway replay/metrics/error helpers extracted from the monolith."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


def handle_replay_command(
    *,
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    target: str,
    send: Any,
    log_event: Any,
    load_state: Callable[[Path], Dict[str, Any]],
    save_state: Callable[[Path, Dict[str, Any]], Any],
    normalize_failed_queue: Callable[[Any, int], Any],
    failed_queue_keep_limit: Callable[[], int],
    state_failed_queue_key: str,
    summarize_failed_queue: Callable[[Dict[str, Any], str], str],
    purge_failed_queue_for_chat: Callable[[Dict[str, Any], str], int],
    resolve_failed_queue_item: Callable[[Dict[str, Any], str, str], Tuple[Optional[Dict[str, Any]], str]],
    format_failed_queue_item_detail: Callable[[Dict[str, Any]], str],
    remove_failed_queue_item: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    parse_command: Callable[[str], Tuple[str, str]],
    handle_text_message: Callable[..., Any],
    preferred_command_prefix: Callable[[], str],
    replay_usage: str,
) -> bool:
    loop_state = load_state(args.state_file)
    loop_state[state_failed_queue_key] = normalize_failed_queue(
        loop_state.get(state_failed_queue_key),
        failed_queue_keep_limit(),
    )
    save_state(args.state_file, loop_state)
    pick = str(target or "").strip()
    pick_lower = pick.lower()
    if pick_lower in {"", "list", "ls", "status"}:
        send(summarize_failed_queue(loop_state, chat_id), context="replay-list", with_menu=True)
        return True

    if pick_lower == "purge":
        removed = purge_failed_queue_for_chat(loop_state, chat_id)
        save_state(args.state_file, loop_state)
        send(
            f"replay purge done\n- removed: {removed}\n- chat: {chat_id}",
            context="replay-purge",
            with_menu=True,
        )
        log_event(
            event="replay_purged",
            stage="intake",
            status="accepted",
            detail=f"chat={chat_id} removed={removed}",
        )
        return True

    show_target = ""
    parts = pick.split(None, 1)
    action = str(parts[0]).strip().lower() if parts else ""
    if action == "show":
        if len(parts) < 2 or not str(parts[1]).strip():
            send(replay_usage, context="replay-usage", with_menu=True)
            return True
        show_target = str(parts[1]).strip()

    resolve_target = show_target or pick
    item, err = resolve_failed_queue_item(loop_state, chat_id, resolve_target)
    if item is None:
        send(f"{err}\n{summarize_failed_queue(loop_state, chat_id)}", context="replay-miss", with_menu=True)
        return True

    if show_target:
        send(format_failed_queue_item_detail(item), context="replay-show", with_menu=True)
        return True

    removed = remove_failed_queue_item(loop_state, str(item.get("id", "")).strip()) or item
    save_state(args.state_file, loop_state)

    replay_text = str(removed.get("text", "")).strip()
    if not replay_text:
        send("replay item has empty text", context="replay-empty", with_menu=True)
        return True
    replay_cmd, _ = parse_command(replay_text)
    if str(replay_cmd or "").strip().lower() == "replay":
        send("replay blocked: nested /replay payload", context="replay-blocked", with_menu=True)
        return True

    replay_id = str(removed.get("id", "")).strip() or "n/a"
    send(
        f"replay start\n- id: {replay_id}\n- source_cmd: {removed.get('cmd') or '-'}\n- source_error: {removed.get('error_code') or '-'}",
        context="replay-start",
    )
    log_event(
        event="replay_started",
        stage="intake",
        status="accepted",
        detail=f"id={replay_id} source_cmd={removed.get('cmd') or '-'} source_error={removed.get('error_code') or '-'}",
    )
    handle_text_message(args, token, chat_id, replay_text, trace_id=f"replay-{replay_id}")
    return True


def summarize_gateway_metrics(
    team_dir: Path,
    project_name: str,
    hours: int = 24,
    state_file: Optional[Any] = None,
    *,
    summarize_gateway_poll_state: Callable[[Any], str],
    parse_iso_ts: Callable[[str], Any],
    percentile: Callable[[List[int], float], int],
    error_internal: str,
) -> str:
    cap_hours = max(1, min(168, int(hours or 24)))
    poll_state_path = state_file if state_file is not None else (team_dir / "telegram_gateway_state.json")
    poll_summary = summarize_gateway_poll_state(poll_state_path, project_name=project_name)
    path = team_dir / "logs" / "gateway_events.jsonl"
    if not path.exists():
        return f"orch: {project_name}\nmetrics: no data file\nwindow_hours: {cap_hours}\n{poll_summary}"

    cutoff = datetime.now(timezone.utc) - timedelta(hours=cap_hours)
    total = 0
    incoming = 0
    accepted = 0
    rejected = 0
    sent_ok = 0
    sent_fail = 0
    dispatch_done = 0
    direct_done = 0
    errors = 0
    error_codes: Dict[str, int] = {}
    latencies: List[int] = []
    trace_state: Dict[str, Dict[str, bool]] = {}

    def touch_trace(trace: str) -> Optional[Dict[str, bool]]:
        token = str(trace or "").strip()
        if not token:
            return None
        row = trace_state.get(token)
        if row is None:
            row = {"accepted": False, "success": False, "failed": False}
            trace_state[token] = row
        return row

    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    row = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(row, dict):
                    continue
                ts = parse_iso_ts(str(row.get("timestamp", "")))
                if ts is None:
                    continue
                if ts.astimezone(timezone.utc) < cutoff:
                    continue

                total += 1
                event = str(row.get("event", "")).strip()
                status = str(row.get("status", "")).strip().lower()
                trace_id = str(row.get("trace_id", "")).strip()
                trace = touch_trace(trace_id)
                if event == "incoming_message":
                    incoming += 1
                elif event == "command_resolved":
                    if status == "accepted":
                        accepted += 1
                        if trace is not None:
                            trace["accepted"] = True
                elif event == "input_rejected":
                    rejected += 1
                elif event == "send_message":
                    if status == "sent":
                        sent_ok += 1
                        if trace is not None:
                            trace["success"] = True
                    else:
                        sent_fail += 1
                        if trace is not None:
                            trace["failed"] = True
                elif event == "dispatch_completed":
                    dispatch_done += 1
                    if trace is not None:
                        trace["success"] = True
                elif event == "direct_reply":
                    direct_done += 1
                    if trace is not None:
                        trace["success"] = True
                elif event == "dispatch_result":
                    if status == "failed":
                        if trace is not None:
                            trace["failed"] = True
                    else:
                        if trace is not None:
                            trace["success"] = True
                elif event == "handler_error":
                    errors += 1
                    code = str(row.get("error_code", "")).strip() or error_internal
                    error_codes[code] = error_codes.get(code, 0) + 1
                    if trace is not None:
                        trace["failed"] = True

                try:
                    latency = int(row.get("latency_ms", 0) or 0)
                except Exception:
                    latency = 0
                if latency > 0:
                    latencies.append(latency)
    except Exception:
        return f"orch: {project_name}\nmetrics: failed to read log\nwindow_hours: {cap_hours}\n{poll_summary}"

    send_total = sent_ok + sent_fail
    send_success_rate = (100.0 * sent_ok / send_total) if send_total > 0 else 0.0
    accepted_traces = [v for v in trace_state.values() if bool(v.get("accepted"))]
    cmd_success = 0
    cmd_failed = 0
    cmd_pending = 0
    for row in accepted_traces:
        failed = bool(row.get("failed"))
        success = bool(row.get("success"))
        if failed:
            cmd_failed += 1
        elif success:
            cmd_success += 1
        else:
            cmd_pending += 1
    cmd_done = cmd_success + cmd_failed
    cmd_success_rate = (100.0 * cmd_success / cmd_done) if cmd_done > 0 else 0.0

    p50 = percentile(latencies, 0.50)
    p95 = percentile(latencies, 0.95)

    lines = [
        f"orch: {project_name}",
        f"window_hours: {cap_hours}",
        f"events: total={total} incoming={incoming} accepted={accepted} rejected={rejected}",
        f"commands: success={cmd_success} failed={cmd_failed} pending={cmd_pending} success_rate={cmd_success_rate:.1f}%",
        f"send: ok={sent_ok} fail={sent_fail} success_rate={send_success_rate:.1f}%",
        f"completion: dispatch={dispatch_done} direct={direct_done} errors={errors}",
        f"latency_ms: p50={p50} p95={p95} samples={len(latencies)}",
    ]
    if error_codes:
        rows = ", ".join(f"{k}={v}" for k, v in sorted(error_codes.items()))
        lines.append(f"error_codes: {rows}")
    lines.append(poll_summary)
    return "\n".join(lines)


def classify_handler_error(
    err: Exception,
    *,
    error_timeout: str,
    error_command: str,
    error_gate: str,
    error_auth: str,
    error_request: str,
    error_telegram: str,
    error_orch: str,
    error_internal: str,
) -> Tuple[str, str, str]:
    if isinstance(err, subprocess.TimeoutExpired):
        return (
            error_timeout,
            "요청 처리 시간이 제한을 초과했습니다.",
            "/task 또는 /check로 진행 상태를 확인하세요.",
        )

    msg = str(err or "").strip()
    low = msg.lower()
    if (
        ("usage:" in low)
        or ("unknown option" in low)
        or ("unknown command" in low)
        or ("invalid cli format" in low)
        or ("invalid priority" in low)
        or ("must be integer" in low)
        or ("unknown orch project" in low)
        or ("unknown chat alias" in low)
        or ("chat target must be" in low)
    ):
        return (error_command, "명령 형식이 올바르지 않습니다.", "/help로 명령 예시를 확인하세요.")
    if "plan gate blocked" in low or "critic" in low:
        return (error_gate, "계획 검증 게이트에서 차단되었습니다.", "요청 범위를 좁혀 /dispatch로 다시 실행하세요.")
    if "verifier gate" in low:
        return (error_gate, "검증 역할(verifier) 요건이 충족되지 않았습니다.", "/status로 역할 구성을 확인하세요.")
    if "permission denied" in low or "unauthorized" in low:
        return (error_auth, "권한이 없습니다.", "/whoami로 현재 chat 권한을 확인하세요.")
    if "aoe-team request failed" in low or "request returned non-json" in low:
        return (error_request, "요청 상태를 조회하지 못했습니다.", "잠시 후 /check 또는 /task를 다시 실행하세요.")
    if "telegram api" in low or "sendmessage failed" in low:
        return (error_telegram, "텔레그램 전송 과정에서 오류가 발생했습니다.", "잠시 후 같은 명령을 다시 실행하세요.")
    if "aoe-orch run failed" in low or "aoe-orch" in low:
        return (error_orch, "오케스트레이터 실행 중 오류가 발생했습니다.", "/status로 시스템 상태를 확인하세요.")
    return (error_internal, "내부 처리 중 오류가 발생했습니다.", "/help 또는 /status로 상태를 확인하세요.")


def format_error_message(
    error_code: str,
    user_message: str,
    next_step: str,
    detail: str = "",
    *,
    mask_sensitive_text: Callable[[str], str],
) -> str:
    lines = [
        f"error_code: {error_code}",
        user_message,
    ]
    token = mask_sensitive_text(str(detail or "").strip())
    if token:
        lines.append(f"detail: {token[:180]}")
    lines.append(f"next: {next_step}")
    return "\n".join(lines)
