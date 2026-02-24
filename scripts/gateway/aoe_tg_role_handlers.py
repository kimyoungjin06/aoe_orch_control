#!/usr/bin/env python3
"""Role mutation handlers for Telegram gateway."""

from typing import Any, Callable, Dict, Optional

def handle_add_role_command(
    *,
    cmd: str,
    args: Any,
    add_role_name: Optional[str],
    add_role_provider: Optional[str],
    add_role_launch: Optional[str],
    add_role_spawn: bool,
    send: Callable[..., bool],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    run_aoe_add_role: Callable[..., str],
) -> bool:
    if cmd != "add-role":
        return False

    if not add_role_name:
        send("usage: aoe add-role <Role> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]", context="add-role usage")
        return True
    key, _entry, p_args = get_context(None)
    if args.dry_run:
        send(
            "[DRY-RUN] add-role\n"
            f"- orch: {key}\n"
            f"- role: {add_role_name}\n"
            f"- provider: {add_role_provider or 'codex'}\n"
            f"- launch: {add_role_launch or '(default)'}\n"
            f"- spawn: {'yes' if add_role_spawn else 'no'}",
            context="add-role dry-run",
        )
        return True
    result = run_aoe_add_role(
        p_args,
        role=add_role_name,
        provider=add_role_provider,
        launch=add_role_launch,
        spawn=add_role_spawn,
    )
    send(f"orch: {key}\n{result}", context="add-role")
    return True


