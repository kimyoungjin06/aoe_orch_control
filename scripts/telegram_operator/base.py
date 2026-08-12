"""Adapter result envelope and choice-surface helpers.

Shared plumbing used by every command path (chat, feedback, dispatch, plan
session, projection): result_base builds the read-only adapter result skeleton
and attach_choice_surface attaches the reply keyboard and choice-surface
contract. Kept in a small base module so the plan-session workflow and the main
script share them without a circular import.
"""

from __future__ import annotations

import argparse
from typing import Any

from .common import utc_now
from .effects import no_effect
from .rendering import choice_keyboard, choice_surface_contract
from .schemas import FORBIDDEN_REMOTE_INTENTS, RESULT_SCHEMA


def result_base(args: argparse.Namespace, config: dict[str, Any], mode: str) -> dict[str, Any]:
    return {
        "schema": RESULT_SCHEMA,
        "generated_at": utc_now(),
        "mode": mode,
        "profile": args.profile,
        "target_chat_id_hash": config.get("target_chat_id_hash"),
        "chat_allowlist_configured": bool(config.get("chat_allowlist_configured")),
        "user_allowlist_configured": bool(config.get("user_allowlist_configured")),
        "read_only": True,
        "mutation_authorized": False,
        "approval_authorized": False,
        "approval_resolution_authorized": False,
        "authority_domain": "none",
        "effect": no_effect(),
        "forbidden_remote_intents": list(FORBIDDEN_REMOTE_INTENTS),
    }


def attach_choice_surface(result: dict[str, Any], context: dict[str, Any] | None) -> None:
    reply_markup = choice_keyboard(context)
    result["reply_markup_preview"] = reply_markup
    result["choice_surface_contract"] = choice_surface_contract(reply_markup, context)
    if isinstance(context, dict):
        result["interaction_context"] = context
