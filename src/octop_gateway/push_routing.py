"""Shared helpers for proactive (bot-initiated) push routing."""

from __future__ import annotations

from typing import Any

# Fields that tie a send to a single inbound turn — must not survive cron/webhook push.
EPHEMERAL_PUSH_META = frozenset(
    {
        "msg_id",
        "message_id",
        "_frame",
        "_ws_client",
        "response_url",
        "context_token",
        "webhook_url",
    },
)


def alias_subject_fields(
    meta: dict[str, Any],
    subject_id: str,
    *field_names: str,
) -> dict[str, Any]:
    """Backfill routing metadata keys from ``subject_id`` when absent."""
    if not subject_id:
        return meta
    for name in field_names:
        meta.setdefault(name, subject_id)
    return meta


def strip_ephemeral_push_meta(meta: dict[str, Any]) -> dict[str, Any]:
    """Remove passive-reply-only fields so proactive sends use the right path."""
    for key in EPHEMERAL_PUSH_META:
        meta.pop(key, None)
    return meta
