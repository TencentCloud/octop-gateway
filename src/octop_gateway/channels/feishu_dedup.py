"""Persistent Feishu message-id dedup and stale-event age checks.

Feishu long connections redeliver events that were not acknowledged. The
in-memory cache on a channel instance disappears on restart and on
re-register, so ids are also stored in a small SQLite file for 24 hours.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEDUP_TTL_SECONDS = 24 * 60 * 60
_DEFAULT_MAX_MSG_AGE_SECONDS = 180.0
_MILLISECOND_THRESHOLD = 10**11

_STORES: dict[str, FeishuDedupStore] = {}
_STORES_LOCK = threading.Lock()


def resolve_dedup_db_path(explicit: str) -> str:
    """Return the SQLite path from config or ``OCTOP_FEISHU_DEDUP_DB``.

    An empty result means this process keeps ids in memory only.
    """
    path = (explicit or "").strip()
    if path:
        return path
    return os.environ.get("OCTOP_FEISHU_DEDUP_DB", "").strip()


def max_message_age_seconds() -> float:
    """Age gate from ``OCTOP_FEISHU_MAX_MSG_AGE_SECONDS``.

    The default is 180 seconds. ``0`` turns the gate off. An invalid value
    falls back to the default.
    """
    raw = os.environ.get("OCTOP_FEISHU_MAX_MSG_AGE_SECONDS", "").strip()
    if not raw:
        return _DEFAULT_MAX_MSG_AGE_SECONDS
    try:
        value = float(raw)
    except ValueError:
        logger.warning("ignoring invalid OCTOP_FEISHU_MAX_MSG_AGE_SECONDS=%r", raw)
        return _DEFAULT_MAX_MSG_AGE_SECONDS
    if value < 0:
        return _DEFAULT_MAX_MSG_AGE_SECONDS
    return value


def message_create_time_seconds(raw: Any) -> float | None:
    """Parse a Feishu ``create_time`` into Unix seconds.

    Feishu sends milliseconds. Values that are already in seconds are accepted.
    Missing or non-positive values return ``None`` so the caller does not
    treat them as ancient messages.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        value = float(int(text))
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    if value >= _MILLISECOND_THRESHOLD:
        return value / 1000.0
    return value


class FeishuDedupStore:
    """SQLite set of recently handled ``(channel_id, message_id)`` pairs."""

    def __init__(self, path: str) -> None:
        self.path = str(Path(path).expanduser())
        self._lock = threading.Lock()
        self._ready = False

    def _connect(self) -> sqlite3.Connection:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=5)
        if not self._ready:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feishu_seen (
                    channel_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    seen_at REAL NOT NULL,
                    PRIMARY KEY (channel_id, message_id)
                )
                """
            )
            conn.commit()
            self._ready = True
        return conn

    def already_seen(self, channel_id: str, message_id: str, *, now: float, ttl_seconds: float) -> bool:
        """Return True when this id was stored within the TTL. New ids are inserted."""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM feishu_seen WHERE seen_at < ?", (now - ttl_seconds,))
                row = conn.execute(
                    "SELECT 1 FROM feishu_seen WHERE channel_id = ? AND message_id = ?",
                    (channel_id, message_id),
                ).fetchone()
                if row is not None:
                    conn.commit()
                    return True
                conn.execute(
                    "INSERT INTO feishu_seen (channel_id, message_id, seen_at) VALUES (?, ?, ?)",
                    (channel_id, message_id, now),
                )
                conn.commit()
                return False
            except sqlite3.Error:
                conn.rollback()
                raise
            finally:
                conn.close()


def open_dedup_store(path: str) -> FeishuDedupStore | None:
    """Open (and cache) a store. Return None when the path cannot be created."""
    resolved = str(Path(path).expanduser())
    with _STORES_LOCK:
        cached = _STORES.get(resolved)
        if cached is not None:
            return cached
        store = FeishuDedupStore(resolved)
        try:
            store._connect().close()
        except (OSError, sqlite3.Error) as exc:
            logger.warning("Feishu dedup store unavailable at %s: %s", resolved, exc)
            return None
        _STORES[resolved] = store
        return store
