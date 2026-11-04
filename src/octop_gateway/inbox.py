"""Durable inbound event storage for at-least-once channel transports."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast

InboxStatus = Literal[
    "pending",
    "processing",
    "waiting",
    "recovery_required",
    "completed",
    "ignored",
    "failed",
    "cancelled",
]
FinalStatus = Literal["completed", "ignored", "failed", "cancelled"]

_FINAL_STATUSES: tuple[FinalStatus, ...] = ("completed", "ignored", "failed", "cancelled")
_PRUNABLE_STATUSES = ("completed", "ignored", "cancelled")
_SCHEMA_LOCK = threading.Lock()


@dataclass(frozen=True)
class InboxEntry:
    """One accepted inbound event and its local processing state."""

    message_id: str
    payload: dict[str, Any]
    status: InboxStatus
    created_at: float
    updated_at: float


class InboxStore(Protocol):
    """Persistence contract for accepting and recovering inbound events."""

    def accept(self, scope: str, message_id: str, payload: dict[str, Any]) -> bool:
        """Persist an event once, returning True only for the first acceptance."""
        ...

    def pending(self, scope: str, limit: int = 100, offset: int = 0) -> list[InboxEntry]:
        """Return pending events in acceptance order."""
        ...

    def claim(self, scope: str, message_id: str, *, waiting: bool = False) -> bool:
        """Atomically reserve pending work, optionally waiting for a session lock."""
        ...

    def begin(self, scope: str, message_id: str) -> bool:
        """Move a waiting receipt to processing once its session is available."""
        ...

    def release(self, scope: str, message_id: str) -> None:
        """Release work that never entered processing back to pending."""
        ...

    def finish(self, scope: str, message_id: str, status: FinalStatus) -> None:
        """Finish a processing event with an explicit terminal status."""
        ...

    def recover(self, scope: str) -> int:
        """Release waiting work and quarantine interrupted processor work for reconciliation."""
        ...

    def entries(self, scope: str, status: str) -> list[InboxEntry]:
        """Return events in a given state in acceptance order."""
        ...

    def resolve(self, scope: str, message_id: str, *, retry: bool) -> bool:
        """Explicitly retry or complete a failed or interrupted event."""
        ...

    def prune(self, before: float) -> int:
        """Delete old, safely terminal events."""
        ...


class SqliteInbox:
    """SQLite-backed inbox with one short-lived connection per operation."""

    def __init__(self, path: str | Path) -> None:
        raw_path = str(path)
        if raw_path == ":memory:":
            raise ValueError("SqliteInbox requires a file path; ':memory:' cannot persist across connections")
        self._path = Path(path).expanduser()
        self._initialized = False

    def accept(self, scope: str, message_id: str, payload: dict[str, Any]) -> bool:
        self._validate_key(scope, message_id)
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        now = time.time()
        with self._connection() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO inbound_events
                    (scope, message_id, payload_json, status, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?)
                """,
                (scope, message_id, payload_json, now, now),
            )
        return cursor.rowcount == 1

    def pending(self, scope: str, limit: int = 100, offset: int = 0) -> list[InboxEntry]:
        if limit < 1:
            return []
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT message_id, payload_json, status, created_at, updated_at
                FROM inbound_events
                WHERE scope = ? AND status = 'pending'
                ORDER BY rowid
                LIMIT ? OFFSET ?
                """,
                (scope, limit, offset),
            ).fetchall()
        return [self._entry(row) for row in rows]

    def claim(self, scope: str, message_id: str, *, waiting: bool = False) -> bool:
        now = time.time()
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE inbound_events
                SET status = ?, updated_at = ?
                WHERE scope = ? AND message_id = ? AND status = 'pending'
                """,
                ("waiting" if waiting else "processing", now, scope, message_id),
            )
        return cursor.rowcount == 1

    def begin(self, scope: str, message_id: str) -> bool:
        with self._connection() as connection:
            cursor = connection.execute(
                "UPDATE inbound_events SET status = 'processing', updated_at = ? "
                "WHERE scope = ? AND message_id = ? AND status = 'waiting'",
                (time.time(), scope, message_id),
            )
        return cursor.rowcount == 1

    def release(self, scope: str, message_id: str) -> None:
        with self._connection() as connection:
            connection.execute(
                "UPDATE inbound_events SET status = 'pending', updated_at = ? "
                "WHERE scope = ? AND message_id = ? AND status = 'waiting'",
                (time.time(), scope, message_id),
            )

    def finish(self, scope: str, message_id: str, status: FinalStatus) -> None:
        if status not in _FINAL_STATUSES:
            raise ValueError(f"invalid final inbox status: {status}")
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE inbound_events
                SET status = ?, updated_at = ?
                WHERE scope = ? AND message_id = ? AND status = 'processing'
                """,
                (status, time.time(), scope, message_id),
            )

    def recover(self, scope: str) -> int:
        with self._connection() as connection:
            connection.execute(
                "UPDATE inbound_events SET status = 'pending', updated_at = ? WHERE scope = ? AND status = 'waiting'",
                (time.time(), scope),
            )
            cursor = connection.execute(
                """
                UPDATE inbound_events
                SET status = 'recovery_required', updated_at = ?
                WHERE scope = ? AND status = 'processing'
                """,
                (time.time(), scope),
            )
        return cursor.rowcount

    def entries(self, scope: str, status: str) -> list[InboxEntry]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT message_id, payload_json, status, created_at, updated_at
                FROM inbound_events
                WHERE scope = ? AND status = ?
                ORDER BY rowid
                """,
                (scope, status),
            ).fetchall()
        return [self._entry(row) for row in rows]

    def resolve(self, scope: str, message_id: str, *, retry: bool) -> bool:
        target = "pending" if retry else "completed"
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE inbound_events
                SET status = ?, updated_at = ?
                WHERE scope = ? AND message_id = ?
                    AND status IN ('recovery_required', 'failed')
                """,
                (target, time.time(), scope, message_id),
            )
        return cursor.rowcount == 1

    def prune(self, before: float) -> int:
        placeholders = ", ".join("?" for _ in _PRUNABLE_STATUSES)
        with self._connection() as connection:
            cursor = connection.execute(
                f"DELETE FROM inbound_events WHERE status IN ({placeholders}) AND updated_at < ?",
                (*_PRUNABLE_STATUSES, before),
            )
        return cursor.rowcount

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        self._ensure_initialized()
        connection = sqlite3.connect(self._path, timeout=0.5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 500")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with _SCHEMA_LOCK:
            if self._initialized:
                return
            self._prepare_path()
            connection = sqlite3.connect(self._path, timeout=0.5)
            try:
                connection.execute("PRAGMA busy_timeout = 500")
                connection.execute("PRAGMA journal_mode = WAL")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS inbound_events (
                        scope TEXT NOT NULL,
                        message_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        PRIMARY KEY (scope, message_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_inbound_events_ready
                        ON inbound_events (scope, status, created_at);
                    """
                )
                connection.commit()
            finally:
                connection.close()
            self._initialized = True

    def _prepare_path(self) -> None:
        """Create new storage paths without widening permissions on existing parents."""
        missing: list[Path] = []
        parent = self._path.parent
        while not parent.exists():
            missing.append(parent)
            parent = parent.parent
        for directory in reversed(missing):
            try:
                directory.mkdir(mode=0o700)
                directory.chmod(0o700)
            except FileExistsError:
                pass

        try:
            descriptor = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return
        if os.name == "posix":
            os.fchmod(descriptor, 0o600)
        os.close(descriptor)

    @staticmethod
    def _entry(row: sqlite3.Row) -> InboxEntry:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise ValueError("stored inbox payload is not a JSON object")
        return InboxEntry(
            message_id=str(row["message_id"]),
            payload=payload,
            status=cast(InboxStatus, str(row["status"])),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    @staticmethod
    def _validate_key(scope: str, message_id: str) -> None:
        if not scope or not message_id:
            raise ValueError("scope and message_id must be non-empty")
