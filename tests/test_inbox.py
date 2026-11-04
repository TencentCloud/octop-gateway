"""Tests for the durable inbound event inbox."""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from octop_gateway.inbox import FinalStatus, SqliteInbox


def test_rejects_in_memory_database() -> None:
    with pytest.raises(ValueError, match="file path"):
        SqliteInbox(":memory:")


@pytest.mark.skipif(os.name != "posix", reason="POSIX permissions")
def test_new_storage_is_private_without_changing_existing_parent(tmp_path: Path) -> None:
    shared_parent = tmp_path / "shared"
    shared_parent.mkdir(mode=0o755)
    shared_parent.chmod(0o755)
    database = shared_parent / "private" / "inbox.sqlite3"

    assert SqliteInbox(database).accept("scope", "message", {}) is True

    assert shared_parent.stat().st_mode & 0o777 == 0o755
    assert database.parent.stat().st_mode & 0o777 == 0o700
    assert database.stat().st_mode & 0o777 == 0o600


def test_duplicate_is_suppressed_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "inbox.sqlite3"
    first = SqliteInbox(path)
    assert first.accept("feishu:app-a", "msg-1", {"text": "original"}) is True

    restarted = SqliteInbox(path)
    assert restarted.accept("feishu:app-a", "msg-1", {"text": "redelivery"}) is False
    assert restarted.pending("feishu:app-a") == first.pending("feishu:app-a")
    assert restarted.pending("feishu:app-a")[0].payload == {"text": "original"}


def test_scopes_are_isolated(tmp_path: Path) -> None:
    inbox = SqliteInbox(tmp_path / "inbox.sqlite3")
    assert inbox.accept("feishu:app-a", "shared-id", {"app": "a"}) is True
    assert inbox.accept("feishu:app-b", "shared-id", {"app": "b"}) is True
    assert inbox.pending("feishu:app-a")[0].payload == {"app": "a"}
    assert inbox.pending("feishu:app-b")[0].payload == {"app": "b"}


def test_concurrent_accept_is_atomic_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "inbox.sqlite3"

    def accept_once(index: int) -> bool:
        return SqliteInbox(path).accept("feishu:app-a", "msg-1", {"attempt": index})

    with ThreadPoolExecutor(max_workers=16) as pool:
        accepted = list(pool.map(accept_once, range(100)))

    assert accepted.count(True) == 1
    assert accepted.count(False) == 99
    assert len(SqliteInbox(path).pending("feishu:app-a")) == 1


def test_failed_and_interrupted_events_require_explicit_resolution(tmp_path: Path) -> None:
    inbox = SqliteInbox(tmp_path / "inbox.sqlite3")
    assert inbox.accept("scope", "failed", {}) is True
    assert inbox.claim("scope", "failed") is True
    inbox.finish("scope", "failed", "failed")
    assert inbox.resolve("scope", "failed", retry=True) is True
    assert inbox.claim("scope", "failed") is True

    assert inbox.accept("scope", "interrupted", {}) is True
    assert inbox.claim("scope", "interrupted") is True
    assert inbox.recover("scope") == 2
    assert {entry.message_id for entry in inbox.entries("scope", "recovery_required")} == {
        "failed",
        "interrupted",
    }
    assert inbox.pending("scope") == []
    assert inbox.resolve("scope", "interrupted", retry=False) is True
    assert inbox.entries("scope", "completed")[0].message_id == "interrupted"


def test_old_first_delivery_is_kept_pending(tmp_path: Path) -> None:
    inbox = SqliteInbox(tmp_path / "inbox.sqlite3")
    payload = {"create_time": "1", "text": "delayed but new"}
    assert inbox.accept("scope", "delayed", payload) is True
    assert inbox.pending("scope")[0].payload == payload


def test_prune_only_removes_safe_terminal_states(tmp_path: Path) -> None:
    inbox = SqliteInbox(tmp_path / "inbox.sqlite3")
    for message_id in ("pending", "completed", "ignored", "cancelled", "failed"):
        assert inbox.accept("scope", message_id, {}) is True
    terminal_states: tuple[tuple[str, FinalStatus], ...] = (
        ("completed", "completed"),
        ("ignored", "ignored"),
        ("cancelled", "cancelled"),
        ("failed", "failed"),
    )
    for message_id, status in terminal_states:
        assert inbox.claim("scope", message_id) is True
        inbox.finish("scope", message_id, status)

    assert inbox.prune(time.time() + 1) == 3
    assert [entry.message_id for entry in inbox.pending("scope")] == ["pending"]
    assert [entry.message_id for entry in inbox.entries("scope", "failed")] == ["failed"]


def test_process_crash_preserves_receipts_and_recovery_boundaries(tmp_path: Path) -> None:
    import subprocess
    import sys

    path = tmp_path / "restart.sqlite3"
    script = """
from octop_gateway.inbox import SqliteInbox
import sys
s = SqliteInbox(sys.argv[1])
s.accept('app', 'pending', {'text':'first delivery'})
s.accept('app', 'waiting', {})
s.claim('app', 'waiting', waiting=True)
s.accept('app', 'processing', {})
s.claim('app', 'processing')
import os
os._exit(0)
"""
    subprocess.run([sys.executable, "-c", script, str(path)], check=True)
    restarted = SqliteInbox(path)
    assert restarted.recover("app") == 1
    assert {item.message_id for item in restarted.pending("app")} == {"pending", "waiting"}
    assert restarted.entries("app", "recovery_required")[0].message_id == "processing"
    assert not restarted.accept("app", "processing", {})
