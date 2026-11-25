from __future__ import annotations

import asyncio
import os
import sys
from collections import Counter
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

import octop_gateway.channels.feishu_ws_supervisor as transport
from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig
from octop_gateway.inbox import SqliteInbox
from octop_gateway.manager import ChannelManager
from octop_gateway.models import InboundMessage, MessageEvent

_WORKER = r"""
import json, os, pathlib, sys, time

root = pathlib.Path(sys.argv[1])
interval = float(sys.argv[2])
cfg = json.loads(sys.stdin.readline())
app = cfg["app_id"]
pid = os.getpid()
freeze = root / (app + ".freeze")
outage = root / "offline"

def send(frame):
    print(json.dumps(frame), flush=True)

if outage.exists():
    sys.exit(3)
send({"kind": "ready"})
sequence = 0
while True:
    if freeze.exists() and freeze.read_text() == str(pid):
        time.sleep(120)
    if outage.exists():
        sys.exit(3)
    message_id = "shared-replay" if sequence == 0 else f"{pid}-{sequence}"
    send({"kind": "message", "id": str(sequence), "payload": {"event": {
        "message": {
            "message_id": message_id, "message_type": "text",
            "content": json.dumps({"text": message_id}), "chat_type": "p2p", "chat_id": "chat",
        },
        "sender": {"sender_type": "user", "sender_id": {"open_id": "user"}},
    }}})
    ack = sys.stdin.readline()
    if not ack:
        break
    if json.loads(ack) != {"id": str(sequence), "accepted": True}:
        sys.exit(4)
    sequence += 1
    send({"kind": "heartbeat"})
    time.sleep(interval)
"""


async def _until(predicate: Callable[[], bool], seconds: float = 15) -> None:
    async with asyncio.timeout(seconds):
        while not predicate():
            await asyncio.sleep(0.03)


@pytest.mark.parametrize("bot_count", [2, 5, 20])
async def test_frozen_feishu_process_only_restarts_its_own_app(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bot_count: int
) -> None:
    fake_worker = tmp_path / "fake_feishu.py"
    fake_worker.write_text(_WORKER, encoding="utf-8")
    # The Windows venv launcher can have a different PID from the interpreter.
    worker_python = sys._base_executable
    real_spawn = asyncio.create_subprocess_exec
    processes: dict[int, asyncio.subprocess.Process] = {}
    rate = "0.35" if bot_count > 5 else "0.12"

    async def spawn(_executable: str, **kwargs: Any) -> asyncio.subprocess.Process:
        child = await real_spawn(worker_python, "-I", str(fake_worker), str(tmp_path), rate, **kwargs)
        processes[child.pid] = child
        return child

    monkeypatch.setattr(transport.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(transport, "_HEARTBEAT_TIMEOUT", 1.75 if bot_count > 5 else 0.65)
    monkeypatch.setattr(transport, "_STOP_TIMEOUT", 0.15)
    monkeypatch.setattr(transport.random, "uniform", lambda a, b: min(a, 0.15))
    monkeypatch.setenv("OCTOP_FEISHU_WS_WORKER", worker_python)
    monkeypatch.setattr(FeishuChannel, "_refresh_token", AsyncMock(return_value="fixture"))
    monkeypatch.setattr(FeishuChannel, "_ensure_bot_open_id", AsyncMock(return_value="fixture"))
    monkeypatch.setattr(FeishuChannel, "_add_reaction_async", lambda *_args: None)

    processed: Counter[tuple[str, str]] = Counter()
    subjects: dict[str, set[str]] = {}
    gateway_pid = os.getpid()

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        processed[(message.channel_id, str(message.metadata["message_id"]))] += 1
        assert message.channel_subject is not None
        subjects.setdefault(message.channel_id, set()).add(message.channel_subject.subject_id)
        yield MessageEvent.completed()

    manager = ChannelManager(processor=processor)
    channels = []
    await manager.start()
    try:
        for index in range(bot_count):
            channel = FeishuChannel(
                processor,
                FeishuConfig(
                    app_id=f"bot{index}",
                    app_secret="fixture",
                    dedup_db_path=str(tmp_path / "old-dedup.sqlite"),
                ),
                channel_id=f"channel{index}",
            )
            await manager.add_channel(channel)
            channels.append(channel)

        def counts() -> list[int]:
            return [sum(count for (cid, _), count in processed.items() if cid == ch.channel_id) for ch in channels]

        def pids() -> list[int | None]:
            return [ch.health_metrics()["feishu_ws_worker_pid"] for ch in channels]

        def replay_counts() -> list[int]:
            return [processed[(channel.channel_id, "shared-replay")] for channel in channels]

        def processed_from(channel: FeishuChannel, pid: int | None) -> bool:
            return pid is not None and any(
                cid == channel.channel_id and message.startswith(f"{pid}-") for cid, message in processed
            )

        await _until(lambda: all(count >= 3 for count in counts()) and all(pids()) and all(replay_counts()))
        before = pids()
        expected = counts()
        old_pid = before[0]
        assert old_pid is not None
        freeze = tmp_path / "bot0.freeze"
        freeze.write_text(str(old_pid), encoding="utf-8")
        await _until(
            lambda: (
                processes[old_pid].returncode is not None
                and pids()[0] not in (old_pid, None)
                and processed_from(channels[0], pids()[0])
            )
        )
        assert pids()[1:] == before[1:]
        assert all(current > prior for current, prior in zip(counts()[1:], expected[1:], strict=True))
        assert replay_counts() == [1] * bot_count
        assert all(subjects[channel.channel_id] == {"user"} for channel in channels)
        assert os.getpid() == gateway_pid

        if bot_count >= 5:
            recovered = pids()
            outage = tmp_path / "offline"
            outage.touch()
            await _until(lambda: all(pid is not None and processes[pid].returncode is not None for pid in recovered))
            outage.unlink()
            await _until(
                lambda: (
                    all(pid is not None and pid != prior for pid, prior in zip(pids(), recovered, strict=True))
                    and all(processed_from(ch, pid) for ch, pid in zip(channels, pids(), strict=True))
                )
            )
            assert replay_counts() == [1] * bot_count
    finally:
        await manager.stop()
    assert all(child.returncode is not None for child in processes.values())


def test_inbox_replays_pending_but_quarantines_interrupted_work(tmp_path: Path) -> None:
    path = tmp_path / "inbox.sqlite3"
    first = SqliteInbox(path)
    for id_ in ("started", "waiting", "pending"):
        assert first.accept("bot", id_, {"message_id": id_})
    assert first.claim("bot", "started")
    assert first.claim("bot", "waiting", waiting=True)
    reopened = SqliteInbox(path)
    assert reopened.recover("bot") == 1
    assert {entry.message_id for entry in reopened.pending("bot")} == {"waiting", "pending"}
    assert [entry.message_id for entry in reopened.entries("bot", "recovery_required")] == ["started"]
    assert not reopened.accept("bot", "started", {"message_id": "started"})
    assert reopened.accept("other-app", "started", {"message_id": "started"})


def test_pruning_advances_late_delivery_coverage_without_resetting_on_reopen(tmp_path: Path) -> None:
    path = tmp_path / "inbox.sqlite3"
    store = SqliteInbox(path)
    store.establish_coverage("bot", 100.0)
    store.establish_coverage("other-bot", 300.0)
    store.prune(200.0)

    reopened = SqliteInbox(path)
    reopened.establish_coverage("bot", 100.0)
    assert reopened.accept_late("bot", "old", {"message_id": "old"}, 199.0) == "recovery_required"
    assert reopened.accept_late("bot", "new", {"message_id": "new"}, 201.0) == "pending"
    assert reopened.accept_late("other-bot", "new", {"message_id": "new"}, 201.0) == "recovery_required"
