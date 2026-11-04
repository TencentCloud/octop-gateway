"""Durable Feishu acceptance and manager consumption, without network access."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig
from octop_gateway.manager import ChannelManager
from octop_gateway.models import MessageEvent


def event(mid="m1", created="1000000000000"):
    return SimpleNamespace(
        header=SimpleNamespace(create_time=created, event_id="e1"),
        event=SimpleNamespace(
            message=SimpleNamespace(
                message_id=mid,
                message_type="text",
                content=json.dumps({"text": mid}),
                chat_id="chat",
                chat_type="p2p",
                thread_id="",
                mentions=[],
            ),
            sender=SimpleNamespace(sender_type="user", sender_id=SimpleNamespace(open_id="user")),
        ),
    )


def channel(tmp_path, processor=None, app="app"):
    async def complete(msg):
        yield MessageEvent.completed()

    ch = FeishuChannel(
        processor or complete, FeishuConfig(app_id=app, app_secret="test", inbox_path=str(tmp_path / "inbox.db"))
    )
    ch.start = AsyncMock()
    ch.stop = AsyncMock()
    ch._running = True
    ch._add_reaction_async = lambda *args: None
    ch._send_text = AsyncMock()
    return ch


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_old_first_delivery_and_reconstructed_channel(tmp_path):
    seen = []

    async def process(msg):
        seen.append(msg.text)
        yield MessageEvent.completed()

    ch = channel(tmp_path, process)
    ch._on_message_event(event())
    mgr = ChannelManager({ch.channel_id: ch}, queue_maxsize=1)
    await mgr.start()
    try:
        await until(lambda: len(seen) == 1)
        await until(lambda: ch.inbound_store.entries(ch.inbound_scope, "completed"))
    finally:
        await mgr.stop()
    other = channel(tmp_path, process)
    other._on_message_event(event())
    mgr = ChannelManager({other.channel_id: other})
    await mgr.start()
    try:
        await asyncio.sleep(0.1)
        assert seen == ["m1"]
    finally:
        await mgr.stop()


async def test_processor_failure_is_not_marked_completed(tmp_path):
    async def fail(msg):
        raise RuntimeError("processor failed")
        yield MessageEvent.completed()

    ch = channel(tmp_path, fail)
    ch._on_message_event(event())
    mgr = ChannelManager({ch.channel_id: ch})
    await mgr.start()
    try:
        await until(lambda: ch.inbound_store.entries(ch.inbound_scope, "failed"))
        assert not ch.inbound_store.entries(ch.inbound_scope, "completed")
    finally:
        await mgr.stop()


def test_accept_failure_propagates_to_sdk(tmp_path):
    ch = channel(tmp_path)
    ch.inbound_store.accept = lambda *args: (_ for _ in ()).throw(OSError("storage unavailable"))
    with pytest.raises(OSError, match="storage unavailable"):
        ch._on_message_event(event())


def test_cross_app_and_same_content_distinct_ids(tmp_path):
    a, b = channel(tmp_path), channel(tmp_path, app="other")
    a._on_message_event(event())
    a._on_message_event(event())
    a._on_message_event(event("m2"))
    b._on_message_event(event())
    assert len(a.inbound_store.pending(a.inbound_scope)) == 2
    assert len(b.inbound_store.pending(b.inbound_scope)) == 1


async def test_lost_queue_hint_does_not_lose_work(tmp_path):
    ch = channel(tmp_path)
    ch._enqueue_callback = lambda payload: (_ for _ in ()).throw(RuntimeError("queue full"))
    ch._on_message_event(event())
    mgr = ChannelManager({ch.channel_id: ch}, queue_maxsize=1)
    await mgr.start()
    try:
        await until(lambda: ch.inbound_store.entries(ch.inbound_scope, "completed"))
    finally:
        await mgr.stop()


async def test_interrupted_turn_is_quarantined_on_restart(tmp_path):
    started = asyncio.Event()
    calls = []

    async def blocking(msg):
        calls.append(msg.text)
        started.set()
        await asyncio.Event().wait()
        yield MessageEvent.completed()

    ch = channel(tmp_path, blocking)
    ch._on_message_event(event())
    mgr = ChannelManager({ch.channel_id: ch})
    await mgr.start()
    await asyncio.wait_for(started.wait(), 2)
    await mgr.stop()
    restarted = channel(tmp_path, blocking)
    mgr = ChannelManager({restarted.channel_id: restarted})
    await mgr.start()
    try:
        assert restarted.inbound_store.entries(restarted.inbound_scope, "recovery_required")
        restarted._on_message_event(event())
        await asyncio.sleep(0.1)
        assert calls == ["m1"]
    finally:
        await mgr.stop()


async def test_interrupt_bypasses_busy_session_and_full_hint_queue(tmp_path):
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def process(msg):
        if msg.text == "/stop":
            stopped.set()
        else:
            started.set()
            await stopped.wait()
        yield MessageEvent.completed()

    ch = channel(tmp_path, process)
    mgr = ChannelManager(
        {ch.channel_id: ch},
        workers_per_channel=1,
        queue_maxsize=1,
        interrupt_message_predicate=lambda msg: msg.text == "/stop",
    )
    await mgr.start()
    try:
        ch._on_message_event(event())
        await asyncio.wait_for(started.wait(), 2)
        # Put the command behind a page of same-session messages.
        for i in range(105):
            ch._on_message_event(event(f"queued-{i}"))
        ch._on_message_event(event("/stop"))
        await asyncio.wait_for(stopped.wait(), 2)
        await until(
            lambda: any(e.message_id == "/stop" for e in ch.inbound_store.entries(ch.inbound_scope, "completed"))
        )
    finally:
        await mgr.stop()


async def test_sdk_ack_fails_when_receipt_cannot_be_saved(tmp_path):
    import lark_oapi as lark
    from lark_oapi.ws.client import Client
    from lark_oapi.ws.pb.pbbp2_pb2 import Frame

    ch = channel(tmp_path)
    handler = (
        lark.EventDispatcherHandler.builder("", "").register_p2_im_message_receive_v1(ch._on_message_event).build()
    )
    client = Client("app", "test", event_handler=handler)
    client._write_message = AsyncMock()
    body = {
        "schema": "2.0",
        "header": {"event_id": "e1", "event_type": "im.message.receive_v1", "create_time": "1"},
        "event": {
            "sender": {"sender_type": "user", "sender_id": {"open_id": "user"}},
            "message": {
                "message_id": "m1",
                "message_type": "text",
                "content": '{"text":"test"}',
                "chat_id": "chat",
                "chat_type": "p2p",
            },
        },
    }

    def frame():
        item = Frame(SeqID=1, LogID=1, service=1, method=1)
        for key, value in {
            "message_id": "frame-id",
            "trace_id": "trace",
            "sum": "1",
            "seq": "0",
            "type": "event",
        }.items():
            h = item.headers.add()
            h.key = key
            h.value = value
        item.payload = json.dumps(body).encode()
        return item

    good = frame()
    await client._handle_data_frame(good)
    assert json.loads(good.payload)["code"] == 200
    ch.inbound_store.accept = lambda *args: (_ for _ in ()).throw(OSError("storage unavailable"))
    failed = frame()
    await client._handle_data_frame(failed)
    assert json.loads(failed.payload)["code"] == 500


async def test_pre_lock_failure_is_isolated_from_processing(tmp_path):
    async def broken_hook(channel_id, message):
        raise RuntimeError("hook failed")

    ch = channel(tmp_path)
    ch._on_message_event(event())
    mgr = ChannelManager({ch.channel_id: ch}, on_pre_lock=broken_hook)
    await mgr.start()
    try:
        await until(lambda: ch.inbound_store.entries(ch.inbound_scope, "completed"))
    finally:
        await mgr.stop()


async def test_pre_lock_can_release_busy_session(tmp_path):
    busy = asyncio.Event()
    release = asyncio.Event()

    async def process(msg):
        if msg.text == "m1":
            busy.set()
            await release.wait()
        yield MessageEvent.completed()

    async def hook(channel_id, msg):
        if msg.text == "m2":
            release.set()

    ch = channel(tmp_path, process)
    mgr = ChannelManager({ch.channel_id: ch}, workers_per_channel=2, on_pre_lock=hook)
    await mgr.start()
    try:
        ch._on_message_event(event("m1"))
        await asyncio.wait_for(busy.wait(), 2)
        ch._on_message_event(event("m2"))
        await until(lambda: len(ch.inbound_store.entries(ch.inbound_scope, "completed")) == 2)
    finally:
        await mgr.stop()


async def test_default_standalone_remains_compatible():
    async def process(msg):
        yield MessageEvent.completed()

    ch = FeishuChannel(process, FeishuConfig(app_id="app", app_secret="test"))
    ch._refresh_token = AsyncMock()
    ch._ensure_bot_open_id = AsyncMock()
    ch._start_ws_client = AsyncMock()
    try:
        await ch.start()
        assert ch.inbound_store is None
    finally:
        await ch.stop()


async def test_invalid_inbox_path_fails_before_transport_start(tmp_path):
    parent = tmp_path / "not-a-directory"
    parent.write_text("file")
    ch = channel(tmp_path)
    from octop_gateway.inbox import SqliteInbox

    ch.inbound_store = SqliteInbox(parent / "inbox.db")
    mgr = ChannelManager({ch.channel_id: ch})
    with pytest.raises(OSError):
        await mgr.start()
    ch.start.assert_not_awaited()


def test_default_store_only_enabled_by_manager_preparation():
    async def process(msg):
        yield MessageEvent.completed()

    ch = FeishuChannel(process, FeishuConfig(app_id="app", app_secret="test"))
    ch.set_enqueue_callback(lambda payload: None)
    assert ch.inbound_store is None
    ch.prepare_inbound_store()
    assert ch.inbound_store is not None


async def test_waiting_receipt_remains_pending_after_stop(tmp_path):
    started = asyncio.Event()

    async def process(msg):
        started.set()
        await asyncio.Event().wait()
        yield MessageEvent.completed()

    async def hook(channel_id, msg):
        pass

    ch = channel(tmp_path, process)
    mgr = ChannelManager({ch.channel_id: ch}, workers_per_channel=2, on_pre_lock=hook)
    await mgr.start()
    ch._on_message_event(event("m1"))
    await asyncio.wait_for(started.wait(), 2)
    ch._on_message_event(event("m2"))
    await until(lambda: ch.inbound_store.entries(ch.inbound_scope, "waiting"))
    await mgr.stop()
    assert [e.message_id for e in ch.inbound_store.pending(ch.inbound_scope)] == ["m2"]
    assert [e.message_id for e in ch.inbound_store.entries(ch.inbound_scope, "processing")] == ["m1"]
