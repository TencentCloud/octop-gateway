"""Feishu session scoping must not change platform reply routing."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest

from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig
from octop_gateway.group_context import GroupContextConfig, GroupContextManager
from octop_gateway.manager import ChannelManager
from octop_gateway.models import ChannelSubject, InboundMessage, MessageEvent

SCOPES = ("group", "group_sender", "group_topic", "group_topic_sender")


async def _noop_processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
    yield MessageEvent.completed()


def _channel(group_context: dict[str, object]) -> FeishuChannel:
    config = FeishuConfig.from_dict({"group_context": group_context})
    return FeishuChannel(processor=_noop_processor, config=config)


def _payload(
    sender: str = "ou_alice", *, thread: str = "", kind: str = "text", text: str = "hello"
) -> dict[str, object]:
    content = {"text": text, "image_key": "img_test", "file_key": "file_test", "file_name": "notes.txt"}
    return {
        "message_id": f"om_{sender}_{text}",
        "message_type": kind,
        "content": json.dumps(content),
        "chat_id": "oc_group",
        "chat_type": "group",
        "thread_id": thread,
        "sender": {"sender_id": sender, "sender_type": "user"},
    }


@pytest.mark.parametrize("scope", SCOPES)
@pytest.mark.parametrize("thread", ["", "omt_topic"])
@pytest.mark.parametrize("kind", ["text", "image", "file"])
def test_group_session_scope(scope: str, thread: str, kind: str) -> None:
    # Session scoping works even when passive group context is disabled.
    channel = _channel({"session_scope": scope})
    message = channel.parse_inbound(_payload(thread=thread, kind=kind))
    expected = (thread or "oc_group") if scope in ("group_topic", "group_topic_sender") else "oc_group"
    if scope in ("group_sender", "group_topic_sender"):
        expected += "#ou_alice"

    assert message.channel_subject is not None
    assert message.channel_subject.subject_id == expected
    assert message.channel_subject.chat_type == "group"
    assert channel.get_debounce_key(message) == expected
    assert message.metadata["conversation_id"] == (thread or "oc_group")
    assert message.metadata["to_handle"] == "oc_group"
    assert message.metadata["sender_id"] == "ou_alice"
    assert message.has_media is (kind != "text")


@pytest.mark.parametrize("config", [{}, {"session_scope": "invalid"}])
@pytest.mark.parametrize("thread", ["", "omt_topic"])
def test_default_scope_preserves_existing_subjects(config: dict[str, object], thread: str) -> None:
    message = _channel(config).parse_inbound(_payload(thread=thread))
    assert message.channel_subject is not None
    assert message.channel_subject.subject_id == (thread or "oc_group")


@pytest.mark.parametrize("scope", SCOPES)
def test_direct_messages_are_not_rescoped(scope: str) -> None:
    payload = _payload()
    payload["chat_type"] = "p2p"
    message = _channel({"session_scope": scope}).parse_inbound(payload)
    assert message.channel_subject is not None
    assert message.channel_subject.subject_id == "ou_alice"
    assert message.channel_subject.chat_type == "direct"
    assert message.metadata["to_handle"] == "ou_alice"


@pytest.mark.parametrize("scope", ["group_sender", "group_topic_sender"])
def test_interleaved_senders_and_groups_have_stable_separate_keys(scope: str) -> None:
    channel = _channel({"session_scope": scope})
    messages = [
        channel.parse_inbound(_payload(sender, thread="omt_topic")) for sender in ("ou_alice", "ou_bob", "ou_alice")
    ]
    keys = [channel.get_debounce_key(message) for message in messages]
    assert keys[0] == keys[2]
    assert keys[0] != keys[1]
    other_topic = channel.parse_inbound(_payload(thread="omt_other"))
    assert (channel.get_debounce_key(other_topic) == keys[0]) is (scope == "group_sender")
    other_group = _payload()
    other_group["chat_id"] = "oc_other"
    assert channel.get_debounce_key(channel.parse_inbound(other_group)) != channel.get_debounce_key(
        channel.parse_inbound(_payload())
    )


def test_scope_config_roundtrip_and_overrides() -> None:
    config = GroupContextConfig.from_dict(
        {
            "session_scope": "group_topic_sender",
            "groups": {"*": {"session_scope": "group_sender"}, "oc_group": {"session_scope": "group"}},
        }
    )
    assert config.session_scope == "group_topic_sender"
    assert config.resolve("oc_other").session_scope == "group_sender"
    assert config.resolve("oc_group").session_scope == "group"
    assert GroupContextConfig.from_dict(json.loads(json.dumps(asdict(config)))) == config
    config.groups.clear()
    assert config.resolve("oc_group").session_scope == "group_topic_sender"
    assert GroupContextConfig.from_dict({}).resolve("oc_group").session_scope == "group_topic"


@pytest.mark.parametrize("thread", ["", "omt_topic"])
@pytest.mark.parametrize("history", ["recent", "none"])
def test_native_conversation_overrides_and_passive_history_are_preserved(thread: str, history: str) -> None:
    conversation_id = thread or "oc_group"
    channel = _channel(
        {
            "groups": {
                conversation_id: {"enabled": True, "session_scope": "group_topic_sender", "history": history},
            },
        }
    )
    manager = GroupContextManager(channel._config.group_context)
    passive = channel.parse_inbound(_payload(thread=thread))
    assert manager.prepare(passive) is None
    current = channel.parse_inbound(_payload("ou_bob", thread=thread))
    current.metadata["bot_mentioned"] = True
    assert current.channel_subject is not None
    assert current.channel_subject.subject_id == f"{conversation_id}#ou_bob"
    assert not channel.should_batch_inbound(current)
    prepared = manager.prepare(current)
    assert prepared is not None and prepared.group_context is not None
    assert prepared.group_context.conversation_id == conversation_id
    assert [item.sender_id for item in prepared.group_context.messages] == (["ou_alice"] if history == "recent" else [])
    manager.mark_replied(current)
    next_message = channel.parse_inbound(_payload("ou_bob", thread=thread, text="next"))
    next_message.metadata["bot_mentioned"] = True
    next_turn = manager.prepare(next_message)
    assert next_turn is not None and next_turn.group_context is not None
    assert next_turn.group_context.messages == []


@pytest.mark.parametrize("scope", SCOPES)
@pytest.mark.parametrize("thread", ["", "omt_topic"])
@pytest.mark.asyncio
async def test_scoped_subjects_keep_reply_and_push_routes(
    scope: str, thread: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    channel = _channel({"session_scope": scope})
    message = channel.parse_inbound(_payload(thread=thread))
    assert message.channel_subject is not None
    reply = AsyncMock(return_value={"code": 0})
    send = AsyncMock(return_value={"code": 0})
    monkeypatch.setattr(channel, "_reply_message", reply)
    monkeypatch.setattr(channel, "_send_message", send)
    await channel._send_text(message.channel_subject, "answer")
    if thread:
        assert reply.await_args.args[0] == message.metadata["message_id"]
        send.assert_not_awaited()
        reply.return_value = {"code": 230002}
        await channel._send_text(message.channel_subject, "fallback")
    else:
        reply.assert_not_awaited()
    assert send.await_args.kwargs["receive_id"] == "oc_group"
    assert send.await_args.kwargs["receive_id_type"] == "chat_id"

    # Hosts may restore a subject with only its native chat routing metadata.
    stored = ChannelSubject(
        subject_id=message.channel_subject.subject_id,
        chat_type="group",
        metadata={"chat_id": "oc_group", "channel_type": "feishu"},
    )
    restored = ChannelSubject(**json.loads(json.dumps(asdict(stored))))
    await channel.push_text(restored, "reminder")
    assert send.await_args.kwargs["receive_id"] == "oc_group"


@pytest.mark.asyncio
async def test_sender_sessions_process_concurrently_without_batching_each_other(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = {sender: asyncio.Event() for sender in ("ou_alice", "ou_bob")}
    release = asyncio.Event()
    seen: list[tuple[str, str]] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        sender = message.metadata["sender_id"]
        seen.append((sender, message.text))
        started[sender].set()
        await release.wait()
        yield MessageEvent.completed()

    config = FeishuConfig.from_dict({"group_context": {"session_scope": "group_sender"}})
    channel = FeishuChannel(processor=processor, config=config)
    monkeypatch.setattr(channel, "start", AsyncMock())
    monkeypatch.setattr(channel, "stop", AsyncMock())
    manager = ChannelManager({channel.channel_id: channel}, workers_per_channel=2)
    await manager.start()
    try:
        manager.enqueue(channel.channel_id, _payload("ou_alice", text="first"))
        manager.enqueue(channel.channel_id, _payload("ou_bob", text="second"))
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started.values())), timeout=2)
        assert sorted(seen) == [("ou_alice", "first"), ("ou_bob", "second")]
    finally:
        release.set()
        await manager.stop()
