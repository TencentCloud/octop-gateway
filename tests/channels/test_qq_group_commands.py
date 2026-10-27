"""QQ group command authorization without live credentials or network calls."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from octop_gateway.channel import MessageProcessor
from octop_gateway.channels.qq import QQChannel, QQConfig
from octop_gateway.group_context import GroupContextConfig, GroupContextManager
from octop_gateway.manager import ChannelManager
from octop_gateway.media import FileSystemMediaBackend
from octop_gateway.models import GroupContext, GroupContextMessage, InboundMessage, MessageEvent


def _payload(text: str, sender: str = "member", *, mentioned: bool = True) -> dict[str, Any]:
    return {
        "t": "GROUP_AT_MESSAGE_CREATE" if mentioned else "GROUP_MESSAGE_CREATE",
        "d": {
            "id": f"{sender}:{text}",
            "content": f"<@!99999> {text}" if mentioned else text,
            "author": {"member_openid": sender},
            "group_openid": "group-1",
        },
    }


def _channel(processor: MessageProcessor, **config: Any) -> QQChannel:
    channel = QQChannel(
        processor=processor,
        config=QQConfig.from_dict({"app_id": "offline", "secret": "offline", "c2c_streaming": False, **config}),
    )
    channel._send_text = AsyncMock()  # type: ignore[method-assign]
    return channel


@pytest.mark.parametrize(
    "text",
    [
        "/help",
        " /MODEL provider:model",
        "/models",
        "/new",
        "/clear",
        "/delete synthetic",
        "/list",
        "/switch id",
        "/approve",
        "/stop",
        "/cancel",
    ],
)
async def test_non_owner_octop_commands_never_reach_processor(text: str, tmp_path: Path) -> None:
    received: list[str] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message.text)
        yield MessageEvent.text("reply")

    channel = _channel(processor)
    channel.set_media_backend(FileSystemMediaBackend(tmp_path))
    channel.fetch_remote_media = AsyncMock(return_value=(b"%PDF-1.7", "application/pdf"))  # type: ignore[method-assign]
    payload = _payload(text)
    payload["d"]["attachments"] = [{"url": "https://example.com/file.pdf", "content_type": "application/pdf"}]

    await channel.handle_inbound(payload)

    assert received == []
    channel._send_text.assert_not_awaited()
    channel.fetch_remote_media.assert_not_awaited()


@pytest.mark.parametrize(
    "text",
    [
        "请解释 /model 的用途",
        "hello\n/stop",
        "a/b",
        "https://example.com/help",
        "/tmp/example.txt",
        "/tmp",
        "/help.txt",
        "/help/intro",
        "/help?topic=1",
        "`/delete synthetic`",
        "```\n/new\n```",
        "/custom-command value",
        "/newton",
    ],
)
async def test_normal_group_text_with_slashes_preserves_request_and_reply(text: str) -> None:
    received: list[str] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message.text)
        yield MessageEvent.text("reply")
        yield MessageEvent.completed()

    channel = _channel(processor)
    await channel.handle_inbound(_payload(text))

    assert received == [text]
    assert channel._send_text.await_args.args[1] == "reply"


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("sender, accepted", [("owner", True), ("member", False), ("", False), ("unknown", False)])
async def test_owner_check_is_independent_of_context_switch(enabled: bool, sender: str, accepted: bool) -> None:
    received: list[str] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message.text)
        yield MessageEvent.completed()

    channel = _channel(
        processor,
        group_context={"enabled": enabled, "owner_ids": ["owner"], "visibility": "all", "activation": "always"},
    )
    await channel.handle_inbound(_payload("/new", sender, mentioned=False))

    assert received == (["/new"] if accepted else [])


async def test_group_policy_does_not_restrict_private_commands() -> None:
    received: list[str] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message.text)
        yield MessageEvent.completed()

    channel = _channel(processor)
    await channel.handle_inbound(
        {"t": "C2C_MESSAGE_CREATE", "d": {"id": "private", "content": "/new", "author": {"user_openid": "member"}}}
    )

    assert received == ["/new"]


@pytest.mark.parametrize("policy", [{"owner_commands_only": False}, {"groups": {"group-1": {"owner_ids": ["member"]}}}])
async def test_explicit_policy_overrides_are_honored(policy: dict[str, Any]) -> None:
    received: list[str] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message.text)
        yield MessageEvent.completed()

    channel = _channel(processor, group_context=policy)
    await channel.handle_inbound(_payload("/help"))

    assert received == ["/help"]


async def test_blocked_commands_are_absent_from_passive_and_platform_history() -> None:
    received: list[InboundMessage] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message)
        yield MessageEvent.completed()

    channel = _channel(processor)
    await channel.handle_inbound(_payload("/delete synthetic", mentioned=False))
    await channel.handle_inbound(_payload("normal a/b", mentioned=False))
    current = channel.parse_inbound(_payload("question"))
    current.group_context = GroupContext(
        conversation_id="group-1",
        visibility="mention_recent",
        activation="mention",
        messages=[GroupContextMessage(sender_id="member", text="/stop")],
    )
    await channel.handle_inbound(current)

    assert len(received) == 1
    assert received[0].group_context is not None
    assert [item.text for item in received[0].group_context.messages] == ["normal a/b"]


async def test_queue_filters_before_pre_lock_and_keeps_senders_separate() -> None:
    received: list[str] = []
    pre_lock: list[str] = []
    finished = asyncio.Event()

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message.text)
        if "done a/b" in message.text:
            finished.set()
        yield MessageEvent.completed()

    async def before_lock(channel_id: str, message: InboundMessage) -> None:
        pre_lock.append(message.text)

    channel = _channel(processor, group_context={"enabled": False, "owner_ids": ["owner"]})
    channel.start = AsyncMock()  # type: ignore[method-assign]
    channel.stop = AsyncMock()  # type: ignore[method-assign]
    manager = ChannelManager({channel.channel_id: channel}, workers_per_channel=1, on_pre_lock=before_lock)
    await manager.start()
    try:
        queue = manager._queues[channel.channel_id]
        for text, sender in [
            ("/stop", "member"),
            ("/delete", "owner"),
            ("synthetic", "member"),
            ("done a/b", "member"),
        ]:
            queue.put_nowait(_payload(text, sender, mentioned=False))
        await asyncio.wait_for(finished.wait(), timeout=1)
        # Ordinary messages keep batching while commands keep their own author.
        assert received == ["/delete", "synthetic\ndone a/b"]
        assert pre_lock == received
    finally:
        await manager.stop()


async def test_interrupt_path_rejects_non_owner_and_still_accepts_owner() -> None:
    work_started = asyncio.Event()
    release_work = asyncio.Event()
    owner_stop = asyncio.Event()
    received: list[tuple[str, str]] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append((message.text, message.metadata["sender_id"]))
        if message.text == "work":
            work_started.set()
            await release_work.wait()
        if message.text == "/stop":
            owner_stop.set()
        yield MessageEvent.completed()

    channel = _channel(processor, group_context={"owner_ids": ["owner"]})
    channel.start = AsyncMock()  # type: ignore[method-assign]
    channel.stop = AsyncMock()  # type: ignore[method-assign]
    manager = ChannelManager(
        {channel.channel_id: channel},
        workers_per_channel=1,
        interrupt_message_predicate=lambda message: message.text == "/stop",
    )
    await manager.start()
    try:
        manager.enqueue(channel.channel_id, _payload("work"))
        await asyncio.wait_for(work_started.wait(), timeout=1)
        manager.enqueue(channel.channel_id, _payload("/stop", "member"))
        manager.enqueue(channel.channel_id, _payload("/stop", "owner"))
        await asyncio.wait_for(owner_stop.wait(), timeout=1)
        assert received == [("work", "member"), ("/stop", "owner")]
    finally:
        release_work.set()
        await manager.stop()


@pytest.mark.parametrize("enabled", [False, True])
async def test_qq_python_group_config_uses_command_protection(enabled: bool) -> None:
    received: list[str] = []

    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        received.append(message.metadata["sender_id"])
        yield MessageEvent.completed()

    channel = _channel(processor, group_context=GroupContextConfig(enabled=enabled, owner_ids=["owner"]))
    await channel.handle_inbound(_payload("/new", "member"))
    await channel.handle_inbound(_payload("/new", "owner"))
    assert received == ["owner"]


def test_shared_group_policy_can_opt_out_of_command_protection() -> None:
    async def processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
        yield MessageEvent.completed()

    message = _channel(processor).parse_inbound(_payload("/new"))
    manager = GroupContextManager(GroupContextConfig(enabled=True, owner_commands_only=False))
    assert manager.prepare(message) is message


@pytest.mark.parametrize("policy", [{"owner_commands_only": "false"}, {"owner_ids": "owner"}, {"owner_ids": [1]}])
def test_invalid_security_config_is_rejected(policy: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        QQConfig.from_dict({"group_context": policy})
