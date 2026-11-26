"""Unit tests for SlackChannel and SlackConfig."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from octop_gateway.channels.slack import SlackChannel, SlackConfig, split_slack_text
from octop_gateway.models import (
    ChannelSubject,
    FileContent,
    ImageContent,
    InboundMessage,
    MessageEvent,
)


async def _echo_processor(msg: InboundMessage) -> AsyncIterator[MessageEvent]:
    yield MessageEvent.text(f"Echo: {msg.text}")
    yield MessageEvent.completed()


class _FakeResponse:
    def __init__(self, status: int = 200, json_data: dict[str, Any] | None = None, body: bytes = b"") -> None:
        self.status = status
        self._json = json_data or {}
        self._body = body
        self.content_type = "application/json" if json_data is not None else "image/png"

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self) -> dict[str, Any]:
        return self._json

    async def read(self) -> bytes:
        return self._body

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


def test_slack_config_aliases_and_credentials() -> None:
    cfg = SlackConfig.from_dict(
        {
            "botToken": "  xoxb-123  ",
            "appToken": "xapp-456",
            "signingSecret": "sec-789",
            "allowed_channel_ids": "C111, C222",
            "allowed_user_ids": ["U111", "U222"],
        }
    )
    assert cfg.bot_token == "xoxb-123"
    assert cfg.app_token == "xapp-456"
    assert cfg.signing_secret == "sec-789"
    assert cfg.allowed_channel_ids == ["C111", "C222"]
    assert cfg.allowed_user_ids == ["U111", "U222"]
    assert cfg.missing_credentials() == []
    assert SlackConfig().missing_credentials() == ["bot_token"]


def test_split_slack_text_balances_code_fences() -> None:
    code_lines = "\n".join(f"print({i})" for i in range(40))
    text = f"Here is code:\n```python\n{code_lines}\n```\nDone."
    chunks = split_slack_text(text, limit=180)
    assert len(chunks) >= 2
    for chunk in chunks[:-1]:
        assert chunk.count("```") % 2 == 0


def test_slack_parse_inbound_dm_and_channel_mentions() -> None:
    channel = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1", thread_isolation=True))
    channel._bot_user_id = "UBOT999"
    channel._team_id = "T001"

    # DM event
    dm_msg = channel.parse_inbound(
        {
            "type": "message",
            "channel": "D12345",
            "channel_type": "im",
            "user": "UUSER1",
            "text": "Hello in DM",
            "ts": "1700000000.000100",
        }
    )
    assert dm_msg.channel_subject is not None
    assert dm_msg.channel_subject.subject_id == "D12345"
    assert dm_msg.channel_subject.chat_type == "direct"
    assert dm_msg.text == "Hello in DM"
    assert dm_msg.metadata["is_dm"] is True

    # Group / channel event with bot mention, file attachment, and quoted message
    group_msg = channel.parse_inbound(
        {
            "type": "app_mention",
            "channel": "C99999",
            "channel_type": "channel",
            "user": "UUSER2",
            "text": "<@UBOT999> check this image",
            "ts": "1700000002.000200",
            "thread_ts": "1700000001.000100",
            "files": [
                {
                    "id": "F001",
                    "name": "diagram.png",
                    "mimetype": "image/png",
                    "size": 1024,
                    "url_private_download": "https://files.slack.com/files-pri/T001-F001/diagram.png",
                }
            ],
            "attachments": [
                {
                    "author_name": "Alice",
                    "text": "Earlier quoted context",
                }
            ],
        }
    )
    assert group_msg.channel_subject is not None
    assert group_msg.channel_subject.subject_id == "C99999:thread:1700000001.000100"
    assert group_msg.channel_subject.chat_type == "group"
    assert group_msg.text == "check this image"
    assert group_msg.metadata["bot_mentioned"] is True
    assert group_msg.metadata["has_mentions"] is True
    assert group_msg.metadata["reply_quote_text"] == "Alice: Earlier quoted context"
    assert any(isinstance(p, ImageContent) for p in group_msg.content)


def test_slack_deduplication_and_filtering() -> None:
    cfg = SlackConfig(
        bot_token="xoxb-1",
        allow_all_channels=False,
        allowed_channel_ids=["C_ALLOWED"],
    )
    channel = SlackChannel(_echo_processor, cfg)
    channel._bot_user_id = "UBOT"

    # Bot's own message ignored
    assert not channel._should_handle_event({"type": "message", "channel": "C_ALLOWED", "user": "UBOT", "ts": "1.0"})
    # Subtype message_changed ignored
    assert not channel._should_handle_event(
        {"type": "message", "subtype": "message_changed", "channel": "C_ALLOWED", "user": "U1", "ts": "1.1"}
    )
    # Disallowed channel ignored
    assert not channel._should_handle_event({"type": "message", "channel": "C_OTHER", "user": "U1", "ts": "1.2"})
    # Valid message accepted once, duplicate rejected (e.g. message + app_mention with same ts)
    assert channel._should_handle_event({"type": "message", "channel": "C_ALLOWED", "user": "U1", "ts": "1.3"})
    assert not channel._should_handle_event({"type": "app_mention", "channel": "C_ALLOWED", "user": "U1", "ts": "1.3"})
    assert channel.dropped_duplicate == 1


def test_slack_webhook_signature_and_events() -> None:
    secret = "my_slack_signing_secret"
    channel = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1", signing_secret=secret))
    enqueued: list[Any] = []
    channel.set_enqueue_callback(enqueued.append)

    now_str = str(int(time.time()))
    raw_body = json.dumps(
        {
            "type": "event_callback",
            "event": {"type": "message", "channel": "D111", "user": "U111", "text": "hi", "ts": "10.1"},
        }
    )
    sig = "v0=" + hmac.new(secret.encode(), f"v0:{now_str}:{raw_body}".encode(), hashlib.sha256).hexdigest()

    assert channel.verify_signature(now_str, sig, raw_body) is True
    assert channel.verify_signature(now_str, "v0=bad", raw_body) is False

    # URL verification challenge
    res = channel.handle_webhook_payload({"type": "url_verification", "challenge": "chal_xyz"})
    assert res == {"challenge": "chal_xyz"}

    # Event callback enqueues message
    res2 = channel.handle_webhook_payload(json.loads(raw_body))
    assert res2 == {"ok": True}
    assert len(enqueued) == 1
    assert enqueued[0]["text"] == "hi"


@pytest.mark.asyncio
async def test_slack_socket_mode_frame_ack_and_outbound_send() -> None:
    channel = SlackChannel(
        _echo_processor,
        SlackConfig(bot_token="xoxb-1", app_token="xapp-1", probe_mode="auth_only"),
    )
    posts: list[tuple[str, Any]] = []

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        posts.append((url, kwargs))
        if url.endswith("/auth.test"):
            return _FakeResponse(200, {"ok": True, "user_id": "UBOT1", "team_id": "T1"})
        if url.endswith("/apps.connections.open"):
            return _FakeResponse(200, {"ok": True, "url": "wss://wss-primary.slack.com/link"})
        if url.endswith("/chat.postMessage"):
            return _FakeResponse(200, {"ok": True, "ts": "123.456"})
        if url.endswith("/files.getUploadURLExternal"):
            return _FakeResponse(200, {"ok": True, "upload_url": "https://files.slack.com/upload/v1", "file_id": "F99"})
        if url == "https://files.slack.com/upload/v1":
            return _FakeResponse(200, {"ok": True})
        if url.endswith("/files.completeUploadExternal"):
            return _FakeResponse(200, {"ok": True})
        return _FakeResponse(200, {"ok": True})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.get = MagicMock(return_value=_FakeResponse(200, json_data=None, body=b"PNG_BYTES"))
    fake_http.close = AsyncMock()
    channel._http = fake_http

    await channel.start()
    assert channel.bot_user_id == "UBOT1"

    # Socket frame handling sends envelope_id ACK and enqueues event
    enqueued: list[Any] = []
    channel.set_enqueue_callback(enqueued.append)
    fake_ws = MagicMock(spec=aiohttp.ClientWebSocketResponse)
    fake_ws.send_json = AsyncMock()

    frame_str = json.dumps(
        {
            "envelope_id": "env-123",
            "type": "events_api",
            "payload": {
                "event": {
                    "type": "app_mention",
                    "channel": "C100",
                    "user": "U200",
                    "text": "<@UBOT1> hello",
                    "ts": "1700000100.0001",
                }
            },
        }
    )
    disconnect = await channel._handle_socket_frame(fake_ws, frame_str)
    assert disconnect is False
    fake_ws.send_json.assert_awaited_once_with({"envelope_id": "env-123"})
    assert len(enqueued) == 1

    # Outbound text in channel replies in thread automatically
    subject = ChannelSubject(
        subject_id="C100",
        chat_type="group",
        metadata={"channel_id": "C100", "ts": "1700000100.0001", "is_group": True},
    )
    await channel._send_text(subject, "Hello from bot")
    post_msg_calls = [kw for url, kw in posts if url.endswith("/chat.postMessage")]
    assert len(post_msg_calls) == 1
    assert post_msg_calls[0]["json"] == {
        "channel": "C100",
        "text": "Hello from bot",
        "thread_ts": "1700000100.0001",
        "reply_broadcast": True,
    }

    # Outbound media uses 3-step files.getUploadURLExternal -> upload -> files.completeUploadExternal
    b64_hello = base64.b64encode(b"hello world").decode("ascii")
    await channel._send_media(
        subject,
        FileContent(data=b64_hello, filename="report.txt", mime_type="text/plain"),
    )
    complete_calls = [kw for url, kw in posts if url.endswith("/files.completeUploadExternal")]
    assert len(complete_calls) == 1
    assert complete_calls[0]["json"]["channel_id"] == "C100"
    assert complete_calls[0]["json"]["thread_ts"] == "1700000100.0001"
    assert complete_calls[0]["json"]["files"] == [{"id": "F99", "title": "report.txt"}]

    # Private Slack media download attaches Authorization header
    data, _ = await channel.fetch_remote_media("https://files.slack.com/files-pri/T1-F1/img.png")
    assert data == b"PNG_BYTES"
    get_kwargs = fake_http.get.call_args.kwargs
    assert get_kwargs["headers"]["Authorization"] == "Bearer xoxb-1"

    await channel.stop()


@pytest.mark.asyncio
async def test_slack_security_policies_streaming_and_reactions() -> None:
    from octop_gateway.group_context import GroupContextConfig

    async def _stream_processor(msg: InboundMessage) -> AsyncIterator[MessageEvent]:
        yield MessageEvent.delta("Hello ")
        yield MessageEvent.delta("Slack!")
        yield MessageEvent.completed()

    cfg = SlackConfig(
        bot_token="xoxb-sec",
        owner_only=True,
        ignore_mention_all=True,
        streaming_mode="edit",
        status_reactions=True,
        group_context=GroupContextConfig(
            enabled=True,
            visibility="all",
            activation="mention",
            owner_commands_only=True,
            owner_tools_only=True,
            owner_ids=["U_ADMIN"],
        ),
    )
    channel = SlackChannel(_stream_processor, cfg)
    channel._bot_user_id = "UBOT"

    # 1. owner_only blocks non-owner users in both DM and group
    assert not channel._should_handle_event({"type": "message", "channel": "D1", "user": "U_GUEST", "ts": "1.0"})
    assert channel._should_handle_event({"type": "message", "channel": "D1", "user": "U_ADMIN", "ts": "1.1"})

    # 2. ignore_mention_all prevents <!channel> / <!here> from counting as bot mention
    mass_msg = channel.parse_inbound(
        {
            "type": "message",
            "channel": "C100",
            "channel_type": "channel",
            "user": "U_ADMIN",
            "text": "<!channel> please check",
            "ts": "2.0",
        }
    )
    assert mass_msg.metadata["bot_mentioned"] is False
    assert mass_msg.metadata["conversation_id"] == "C100"

    # 3. GroupContextManager enforces owner_commands_only and owner_tools_only even with thread_isolation
    cfg_group = SlackConfig(
        bot_token="xoxb-sec",
        thread_isolation=True,
        group_context=GroupContextConfig(
            enabled=True,
            visibility="all",
            activation="mention",
            owner_commands_only=True,
            owner_tools_only=True,
            owner_ids=["U_ADMIN"],
        ),
    )
    group_channel = SlackChannel(_stream_processor, cfg_group)
    group_channel._bot_user_id = "UBOT"

    guest_cmd = {
        "type": "app_mention",
        "channel": "C100",
        "channel_type": "channel",
        "user": "U_GUEST",
        "text": "<@UBOT> /model gpt-5",
        "ts": "3.1",
        "thread_ts": "3.0",
    }
    assert await group_channel._prepare_inbound(guest_cmd) is None

    guest_chat = {
        "type": "app_mention",
        "channel": "C100",
        "channel_type": "channel",
        "user": "U_GUEST",
        "text": "<@UBOT> run a tool for me",
        "ts": "3.2",
        "thread_ts": "3.0",
    }
    prepared_guest_res = await group_channel._prepare_inbound(guest_chat)
    assert prepared_guest_res is not None
    prepared_guest, _ = prepared_guest_res
    assert prepared_guest.allowed_tools == []

    admin_chat = {
        "type": "app_mention",
        "channel": "C100",
        "channel_type": "channel",
        "user": "U_ADMIN",
        "text": "<@UBOT> /model gpt-5",
        "ts": "3.3",
        "thread_ts": "3.0",
    }
    prepared_admin_res = await group_channel._prepare_inbound(admin_chat)
    assert prepared_admin_res is not None
    prepared_admin, _ = prepared_admin_res
    assert prepared_admin.allowed_tools is None

    # 4. streaming_mode="edit" + status_reactions posts initial message, updates in place, and toggles reactions
    api_calls: list[tuple[str, Any]] = []

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        api_calls.append((url, kwargs.get("json")))
        if url.endswith("/chat.postMessage"):
            return _FakeResponse(200, {"ok": True, "ts": "999.001"})
        return _FakeResponse(200, {"ok": True})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.close = AsyncMock()
    channel._http = fake_http

    dm_inbound = channel.parse_inbound(
        {
            "type": "message",
            "channel": "D1",
            "channel_type": "im",
            "user": "U_ADMIN",
            "text": "Stream test",
            "ts": "10.0",
        }
    )
    assert dm_inbound.channel_subject is not None
    ok = await channel._process_inbound(dm_inbound, dm_inbound.channel_subject)
    assert ok is True

    endpoints = [u.rsplit("/", 1)[-1] for u, _ in api_calls]
    assert "reactions.add" in endpoints
    assert "reactions.remove" in endpoints
    assert "chat.postMessage" in endpoints
    assert "chat.update" in endpoints
    update_payloads = [p for u, p in api_calls if u.endswith("/chat.update")]
    assert update_payloads[-1]["text"] == "Hello Slack!"

    await channel.stop()
    await group_channel.stop()


@pytest.mark.asyncio
async def test_slack_review_fixes_auth_allowlist_and_stream_update_failure() -> None:
    from octop_gateway.group_context import GroupContextConfig

    # 1. verify_signature fails closed when require_secret=True and signing_secret is empty
    no_secret = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1"))
    assert no_secret.verify_signature("123", "v0=abc", "{}", require_secret=True) is False
    assert no_secret.verify_signature("123", "v0=abc", "{}", require_secret=False) is True

    # 2. allow_all_channels=True ignores stale allowed_channel_ids; owner_only + allow_all_dms=False allows owner_ids
    cfg = SlackConfig(
        bot_token="xoxb-1",
        owner_only=True,
        allow_all_dms=False,
        allow_all_channels=True,
        allowed_channel_ids=["C_OLD"],
        group_context=GroupContextConfig(enabled=True, owner_ids=["U_OWNER"]),
    )
    ch = SlackChannel(_echo_processor, cfg)
    assert ch._should_handle_event(
        {"type": "message", "channel": "D1", "channel_type": "im", "user": "U_OWNER", "ts": "1"}
    )
    assert ch._should_handle_event(
        {"type": "message", "channel": "C_NEW", "channel_type": "channel", "user": "U_OWNER", "ts": "2"}
    )
    assert not ch._should_handle_event(
        {"type": "message", "channel": "D1", "channel_type": "im", "user": "U_OTHER", "ts": "3"}
    )

    # 3. Failed chat.update does not fall through to chat.postMessage
    stream_ch = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1", streaming_mode="edit"))
    posts: list[str] = []

    def _fake_post(url: str, **kw: Any) -> _FakeResponse:
        posts.append(url.rsplit("/", 1)[-1])
        if url.endswith("/chat.update"):
            return _FakeResponse(200, {"ok": False, "error": "cant_update_message"})
        return _FakeResponse(200, {"ok": True, "ts": "1.0"})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.close = AsyncMock()
    stream_ch._http = fake_http

    ts = await stream_ch._post_or_update_stream_text("D1", None, "updated", message_ts="9.0")
    assert ts == "9.0"
    assert posts == ["chat.update"]
    await stream_ch.stop()


@pytest.mark.asyncio
async def test_slack_channel_manager_worker_e2e_and_is_connected() -> None:
    import asyncio

    from octop_gateway.manager import ChannelManager

    delivered = asyncio.Event()
    sent_payloads: list[dict[str, Any]] = []

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/auth.test"):
            return _FakeResponse(200, {"ok": True, "user_id": "UBOT", "team_id": "T1"})
        if url.endswith("/chat.postMessage"):
            sent_payloads.append(kwargs.get("json") or {})
            delivered.set()
            return _FakeResponse(200, {"ok": True, "ts": "999.1"})
        return _FakeResponse(200, {"ok": True})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.close = AsyncMock()

    ch = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1"), channel_id="slack-e2e")
    ch._http = fake_http
    assert ch.is_connected is False

    # 1. parse_inbound accepts already-parsed InboundMessage
    parsed = ch.parse_inbound({"type": "message", "channel": "D1", "channel_type": "im", "user": "U1", "text": "hi"})
    assert ch.parse_inbound(parsed) is parsed

    # 2. Full ChannelManager worker loop: enqueue raw dict -> parse_inbound -> handle_inbound(merged) -> chat.postMessage
    manager = ChannelManager({"slack-e2e": ch})
    await manager.start()
    try:
        assert ch.is_connected is True
        ch.enqueue({"type": "message", "channel": "D1", "channel_type": "im", "user": "U1", "text": "hello", "ts": "1"})
        await asyncio.wait_for(delivered.wait(), timeout=2.0)
        assert len(sent_payloads) == 1
        assert sent_payloads[0]["channel"] == "D1"
        assert sent_payloads[0]["text"] == "Echo: hello"

        # 3. Socket Mode is_connected tracks live websocket state
        fake_ws = MagicMock(closed=False)
        fake_task = MagicMock()
        fake_task.done.return_value = False
        ch._ws = fake_ws
        ch._ws_task = fake_task
        assert ch.is_connected is True
        fake_ws.closed = True
        assert ch.is_connected is False
        ch._ws = None
        ch._ws_task = None
    finally:
        await manager.stop()
    assert ch.is_connected is False


@pytest.mark.asyncio
async def test_slack_thread_followup_broadcast_and_context_fetch() -> None:
    # 1. Config defaults & normalization
    default_cfg = SlackConfig.from_dict({"bot_token": "xoxb-1"})
    assert default_cfg.reply_in_thread is True
    assert default_cfg.reply_broadcast == "first"
    assert default_cfg.thread_follow_up is True
    assert default_cfg.fetch_thread_context is True
    assert SlackConfig.from_dict({"bot_token": "xoxb-1", "reply_broadcast": False}).reply_broadcast == "off"
    assert SlackConfig.from_dict({"bot_token": "xoxb-1", "reply_broadcast": True}).reply_broadcast == "all"

    # 2. Participated thread follow-up + anti-hijack + reply_broadcast="first"
    posted: list[dict[str, Any]] = []
    replies_calls: list[dict[str, Any]] = []

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/chat.postMessage"):
            posted.append(kwargs.get("json") or {})
            return _FakeResponse(200, {"ok": True, "ts": f"bot.{len(posted)}"})
        return _FakeResponse(200, {"ok": True})

    def _fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/conversations.replies"):
            replies_calls.append(kwargs.get("params") or {})
            return _FakeResponse(
                200,
                {
                    "ok": True,
                    "messages": [
                        {"ts": "100.0", "user": "U1", "text": "Root question"},
                        {"ts": "100.1", "user": "UBOT", "text": "Earlier bot answer"},
                        {"ts": "100.2", "user": "U1", "text": "Follow up without mention"},
                    ],
                },
            )
        return _FakeResponse(200, {"ok": True})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.get = MagicMock(side_effect=_fake_get)
    fake_http.close = AsyncMock()

    ch = SlackChannel(
        _echo_processor,
        SlackConfig(
            bot_token="xoxb-1",
            reply_in_thread=True,
            reply_broadcast="first",
            thread_follow_up=True,
            fetch_thread_context=True,
        ),
        channel_id="slack-thread",
    )
    ch._bot_user_id = "UBOT"
    ch._http = fake_http

    # Step A: Mention in main channel -> reply goes into thread WITH reply_broadcast=True
    root_event = {
        "type": "app_mention",
        "channel": "C100",
        "channel_type": "channel",
        "user": "U1",
        "text": "<@UBOT> Hi",
        "ts": "100.0",
    }
    await ch.handle_inbound(root_event)
    assert len(posted) == 1
    assert posted[0]["thread_ts"] == "100.0"
    assert posted[0].get("reply_broadcast") is True

    # Step B: Follow-up in same thread WITHOUT @UBOT -> auto-triggered, NO reply_broadcast, fetches thread history once
    followup_event = {
        "type": "message",
        "channel": "C100",
        "channel_type": "channel",
        "user": "U1",
        "text": "Follow up without mention",
        "ts": "100.2",
        "thread_ts": "100.0",
    }
    prepared = await ch._prepare_inbound(followup_event)
    assert prepared is not None
    followup_msg, followup_subj = prepared
    assert followup_msg.metadata["bot_mentioned"] is True
    assert followup_msg.metadata["thread_follow_up"] is True
    assert followup_msg.group_context is not None
    assert [m.text for m in followup_msg.group_context.messages] == ["Root question", "Earlier bot answer"]
    assert len(replies_calls) == 1

    await ch._send_text(followup_subj, "Second answer")
    assert len(posted) == 2
    assert posted[1]["thread_ts"] == "100.0"
    assert "reply_broadcast" not in posted[1]

    # Step C: Anti-hijack — mentioning another user in the thread does NOT trigger bot
    other_mention = {
        "type": "message",
        "channel": "C100",
        "channel_type": "channel",
        "user": "U1",
        "text": "<@U_ALICE> what do you think?",
        "ts": "100.3",
        "thread_ts": "100.0",
    }
    parsed_other = ch.parse_inbound(other_mention)
    assert parsed_other.metadata["bot_mentioned"] is False

    # Step D: Second follow-up in same thread does NOT re-call conversations.replies
    followup_2 = {
        "type": "message",
        "channel": "C100",
        "channel_type": "channel",
        "user": "U1",
        "text": "Another follow up",
        "ts": "100.4",
        "thread_ts": "100.0",
    }
    assert await ch._prepare_inbound(followup_2) is not None
    assert len(replies_calls) == 1

    # 3. reply_broadcast="all" broadcasts thread follow-up; reply_broadcast="off" never broadcasts
    ch._config.reply_broadcast = "all"
    await ch._send_text(followup_subj, "Broadcast follow-up")
    assert posted[-1].get("reply_broadcast") is True

    ch._config.reply_broadcast = "off"
    root_msg = ch.parse_inbound(root_event)
    assert root_msg.channel_subject is not None
    await ch._send_text(root_msg.channel_subject, "Quiet root reply")
    assert "reply_broadcast" not in posted[-1]

    # 4. thread_follow_up=False requires explicit @mention even in participated threads
    ch._config.thread_follow_up = False
    assert ch.parse_inbound(followup_event).metadata["bot_mentioned"] is False

    # 5. Dual-event dedup: message before bot_user_id resolved does NOT swallow subsequent app_mention
    race_ch = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1"))
    assert race_ch._should_handle_event(
        {"type": "message", "channel": "C1", "user": "U1", "text": "<@UBOT> hi", "ts": "5.0"}
    )
    assert race_ch._should_handle_event(
        {"type": "app_mention", "channel": "C1", "user": "U1", "text": "<@UBOT> hi", "ts": "5.0"}
    )
    # Once mention is recorded, another duplicate app_mention IS dropped
    assert not race_ch._should_handle_event(
        {"type": "app_mention", "channel": "C1", "user": "U1", "text": "<@UBOT> hi", "ts": "5.0"}
    )

    # 6. conversations.replies missing_scope degrades gracefully without breaking reply
    err_http = MagicMock()
    err_http.closed = False
    err_http.get = MagicMock(return_value=_FakeResponse(200, {"ok": False, "error": "missing_scope"}))
    err_http.close = AsyncMock()
    err_ch = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1", fetch_thread_context=True))
    err_ch._bot_user_id = "UBOT"
    err_ch._http = err_http
    err_prepared = await err_ch._prepare_inbound(
        {
            "type": "app_mention",
            "channel": "C200",
            "channel_type": "channel",
            "user": "U1",
            "text": "<@UBOT> summarize",
            "ts": "200.5",
            "thread_ts": "200.0",
        }
    )
    assert err_prepared is not None

    await ch.stop()
    await race_ch.stop()
    await err_ch.stop()


@pytest.mark.asyncio
async def test_slack_start_waits_for_socket_mode_ws_ready() -> None:
    import asyncio

    release_ws = asyncio.Event()

    class _FakeWS:
        closed = False

        async def close(self) -> None:
            self.closed = True
            release_ws.set()

        def __aiter__(self) -> _FakeWS:
            return self

        async def __anext__(self) -> Any:
            await release_ws.wait()
            raise StopAsyncIteration

        async def __aenter__(self) -> _FakeWS:
            return self

        async def __aexit__(self, *args: object) -> None:
            self.closed = True

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/auth.test"):
            return _FakeResponse(200, {"ok": True, "user_id": "UBOT", "team_id": "T1"})
        if url.endswith("/apps.connections.open"):
            return _FakeResponse(200, {"ok": True, "url": "wss://wss-primary.slack.com/link"})
        return _FakeResponse(200, {"ok": True})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.ws_connect = MagicMock(return_value=_FakeWS())
    fake_http.close = AsyncMock()

    ch = SlackChannel(_echo_processor, SlackConfig(bot_token="xoxb-1", app_token="xapp-1"), channel_id="slack-ws-ready")
    ch._http = fake_http
    await ch.start()
    try:
        # Must already be connected immediately upon start() returning (no post-save race)
        assert ch.is_connected is True
    finally:
        await ch.stop()
    assert ch.is_connected is False


@pytest.mark.asyncio
async def test_slack_mpim_treated_as_group_for_routing_and_authorization() -> None:
    from octop_gateway.group_context import GroupContextConfig

    cfg = SlackConfig(
        bot_token="xoxb-mpim",
        owner_only=False,
        allow_all_dms=True,
        allow_all_channels=True,
        group_context=GroupContextConfig(
            enabled=True,
            visibility="all",
            activation="mention",
            owner_commands_only=True,
            owner_tools_only=True,
            owner_ids=["U_OWNER"],
        ),
    )
    ch = SlackChannel(_echo_processor, cfg, channel_id="slack-mpim")
    ch._bot_user_id = "UBOT"

    # 1. MPIM event is classified as a group conversation (not direct)
    mpim_cmd = {
        "type": "message",
        "channel": "G_MPIM1",
        "channel_type": "mpim",
        "user": "U_GUEST",
        "text": "<@UBOT> /model gpt-5",
        "ts": "10.1",
    }
    parsed_mpim = ch.parse_inbound(mpim_cmd)
    assert parsed_mpim.channel_subject is not None
    assert parsed_mpim.channel_subject.chat_type == "group"
    assert parsed_mpim.metadata["is_group"] is True
    assert parsed_mpim.metadata["is_dm"] is False
    assert parsed_mpim.metadata["group_policy_id"] == "G_MPIM1"
    assert parsed_mpim.metadata["conversation_id"] == "G_MPIM1"

    # Non-owner command in MPIM is rejected by GroupContextManager
    assert ch.group_context_manager.accepts(parsed_mpim) is False
    assert await ch._prepare_inbound(mpim_cmd) is None

    # Non-owner normal mention in MPIM has tools restricted
    mpim_guest_chat = {
        "type": "message",
        "channel": "G_MPIM1",
        "channel_type": "mpim",
        "user": "U_GUEST",
        "text": "<@UBOT> run a tool",
        "ts": "10.2",
    }
    prepared_guest = await ch._prepare_inbound(mpim_guest_chat)
    assert prepared_guest is not None
    guest_msg, guest_subj = prepared_guest
    assert guest_subj.chat_type == "group"
    assert guest_msg.allowed_tools == []

    # Owner command in MPIM is allowed without tool restriction
    mpim_owner_cmd = {
        "type": "message",
        "channel": "G_MPIM1",
        "channel_type": "mpim",
        "user": "U_OWNER",
        "text": "<@UBOT> /model gpt-5",
        "ts": "10.3",
    }
    prepared_owner = await ch._prepare_inbound(mpim_owner_cmd)
    assert prepared_owner is not None
    owner_msg, _ = prepared_owner
    assert owner_msg.allowed_tools is None

    # 2. Channel allowlist also applies to MPIMs (since MPIM is a group, not 1:1 DM)
    restricted_ch = SlackChannel(
        _echo_processor,
        SlackConfig(
            bot_token="xoxb-mpim",
            allow_all_dms=True,
            allow_all_channels=False,
            allowed_channel_ids=["C_ONLY"],
        ),
    )
    assert not restricted_ch._should_handle_event(
        {"type": "message", "channel": "G_MPIM1", "channel_type": "mpim", "user": "U_GUEST", "ts": "11.1"}
    )
    assert restricted_ch._should_handle_event(
        {"type": "message", "channel": "D_DM1", "channel_type": "im", "user": "U_GUEST", "ts": "11.2"}
    )

    # 3. Ordinary 1:1 DM (channel_type="im") retains direct behavior without group restrictions
    dm_cmd = {
        "type": "message",
        "channel": "D_DM1",
        "channel_type": "im",
        "user": "U_GUEST",
        "text": "/model gpt-5",
        "ts": "12.1",
    }
    prepared_dm = await ch._prepare_inbound(dm_cmd)
    assert prepared_dm is not None
    dm_msg, dm_subj = prepared_dm
    assert dm_subj.chat_type == "direct"
    assert dm_msg.metadata["is_dm"] is True
    assert dm_msg.metadata["is_group"] is False
    assert "conversation_id" not in dm_msg.metadata
    assert dm_msg.allowed_tools is None

    await ch.stop()
    await restricted_ch.stop()


@pytest.mark.asyncio
async def test_slack_thread_isolation_isolates_passive_context_and_cleanup() -> None:
    from octop_gateway.group_context import GroupContextConfig

    seen_contexts: list[tuple[str, list[str], list[str] | None]] = []

    async def _recording_processor(msg: InboundMessage) -> AsyncIterator[MessageEvent]:
        ctx_id = msg.group_context.conversation_id if msg.group_context else ""
        ctx_texts = [m.text for m in msg.group_context.messages] if msg.group_context else []
        seen_contexts.append((ctx_id, ctx_texts, msg.allowed_tools))
        yield MessageEvent.text(f"Ack: {msg.text}")
        yield MessageEvent.completed()

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(return_value=_FakeResponse(200, {"ok": True, "ts": "999.1"}))
    fake_http.close = AsyncMock()

    # Enable group_context ONLY via channel-level override C_ISO (proving policy key != thread context key)
    cfg = SlackConfig(
        bot_token="xoxb-iso",
        thread_isolation=True,
        fetch_thread_context=False,
        thread_follow_up=False,
        group_context=GroupContextConfig.from_dict(
            {
                "enabled": False,
                "groups": {
                    "C_ISO": {
                        "enabled": True,
                        "visibility": "all",
                        "activation": "mention",
                        "history": "recent",
                        "clear_after_reply": True,
                        "owner_commands_only": True,
                        "owner_tools_only": True,
                        "owner_ids": ["U_OWNER"],
                    }
                },
            }
        ),
    )
    ch = SlackChannel(_recording_processor, cfg, channel_id="slack-iso")
    ch._bot_user_id = "UBOT"
    ch._http = fake_http

    # Interleaved passive chatter across Thread A (100.0) and Thread B (200.0)
    await ch.handle_inbound(
        {
            "type": "message",
            "channel": "C_ISO",
            "channel_type": "channel",
            "user": "U_GUEST",
            "text": "Thread A passive 1",
            "ts": "100.1",
            "thread_ts": "100.0",
        }
    )
    await ch.handle_inbound(
        {
            "type": "message",
            "channel": "C_ISO",
            "channel_type": "channel",
            "user": "U_GUEST",
            "text": "Thread B passive 1",
            "ts": "200.1",
            "thread_ts": "200.0",
        }
    )
    await ch.handle_inbound(
        {
            "type": "message",
            "channel": "C_ISO",
            "channel_type": "channel",
            "user": "U_GUEST",
            "text": "Thread A passive 2",
            "ts": "100.2",
            "thread_ts": "100.0",
        }
    )
    assert seen_contexts == []

    # Channel-level owner_commands_only still blocks non-owner /command inside isolated thread
    assert (
        await ch._prepare_inbound(
            {
                "type": "app_mention",
                "channel": "C_ISO",
                "channel_type": "channel",
                "user": "U_GUEST",
                "text": "<@UBOT> /model gpt-5",
                "ts": "100.25",
                "thread_ts": "100.0",
            }
        )
        is None
    )

    # Reply in Thread B (by non-owner): receives ONLY Thread B context, applies channel-level tool restriction,
    # and clears ONLY Thread B's passive buffer.
    await ch.handle_inbound(
        {
            "type": "app_mention",
            "channel": "C_ISO",
            "channel_type": "channel",
            "user": "U_GUEST",
            "text": "<@UBOT> handle B",
            "ts": "200.2",
            "thread_ts": "200.0",
        }
    )
    assert len(seen_contexts) == 1
    assert seen_contexts[0] == ("C_ISO:thread:200.0", ["Thread B passive 1"], [])

    # Reply in Thread A (by owner): STILL receives Thread A's buffered messages (not cleared by B),
    # contains zero Thread B messages, and clears Thread A's buffer after replying.
    await ch.handle_inbound(
        {
            "type": "app_mention",
            "channel": "C_ISO",
            "channel_type": "channel",
            "user": "U_OWNER",
            "text": "<@UBOT> handle A",
            "ts": "100.3",
            "thread_ts": "100.0",
        }
    )
    assert len(seen_contexts) == 2
    assert seen_contexts[1] == ("C_ISO:thread:100.0", ["Thread A passive 1", "Thread A passive 2"], None)

    # Subsequent trigger in Thread A confirms Thread A's passive buffer was cleared after reply
    await ch.handle_inbound(
        {
            "type": "app_mention",
            "channel": "C_ISO",
            "channel_type": "channel",
            "user": "U_OWNER",
            "text": "<@UBOT> follow-up A",
            "ts": "100.4",
            "thread_ts": "100.0",
        }
    )
    assert len(seen_contexts) == 3
    assert seen_contexts[2] == ("C_ISO:thread:100.0", [], None)

    await ch.stop()


class TestSlackEditStreamOutputProtection:
    """Real SlackChannel streaming_mode='edit' regressions for per-dispatch output protection."""

    @staticmethod
    def _config(*, show_thinking: bool = False, status_reactions: bool = False) -> SlackConfig:
        return SlackConfig(
            bot_token="xoxb-stream-test",
            streaming_mode="edit",
            show_thinking=show_thinking,
            status_reactions=status_reactions,
        )

    @staticmethod
    def _dm_event(ts: str = "100.1", text: str = "hello") -> dict[str, Any]:
        return {
            "type": "message",
            "channel": "D1",
            "channel_type": "im",
            "user": "U1",
            "text": text,
            "ts": ts,
        }

    @staticmethod
    def _wire_http(channel: SlackChannel) -> list[tuple[str, dict[str, Any]]]:
        calls: list[tuple[str, dict[str, Any]]] = []
        post_count = 0

        def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
            nonlocal post_count
            endpoint = url.rsplit("/", 1)[-1]
            payload = dict(kwargs.get("json") or {})
            calls.append((endpoint, payload))
            if endpoint == "chat.postMessage":
                post_count += 1
                return _FakeResponse(200, {"ok": True, "ts": f"999.{post_count}"})
            return _FakeResponse(200, {"ok": True})

        fake_http = MagicMock()
        fake_http.closed = False
        fake_http.post = MagicMock(side_effect=_fake_post)
        fake_http.close = AsyncMock()
        channel._http = fake_http
        return calls

    @staticmethod
    def _patch_slack_clock(monkeypatch: pytest.MonkeyPatch, clock: list[float]) -> None:
        from types import SimpleNamespace

        import octop_gateway.channels.slack as slack_mod

        monkeypatch.setattr(slack_mod, "time", SimpleNamespace(time=time.time, monotonic=lambda: clock[0]))

    @pytest.mark.asyncio
    async def test_empty_filter_suppresses_all_slack_posts_and_updates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from octop_gateway.channel import InboundReceipt

        clock = [10.0]
        self._patch_slack_clock(monkeypatch, clock)

        async def processor(_msg: InboundMessage) -> Any:
            yield MessageEvent.delta("secret part 1 ")
            clock[0] += 1.0
            yield MessageEvent.delta("secret part 2")
            yield MessageEvent.completed()

        ch = SlackChannel(processor, self._config())
        calls = self._wire_http(ch)
        finish_calls: list[bool] = []

        receipt = await ch.dispatch_inbound(
            self._dm_event(),
            receipt=InboundReceipt(
                text_filter=lambda _text: "",
                text_filter_finish=lambda: finish_calls.append(True) or "",
            ),
        )

        message_calls = [c for c in calls if c[0] in ("chat.postMessage", "chat.update")]
        assert message_calls == []
        assert finish_calls == [True]
        assert receipt.attempted is False
        assert receipt.delivered is False
        await ch.stop()

    @pytest.mark.asyncio
    async def test_filter_exception_and_non_str_fail_closed_and_suppress_finish(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from octop_gateway.channel import InboundReceipt

        clock = [10.0]
        self._patch_slack_clock(monkeypatch, clock)

        async def processor(_msg: InboundMessage) -> Any:
            yield MessageEvent.delta("raw chunk 1 ")
            clock[0] += 1.0
            yield MessageEvent.delta("raw chunk 2")
            yield MessageEvent.completed()

        def failing_filter(_text: str) -> str:
            raise RuntimeError("filter boom")

        finish_calls: list[bool] = []
        ch = SlackChannel(processor, self._config())
        calls = self._wire_http(ch)

        receipt = await ch.dispatch_inbound(
            self._dm_event("100.1"),
            receipt=InboundReceipt(
                text_filter=failing_filter,
                text_filter_finish=lambda: finish_calls.append(True) or "leaked tail",
            ),
        )
        assert [c for c in calls if c[0] in ("chat.postMessage", "chat.update")] == []
        assert finish_calls == []
        assert receipt.attempted is False
        assert receipt.delivered is False
        await ch.stop()

        # Non-str return from text_filter also fails closed and suppresses finish.
        ch2 = SlackChannel(processor, self._config())
        calls2 = self._wire_http(ch2)
        finish_calls2: list[bool] = []
        receipt2 = await ch2.dispatch_inbound(
            self._dm_event("100.2"),
            receipt=InboundReceipt(
                text_filter=lambda _text: None,  # type: ignore[arg-type, return-value]
                text_filter_finish=lambda: finish_calls2.append(True) or "leaked tail",
            ),
        )
        assert [c for c in calls2 if c[0] in ("chat.postMessage", "chat.update")] == []
        assert finish_calls2 == []
        assert receipt2.attempted is False
        assert receipt2.delivered is False
        await ch2.stop()

        # Exception inside text_filter_finish also fails closed without posting.
        ch3 = SlackChannel(processor, self._config())
        calls3 = self._wire_http(ch3)

        def failing_finish() -> str:
            raise RuntimeError("finish boom")

        receipt3 = await ch3.dispatch_inbound(
            self._dm_event("100.3"),
            receipt=InboundReceipt(
                text_filter=lambda _text: "",
                text_filter_finish=failing_finish,
            ),
        )
        assert [c for c in calls3 if c[0] in ("chat.postMessage", "chat.update")] == []
        assert receipt3.attempted is False
        assert receipt3.delivered is False
        await ch3.stop()

    @pytest.mark.asyncio
    async def test_stateful_incremental_filter_sees_each_chunk_once_and_flushes_tail_in_place(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from octop_gateway.channel import InboundReceipt

        clock = [10.0]
        self._patch_slack_clock(monkeypatch, clock)

        async def processor(_msg: InboundMessage) -> Any:
            yield MessageEvent.delta("alpha ")
            clock[0] += 1.0
            yield MessageEvent.delta("beta")
            clock[0] += 1.0
            yield MessageEvent.delta(" gamma")
            yield MessageEvent.completed()

        seen_chunks: list[str] = []
        finish_calls: list[bool] = []

        def stateful_filter(chunk: str) -> str:
            seen_chunks.append(chunk)
            return chunk.upper()

        def finish_filter() -> str:
            finish_calls.append(True)
            return " [END]"

        ch = SlackChannel(processor, self._config())
        calls = self._wire_http(ch)

        receipt = await ch.dispatch_inbound(
            self._dm_event(),
            receipt=InboundReceipt(text_filter=stateful_filter, text_filter_finish=finish_filter),
        )

        assert seen_chunks == ["alpha ", "beta", " gamma"]
        assert finish_calls == [True]
        posts = [p for ep, p in calls if ep == "chat.postMessage"]
        updates = [p for ep, p in calls if ep == "chat.update"]
        assert len(posts) == 1
        assert posts[0]["text"] == "ALPHA"
        assert updates
        assert updates[-1]["ts"] == "999.1"
        assert updates[-1]["text"] == "ALPHA BETA GAMMA [END]"
        assert receipt.attempted is True
        assert receipt.delivered is True
        await ch.stop()

    @pytest.mark.asyncio
    async def test_buffering_filter_flushes_on_finish_without_premature_post(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from octop_gateway.channel import InboundReceipt

        clock = [10.0]
        self._patch_slack_clock(monkeypatch, clock)

        async def processor(_msg: InboundMessage) -> Any:
            yield MessageEvent.delta("Hel")
            clock[0] += 1.0
            yield MessageEvent.delta("lo")
            yield MessageEvent.completed()

        buf: list[str] = []

        def buffering_filter(chunk: str) -> str:
            buf.append(chunk)
            return ""

        def finish_filter() -> str:
            return "".join(buf)

        ch = SlackChannel(processor, self._config())
        calls = self._wire_http(ch)

        receipt = await ch.dispatch_inbound(
            self._dm_event(),
            receipt=InboundReceipt(text_filter=buffering_filter, text_filter_finish=finish_filter),
        )

        posts = [p for ep, p in calls if ep == "chat.postMessage"]
        updates = [p for ep, p in calls if ep == "chat.update"]
        assert len(posts) == 1
        assert posts[0]["text"] == "Hello"
        assert updates == []
        assert receipt.attempted is True
        assert receipt.delivered is True
        await ch.stop()

    @pytest.mark.asyncio
    async def test_suppress_thinking_hides_thinking_without_mutating_channel(self) -> None:
        from octop_gateway.channel import InboundReceipt

        async def processor(_msg: InboundMessage) -> Any:
            yield MessageEvent.thinking_delta("secret step 1")
            yield MessageEvent.thinking("secret step 2")
            yield MessageEvent.delta("<think>inline ")
            yield MessageEvent.delta("secret</think>Visible answer.")
            yield MessageEvent.completed()

        ch = SlackChannel(processor, self._config(show_thinking=True))
        calls = self._wire_http(ch)

        receipt = await ch.dispatch_inbound(
            self._dm_event(),
            receipt=InboundReceipt(suppress_thinking=True),
        )

        texts = [p["text"] for ep, p in calls if ep in ("chat.postMessage", "chat.update")]
        assert texts
        assert texts[-1] == "Visible answer."
        assert all("secret" not in text for text in texts)
        assert ch._constraints.show_thinking is True
        assert receipt.delivered is True
        await ch.stop()

    @pytest.mark.asyncio
    async def test_cancelled_stream_restores_persistent_settings_for_next_dispatch(self) -> None:
        from octop_gateway.channel import InboundReceipt

        started = asyncio.Event()
        release = asyncio.Event()
        call_index = 0

        async def processor(_msg: InboundMessage) -> Any:
            nonlocal call_index
            call_index += 1
            if call_index == 1:
                yield MessageEvent.thinking_delta("hidden during cancel")
                yield MessageEvent.delta("first part")
                started.set()
                await release.wait()
                yield MessageEvent.completed()
            else:
                yield MessageEvent.thinking("restored thought")
                yield MessageEvent.delta("normal answer")
                yield MessageEvent.completed()

        ch = SlackChannel(processor, self._config(show_thinking=True, status_reactions=True))
        calls = self._wire_http(ch)

        task = asyncio.create_task(
            ch.dispatch_inbound(
                self._dm_event("100.1"),
                receipt=InboundReceipt(suppress_thinking=True, text_filter=lambda _text: ""),
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert [c for c in calls if c[0] in ("chat.postMessage", "chat.update")] == []
        assert any(ep == "reactions.remove" and p.get("name") == "hourglass_flowing_sand" for ep, p in calls)
        assert not any(ep == "reactions.add" and p.get("name") == "white_check_mark" for ep, p in calls)
        assert ch._constraints.show_thinking is True

        calls.clear()
        next_receipt = await ch.dispatch_inbound(self._dm_event("100.2"))
        texts = [p["text"] for ep, p in calls if ep in ("chat.postMessage", "chat.update")]
        assert any("restored thought" in text for text in texts)
        assert any("normal answer" in text for text in texts)
        assert next_receipt.delivered is True
        await ch.stop()
