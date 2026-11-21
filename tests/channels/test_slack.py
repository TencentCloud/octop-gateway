"""Unit tests for SlackChannel and SlackConfig."""

from __future__ import annotations

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
