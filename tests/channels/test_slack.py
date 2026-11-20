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
