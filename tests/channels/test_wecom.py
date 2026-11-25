"""Unit tests for WeCom (Enterprise WeChat) channel.

Tests cover:
- Config creation and field storage
- Channel initialization
- Inbound message parsing (text, passthrough)
- Session ID resolution

No network calls — all tests are pure unit tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from octop_gateway.channel import InboundReceipt
from octop_gateway.channels.wecom import WeComChannel, WeComConfig
from octop_gateway.models import (
    ChannelSubject,
    FileContent,
    ImageContent,
    InboundMessage,
    MessageEvent,
    MessageEventType,
    TextContent,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> WeComConfig:
    return WeComConfig(bot_id="bot_test_001", secret="secret_xyz_123")


async def _noop_processor(msg: InboundMessage):
    yield MessageEvent.completed()


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestWeComChannelUnit:
    """Unit tests for WeCom channel logic (no API calls)."""

    def test_config_creation(self) -> None:
        """WeComConfig stores bot_id, secret, and optional ws_url."""
        config = _make_config()
        assert config.bot_id == "bot_test_001"
        assert config.secret == "secret_xyz_123"
        assert config.ws_url == ""

    def test_config_with_ws_url(self) -> None:
        """WeComConfig accepts custom ws_url."""
        config = WeComConfig(bot_id="b1", secret="s1", ws_url="wss://custom.example.com")
        assert config.ws_url == "wss://custom.example.com"

    def test_channel_init(self) -> None:
        """WeComChannel accepts processor, config, constraints kwargs."""
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        assert ch.channel_type == "wecom"
        assert ch._config.bot_id == "bot_test_001"
        assert ch._config.secret == "secret_xyz_123"
        assert ch._connected is False
        assert ch._running is False

    def test_channel_init_with_constraints(self) -> None:
        """WeComChannel accepts optional constraints."""
        from octop_gateway.constraints import ChannelConstraints

        custom = ChannelConstraints(reply_timeout=20.0)
        ch = WeComChannel(processor=_noop_processor, config=_make_config(), constraints=custom)
        assert ch._constraints.reply_timeout == 20.0

    def test_parse_inbound_text(self) -> None:
        """Raw payload with msgtype=text parses to InboundMessage with correct fields."""
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        payload = {
            "msgid": "msg_wecom_001",
            "msgtype": "text",
            "text": {"content": "hello"},
            "from": {"userid": "T48500024A"},
            "chattype": "single",
            "chatid": "",
            "response_url": "https://qyapi.weixin.qq.com/callback/reply",
        }
        msg = ch.parse_inbound(payload)

        assert isinstance(msg, InboundMessage)
        assert msg.channel_type == "wecom"
        assert msg.channel_subject is not None
        assert msg.channel_subject.subject_id == "T48500024A"
        assert msg.text == "hello"
        assert msg.metadata["msgid"] == "msg_wecom_001"
        assert msg.metadata["chat_type"] == "single"
        assert msg.metadata["response_url"] == "https://qyapi.weixin.qq.com/callback/reply"

    def test_parse_inbound_text_with_chat_id(self) -> None:
        """When chattype is group and chatid is present, subject_id routes to the group chatid."""
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        payload = {
            "msgid": "msg_002",
            "msgtype": "text",
            "text": {"content": "group msg"},
            "from": {"userid": "user_abc"},
            "chattype": "group",
            "chatid": "chat_room_123",
            "quote": {"text": {"content": "quoted text"}},
        }
        msg = ch.parse_inbound(payload)

        assert msg.channel_subject is not None
        assert msg.channel_subject.subject_id == "chat_room_123"
        assert msg.channel_subject.chat_type == "group"
        assert msg.metadata["sender_id"] == "user_abc"
        assert msg.metadata["reply_quote_text"] == "quoted text"
        assert msg.text == "group msg"
        assert ch._is_duplicate("msg_002") is False
        assert ch._is_duplicate("msg_002") is True
        assert ch.dropped_duplicate == 1

    @pytest.mark.asyncio
    async def test_handle_inbound_enforces_group_context_owner_commands(self) -> None:
        """WeCom WS streaming path enforces group_context owner_commands_only."""
        from octop_gateway.group_context import GroupContextConfig

        called: list[str] = []

        async def _rec_processor(msg: InboundMessage):
            called.append(msg.text)
            yield MessageEvent.delta("ok")
            yield MessageEvent.completed()

        cfg = WeComConfig(
            bot_id="b1",
            secret="s1",
            group_context=GroupContextConfig(
                enabled=True,
                visibility="all",
                activation="mention",
                owner_commands_only=True,
                owner_ids=["owner_1"],
            ),
        )
        ch = WeComChannel(processor=_rec_processor, config=cfg)
        ws_client = AsyncMock()
        ws_client.reply_stream = AsyncMock()
        ch._ws_client = ws_client

        guest_payload = {
            "msgid": "m1",
            "msgtype": "text",
            "text": {"content": "/new"},
            "from": {"userid": "guest_1"},
            "chattype": "group",
            "chatid": "room_1",
            "_frame": {"req_id": "f1"},
        }
        await ch.handle_inbound(guest_payload)
        assert called == []
        ws_client.reply_stream.assert_not_awaited()

        owner_payload = {
            "msgid": "m2",
            "msgtype": "text",
            "text": {"content": "/new"},
            "from": {"userid": "owner_1"},
            "chattype": "group",
            "chatid": "room_1",
            "_frame": {"req_id": "f2"},
        }
        await ch.handle_inbound(owner_payload)
        assert called == ["/new"]
        ws_client.reply_stream.assert_awaited()

    def test_parse_inbound_returns_existing(self) -> None:
        """Passing an InboundMessage directly returns it unchanged."""
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        original = InboundMessage(
            channel_id="wecom",
            content=[TextContent(text="passthrough")],
            channel_subject=ChannelSubject(subject_id="user_pass"),
        )
        result = ch.parse_inbound(original)
        assert result is original
        assert result.text == "passthrough"

    def test_parse_inbound_image(self) -> None:
        """Image payload produces ImageContent."""
        from octop_gateway.models import ImageContent

        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        payload = {
            "msgtype": "image",
            "image": {"url": "https://example.com/pic.jpg", "aeskey": "image-key"},
            "from": {"userid": "img_user"},
        }
        msg = ch.parse_inbound(payload)
        images = [p for p in msg.content if isinstance(p, ImageContent)]
        assert len(images) == 1
        assert images[0].url == "https://example.com/pic.jpg"
        assert msg.metadata["_media_aes_keys"]["https://example.com/pic.jpg"] == "image-key"

    def test_parse_inbound_file(self) -> None:
        """File payload produces FileContent."""
        from octop_gateway.models import FileContent

        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        payload = {
            "msgtype": "file",
            "file": {
                "url": "https://example.com/doc.pdf",
                "file_name": "report.pdf",
                "aeskey": "file-key",
            },
            "from": {"userid": "file_user"},
        }
        msg = ch.parse_inbound(payload)
        files = [p for p in msg.content if isinstance(p, FileContent)]
        assert len(files) == 1
        assert files[0].url == "https://example.com/doc.pdf"
        assert files[0].filename == "report.pdf"
        assert msg.metadata["_media_aes_keys"]["https://example.com/doc.pdf"] == "file-key"

    def test_parse_inbound_mixed_preserves_text_image_and_aeskey(self) -> None:
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        msg = ch.parse_inbound(
            {
                "msgtype": "mixed",
                "mixed": {
                    "msg_item": [
                        {"msgtype": "text", "text": {"content": "see image"}},
                        {
                            "msgtype": "image",
                            "image": {
                                "url": "https://example.com/mixed.png",
                                "aeskey": "mixed-key",
                            },
                        },
                    ]
                },
                "from": {"userid": "mixed-user"},
            }
        )

        assert msg.text == "see image"
        images = [part for part in msg.content if isinstance(part, ImageContent)]
        assert len(images) == 1
        assert msg.metadata["_media_aes_keys"][images[0].url] == "mixed-key"

    @pytest.mark.asyncio
    async def test_persist_media_downloads_and_decrypts_with_aeskey(self) -> None:
        """Inbound media must use the SDK decrypting download path."""

        class _MemoryBackend:
            def __init__(self) -> None:
                self.saved: dict[str, bytes] = {}

            async def save(self, data: bytes, key: str) -> None:
                self.saved[key] = data

            async def read(self, key: str) -> bytes:
                return self.saved[key]

            async def exists(self, key: str) -> bool:
                return key in self.saved

        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        ch._ws_client = AsyncMock()
        ch._ws_client.download_file.return_value = {
            "buffer": b"%PDF-decrypted",
            "filename": "paper.pdf",
        }
        backend = _MemoryBackend()
        ch.set_media_backend(backend)
        message = ch.parse_inbound(
            {
                "msgtype": "file",
                "file": {"url": "https://example.com/encrypted", "aeskey": "secret-key"},
                "from": {"userid": "file_user"},
            }
        )

        await ch._persist_media(message)

        ch._ws_client.download_file.assert_awaited_once_with("https://example.com/encrypted", "secret-key")
        media = next(part for part in message.content if isinstance(part, FileContent))
        assert media.filename == "paper.pdf"
        assert media.mime_type == "application/pdf"
        assert media.local_path is not None
        assert backend.saved[media.local_path] == b"%PDF-decrypted"

    def test_parse_inbound_empty_payload(self) -> None:
        """Empty dict produces InboundMessage with empty TextContent fallback."""
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        msg = ch.parse_inbound({})
        assert msg.channel_type == "wecom"
        assert msg.channel_subject is not None
        assert msg.channel_subject.subject_id == "unknown"
        # Should have at least one content part (empty text fallback)
        assert len(msg.content) >= 1

    def test_default_constraints(self) -> None:
        """Channel provides reasonable default constraints."""
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        defaults = ch._default_constraints()
        assert defaults.reply_timeout == 10.0
        assert defaults.timeout_strategy == "placeholder"
        assert defaults.show_thinking is False

    @pytest.mark.asyncio
    async def test_send_text_proactive_uses_send_message(self) -> None:
        """No frame/response_url → proactive send_message (cron path)."""
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        ch._ws_client = AsyncMock()
        subject = ChannelSubject(
            subject_id="T48500024A",
            metadata={"chat_type": "single"},
        )

        await ch._send_text(subject, "cron hello")

        ch._ws_client.send_message.assert_awaited_once_with(
            "T48500024A",
            {"msgtype": "markdown", "markdown": {"content": "cron hello"}},
        )

    @pytest.mark.asyncio
    async def test_send_media_uploads_and_replies_natively(self) -> None:
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        ws = AsyncMock()
        ws.upload_media.return_value = {"media_id": "MEDIA_123"}
        ch._ws_client = ws
        ch.load_media_bytes = AsyncMock(return_value=(b"pdf-bytes", "application/pdf"))  # type: ignore[method-assign]
        frame = {"headers": {"req_id": "request-1"}}
        subject = ChannelSubject(subject_id="user-1", metadata={"_frame": frame, "_ws_client": ws})

        await ch._send_media(subject, FileContent(filename="paper.pdf", local_path="paper.pdf"))

        ws.upload_media.assert_awaited_once_with(b"pdf-bytes", type="file", filename="paper.pdf")
        ws.reply_media.assert_awaited_once_with(frame, "file", "MEDIA_123")

    @pytest.mark.asyncio
    async def test_send_media_proactive_uses_chat_id(self) -> None:
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        ws = AsyncMock()
        ws.upload_media.return_value = {"media_id": "IMAGE_123"}
        ch._ws_client = ws
        ch.load_media_bytes = AsyncMock(return_value=(b"image-bytes", "image/png"))  # type: ignore[method-assign]
        subject = ChannelSubject(subject_id="user-1", metadata={"chat_id": "chat-1"})

        await ch._send_media(subject, ImageContent(local_path="image.png", mime_type="image/png"))

        ws.send_media_message.assert_awaited_once_with("chat-1", "image", "IMAGE_123")

    def test_resolve_push_subject_enriches_sparse_metadata(self) -> None:
        ch = WeComChannel(processor=_noop_processor, config=_make_config())
        subject = ChannelSubject(subject_id="T48500024A", metadata={})
        resolved = ch.resolve_push_subject(subject)
        assert resolved.metadata.get("chat_id") == "T48500024A"


# ---------------------------------------------------------------------------
# Stream override: rate-limit / first-token timeout / error placeholder
# ---------------------------------------------------------------------------


class _FakeWSClient:
    """Captures every reply_stream call for assertion."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.media_replies: list[tuple[dict, str, str]] = []

    async def reply_stream(self, *, frame, stream_id, content, finish):
        self.calls.append({"content": content, "finish": finish, "stream_id": stream_id})

    async def upload_media(self, data, *, type, filename):
        return {"media_id": "MEDIA_FROM_EVENT"}

    async def reply_media(self, frame, media_type, media_id):
        self.media_replies.append((frame, media_type, media_id))


def _make_stream_payload(ws: _FakeWSClient, *, content: str = "hello") -> dict:
    """Build the dict shape WeComChannel.parse_inbound expects.

    The channel injects ``self._ws_client`` into metadata, so callers must
    set ``ch._ws_client = ws`` before invoking ``handle_inbound``.
    """
    return {
        "msgid": "m1",
        "msgtype": "text",
        "from": {"userid": "user_001"},
        "text": {"content": content},
        "_frame": {"body": {}},
    }


class TestWeComStreamConstraints:
    """Verify the WeCom stream override honours rate-limit, first-token
    timeout, and uses placeholder_text for the error path.
    """

    @pytest.mark.asyncio
    async def test_stream_acquires_one_rate_slot_for_whole_reply(self) -> None:
        """A single user message → one rate-limit acquire, regardless of
        how many partial reply_stream chunks are emitted.
        """
        from octop_gateway.constraints import ChannelConstraints

        async def chatty(msg: InboundMessage):
            for piece in ("Hello ", "world", "!"):
                yield MessageEvent.delta(piece)
            yield MessageEvent.completed()

        ch = WeComChannel(processor=chatty, config=_make_config())
        # Disable timeout for this test — we only care about rate-limit.
        ch._constraints = ChannelConstraints(
            send_rate_limit=(2, 5.0),
            reply_timeout=0,
            timeout_strategy="none",
        )

        counter = [0]

        async def _counting_acquire():
            counter[0] += 1

        ch._acquire_rate_slot = _counting_acquire  # type: ignore[method-assign]

        ws = _FakeWSClient()
        ch._ws_client = ws
        payload = _make_stream_payload(ws)
        # parse_inbound preserves _ws_client / _frame in metadata; verify.
        await ch.handle_inbound(payload)

        assert counter[0] == 1
        # And reply_stream actually streamed the chunks (more than 1 call).
        assert len(ws.calls) >= 2
        assert ws.calls[-1]["finish"] is True

    @pytest.mark.asyncio
    async def test_message_event_media_is_uploaded_instead_of_dropped(self) -> None:
        async def returns_file(msg: InboundMessage):
            yield MessageEvent(
                type=MessageEventType.MESSAGE,
                content=[FileContent(filename="paper.pdf", local_path="paper.pdf")],
            )
            yield MessageEvent.completed()

        ch = WeComChannel(processor=returns_file, config=_make_config())
        ch._constraints.reply_timeout = 0
        ch.load_media_bytes = AsyncMock(return_value=(b"pdf", "application/pdf"))  # type: ignore[method-assign]
        ws = _FakeWSClient()
        ch._ws_client = ws

        await ch.handle_inbound(_make_stream_payload(ws))

        assert ws.media_replies
        assert ws.media_replies[0][1:] == ("file", "MEDIA_FROM_EVENT")

    @pytest.mark.asyncio
    async def test_first_token_timeout_emits_placeholder(self) -> None:
        """If the first event takes longer than reply_timeout * 0.8, a
        placeholder is streamed (finish=False) before the real content.
        """
        import asyncio as _asyncio

        from octop_gateway.constraints import ChannelConstraints

        async def slow_first(msg: InboundMessage):
            # First event is delayed; placeholder should fire first.
            await _asyncio.sleep(0.3)
            yield MessageEvent.delta("late hello")
            yield MessageEvent.completed()

        ch = WeComChannel(processor=slow_first, config=_make_config())
        ch._constraints = ChannelConstraints(
            reply_timeout=0.1,  # 0.1 * 0.8 = 80 ms
            timeout_strategy="placeholder",
            placeholder_texts=["__PLACEHOLDER__"],
        )

        # Bypass rate-limit for clarity.
        async def _noop():
            return None

        ch._acquire_rate_slot = _noop  # type: ignore[method-assign]

        ws = _FakeWSClient()
        ch._ws_client = ws
        await ch.handle_inbound(_make_stream_payload(ws))

        # First call should be the placeholder (finish=False).
        assert ws.calls, "expected at least the placeholder call"
        assert ws.calls[0]["content"] == "__PLACEHOLDER__"
        assert ws.calls[0]["finish"] is False
        # Final call should still complete the stream with the real content.
        assert ws.calls[-1]["finish"] is True
        assert "late hello" in ws.calls[-1]["content"]

    @pytest.mark.asyncio
    async def test_first_token_timeout_skipped_when_strategy_none(self) -> None:
        """timeout_strategy='none' must not wrap the first token in
        wait_for, even if reply_timeout > 0.
        """
        from octop_gateway.constraints import ChannelConstraints

        async def fast(msg: InboundMessage):
            yield MessageEvent.delta("hi")
            yield MessageEvent.completed()

        ch = WeComChannel(processor=fast, config=_make_config())
        ch._constraints = ChannelConstraints(
            reply_timeout=0.1,
            timeout_strategy="none",  # ← disables placeholder wrapping
            placeholder_texts=["NEVER"],
        )

        async def _noop():
            return None

        ch._acquire_rate_slot = _noop  # type: ignore[method-assign]

        ws = _FakeWSClient()
        ch._ws_client = ws
        await ch.handle_inbound(_make_stream_payload(ws))

        # No placeholder should appear among the streamed chunks.
        for call in ws.calls:
            assert "NEVER" not in call["content"]

    @pytest.mark.asyncio
    async def test_error_path_uses_constraints_placeholder_text(self) -> None:
        """An exception inside the stream loop emits a finish=True chunk
        whose content comes from ``placeholder_text``, not a hardcoded
        Chinese string.
        """
        from octop_gateway.constraints import ChannelConstraints

        async def explodes(msg: InboundMessage):
            yield MessageEvent.delta("partial...")
            raise RuntimeError("boom")

        ch = WeComChannel(processor=explodes, config=_make_config())
        ch._constraints = ChannelConstraints(
            reply_timeout=0,
            timeout_strategy="none",
            placeholder_texts=["__ERROR_PLACEHOLDER__"],
        )

        async def _noop():
            return None

        ch._acquire_rate_slot = _noop  # type: ignore[method-assign]

        ws = _FakeWSClient()
        ch._ws_client = ws
        await ch.handle_inbound(_make_stream_payload(ws))

        # The terminating chunk must be the configured placeholder.
        assert ws.calls
        assert ws.calls[-1]["finish"] is True
        assert ws.calls[-1]["content"] == "__ERROR_PLACEHOLDER__"


class TestWeComDispatchProtection:
    """Real WeComChannel WebSocket streaming tests for InboundReceipt protection."""

    @staticmethod
    def _configure_channel(ch: WeComChannel, *, show_thinking: bool = False) -> _FakeWSClient:
        ch._constraints.reply_timeout = 0
        ch._constraints.timeout_strategy = "none"
        ch._constraints.show_thinking = show_thinking

        async def _noop() -> None:
            return None

        ch._acquire_rate_slot = _noop  # type: ignore[method-assign]
        ws = _FakeWSClient()
        ch._ws_client = ws
        return ws

    @pytest.mark.asyncio
    async def test_websocket_inbound_invokes_input_filter_exactly_once(self) -> None:
        processor_calls: list[str] = []

        async def processor(msg: InboundMessage):
            processor_calls.append(msg.text)
            yield MessageEvent.delta("admitted reply")
            yield MessageEvent.completed()

        ch = WeComChannel(processor=processor, config=_make_config())
        ws = self._configure_channel(ch)

        filter_calls = 0

        def one_shot_filter(_msg: InboundMessage) -> bool:
            nonlocal filter_calls
            filter_calls += 1
            return filter_calls == 1

        receipt = await ch.dispatch_inbound(
            _make_stream_payload(ws, content="ping"),
            receipt=InboundReceipt(input_filter=one_shot_filter),
        )

        assert filter_calls == 1
        assert processor_calls == ["ping"]
        assert ws.calls
        assert ws.calls[-1]["finish"] is True
        assert ws.calls[-1]["content"] == "admitted reply"
        assert receipt.delivered is True

        # Rejecting input_filter must skip processing and streaming completely.
        ws2 = self._configure_channel(ch)
        rejected_calls = 0

        def reject_filter(_msg: InboundMessage) -> bool:
            nonlocal rejected_calls
            rejected_calls += 1
            return False

        receipt2 = await ch.dispatch_inbound(
            _make_stream_payload(ws2, content="blocked"),
            receipt=InboundReceipt(input_filter=reject_filter),
        )
        assert rejected_calls == 1
        assert processor_calls == ["ping"]
        assert ws2.calls == []
        assert receipt2.attempted is False
        assert receipt2.delivered is False

    @pytest.mark.asyncio
    async def test_empty_text_filter_suppresses_stream_and_checkmark_fallback(self) -> None:
        async def processor(_msg: InboundMessage):
            yield MessageEvent.delta("secret")
            yield MessageEvent.completed()

        ch = WeComChannel(processor=processor, config=_make_config())
        ws = self._configure_channel(ch)

        receipt = await ch.dispatch_inbound(
            _make_stream_payload(ws),
            receipt=InboundReceipt(text_filter=lambda _text: ""),
        )

        assert ws.calls == []
        assert receipt.attempted is False
        assert receipt.delivered is False

    @pytest.mark.asyncio
    async def test_raising_and_non_string_text_filter_fail_closed(self) -> None:
        async def processor(_msg: InboundMessage):
            yield MessageEvent.delta("first ")
            yield MessageEvent.delta("second")
            yield MessageEvent.completed()

        finish_calls: list[bool] = []

        def raising_filter(_text: str) -> str:
            raise RuntimeError("boom")

        ch = WeComChannel(processor=processor, config=_make_config())
        ws = self._configure_channel(ch)
        receipt = await ch.dispatch_inbound(
            _make_stream_payload(ws),
            receipt=InboundReceipt(
                text_filter=raising_filter,
                text_filter_finish=lambda: finish_calls.append(True) or "tail",
            ),
        )
        assert ws.calls == []
        assert finish_calls == []
        assert receipt.attempted is False
        assert receipt.delivered is False

        # Non-string return also fails closed without leaking raw text.
        ws2 = self._configure_channel(ch)
        receipt2 = await ch.dispatch_inbound(
            _make_stream_payload(ws2),
            receipt=InboundReceipt(text_filter=lambda _text: None),  # type: ignore[arg-type, return-value]
        )
        assert ws2.calls == []
        assert receipt2.attempted is False
        assert receipt2.delivered is False

        # Failure on a later chunk closes the open stream without leaking raw chunk 2.
        ws3 = self._configure_channel(ch)
        seen = 0

        def fail_on_second(text: str) -> str:
            nonlocal seen
            seen += 1
            if seen == 2:
                raise RuntimeError("second chunk boom")
            return text.upper()

        await ch.dispatch_inbound(
            _make_stream_payload(ws3),
            receipt=InboundReceipt(text_filter=fail_on_second),
        )
        assert len(ws3.calls) == 2
        assert (ws3.calls[0]["content"], ws3.calls[0]["finish"]) == ("FIRST ", False)
        assert (ws3.calls[1]["content"], ws3.calls[1]["finish"]) == ("FIRST ", True)

    @pytest.mark.asyncio
    async def test_stateful_incremental_filter_sees_each_chunk_once_and_flushes_tail(self) -> None:
        async def processor(_msg: InboundMessage):
            yield MessageEvent.delta("one ")
            yield MessageEvent.delta("two")
            yield MessageEvent.text("one two three")
            yield MessageEvent.completed()

        seen_chunks: list[str] = []
        finish_calls: list[bool] = []

        def stateful_filter(chunk: str) -> str:
            seen_chunks.append(chunk)
            return chunk.upper()

        def finish_filter() -> str:
            finish_calls.append(True)
            return " [DONE]"

        ch = WeComChannel(processor=processor, config=_make_config())
        ws = self._configure_channel(ch)

        receipt = await ch.dispatch_inbound(
            _make_stream_payload(ws),
            receipt=InboundReceipt(text_filter=stateful_filter, text_filter_finish=finish_filter),
        )

        assert seen_chunks == ["one ", "two", " three"]
        assert finish_calls == [True]
        assert [c["content"] for c in ws.calls] == ["ONE ", "ONE TWO", "ONE TWO THREE [DONE]"]
        assert ws.calls[-1]["finish"] is True
        assert receipt.attempted is True
        assert receipt.delivered is True

    @pytest.mark.asyncio
    async def test_suppress_thinking_hides_thinking_without_mutating_channel(self) -> None:
        async def processor(_msg: InboundMessage):
            yield MessageEvent.thinking_delta("secret thought")
            yield MessageEvent.thinking("more secret")
            yield MessageEvent.delta("<think>inline secret</think>final answer")
            yield MessageEvent.completed()

        ch = WeComChannel(processor=processor, config=_make_config())
        ws = self._configure_channel(ch, show_thinking=True)

        receipt = await ch.dispatch_inbound(
            _make_stream_payload(ws),
            receipt=InboundReceipt(suppress_thinking=True),
        )

        assert all("secret" not in c["content"] for c in ws.calls)
        assert (ws.calls[-1]["content"], ws.calls[-1]["finish"]) == ("final answer", True)
        assert ch._constraints.show_thinking is True
        assert receipt.delivered is True

    @pytest.mark.asyncio
    async def test_cancelled_stream_restores_persistent_settings_for_next_dispatch(self) -> None:
        import asyncio as _asyncio

        started = _asyncio.Event()
        release = _asyncio.Event()
        call_index = 0

        async def processor(_msg: InboundMessage):
            nonlocal call_index
            call_index += 1
            if call_index == 1:
                yield MessageEvent.thinking_delta("hidden during cancel")
                started.set()
                await release.wait()
                yield MessageEvent.delta("never reached")
                yield MessageEvent.completed()
            else:
                yield MessageEvent.thinking_delta("restored thought")
                yield MessageEvent.delta("restored answer")
                yield MessageEvent.completed()

        ch = WeComChannel(processor=processor, config=_make_config())
        ws = self._configure_channel(ch, show_thinking=True)

        task = _asyncio.create_task(
            ch.dispatch_inbound(
                _make_stream_payload(ws, content="first"),
                receipt=InboundReceipt(suppress_thinking=True, text_filter=lambda _text: ""),
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(_asyncio.CancelledError):
            await task

        assert ws.calls == []
        assert ch._constraints.show_thinking is True

        next_receipt = await ch.dispatch_inbound(_make_stream_payload(ws, content="second"))
        assert any("restored thought" in c["content"] for c in ws.calls)
        assert (ws.calls[-1]["content"], ws.calls[-1]["finish"]) == ("restored answer", True)
        assert next_receipt.delivered is True
