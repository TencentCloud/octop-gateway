"""Unit tests for the NATS channel."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from octop_gateway.channels.nats import NATSChannel, NATSConfig
from octop_gateway.models import InboundMessage, MessageEvent


async def _noop_processor(msg: InboundMessage):
    yield MessageEvent.completed()


def _make_channel(**overrides) -> NATSChannel:
    config = NATSConfig(servers="nats://localhost:4222", **overrides)
    return NATSChannel(processor=_noop_processor, config=config)


class TestNATSConfig:
    def test_missing_credentials(self) -> None:
        config = NATSConfig(servers="")
        assert config.missing_credentials() == ["servers"]

    def test_from_dict(self) -> None:
        config = NATSConfig.from_dict({"servers": "nats://a:4222", "subscribe_subject": "x.>"})
        assert config.servers == "nats://a:4222"
        assert config.subscribe_subject == "x.>"


class TestNATSChannelUnit:
    def test_channel_type(self) -> None:
        ch = _make_channel()
        assert ch.channel_type == "nats"

    def test_default_constraints(self) -> None:
        ch = _make_channel()
        assert ch.constraints.show_thinking is False
        assert ch.constraints.show_tool_hints is True

    def test_parse_inbound_text(self) -> None:
        ch = _make_channel()
        msg = ch.parse_inbound(
            {
                "client_id": "robot-01",
                "text": "hello nats",
                "topic": "bots.robot-01.in",
            }
        )
        assert msg.channel_type == "nats"
        assert msg.channel_subject is not None
        assert msg.channel_subject.subject_id == "robot-01"
        assert msg.content[0].text == "hello nats"  # type: ignore[union-attr]
        assert msg.metadata["client_id"] == "robot-01"

    def test_parse_inbound_passthrough(self) -> None:
        ch = _make_channel()
        msg = InboundMessage.model_validate(
            {
                "channel_id": "c1",
                "channel_type": "nats",
                "channel_subject": {"subject_id": "s1"},
                "content": [{"type": "text", "text": "hi"}],
            }
        )
        assert ch.parse_inbound(msg) is msg

    async def test_on_message_plain_text(self) -> None:
        ch = _make_channel()
        ch.enqueue = MagicMock()
        msg = SimpleNamespace(subject="bots.robot-01.in", data=b"hello nats")
        await ch._on_message(msg)
        ch.enqueue.assert_called_once()
        native = ch.enqueue.call_args[0][0]
        assert native["client_id"] == "robot-01"  # derived from the subject
        assert native["text"] == "hello nats"

    async def test_on_message_json(self) -> None:
        ch = _make_channel()
        ch.enqueue = MagicMock()
        payload = '{"msg_id": "m1", "client_id": "dev-9", "text": "hi"}'
        msg = SimpleNamespace(subject="bots.dev-9.in", data=payload.encode())
        await ch._on_message(msg)
        native = ch.enqueue.call_args[0][0]
        assert native["client_id"] == "dev-9"
        assert native["raw_payload"] == payload

    async def test_on_message_dedup(self) -> None:
        ch = _make_channel()
        ch.enqueue = MagicMock()
        payload = '{"msg_id": "dup-1", "text": "hi"}'
        msg = SimpleNamespace(subject="bots.dev-9.in", data=payload.encode())
        await ch._on_message(msg)
        await ch._on_message(msg)
        assert ch.enqueue.call_count == 1
        assert ch._is_duplicate("dup-1") is True

    async def test_on_message_empty_dropped(self) -> None:
        ch = _make_channel()
        ch.enqueue = MagicMock()
        msg = SimpleNamespace(subject="bots.dev-9.in", data=b"   ")
        await ch._on_message(msg)
        ch.enqueue.assert_not_called()

    async def test_on_message_malformed_isolated(self) -> None:
        ch = _make_channel()
        ch.enqueue = MagicMock()
        msg = SimpleNamespace(subject="bots.dev-9.in", data=None)  # decode() raises
        await ch._on_message(msg)  # must not raise
        ch.enqueue.assert_not_called()

    async def test_send_text(self) -> None:
        ch = _make_channel()
        nc = MagicMock()
        nc.publish = AsyncMock()
        nc.flush = AsyncMock()
        ch._nc = nc
        ch._connected = True
        subject = ch.parse_inbound({"client_id": "dev-9", "text": "in"}).channel_subject
        await ch._send_text(subject, "reply text")  # type: ignore[arg-type]
        topic, data = nc.publish.call_args[0]
        assert topic == "bots.dev-9.out"
        assert data == b"reply text"

    async def test_send_text_not_connected_is_noop(self) -> None:
        ch = _make_channel()
        subject = ch.parse_inbound({"client_id": "dev-9", "text": "in"}).channel_subject
        await ch._send_text(subject, "reply")  # type: ignore[arg-type]  # _nc is None — no raise

    async def test_start_stop_without_sdk(self) -> None:
        ch = _make_channel()
        try:
            import nats  # noqa: F401

            has_sdk = True
        except ImportError:
            has_sdk = False
        if has_sdk:
            return  # skip: a live broker is required; covered by integration tests
        try:
            await ch.start()
            raised = False
        except ImportError:
            raised = True
        assert raised

    def test_subject_tokens_use_dot(self) -> None:
        ch = _make_channel()
        assert ch._config.subscribe_subject == "bots.*.in"
        assert "/" not in ch._config.publish_subject
