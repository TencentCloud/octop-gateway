"""Reaction-event dispatch tests for the Feishu channel (#843).

The lark-oapi WebSocket client answers the platform with a ``code=500``
business ACK when an event type has no registered processor
(``processor not found``). Reaction events subscribed by the app
(``im.message.reaction.created_v1`` / ``..._deleted_v1``) used to hit that
path on every reaction. They must now be acknowledged without triggering
the model, while normal message events and unknown event types keep their
semantics.
"""

from __future__ import annotations

import json

import pytest

from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig
from octop_gateway.models import InboundMessage, MessageEvent

pytest.importorskip("lark_oapi")

from lark_oapi.event.dispatcher_handler import EventException

_REACTION_TYPES = (
    "im.message.reaction.created_v1",
    "im.message.reaction.deleted_v1",
)


def _make_config() -> FeishuConfig:
    return FeishuConfig(app_id="cli_test_app_id", app_secret="test_secret")


async def _noop_processor(msg: InboundMessage):
    yield MessageEvent.completed()


def _event_payload(event_type: str) -> bytes:
    return json.dumps(
        {"schema": "2.0", "header": {"event_type": event_type}, "event": {}}
    ).encode()


def _make_channel() -> FeishuChannel:
    return FeishuChannel(processor=_noop_processor, config=_make_config())


class TestReactionEventDispatch:
    def test_reaction_events_are_acknowledged(self) -> None:
        """Reaction frames dispatch cleanly (no processor-not-found error)."""
        ch = _make_channel()
        captured: list[str] = []
        ch._on_reaction_event = lambda event: captured.append(
            getattr(getattr(event, "header", None), "event_type", None)
        )
        handler = ch._build_event_handler()

        for event_type in _REACTION_TYPES:
            handler._do_without_validation(_event_payload(event_type))

        assert captured == list(_REACTION_TYPES)

    @pytest.mark.parametrize("event_type", _REACTION_TYPES)
    def test_reaction_handler_never_raises(self, event_type: str) -> None:
        ch = _make_channel()
        handler = ch._build_event_handler()
        # Must not raise EventException("processor not found")
        handler._do_without_validation(_event_payload(event_type))

    def test_normal_message_event_still_dispatches(self) -> None:
        """The message-receive processor is untouched by the reaction handlers."""
        ch = _make_channel()
        received: list[str] = []

        def _record(event: object) -> None:
            received.append(
                getattr(getattr(event, "header", None), "event_type", None)
            )

        ch._on_message_event = _record
        handler = ch._build_event_handler()
        handler._do_without_validation(_event_payload("im.message.receive_v1"))
        assert received == ["im.message.receive_v1"]

    def test_unknown_event_still_fails_closed(self) -> None:
        """Genuinely unknown event types keep the SDK error semantics."""
        ch = _make_channel()
        handler = ch._build_event_handler()
        with pytest.raises(EventException):
            handler._do_without_validation(
                _event_payload("im.message.not_a_real_event_v1")
            )

    def test_reaction_handler_returns_none(self) -> None:
        """The acknowledgement callback returns None (plain ACK, no reply)."""

        class _DummyEvent:
            header = None

        ch = _make_channel()
        assert ch._on_reaction_event(_DummyEvent()) is None
