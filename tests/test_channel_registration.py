"""Tests for runtime channel-kind registration (plugin extension point)."""

from __future__ import annotations

import pytest

from octop_gateway.channel import BaseChannel, ChannelConfig, MessageProcessor
from octop_gateway.channels import (
    BUILTIN_CHANNELS,
    ChannelKind,
    register_channel_kind,
)


class _StubChannel(BaseChannel):
    """Minimal channel implementation for registration tests."""

    channel_type = "acme"

    class Config(ChannelConfig):
        token: str = ""

    def __init__(
        self, processor: MessageProcessor, config: ChannelConfig | None = None
    ) -> None:
        super().__init__(processor, config=config)


@pytest.fixture(autouse=True)
def _cleanup_registration() -> None:
    yield
    # Remove any kind registered during the test (builtins must survive).
    for key in list(BUILTIN_CHANNELS.keys()):
        if key not in {k.value for k in ChannelKind}:
            dict.__delitem__(BUILTIN_CHANNELS, key)
            BUILTIN_CHANNELS._loaded = True


def test_register_and_resolve() -> None:
    register_channel_kind("acme", _StubChannel)
    assert "acme" in BUILTIN_CHANNELS
    assert BUILTIN_CHANNELS["acme"] is _StubChannel


def test_register_is_idempotent() -> None:
    register_channel_kind("acme", _StubChannel)

    class _Other(BaseChannel):
        channel_type = "acme-other"

    # Second apply with another class: first registration wins (stable
    # across host reloads), no error.
    register_channel_kind("acme", _Other)
    assert BUILTIN_CHANNELS["acme"] is _StubChannel


def test_register_rejects_builtin_override() -> None:
    with pytest.raises(ValueError, match="builtin"):
        register_channel_kind("telegram", _StubChannel)


def test_register_rejects_empty_kind() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        register_channel_kind("  ", _StubChannel)


def test_register_rejects_non_class() -> None:
    with pytest.raises(TypeError, match="must be a class"):
        register_channel_kind("acme", lambda: None)  # type: ignore[arg-type]
