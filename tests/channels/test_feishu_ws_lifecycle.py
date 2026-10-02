"""Feishu WS thread lifecycle safety (#1392).

Two production incidents (both triggered by the dashboard 连接检测 probe) saw
the channel's WS thread orphaned and the channel muted:

* a stop arriving ~15ms after ``start()`` — before the newborn thread has
  assigned ``_ws_loop`` — queued no cancellation at all (the shutdown request
  early-returns on a missing loop), so the thread missed the 10s join and was
  orphaned with its live connection;

* once orphaned threads existed, the watchdog deferred every health restart
  forever, so nothing ever reclaimed the platform's event-delivery routing.

Defenses under test:
* ``_stop_ws_client`` waits (bounded) for the newborn thread's entry point
  before requesting the shutdown, so the cancel is always delivered;
* the watchdog escalates after ``_WS_ORPHAN_ESCALATE_SECONDS`` and restarts
  despite live orphans instead of deferring indefinitely.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any
from unittest.mock import patch

import pytest

from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig

pytest.importorskip("lark_oapi")
pytest.importorskip("websockets")

_CONNECT_URL = "wss://fake.local/ws?device_id=test&service_id=42"


def _config(**overrides: Any) -> FeishuConfig:
    return FeishuConfig(app_id="cli_test_app_id", app_secret="test_secret", **overrides)


async def _noop_processor(msg: Any):
    yield None


async def _async_noop(*args: Any, **kwargs: Any) -> None:
    return None


class _FakeConn:
    """Local stand-in for the websockets client connection."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[bytes] = asyncio.Queue()
        self.sent: list[bytes] = []

    async def recv(self) -> bytes:
        return await self.inbox.get()

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        pass


class _TransportHarness:
    """Stubs the SDK transport so the real channel + SDK machinery run offline."""

    def __init__(self) -> None:
        self.conns: list[_FakeConn] = []

    async def _fake_connect(self, url: str, **kwargs: Any) -> _FakeConn:
        conn = _FakeConn()
        self.conns.append(conn)
        return conn

    def _fake_conn_url(self) -> str:
        # Patched onto lark's Client as a plain function, so the client
        # instance arrives as ``self``.
        return _CONNECT_URL

    def __enter__(self) -> "_TransportHarness":
        import websockets

        import lark_oapi.ws.client as lark_ws_client

        self._patches = [
            patch.object(websockets, "connect", self._fake_connect),
            patch.object(lark_ws_client.Client, "_get_conn_url", self._fake_conn_url),
        ]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        for p in self._patches:
            p.stop()


def _make_channel() -> FeishuChannel:
    ch = FeishuChannel(processor=_noop_processor, config=_config())
    ch._enqueue_callback = lambda payload: None  # type: ignore[method-assign]
    ch._refresh_token = _async_noop  # type: ignore[method-assign]
    ch._ensure_bot_open_id = _async_noop  # type: ignore[method-assign]
    ch._add_reaction_async = lambda *a, **k: None  # type: ignore[method-assign]
    ch._close_http = _async_noop  # type: ignore[method-assign]
    return ch


class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())

    def has(self, needle: str) -> bool:
        return any(needle in line for line in self.lines)


async def test_stop_before_thread_start_does_not_orphan() -> None:
    """A stop racing a newborn thread waits for it and exits clean (#1392).

    Regression for the production incidents: ``probe_channel`` calls
    ``stop()`` ~15ms after ``start()``; without the bounded wait the shutdown
    request early-returns (``_ws_loop`` still unset), the join times out and
    the thread — with its live connection — is orphaned forever.
    """
    captured = _LogCapture()
    logger = logging.getLogger("octop_gateway.channels.feishu")
    logger.addHandler(captured)
    try:
        with (
            _TransportHarness(),
            patch("octop_gateway.channels.feishu._WS_THREAD_START_GRACE_SECONDS", 2.0),
            patch("octop_gateway.channels.feishu._WS_THREAD_JOIN_TIMEOUT", 2.0),
        ):
            ch = _make_channel()
            # Delay the thread's entry point to emulate newborn-thread startup
            # latency: the thread runs but has not assigned ``_ws_loop`` yet.
            original_run = type(ch)._run_ws_thread

            def _delayed_run(channel: FeishuChannel) -> None:
                time.sleep(0.3)
                original_run(channel)

            ch._run_ws_thread = _delayed_run  # type: ignore[method-assign]

            await ch.start()
            stopped = await ch._stop_ws_client(force=True)

        assert stopped is True
        assert ch._ws_orphan_threads == [], "the newborn thread must not be orphaned"
        assert not captured.has("orphaned"), "no orphan may be logged"
        assert not captured.has("did not exit within"), "the thread must exit within the join window"
    finally:
        logger.removeHandler(captured)


async def test_watchdog_escalates_after_orphan_deadline() -> None:
    """Live orphans defer restarts only until the escalation deadline (#1392)."""
    captured = _LogCapture()
    logger = logging.getLogger("octop_gateway.channels.feishu")
    logger.addHandler(captured)
    try:
        with (
            _TransportHarness(),
            patch("octop_gateway.channels.feishu._WS_WATCHDOG_INTERVAL", 0.05),
            patch("octop_gateway.channels.feishu._WS_ORPHAN_ESCALATE_SECONDS", 0.3),
            patch("octop_gateway.channels.feishu._WS_RESTART_BACKOFF_SECONDS", 0.05),
            patch("octop_gateway.channels.feishu._WS_THREAD_JOIN_TIMEOUT", 0.5),
            patch("octop_gateway.channels.feishu._WS_THREAD_START_GRACE_SECONDS", 0.5),
        ):
            ch = _make_channel()
            await ch.start()
            first_client = ch._ws_client
            assert first_client is not None

            # Simulate the post-incident state: the previous generation was
            # force stopped, its thread lingered as an orphan, and the
            # channel's own thread reference was cleared — exactly the state
            # in which the watchdog used to defer forever.
            zombie = threading.Thread(target=time.sleep, args=(30,), daemon=True)
            zombie.start()
            ch._ws_orphan_threads = [zombie]
            ch._ws_thread = None
            ch._ws_loop = None
            ch._ws_client = None
            ch._ws_reconnecting_since = None
            ch._last_ws_activity_at = time.time()

            def _restarted() -> bool:
                return ch._ws_thread is not None and ch._ws_client is not None

            deadline = time.time() + 5
            while time.time() < deadline and not _restarted():
                await asyncio.sleep(0.05)

            assert _restarted(), "watchdog must restart despite live orphans after the deadline"
            assert ch._ws_client is not first_client
            assert captured.has("restarting despite the"), "the escalation must be logged"

            await ch.stop()
    finally:
        logger.removeHandler(captured)


async def test_watchdog_defers_while_orphans_are_young() -> None:
    """Before the escalation deadline the restart stays deferred."""
    with (
        _TransportHarness(),
        patch("octop_gateway.channels.feishu._WS_WATCHDOG_INTERVAL", 0.05),
        patch("octop_gateway.channels.feishu._WS_ORPHAN_ESCALATE_SECONDS", 60.0),
        patch("octop_gateway.channels.feishu._WS_THREAD_JOIN_TIMEOUT", 0.5),
        patch("octop_gateway.channels.feishu._WS_THREAD_START_GRACE_SECONDS", 0.5),
    ):
        ch = _make_channel()
        await ch.start()

        zombie = threading.Thread(target=time.sleep, args=(5,), daemon=True)
        zombie.start()
        ch._ws_orphan_threads = [zombie]
        ch._ws_thread = None
        ch._ws_loop = None
        ch._ws_client = None
        ch._ws_reconnecting_since = None
        ch._last_ws_activity_at = time.time()

        await asyncio.sleep(0.4)  # several watchdog ticks, far below the deadline

        assert ch._ws_thread is None, "restart must stay deferred while orphans are young"

        await ch.stop()
