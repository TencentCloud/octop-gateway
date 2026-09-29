"""Feishu in-process restart safety (#757).

lark-oapi's ``ws.Client`` schedules every callback through one module-global
event loop, and a full WS start rebinds that global. A second full start
while a live channel is connected (the dashboard 测试 probe, a channel PATCH)
therefore rerouted the live channel's callbacks onto a loop that never ran
them — messages silently dropped while the connection stayed open.

Two defenses:
* ``probe_channel("feishu", …)`` verifies credentials via a token fetch only
  (``probe_mode="token"``), never opening a second WS connection;
* ``_run_ws_thread`` refuses to rebind the global loop while another live
  loop owns it.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from unittest.mock import patch

import pytest

from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig

pytest.importorskip("lark_oapi")


def _config(**overrides: Any) -> FeishuConfig:
    return FeishuConfig(app_id="cli_test_app_id", app_secret="test_secret", **overrides)


async def _noop_processor(msg: Any):
    yield None


class TestProbeModeToken:
    async def test_probe_channel_never_opens_websocket(self) -> None:
        """probe_mode='token' verifies credentials without any WS client."""
        ch = FeishuChannel(processor=_noop_processor, config=_config(probe_mode="token"))

        async def _token_ok() -> None:
            return None

        ch._refresh_token = _token_ok  # type: ignore[method-assign]
        await ch.start()

        assert ch._ws_client is None, "probe must not open a WebSocket connection"
        assert ch._running is False

    async def test_full_start_still_opens_websocket(self) -> None:
        """Default probe_mode='full' keeps the real connection behaviour."""
        ch = FeishuChannel(processor=_noop_processor, config=_config())

        async def _ok() -> None:
            return None

        async def _noop_watchdog() -> None:
            return None

        started: list[bool] = []

        async def _start_ws_client() -> None:
            started.append(True)

        ch._refresh_token = _ok  # type: ignore[method-assign]
        ch._ensure_bot_open_id = _ok  # type: ignore[method-assign]
        ch._start_ws_client = _start_ws_client  # type: ignore[method-assign]
        ch._ws_watchdog_loop = _noop_watchdog  # type: ignore[method-assign]
        await ch.start()

        assert started == [True]
        assert ch._running is True

    async def test_manager_probe_sets_feishu_token_mode(self) -> None:
        """ChannelManager.probe_channel guards feishu like yuanbao."""
        from octop_gateway.manager import ChannelManager

        mgr = ChannelManager(channels={})
        captured: dict[str, Any] = {}

        class _FakeChannel:
            def __init__(self, cfg: Any) -> None:
                self.config = cfg

            def set_media_backend(self, _backend: Any) -> None:
                pass

            async def start(self) -> None:
                captured["probe_mode"] = getattr(self.config, "probe_mode", None)

            async def stop(self) -> None:
                pass

        def _fake_instantiate(_kind: str, cfg: Any, **_kwargs: Any) -> _FakeChannel:
            return _FakeChannel(cfg)

        with patch.object(mgr, "_instantiate_channel", side_effect=_fake_instantiate) as inst:
            await mgr.probe_channel(
                "feishu",
                {"app_id": "cli_x", "app_secret": "s"},
                channel_id="__probe__",
                tenant_id="t",
                processor=_noop_processor,
            )
        assert captured["probe_mode"] == "token"
        inst.assert_called_once()


class TestGlobalLoopRebindGuard:
    @staticmethod
    def _run_ws_thread_with(ch: FeishuChannel) -> None:
        thread = threading.Thread(target=ch._run_ws_thread, daemon=True)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_rebinds_when_no_live_owner(self) -> None:
        """No live owner of the global loop: rebind happens (sequential restart)."""
        import lark_oapi.ws.client as ws_client_module

        previous = getattr(ws_client_module, "loop", None)
        try:
            ws_client_module.loop = None

            async def _noop_start() -> None:
                return None

            client = type("C", (), {"start": staticmethod(_noop_start)})()
            ch = FeishuChannel(processor=_noop_processor, config=_config())
            ch._ws_client = client

            self._run_ws_thread_with(ch)
            assert ws_client_module.loop is not None
            assert not ws_client_module.loop.is_running()
        finally:
            ws_client_module.loop = previous

    def test_skips_rebind_while_another_loop_is_live(self) -> None:
        """A live foreign loop keeps ownership of the global; no deafening."""
        import lark_oapi.ws.client as ws_client_module

        previous = getattr(ws_client_module, "loop", None)
        foreign = asyncio.new_event_loop()
        thread = threading.Thread(target=foreign.run_forever, daemon=True)
        thread.start()
        try:
            assert foreign.is_running()
            ws_client_module.loop = foreign

            async def _noop_start() -> None:
                return None

            client = type("C", (), {"start": staticmethod(_noop_start)})()
            ch = FeishuChannel(processor=_noop_processor, config=_config())
            ch._ws_client = client

            self._run_ws_thread_with(ch)
            assert ws_client_module.loop is foreign, (
                "the live channel's loop must keep owning the SDK global"
            )
        finally:
            foreign.call_soon_threadsafe(foreign.stop)
            thread.join(timeout=5)
            foreign.close()
            ws_client_module.loop = previous
