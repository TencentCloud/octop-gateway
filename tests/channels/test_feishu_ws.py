"""Regression tests for Feishu WebSocket client isolation."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import lark_oapi as lark
import lark_oapi.ws.client as shared_sdk_ws_client

from octop_gateway.channels._feishu_ws import create_isolated_ws_client, run_isolated_ws_client


def _client(on_transport_activity: MagicMock | None = None) -> Any:
    return create_isolated_ws_client(
        lark,
        "app-id",
        "app-secret",
        event_handler=MagicMock(),
        log_level=lark.LogLevel.INFO,
        on_transport_activity=on_transport_activity or MagicMock(),
    )


def test_clients_keep_independent_event_loop_modules() -> None:
    first = _client()
    second = _client()
    shared_loop = shared_sdk_ws_client.loop

    assert first._octop_ws_module is not second._octop_ws_module
    assert type(first).__mro__[1].start.__globals__ is first._octop_ws_module.__dict__
    assert type(second).__mro__[1]._connect.__globals__ is second._octop_ws_module.__dict__

    barrier = threading.Barrier(2)
    observed: dict[str, tuple[asyncio.AbstractEventLoop, asyncio.AbstractEventLoop]] = {}

    def run(name: str, client: Any) -> None:
        loop = asyncio.new_event_loop()

        def start() -> None:
            barrier.wait(timeout=2)
            observed[name] = (loop, client._octop_ws_module.loop)

        client.start = start
        try:
            run_isolated_ws_client(client, loop)
        finally:
            loop.close()

    threads = [
        threading.Thread(target=run, args=("first", first)),
        threading.Thread(target=run, args=("second", second)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert not any(thread.is_alive() for thread in threads)
    assert observed["first"][0] is observed["first"][1]
    assert observed["second"][0] is observed["second"][1]
    assert observed["first"][1] is not observed["second"][1]
    assert shared_sdk_ws_client.loop is shared_loop


def test_reaction_callback_is_acknowledged_by_dispatcher() -> None:
    from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig

    async def processor(message: Any):
        yield message

    channel = FeishuChannel(processor, FeishuConfig(app_id="app-id", app_secret="app-secret"))
    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_reaction_created_v1(channel._on_reaction_event)
        .build()
    )
    payload = {
        "schema": "2.0",
        "header": {
            "event_id": "event-id",
            "event_type": "im.message.reaction.created_v1",
            "create_time": "1",
            "token": "",
            "app_id": "app-id",
            "tenant_key": "tenant-key",
        },
        "event": {
            "message_id": "message-id",
            "reaction_type": {"emoji_type": "SMILE"},
            "operator_type": "user",
            "user_id": {"open_id": "user-id"},
        },
    }

    assert handler._do_without_validation(json.dumps(payload).encode()) is None


async def test_control_frame_marks_transport_activity() -> None:
    activity = MagicMock()
    client = _client(activity)
    sdk_base = type(client).__mro__[1]
    original = sdk_base._handle_control_frame
    sdk_base._handle_control_frame = AsyncMock()
    try:
        await client._handle_control_frame(MagicMock())
    finally:
        sdk_base._handle_control_frame = original

    activity.assert_called_once_with()
