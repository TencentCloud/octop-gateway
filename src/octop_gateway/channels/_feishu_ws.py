"""Isolate lark-oapi WebSocket event loops per client instance."""

from __future__ import annotations

import asyncio
import importlib.util
import types
import uuid
from collections.abc import Callable
from typing import Any


def create_isolated_ws_client(
    lark: Any,
    app_id: str,
    app_secret: str,
    *,
    event_handler: Any,
    log_level: Any,
    on_transport_activity: Callable[[], None],
) -> Any:
    """Create a lark-oapi client whose module-level loop is instance-local.

    lark-oapi stores its event loop in ``lark_oapi.ws.client.loop``. Loading
    that module into a private namespace gives every long-lived client its own
    loop binding without changing process-wide SDK state.
    """
    import lark_oapi.ws.client as sdk_ws_client

    module_path = getattr(sdk_ws_client, "__file__", None)
    if not module_path:
        raise RuntimeError("lark-oapi WebSocket client module has no source path")

    module_name = f"lark_oapi.ws._octop_client_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load an isolated lark-oapi WebSocket client")

    isolated_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(isolated_module)
    base_client = isolated_module.Client

    class TransportAwareClient(base_client):  # type: ignore[misc, valid-type]
        async def _handle_control_frame(self, frame: Any) -> None:
            await super()._handle_control_frame(frame)
            on_transport_activity()

    client = TransportAwareClient(
        app_id,
        app_secret,
        event_handler=event_handler,
        log_level=log_level,
    )
    client._octop_ws_module = isolated_module
    return client


def run_isolated_ws_client(client: Any, loop: asyncio.AbstractEventLoop) -> None:
    """Bind and run one isolated lark-oapi client on ``loop``."""
    module = getattr(client, "_octop_ws_module", None)
    if not isinstance(module, types.ModuleType):
        raise RuntimeError("lark-oapi client was not created by create_isolated_ws_client")
    isolated_module: Any = module
    isolated_module.loop = loop
    client.start()
