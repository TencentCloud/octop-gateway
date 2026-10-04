"""Example: NATS bot (core NATS pub/sub)."""

from __future__ import annotations

import asyncio
import logging
import os

from dotenv import load_dotenv

from octop_gateway.channels.nats import NATSConfig
from octop_gateway.manager import ChannelManager

load_dotenv()
logging.basicConfig(level=logging.INFO)


async def main() -> None:
    async def processor(msg):
        from octop_gateway.models import MessageEvent

        user_text = msg.content[0].text if msg.content else ""  # type: ignore[union-attr]
        yield MessageEvent.text(f"Echo: {user_text}")
        yield MessageEvent.completed()

    manager = ChannelManager(processor=processor)
    await manager.start()
    await manager.add_nats_channel(
        NATSConfig(
            servers=os.getenv("NATS_SERVERS", "nats://localhost:4222"),
            username=os.getenv("NATS_USERNAME", ""),
            password=os.getenv("NATS_PASSWORD", ""),
            subscribe_subject=os.getenv("NATS_SUBSCRIBE_SUBJECT", "bots.*.in"),
            publish_subject=os.getenv("NATS_PUBLISH_SUBJECT", "bots.{client_id}.out"),
            tls_enabled=os.getenv("NATS_TLS_ENABLED", "false").lower() == "true",
            show_thinking=os.getenv("NATS_SHOW_THINKING", "false").lower() == "true",
            show_tool_hints=os.getenv("NATS_SHOW_TOOL_HINTS", "true").lower() == "true",
        )
    )
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
