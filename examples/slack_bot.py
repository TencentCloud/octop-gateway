"""Slack Bot example using octop-gateway (Socket Mode)."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

from octop_gateway import ChannelManager, InboundMessage, MessageEvent, SlackConfig


async def echo_processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
    """Simple echo processor for Slack messages."""
    yield MessageEvent.text(f"Echo from Octop Slack Bot: {message.text}")
    yield MessageEvent.completed()


async def main() -> None:
    config = SlackConfig(
        bot_token=os.environ.get("SLACK_BOT_TOKEN", ""),
        app_token=os.environ.get("SLACK_APP_TOKEN", ""),
        signing_secret=os.environ.get("SLACK_SIGNING_SECRET", ""),
    )
    manager = ChannelManager(processor=echo_processor)
    await manager.start()
    await manager.add_slack_channel(config)
    try:
        await asyncio.Event().wait()
    finally:
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
