"""WhatsApp Bot example using octop-gateway (Meta Cloud API or WAHA / Evolution QR Bridge)."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

from octop_gateway import ChannelManager, InboundMessage, MessageEvent, WhatsAppConfig


async def echo_processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
    """Simple echo processor for WhatsApp messages."""
    yield MessageEvent.text(f"Echo from Octop WhatsApp Bot: {message.text}")
    yield MessageEvent.completed()


async def main() -> None:
    provider = os.environ.get("WHATSAPP_PROVIDER", "cloud")
    config = WhatsAppConfig(
        provider=provider,
        access_token=os.environ.get("WHATSAPP_ACCESS_TOKEN", ""),
        phone_number_id=os.environ.get("WHATSAPP_PHONE_NUMBER_ID", ""),
        verify_token=os.environ.get("WHATSAPP_VERIFY_TOKEN", ""),
        app_secret=os.environ.get("WHATSAPP_APP_SECRET", ""),
        bridge_url=os.environ.get("WHATSAPP_BRIDGE_URL", ""),
        bridge_api_key=os.environ.get("WHATSAPP_BRIDGE_API_KEY", ""),
        session_name=os.environ.get("WHATSAPP_SESSION_NAME", "default"),
    )
    manager = ChannelManager(processor=echo_processor)
    await manager.start()
    await manager.add_whatsapp_channel(config)
    try:
        await asyncio.Event().wait()
    finally:
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
