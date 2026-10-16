"""Telegram SDK photos must retain an image MIME through download and storage."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from telegram import Message

from octop_gateway.channels.telegram.channel import TelegramChannel, TelegramConfig
from octop_gateway.models import MessageEvent


async def test_sdk_photo_mime_survives_persistence():
    async def processor(message):
        yield MessageEvent.completed()

    channel = TelegramChannel(processor, config=TelegramConfig(bot_token="dummy"))
    message = Message.de_json(
        {
            "message_id": 1,
            "date": 0,
            "chat": {"id": 42, "type": "private"},
            "from": {"id": 7, "first_name": "Test", "is_bot": False},
            "photo": [{"file_id": "test", "file_unique_id": "test", "width": 1, "height": 1}],
        },
        None,
    )
    native = await channel._extract_message(message)
    assert native["media"][0]["mime_type"] == "image/jpeg"
    photo = native["content"][0]
    assert photo.mime_type == "image/jpeg"
    data = b"\xff\xd8\xff" + b"\x00" * 32
    remote = SimpleNamespace(file_size=len(data), download_as_bytearray=AsyncMock(return_value=data))
    channel._app = SimpleNamespace(bot=SimpleNamespace(get_file=AsyncMock(return_value=remote)))
    channel._media_backend = SimpleNamespace(save=AsyncMock())
    await channel._persist_media_part(photo, 0, 123)
    assert photo.mime_type == "image/jpeg"
    assert photo.local_path.endswith(".jpg")
    assert await channel.fetch_remote_media(photo.url) == (data, "image/jpeg")
