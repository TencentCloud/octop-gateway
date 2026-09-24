"""Telegram channel: Bot API with HTTP long polling."""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from io import BytesIO
from typing import Any

from telegram.error import TelegramError

from octop_gateway.channel import BaseChannel, ChannelConfig, MessageProcessor
from octop_gateway.channels.telegram.format_html import markdown_to_telegram_html
from octop_gateway.constraints import ChannelConstraints
from octop_gateway.models import (
    AudioContent,
    ChannelSubject,
    ContentPart,
    FileContent,
    ImageContent,
    InboundMessage,
    TextContent,
    VideoContent,
)

logger = logging.getLogger(__name__)

_TELEGRAM_MAX_MESSAGE_LENGTH = 4096
_TELEGRAM_SEND_CHUNK_SIZE = 4000
_TELEGRAM_MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024


@dataclass
class TelegramConfig(ChannelConfig):
    """Configuration for Telegram Bot channel.

    Attributes:
        bot_token: Telegram Bot API token from @BotFather.
        http_proxy: Optional HTTP proxy URL.
        show_typing: Send ``typing`` chat action while processing.
        show_thinking: Forward thinking/reasoning content to the chat.
        show_tool_hints: Show tool-call status messages in the chat.
    """

    bot_token: str = ""
    http_proxy: str = ""
    show_typing: bool = True

    required_credentials = ("bot_token",)


class TelegramChannel(BaseChannel):
    """Telegram Bot channel using python-telegram-bot long polling."""

    channel_type = "telegram"

    def __init__(
        self,
        processor: MessageProcessor,
        *,
        config: TelegramConfig,
        channel_id: str | None = None,
        tenant_id: str | None = None,
        debounce_seconds: float = 0.0,
        constraints: ChannelConstraints | None = None,
    ) -> None:
        self._config = config
        super().__init__(
            processor,
            channel_id=channel_id,
            tenant_id=tenant_id,
            debounce_seconds=debounce_seconds,
            constraints=constraints,
            config=config,
        )
        self._app: Any = None
        self._polling_task: asyncio.Task[None] | None = None
        self._connection_session: str | None = None

    def _default_constraints(self) -> ChannelConstraints:
        interval = 4.0 if self._config.show_typing else 0.0
        return ChannelConstraints(
            send_rate_limit=(20, 60.0),
            typing_keepalive_interval=interval,
            show_thinking=False,
            show_tool_hints=True,
        )

    async def start(self) -> None:
        if not self._config.bot_token:
            raise RuntimeError("TelegramChannel: bot_token is required")

        try:
            from telegram.ext import Application, ContextTypes, MessageHandler, filters
        except ImportError as exc:
            raise ImportError(
                "python-telegram-bot is required for TelegramChannel. Install with: pip install python-telegram-bot"
            ) from exc

        builder = Application.builder().token(self._config.bot_token)
        if self._config.http_proxy:
            builder = builder.proxy(self._config.http_proxy).get_updates_proxy(self._config.http_proxy)

        self._app = builder.build()
        self._connection_session = f"telegram-{uuid.uuid4().hex[:12]}"

        async def handle_message(update: Any, _context: ContextTypes.DEFAULT_TYPE) -> None:
            message = update.message or update.edited_message
            if not message:
                return
            native = await self._build_native_from_update(update)
            if native:
                self.enqueue(native)

        self._app.add_handler(MessageHandler(filters.ALL, handle_message))
        await self._app.initialize()
        await self._app.start()
        if self._app.updater:
            await self._app.updater.start_polling(allowed_updates=["message", "edited_message"])
        logger.info("TelegramChannel started")

    async def stop(self) -> None:
        if self._app:
            if self._app.updater and self._app.updater.running:
                await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            self._app = None
        self._connection_session = None
        logger.info("TelegramChannel stopped")

    async def _build_native_from_update(self, update: Any) -> dict[str, Any] | None:
        message = update.message or update.edited_message
        if not message:
            return None

        content_parts: list[ContentPart] = []
        text = (getattr(message, "text", None) or getattr(message, "caption", None) or "").strip()
        if text:
            content_parts.append(TextContent(text=text))

        bot = self._app.bot if self._app else None
        for attr in ("photo", "document", "video", "voice", "audio"):
            media = getattr(message, attr, None)
            if not media:
                continue
            file_obj = media[-1] if attr == "photo" else media
            file_id = getattr(file_obj, "file_id", "")
            filename = getattr(file_obj, "file_name", "") or attr
            url = ""
            if bot and file_id:
                url = await self._resolve_file_url(bot, file_id)
            if attr == "photo":
                content_parts.append(ImageContent(url=url, alt_text=filename))
            elif attr == "video":
                content_parts.append(VideoContent(url=url))
            elif attr in ("voice", "audio"):
                content_parts.append(AudioContent(url=url))
            else:
                content_parts.append(FileContent(url=url, filename=filename))

        if not content_parts:
            return None

        chat = message.chat
        user = message.from_user
        chat_id = str(getattr(chat, "id", ""))
        user_id = str(getattr(user, "id", chat_id)) if user else chat_id
        is_group = getattr(chat, "type", "") in ("group", "supergroup")

        return {
            "chat_id": chat_id,
            "user_id": user_id,
            "message_id": str(getattr(message, "message_id", "")),
            "is_group": is_group,
            "message_thread_id": getattr(message, "message_thread_id", None),
            "content": content_parts,
        }

    async def _resolve_file_url(self, bot: Any, file_id: str) -> str:
        try:
            tg_file = await bot.get_file(file_id)
            file_path = getattr(tg_file, "file_path", "") or ""
            if file_path.startswith("http"):
                return file_path
            if file_path:
                return f"https://api.telegram.org/file/bot{self._config.bot_token}/{file_path}"
        except TelegramError:
            logger.debug("Telegram get_file failed", exc_info=True)
        return ""

    async def _send_typing_indicator(self, subject: ChannelSubject) -> None:
        if not self._config.show_typing or not self._app:
            return
        chat_id = subject.metadata.get("chat_id") or subject.subject_id
        try:
            await self._app.bot.send_chat_action(chat_id=int(chat_id), action="typing")
        except TelegramError:
            logger.debug("Telegram typing indicator failed", exc_info=True)

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        if not self._app or not text.strip():
            return
        chat_id = int(subject.metadata.get("chat_id") or subject.subject_id)
        thread_id = subject.metadata.get("message_thread_id")
        kwargs: dict[str, Any] = {}
        if thread_id:
            kwargs["message_thread_id"] = thread_id

        html = markdown_to_telegram_html(text)
        for chunk in self._chunk_text(html):
            try:
                from telegram.constants import ParseMode

                await self._app.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode=ParseMode.HTML,
                    **kwargs,
                )
            except TelegramError:
                try:
                    await self._app.bot.send_message(chat_id=chat_id, text=chunk, **kwargs)
                except TelegramError:
                    logger.exception("Telegram send_text failed chat_id=%s", chat_id)

    async def _send_content(self, subject: ChannelSubject, parts: list[ContentPart]) -> None:
        for part in parts:
            if isinstance(part, TextContent):
                await self._send_text(subject, part.text)
            else:
                await self._send_media(subject, part)

    async def _send_media(self, subject: ChannelSubject, media: ContentPart) -> None:
        if not self._app:
            return
        chat_id = int(subject.metadata.get("chat_id") or subject.subject_id)
        thread_id = subject.metadata.get("message_thread_id")
        kwargs: dict[str, Any] = {}
        if thread_id:
            kwargs["message_thread_id"] = thread_id

        url = self._get_media_url(media)
        has_inline = bool(getattr(media, "data", None) or getattr(media, "local_path", None))
        if not url and has_inline:
            try:
                data, mime = await self.load_media_bytes(media)
                if len(data) > _TELEGRAM_MAX_FILE_SIZE_BYTES:
                    await self._send_text(subject, f"[{self._get_media_label(media)}: file too large]")
                    return
                bio = BytesIO(data)
                bio.name = getattr(media, "filename", None) or f"file.{mime.split('/')[-1]}"
                if isinstance(media, ImageContent):
                    await self._app.bot.send_photo(chat_id=chat_id, photo=bio, **kwargs)
                elif isinstance(media, VideoContent):
                    await self._app.bot.send_video(chat_id=chat_id, video=bio, **kwargs)
                elif isinstance(media, AudioContent):
                    await self._app.bot.send_audio(chat_id=chat_id, audio=bio, **kwargs)
                else:
                    await self._app.bot.send_document(chat_id=chat_id, document=bio, **kwargs)
                return
            except Exception:  # pylint: disable=broad-except
                # MediaBackend and Telegram upload failures share this fail-soft boundary.
                logger.exception("Telegram local media upload failed")
                await self._send_text(subject, f"[{self._get_media_label(media)} (upload failed)]")
                return

        if url:
            if isinstance(media, ImageContent):
                await self._app.bot.send_photo(chat_id=chat_id, photo=url, **kwargs)
            elif isinstance(media, VideoContent):
                await self._app.bot.send_video(chat_id=chat_id, video=url, **kwargs)
            elif isinstance(media, AudioContent):
                await self._app.bot.send_audio(chat_id=chat_id, audio=url, **kwargs)
            else:
                await self._app.bot.send_document(chat_id=chat_id, document=url, **kwargs)
        elif has_inline:
            await self._send_text(subject, f"[{self._get_media_label(media)} (not deliverable)]")

    def _chunk_text(self, text: str) -> list[str]:
        if len(text) <= _TELEGRAM_SEND_CHUNK_SIZE:
            return [text]
        chunks: list[str] = []
        while text:
            chunks.append(text[:_TELEGRAM_SEND_CHUNK_SIZE])
            text = text[_TELEGRAM_SEND_CHUNK_SIZE:]
        return chunks

    def parse_inbound(self, raw_payload: Any) -> InboundMessage:
        if isinstance(raw_payload, InboundMessage):
            return raw_payload

        data = raw_payload if isinstance(raw_payload, dict) else {}
        chat_id = str(data.get("chat_id") or "unknown")
        user_id = str(data.get("user_id") or chat_id)
        content = data.get("content") or []
        is_group = bool(data.get("is_group"))

        metadata = {
            "chat_id": chat_id,
            "user_id": user_id,
            "message_id": data.get("message_id", ""),
            "message_thread_id": data.get("message_thread_id"),
            "is_group": is_group,
        }

        return InboundMessage(
            channel_id=self.channel_id,
            channel_type=self.channel_type,
            tenant_id=self._tenant_id,
            channel_subject=ChannelSubject(
                subject_id=chat_id,
                display_name=user_id,
                chat_type="group" if is_group else "direct",
                metadata=metadata,
            ),
            channel_session_id=self._connection_session or f"{self.channel_id}-unconnected",
            content=content,
            metadata=metadata,
        )
