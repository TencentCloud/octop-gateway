"""Unit tests for Telegram channel."""

from __future__ import annotations

from octop_gateway.channels.telegram import TelegramChannel, TelegramConfig
from octop_gateway.channels.telegram.format_html import markdown_to_telegram_html
from octop_gateway.models import InboundMessage, MessageEvent, TextContent


async def _noop_processor(msg: InboundMessage):
    yield MessageEvent.completed()


class TestTelegramChannelUnit:
    def test_config_display_flags(self) -> None:
        config = TelegramConfig(bot_token="token", show_tool_hints=False, show_thinking=True)
        ch = TelegramChannel(processor=_noop_processor, config=config)
        assert ch.channel_type == "telegram"
        assert ch.constraints.show_tool_hints is False
        assert ch.constraints.show_thinking is True

    def test_parse_inbound(self) -> None:
        ch = TelegramChannel(processor=_noop_processor, config=TelegramConfig(bot_token="token"))
        msg = ch.parse_inbound(
            {
                "chat_id": "12345",
                "user_id": "67890",
                "message_id": "99",
                "is_group": False,
                "content": [TextContent(text="hi telegram")],
            }
        )
        assert msg.channel_subject is not None
        assert msg.channel_subject.subject_id == "12345"
        assert msg.metadata["user_id"] == "67890"
        assert msg.content[0].text == "hi telegram"  # type: ignore[union-attr]

    def test_markdown_to_html(self) -> None:
        html = markdown_to_telegram_html("**bold** and `code`")
        assert "<b>bold</b>" in html
        assert "<code>code</code>" in html
