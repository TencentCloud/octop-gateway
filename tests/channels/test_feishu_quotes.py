"""Offline checks for Feishu quoted text/media enrichment."""

from __future__ import annotations

import json
import unittest
from collections.abc import AsyncIterator
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import aiohttp

from octop_gateway.channels.feishu import FeishuChannel, FeishuConfig
from octop_gateway.media import FileSystemMediaBackend
from octop_gateway.models import ImageContent, InboundMessage, MessageEvent


async def _noop_processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
    yield MessageEvent.completed()


class Response:
    """Minimal asynchronous Feishu HTTP response."""

    def __init__(self, payload: Any) -> None:
        self.payload = payload

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, *args: Any) -> bool:
        return False

    def raise_for_status(self) -> None:
        pass

    async def json(self) -> Any:
        return self.payload


class FeishuQuoteTests(unittest.IsolatedAsyncioTestCase):
    """Exercise quoted content without credentials or network access."""

    def setUp(self) -> None:
        self.channel = FeishuChannel(_noop_processor, FeishuConfig())
        self.channel._bot_open_id = "bot-id"
        self.channel._get_auth_headers = AsyncMock(return_value={})
        self.http = SimpleNamespace(get=Mock())
        self.channel._ensure_http = AsyncMock(return_value=self.http)

    def message(self, *, parent: str = "parent", mentioned: bool = True, thread: str = "") -> InboundMessage:
        return self.channel.parse_inbound(
            {
                "message_id": "current",
                "message_type": "text",
                "content": json.dumps({"text": "Current question"}),
                "chat_id": "same-chat",
                "chat_type": "group",
                "sender": {"sender_id": "user"},
                "parent_id": parent,
                "root_id": "root",
                "thread_id": thread,
                "mentions": [{"key": "@bot", "open_id": "bot-id"}] if mentioned else [],
            }
        )

    def parent(self, content: dict[str, Any] | None = None, **updates: Any) -> dict[str, Any]:
        item = {
            "message_id": "parent",
            "chat_id": "same-chat",
            "msg_type": "text",
            "body": {"content": json.dumps(content or {"text": "Quoted content"})},
        }
        item.update(updates)
        return {"code": 0, "data": {"items": [item]}}

    async def test_event_keeps_quote_ids(self) -> None:
        incoming = []
        self.channel._running = True
        self.channel._enqueue_callback = incoming.append
        self.channel._add_reaction_async = Mock()
        msg = SimpleNamespace(
            message_id="current",
            message_type="text",
            content='{"text":"Question"}',
            chat_id="same-chat",
            chat_type="group",
            thread_id="",
            parent_id="parent",
            root_id="root",
            mentions=[],
        )
        self.channel._on_message_event(
            SimpleNamespace(
                event=SimpleNamespace(
                    message=msg, sender=SimpleNamespace(sender_type="user", sender_id=SimpleNamespace(open_id="user"))
                ),
                header=SimpleNamespace(create_time=""),
            )
        )
        self.assertEqual(incoming[0]["parent_id"], "parent")
        self.assertEqual(incoming[0]["root_id"], "root")

    async def test_plain_quote_is_added_without_changing_routing(self) -> None:
        self.http.get.return_value = Response(self.parent())
        msg = self.message(thread="topic")
        before = msg.channel_subject.subject_id
        await self.channel._preprocess_inbound(msg)
        self.assertIn("Quoted content", msg.text)
        self.assertIn("Current question", msg.text)
        self.assertEqual(msg.metadata["quoted_message_status"], "loaded")
        self.assertEqual(msg.metadata["root_id"], "root")
        self.assertEqual(msg.channel_subject.subject_id, before)
        self.assertEqual(msg.metadata["thread_id"], "topic")
        self.assertEqual(msg.metadata["message_id"], "current")
        self.assertTrue(msg.metadata["bot_mentioned"])
        self.assertEqual(self.http.get.call_args.kwargs["timeout"].total, 5)

    async def test_media_uses_quoted_message_resource_id(self) -> None:
        self.http.get.return_value = Response(self.parent({"image_key": "image-key"}, msg_type="image"))
        msg = self.message()
        await self.channel._preprocess_inbound(msg)
        image = next(p for p in msg.content if isinstance(p, ImageContent))
        self.assertIn("/messages/parent/resources/", image.url)
        self.assertNotIn("/messages/current/resources/", image.url)

    async def test_post_quote_keeps_text_and_image(self) -> None:
        content = {
            "zh_cn": {
                "title": "Title",
                "content": [[{"tag": "text", "text": "Post quote"}, {"tag": "img", "image_key": "image-key"}]],
            }
        }
        self.http.get.return_value = Response(self.parent(content, msg_type="post"))
        msg = self.message()
        await self.channel._preprocess_inbound(msg)
        self.assertIn("Post quote", msg.text)
        self.assertTrue(msg.has_media)

    async def test_quoted_image_is_persisted_before_processing(self) -> None:
        self.http.get.return_value = Response(self.parent({"image_key": "image-key"}, msg_type="image"))
        self.channel.fetch_remote_media = AsyncMock(return_value=(b"image-bytes", "image/png"))
        with TemporaryDirectory() as root:
            backend = FileSystemMediaBackend(root)
            self.channel.set_media_backend(backend)
            prepared = await self.channel._prepare_inbound(self.message())
            self.assertIsNotNone(prepared)
            message, _subject = prepared
            image = next(part for part in message.content if isinstance(part, ImageContent))
            self.assertTrue(image.local_path)
            self.assertEqual(await backend.read(image.local_path), b"image-bytes")
            self.assertIn("/messages/parent/resources/", self.channel.fetch_remote_media.call_args.args[0])

    async def test_unavailable_quote_preserves_current_question(self) -> None:
        for payload in [
            {"code": 99991672},
            {"code": 0, "data": {"items": []}},
            {"code": 0, "data": []},
            self.parent(deleted=True),
            self.parent(body=[]),
        ]:
            with self.subTest(payload=payload):
                self.http.get.return_value = Response(payload)
                msg = self.message()
                await self.channel._preprocess_inbound(msg)
                self.assertEqual(msg.metadata["quoted_message_status"], "unavailable")
                self.assertIn("Current question", msg.text)
                self.assertIn("引用消息暂不可读取", msg.text)

    async def test_cross_chat_quote_never_leaks_content(self) -> None:
        self.http.get.return_value = Response(self.parent({"text": "OTHER_CHAT_CONTENT"}, chat_id="other-chat"))
        msg = self.message()
        await self.channel._preprocess_inbound(msg)
        self.assertNotIn("OTHER_CHAT_CONTENT", msg.text)
        self.assertEqual(msg.metadata["quoted_message_status"], "unavailable")

    async def test_invalid_json_is_nonfatal(self) -> None:
        self.http.get.return_value = Response(self.parent(body={"content": "{"}))
        msg = self.message()
        await self.channel._preprocess_inbound(msg)
        self.assertEqual(msg.metadata["quoted_message_status"], "unavailable")

    async def test_transport_failure_is_nonfatal(self) -> None:
        for exc in [TimeoutError(), aiohttp.ClientConnectionError()]:
            with self.subTest(exc=exc):
                self.http.get.side_effect = exc
                msg = self.message()
                await self.channel._preprocess_inbound(msg)
                self.assertEqual(msg.metadata["quoted_message_status"], "unavailable")
                self.assertIn("Current question", msg.text)

    async def test_no_quote_does_not_call_api(self) -> None:
        msg = self.message(parent="")
        await self.channel._preprocess_inbound(msg)
        self.channel._ensure_http.assert_not_awaited()

    async def test_token_refresh_failure_preserves_current_question(self) -> None:
        self.channel._get_auth_headers.side_effect = RuntimeError("Feishu token refresh failed")
        msg = self.message()
        await self.channel._preprocess_inbound(msg)
        self.assertEqual(msg.metadata["quoted_message_status"], "unavailable")
        self.assertIn("Current question", msg.text)
        self.http.get.assert_not_called()

    async def test_quote_is_not_loaded_twice(self) -> None:
        self.http.get.return_value = Response(self.parent())
        msg = self.message()
        await self.channel._preprocess_inbound(msg)
        before = msg.text
        await self.channel._preprocess_inbound(msg)
        self.assertEqual(msg.text, before)
        self.assertEqual(self.http.get.call_count, 1)

    async def test_quote_does_not_activate_an_unmentioned_group_message(self) -> None:
        self.http.get.return_value = Response(self.parent({"text": "@bot-id quoted mention"}))
        msg = self.message(mentioned=False)
        await self.channel._preprocess_inbound(msg)
        self.assertFalse(msg.metadata["bot_mentioned"])

    async def test_quote_text_has_a_bounded_size(self) -> None:
        self.http.get.return_value = Response(self.parent({"text": "x" * 20000}))
        msg = self.message()
        await self.channel._preprocess_inbound(msg)
        self.assertEqual(msg.text.count("x"), 6000)
        self.assertIn("已截断", msg.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
