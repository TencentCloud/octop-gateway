"""Adapter-level regressions migrated from Octop's runtime patches."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.error import BadRequest, EndPointNotFound, InvalidToken, NetworkError

from octop_gateway.channels.telegram import TelegramChannel, TelegramConfig
from octop_gateway.channels.telegram.policy import strip_own_mention
from octop_gateway.models import ChannelSubject, ImageContent, MessageEvent, TextContent

TABLE = "| name | value |\n|---|---|\n| cpu | 80% |"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("@OurBot, hello", "hello"),
        ("@OurBot check the site", "check the site"),
        ("does @ourbot know?", "does @ourbot know?"),
        ("line one\n@OurBot line two", "line one\n@OurBot line two"),
        ("ask @OtherBot and @hr together", "ask @OtherBot and @hr together"),
        ("mail john@ourbot.com", "mail john@ourbot.com"),
        ("@robot please", "@robot please"),
        ("@OurBot.com is a domain", "@OurBot.com is a domain"),
        ("@OurBot /help@OurBot", "/help"),
        ("@OurBot, /stop@OurBot", "/stop"),
        ("/help@OurBot", "/help"),
        ("@OurBot explain @OurBot", "explain @OurBot"),
        ("@OurBot explain:\n@OurBot stays", "explain:\n@OurBot stays"),
        ("@OurBotX stays", "@OurBotX stays"),
        ("@OurBot fix:\ndef f():\n    return  1", "fix:\ndef f():\n    return  1"),
    ],
)
def test_only_own_addressing_is_removed(raw, expected):
    assert strip_own_mention(raw, "OurBot") == expected


def test_unknown_bot_identity_keeps_mentions():
    text = "@somebot run\n    indented  code"
    assert strip_own_mention(text, None) == text


async def noop(_message):
    yield MessageEvent.completed()


def channel(**options):
    result = TelegramChannel(noop, config=TelegramConfig(bot_token="test", **options))
    result._bot_id, result._bot_username = 99, "OurBot"
    result._app = SimpleNamespace(
        bot=SimpleNamespace(
            **{
                name: AsyncMock(return_value={"message_id": 50})
                for name in (
                    "do_api_request",
                    "send_message",
                    "send_photo",
                    "send_media_group",
                    "edit_message_text",
                    "edit_message_reply_markup",
                    "send_message_draft",
                )
            }
        )
    )

    async def request(method, **kwargs):
        return await method(**kwargs)

    result._request = AsyncMock(side_effect=request)
    return result


def subject():
    return ChannelSubject(
        subject_id="-42",
        chat_type="group",
        metadata={
            "chat_id": "-42",
            "user_id": "7",
            "message_thread_id": 12,
            "source_message_id": "1",
        },
    )


async def test_rich_table_keeps_delivery_options():
    ch = channel(disable_notification=True, protect_content=True)
    await ch._send_text(subject(), TABLE)
    endpoint = ch._app.bot.do_api_request.call_args.args[0]
    data = ch._app.bot.do_api_request.call_args.kwargs["api_kwargs"]
    assert endpoint == "sendRichMessage"
    assert data["rich_message"] == {"markdown": TABLE}
    assert data["message_thread_id"] == 12
    assert data["reply_parameters"].message_id == 1
    assert data["disable_notification"] and data["protect_content"]
    ch._app.bot.send_message.assert_not_awaited()
    assert ch._request.call_args.kwargs["chat_id"] == -42


async def test_rich_preview_released_before_next_segment():
    ch = channel()
    state = {"text": TABLE, "finalizing": True, "message_id": 99, "stop_token": "stop"}
    ch.interactions.pending["stop"] = object()
    ch._stream.set(state)
    await ch._send_text(subject(), TABLE)
    endpoint = ch._app.bot.do_api_request.call_args.args[0]
    data = ch._app.bot.do_api_request.call_args.kwargs["api_kwargs"]
    assert endpoint == "editMessageText" and data["message_id"] == 99
    assert data["reply_markup"].inline_keyboard == ()
    assert "message_id" not in state and not state["finalizing"]
    assert not ch.interactions.pending
    state.update(text=TABLE, finalizing=True)
    await ch._send_text(subject(), TABLE)
    assert ch._app.bot.do_api_request.call_args.args[0] == "sendRichMessage"


async def test_nonfinal_table_does_not_overwrite_preview():
    ch = channel()
    state = {"text": TABLE, "finalizing": False, "message_id": 99}
    ch._stream.set(state)
    await ch._send_text(subject(), TABLE)
    assert ch._app.bot.do_api_request.call_args.args[0] == "sendRichMessage"
    assert state["message_id"] == 99


async def test_rich_final_clears_private_draft():
    ch = channel()
    state = {"text": TABLE, "finalizing": True, "draft_sent": True, "draft_id": 123}
    ch._stream.set(state)
    await ch._send_text(subject(), TABLE)
    assert ch._app.bot.send_message_draft.call_args.kwargs["text"] == ""
    assert not state["finalizing"] and not state.get("draft_sent")


@pytest.mark.parametrize(
    "error", [BadRequest("rich unsupported"), EndPointNotFound("unknown endpoint"), InvalidToken("unknown method")]
)
async def test_explicit_rejection_uses_aligned_code_block(error):
    ch = channel()
    ch._app.bot.do_api_request.side_effect = error
    await ch._send_text(subject(), TABLE)
    sent = ch._app.bot.send_message.call_args.kwargs
    assert sent["parse_mode"] == "HTML" and "<pre>" in sent["text"]
    assert "|---|" not in sent["text"]


async def test_network_failure_is_not_resent():
    ch = channel()
    state = {"text": TABLE}
    ch._stream.set(state)
    ch._app.bot.do_api_request.side_effect = NetworkError("ambiguous timeout")
    with pytest.raises(NetworkError):
        await ch._send_text(subject(), TABLE)
    assert state["delivery_failed"]
    await ch._send_text(subject(), TABLE)
    ch._app.bot.do_api_request.assert_awaited_once()
    ch._app.bot.send_message.assert_not_awaited()


async def test_fenced_table_uses_ordinary_delivery():
    ch = channel()
    await ch._send_text(subject(), f"```\n{TABLE}\n```")
    ch._app.bot.do_api_request.assert_not_awaited()
    ch._app.bot.send_message.assert_awaited_once()


def query(token, *, label="Alpha", user=7, topic=12, is_topic=True, text="Pick one"):
    return SimpleNamespace(
        data=token,
        answer=AsyncMock(),
        from_user=SimpleNamespace(id=user, language_code="en"),
        message=SimpleNamespace(
            chat=SimpleNamespace(id=-42),
            message_id=50,
            message_thread_id=topic,
            is_topic_message=is_topic,
            text=text,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=token)]]),
        ),
    )


async def test_choice_feedback_and_keyboard_settlement():
    ch = channel()
    action = AsyncMock()
    token = ch.register_callback(subject(), action)
    tap = query(token)
    await ch._handle_callback(SimpleNamespace(callback_query=tap))
    action.assert_awaited_once()
    assert tap.answer.call_args.kwargs["text"] == "✓ Alpha"
    settled = ch._app.bot.edit_message_text.call_args.kwargs
    assert settled["text"] == "Pick one\n✓ Alpha"
    assert not settled["reply_markup"].inline_keyboard


@pytest.mark.parametrize("user,topic", [(8, 12), (7, 13)])
async def test_foreign_tap_preserves_live_keyboard(user, topic):
    ch = channel()
    action = AsyncMock()
    token = ch.register_callback(subject(), action)
    await ch._handle_callback(SimpleNamespace(callback_query=query(token, user=user, topic=topic)))
    action.assert_not_awaited()
    assert token in ch.interactions.pending
    ch._app.bot.edit_message_text.assert_not_awaited()
    ch._app.bot.edit_message_reply_markup.assert_not_awaited()
    await ch._handle_callback(SimpleNamespace(callback_query=query(token)))
    action.assert_awaited_once()


@pytest.mark.parametrize("label", ["Alpha", "→"])
async def test_expired_choice_collapses_keyboard(label):
    ch = channel()
    token = ch.register_callback(subject(), AsyncMock(), ttl=-1)
    tap = query(token, label=label)
    await ch._handle_callback(SimpleNamespace(callback_query=tap))
    assert "expired" in tap.answer.call_args.kwargs["text"]
    if label == "→":
        ch._app.bot.edit_message_reply_markup.assert_awaited_once()
        ch._app.bot.edit_message_text.assert_not_awaited()
    else:
        ch._app.bot.edit_message_text.assert_awaited_once()


async def test_stop_leaves_preview_cleanup_to_stream():
    ch = channel()
    action = AsyncMock()
    token = ch.register_callback(subject(), action, settle=False)
    await ch._handle_callback(SimpleNamespace(callback_query=query(token, label="Stop")))
    action.assert_awaited_once()
    ch._app.bot.edit_message_text.assert_not_awaited()
    ch._app.bot.edit_message_reply_markup.assert_not_awaited()


async def test_long_selection_only_removes_keyboard():
    ch = channel()
    token = ch.register_callback(subject(), AsyncMock())
    await ch._handle_callback(SimpleNamespace(callback_query=query(token, text="x" * 4096)))
    ch._app.bot.edit_message_reply_markup.assert_awaited_once()
    ch._app.bot.edit_message_text.assert_not_awaited()


@pytest.mark.parametrize("own_reply", [False, True])
async def test_quote_fields_survive_normalization_without_payload_leak(own_reply):
    ch = channel()
    raw = Message.de_json(
        {
            "message_id": 2,
            "date": 0,
            "chat": {"id": -42, "type": "supergroup"},
            "from": {"id": 7, "first_name": "User", "is_bot": False},
            "text": "@OurBot, explain this\n    preserve  spacing and mail@ourbot.com @OtherBot",
            "quote": {"text": "selected", "position": 0},
            "reply_to_message": {
                "message_id": 1,
                "date": 0,
                "chat": {"id": -42, "type": "supergroup"},
                "text": "quoted" * 1000,
                "from": {"id": 99 if own_reply else 8, "first_name": "Author", "is_bot": own_reply},
            },
        },
        None,
    )
    native = await ch._extract_message(raw)
    message = ch.parse_inbound(native)
    assert message.text == "explain this\n    preserve  spacing and mail@ourbot.com @OtherBot"
    assert message.metadata["reply_quote_text"] == "selected"
    assert len(message.metadata["reply_to_text"]) == 2000
    assert message.metadata["reply_to_is_own_bot"] is own_reply
    assert message.channel_subject.metadata["reply_to_text"] == message.metadata["reply_to_text"]
    assert "content" not in message.metadata


async def test_own_command_suffix_is_normalized():
    ch = channel()
    raw = Message.de_json(
        {
            "message_id": 1,
            "date": 0,
            "chat": {"id": 42, "type": "private"},
            "from": {"id": 7, "first_name": "User", "is_bot": False},
            "text": "/models@OurBot",
        },
        None,
    )
    assert ch.parse_inbound(await ch._extract_message(raw)).text == "/models"


@pytest.mark.parametrize("origin", ["session", "cache"])
@pytest.mark.parametrize("topic", [None, 12])
@pytest.mark.parametrize("payload", ["text", "content", "image", "album", "table"])
async def test_proactive_push_does_not_reply_to_last_message(origin, topic, payload):
    ch = channel(albums=True)
    inbound = ch.parse_inbound(
        {
            "chat_id": "-42" if topic else "42",
            "user_id": "7",
            "message_id": "123",
            "message_thread_id": topic,
            "is_group": topic is not None,
            "content": [TextContent(text="unrelated previous question")],
            "reply_to_message_id": 122,
            "reply_to_text": "old reply",
            "reply_quote_text": "old selection",
            "reply_to_is_own_bot": True,
        }
    )
    original = inbound.channel_subject
    before = dict(original.metadata)
    if origin == "cache":
        ch._known_subjects[original.subject_id] = original
        target = ChannelSubject(subject_id=original.subject_id)
    else:
        target = original

    if payload in ("text", "table"):
        await ch.push_text(target, TABLE if payload == "table" else "scheduled update")
    elif payload == "content":
        await ch.push_message(target, [TextContent(text="proactive care")])
    else:
        count = 2 if payload == "album" else 1
        await ch.push_message(target, [ImageContent(url=f"https://example.com/{i}.png") for i in range(count)])

    if payload == "table":
        endpoint = ch._app.bot.do_api_request.call_args.args[0]
        sent = ch._app.bot.do_api_request.call_args.kwargs["api_kwargs"]
        assert endpoint == "sendRichMessage"
    else:
        method = "send_photo" if payload == "image" else "send_media_group" if payload == "album" else "send_message"
        sent = getattr(ch._app.bot, method).call_args.kwargs
    assert "reply_parameters" not in sent
    assert sent["chat_id"] == int(before["chat_id"])
    assert sent.get("message_thread_id") == topic
    assert original.metadata == before

    # Preparing a push must not erase the inbound turn's normal reply target.
    await ch.reply_text(original, "direct answer")
    reply = ch._app.bot.send_message.call_args.kwargs
    assert reply["reply_parameters"].message_id == 123
    assert reply.get("message_thread_id") == topic


@pytest.mark.parametrize(
    "error", [ValueError("host failure"), NetworkError("https://example.com/private"), asyncio.CancelledError()]
)
async def test_failed_choice_is_acknowledged_and_retryable(error):
    ch = channel()
    action = AsyncMock(side_effect=[error, None])
    token = ch.register_callback(subject(), action)
    tap = query(token)
    with pytest.raises(type(error)):
        await ch._handle_callback(SimpleNamespace(callback_query=tap))
    tap.answer.assert_awaited_once()
    assert "retry" in tap.answer.call_args.kwargs["text"]
    assert token in ch.interactions.pending
    ch._app.bot.edit_message_text.assert_not_awaited()
    ch._app.bot.edit_message_reply_markup.assert_not_awaited()
    await ch._handle_callback(SimpleNamespace(callback_query=query(token)))
    assert action.await_count == 2
    assert token not in ch.interactions.pending


async def test_duplicate_tap_during_failed_action_keeps_keyboard():
    ch = channel()
    entered, release = asyncio.Event(), asyncio.Event()

    async def action():
        entered.set()
        await release.wait()
        raise ValueError("host failure")

    token = ch.register_callback(subject(), action)
    task = asyncio.create_task(ch._handle_callback(SimpleNamespace(callback_query=query(token))))
    try:
        await entered.wait()
        await ch._handle_callback(SimpleNamespace(callback_query=query(token)))
        ch._app.bot.edit_message_text.assert_not_awaited()
        ch._app.bot.edit_message_reply_markup.assert_not_awaited()
    finally:
        release.set()
        with pytest.raises(ValueError, match="host failure"):
            await task
    assert token in ch.interactions.pending


async def test_failed_acknowledgement_does_not_repeat_successful_action():
    ch = channel()
    action = AsyncMock()
    token = ch.register_callback(subject(), action)
    tap = query(token)
    tap.answer.side_effect = NetworkError("acknowledgement timeout")
    with pytest.raises(NetworkError):
        await ch._handle_callback(SimpleNamespace(callback_query=tap))
    assert token not in ch.interactions.pending
    await ch._handle_callback(SimpleNamespace(callback_query=query(token)))
    action.assert_awaited_once()


async def test_failed_acknowledgement_preserves_original_action_error():
    ch = channel()
    token = ch.register_callback(subject(), AsyncMock(side_effect=ValueError("host failure")))
    tap = query(token)
    tap.answer.side_effect = NetworkError("acknowledgement timeout")
    with pytest.raises(ValueError, match="host failure"):
        await ch._handle_callback(SimpleNamespace(callback_query=tap))
    assert token in ch.interactions.pending


async def test_failed_action_does_not_restore_expired_token(monkeypatch):
    from octop_gateway.channels.telegram.interaction import Interactions

    now = [0.0]
    monkeypatch.setattr("octop_gateway.channels.telegram.interaction.time.monotonic", lambda: now[0])
    interactions = Interactions()

    async def action():
        now[0] = 2.0
        raise ValueError("host failure")

    token = interactions.register("7", "42", None, action, ttl=1)
    with pytest.raises(ValueError):
        await interactions.invoke(token, "7", "42", None)
    assert token not in interactions.pending
    assert not interactions.running


async def test_failed_action_restore_keeps_pending_bound():
    from octop_gateway.channels.telegram.interaction import Interactions

    interactions = Interactions()

    async def action():
        for _ in range(1000):
            interactions.register("7", "42", None, AsyncMock())
        raise ValueError("host failure")

    token = interactions.register("7", "42", None, action)
    with pytest.raises(ValueError):
        await interactions.invoke(token, "7", "42", None)
    assert token in interactions.pending
    assert len(interactions.pending) == 1000
    assert not interactions.running


async def test_rich_table_uses_real_public_sdk_serialization():
    from telegram.request import BaseRequest

    transport = MagicMock(spec=BaseRequest)
    transport.post = AsyncMock(return_value={"id": 99, "is_bot": True, "first_name": "Bot"})
    async with Bot("123456:TEST", request=transport, get_updates_request=transport) as bot:
        ch = channel(disable_notification=True, protect_content=True)
        ch._app.bot = bot
        await ch._send_text(subject(), TABLE)
        sent = transport.post.call_args.kwargs
        assert sent["url"].endswith("/sendRichMessage")
        parameters = sent["request_data"].json_parameters
        assert json.loads(parameters["rich_message"]) == {"markdown": TABLE}
        assert json.loads(parameters["reply_parameters"])["message_id"] == 1
        assert parameters["message_thread_id"] == "12"
        assert parameters["disable_notification"] == "true"
