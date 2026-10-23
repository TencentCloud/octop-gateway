"""Adapter-level regressions migrated from Octop's runtime patches."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.error import BadRequest, InvalidToken, NetworkError

from octop_gateway.channels.telegram import TelegramChannel, TelegramConfig
from octop_gateway.channels.telegram.policy import strip_own_mention
from octop_gateway.models import ChannelSubject, MessageEvent

TABLE = "| name | value |\n|---|---|\n| cpu | 80% |"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("@OurBot, hello", "hello"),
        ("@OurBot check the site", "check the site"),
        ("does @ourbot know?", "does know?"),
        ("line one\n@OurBot line two", "line one\nline two"),
        ("ask @OtherBot and @hr together", "ask @OtherBot and @hr together"),
        ("mail john@ourbot.com", "mail john@ourbot.com"),
        ("@robot please", "@robot please"),
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
                    "_post",
                    "send_message",
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
    endpoint, data = ch._app.bot._post.call_args.args
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
    endpoint, data = ch._app.bot._post.call_args.args
    assert endpoint == "editMessageText" and data["message_id"] == 99
    assert data["reply_markup"].inline_keyboard == ()
    assert "message_id" not in state and not state["finalizing"]
    assert not ch.interactions.pending
    state.update(text=TABLE, finalizing=True)
    await ch._send_text(subject(), TABLE)
    assert ch._app.bot._post.call_args.args[0] == "sendRichMessage"


async def test_nonfinal_table_does_not_overwrite_preview():
    ch = channel()
    state = {"text": TABLE, "finalizing": False, "message_id": 99}
    ch._stream.set(state)
    await ch._send_text(subject(), TABLE)
    assert ch._app.bot._post.call_args.args[0] == "sendRichMessage"
    assert state["message_id"] == 99


async def test_rich_final_clears_private_draft():
    ch = channel()
    state = {"text": TABLE, "finalizing": True, "draft_sent": True, "draft_id": 123}
    ch._stream.set(state)
    await ch._send_text(subject(), TABLE)
    assert ch._app.bot.send_message_draft.call_args.kwargs["text"] == ""
    assert not state["finalizing"] and not state.get("draft_sent")


@pytest.mark.parametrize("error", [BadRequest("rich unsupported"), InvalidToken("unknown method")])
async def test_explicit_rejection_uses_aligned_code_block(error):
    ch = channel()
    ch._app.bot._post.side_effect = error
    await ch._send_text(subject(), TABLE)
    sent = ch._app.bot.send_message.call_args.kwargs
    assert sent["parse_mode"] == "HTML" and "<pre>" in sent["text"]
    assert "|---|" not in sent["text"]


async def test_network_failure_is_not_resent():
    ch = channel()
    state = {"text": TABLE}
    ch._stream.set(state)
    ch._app.bot._post.side_effect = NetworkError("ambiguous timeout")
    with pytest.raises(NetworkError):
        await ch._send_text(subject(), TABLE)
    assert state["delivery_failed"]
    await ch._send_text(subject(), TABLE)
    ch._app.bot._post.assert_awaited_once()
    ch._app.bot.send_message.assert_not_awaited()


async def test_fenced_table_uses_ordinary_delivery():
    ch = channel()
    await ch._send_text(subject(), f"```\n{TABLE}\n```")
    ch._app.bot._post.assert_not_awaited()
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
