"""QQ quote identity and safe send fallback contracts."""

from unittest.mock import AsyncMock

import pytest

from octop_gateway.channels.qq import QQChannel, QQConfig
from octop_gateway.models import ChannelSubject, MessageEvent


async def _processor(message):
    yield MessageEvent.completed()


def _channel():
    return QQChannel(processor=_processor, config=QQConfig(app_id="app", secret="secret", token="token"))


@pytest.mark.asyncio
async def test_ready_stores_authenticated_bot_identity():
    channel = _channel()
    await channel._handle_op(
        AsyncMock(),
        0,
        {"op": 0, "t": "READY", "d": {"session_id": "session", "user": {"id": "bot-user"}}},
    )
    assert channel._bot_user_id == "bot-user"


@pytest.mark.parametrize("command", ["/stop", "/model"])
def test_quote_does_not_prefix_command(command):
    channel = _channel()
    channel._bot_user_id = "self"
    msg = channel.parse_inbound(
        {
            "t": "GROUP_MESSAGE_CREATE",
            "d": {
                "id": "message",
                "content": command,
                "group_openid": "group",
                "author": {"member_openid": "member", "bot": False},
                "message_scene": {"ext": ["msg_idx=current", "ref_msg_idx=previous"]},
                "msg_elements": [{"msg_idx": "previous", "content": "Earlier", "author": {"id": "self", "bot": True}}],
            },
        }
    )
    assert msg.text == command
    assert msg.metadata["reply_quote_text"] == "Earlier"
    assert msg.metadata["reply_to_bot"] is True
    assert msg.metadata["msg_idx"] == "current"


@pytest.mark.asyncio
@pytest.mark.parametrize("status,body", [(0, ""), (429, "{}"), (503, "{}"), (400, '{"code":40034006}')])
async def test_uncertain_or_unrelated_failure_does_not_retry(status, body):
    channel = _channel()
    channel._try_send_message = AsyncMock(return_value=(False, status, body))
    channel._send_message = AsyncMock()
    subject = ChannelSubject(subject_id="group", metadata={"msg_type": "group", "group_openid": "group"})
    with pytest.raises(RuntimeError):
        await channel._send_text(subject, "Reply")
    channel._send_message.assert_not_awaited()
    assert not channel._markdown_unsupported


def test_quote_targets_current_message_and_never_leaks_into_push():
    from octop_gateway.push_routing import strip_ephemeral_push_meta

    channel = _channel()
    meta = {"msg_id": "passive", "msg_idx": "current", "ref_msg_idx": "previous"}
    payload = {}
    channel._apply_quote(payload, "group", meta)
    assert payload["message_reference"] == {"message_id": "current"}
    push_payload = {}
    channel._apply_quote(push_payload, "group", strip_ephemeral_push_meta(dict(meta)))
    assert "message_reference" not in push_payload


def test_sent_reference_identity_is_isolated_by_group():
    channel = _channel()
    channel._remember_reference({"group_openid": "group"}, '{"ext_info":{"ref_idx":"sent"}}')
    assert ("group", "sent") in channel._sent_references
    assert ("other-group", "sent") not in channel._sent_references
