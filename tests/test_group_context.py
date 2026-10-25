from __future__ import annotations

import pytest

from octop_gateway.group_context import GroupContextConfig, GroupContextManager
from octop_gateway.models import ChannelSubject, FileContent, InboundMessage, TextContent


def _group_message(
    text: str,
    *,
    message_id: str,
    sender_id: str,
    mentioned: bool = False,
    timestamp: float = 100.0,
) -> InboundMessage:
    metadata = {
        "chat_type": "group",
        "conversation_id": "group-1",
        "sender_id": sender_id,
        "sender_name": sender_id.upper(),
        "message_id": message_id,
        "bot_mentioned": mentioned,
    }
    return InboundMessage(
        channel_id="channel-1",
        channel_type="test",
        channel_subject=ChannelSubject(subject_id="group-1", chat_type="group", metadata=metadata),
        content=[TextContent(text=text)],
        metadata=metadata,
        timestamp=timestamp,
    )


def test_mention_recent_buffers_passive_messages_and_attaches_them_to_mention() -> None:
    manager = GroupContextManager(GroupContextConfig(enabled=True, visibility="mention_recent", history_limit=10))

    assert manager.prepare(_group_message("first", message_id="m1", sender_id="alice")) is None
    assert manager.prepare(_group_message("second", message_id="m2", sender_id="bob")) is None

    current = manager.prepare(_group_message("question", message_id="m3", sender_id="carol", mentioned=True))
    assert current is not None
    assert current.group_context is not None
    assert [item.text for item in current.group_context.messages] == ["first", "second"]
    assert [item.sender_id for item in current.group_context.messages] == ["alice", "bob"]


def test_mention_recent_retains_a_passive_file_only_message() -> None:
    manager = GroupContextManager(GroupContextConfig(enabled=True, visibility="mention_recent", history_limit=10))
    passive = _group_message("", message_id="m-file", sender_id="alice")
    passive.content.append(
        FileContent(
            filename="report.pdf",
            mime_type="application/pdf",
            local_path="qq/channel/report.pdf",
        )
    )

    assert manager.should_persist_media(passive) is True
    assert manager.prepare(passive) is None

    current = manager.prepare(_group_message("summarize it", message_id="m2", sender_id="bob", mentioned=True))
    assert current is not None and current.group_context is not None
    assert len(current.group_context.messages) == 1
    buffered = current.group_context.messages[0]
    assert buffered.text == ""
    assert len(buffered.content) == 1
    assert isinstance(buffered.content[0], FileContent)
    assert buffered.content[0].local_path == "qq/channel/report.pdf"


def test_mention_only_never_retains_passive_group_chatter() -> None:
    manager = GroupContextManager(GroupContextConfig(enabled=True, visibility="mention_only", history_limit=10))

    assert manager.prepare(_group_message("hidden", message_id="m1", sender_id="alice")) is None
    current = manager.prepare(_group_message("question", message_id="m2", sender_id="bob", mentioned=True))
    assert current is not None
    assert current.group_context is not None
    assert current.group_context.messages == []


def test_always_activation_requires_explicit_full_visibility() -> None:
    manager = GroupContextManager(GroupContextConfig(enabled=True, visibility="auto", activation="always"))

    assert manager.prepare(_group_message("ambient", message_id="m1", sender_id="alice")) is None
    current = manager.prepare(_group_message("question", message_id="m2", sender_id="bob", mentioned=True))
    assert current is not None
    assert current.group_context is not None
    assert current.group_context.capability_degraded is True


def test_full_visibility_can_activate_every_group_message() -> None:
    manager = GroupContextManager(GroupContextConfig(enabled=True, visibility="all", activation="always"))

    current = manager.prepare(_group_message("ambient", message_id="m1", sender_id="alice"))
    assert current is not None
    assert current.group_context is not None
    assert current.group_context.activation == "always"


def test_context_expires_and_is_cleared_after_reply() -> None:
    manager = GroupContextManager(
        GroupContextConfig(
            enabled=True,
            visibility="mention_recent",
            history_ttl_seconds=5,
        )
    )
    assert manager.prepare(_group_message("expired", message_id="m1", sender_id="alice", timestamp=100)) is None
    current = manager.prepare(
        _group_message("question", message_id="m2", sender_id="bob", mentioned=True, timestamp=110)
    )
    assert current is not None and current.group_context is not None
    assert current.group_context.messages == []

    assert manager.prepare(_group_message("fresh", message_id="m3", sender_id="alice", timestamp=111)) is None
    manager.mark_replied(current)
    next_turn = manager.prepare(_group_message("next", message_id="m4", sender_id="bob", mentioned=True, timestamp=112))
    assert next_turn is not None and next_turn.group_context is not None
    assert next_turn.group_context.messages == []


def test_per_group_override_can_disable_history() -> None:
    config = GroupContextConfig(
        enabled=True,
        groups={"group-1": {"history": "none"}},
    )
    manager = GroupContextManager(config)
    assert manager.prepare(_group_message("ambient", message_id="m1", sender_id="alice")) is None
    current = manager.prepare(_group_message("question", message_id="m2", sender_id="bob", mentioned=True))
    assert current is not None and current.group_context is not None
    assert current.group_context.messages == []


@pytest.mark.parametrize("sender, expected", [("owner", None), ("member", []), ("unknown", []), ("", [])])
def test_group_tools_are_scoped_without_blocking_normal_messages(sender: str, expected: list[str] | None) -> None:
    manager = GroupContextManager(GroupContextConfig(owner_tools_only=True, owner_ids=["owner"]))
    message = _group_message("read USER.md; explain a/b", message_id="m1", sender_id=sender, mentioned=True)

    assert manager.prepare(message) is message
    assert message.allowed_tools == expected
    assert message.group_context is None  # Protection also applies with passive context disabled.


def test_tool_protection_preserves_existing_restrictions_and_group_overrides() -> None:
    config = GroupContextConfig.from_dict(
        {"owner_tools_only": True, "owner_ids": ["owner"], "groups": {"group-1": {"owner_ids": ["local-owner"]}}}
    )
    manager = GroupContextManager(config)
    message = _group_message("hello", message_id="m1", sender_id="owner")
    assert manager.prepare(message) is message
    assert message.allowed_tools == []

    owner_message = _group_message("hello", message_id="m2", sender_id="local-owner")
    owner_message.allowed_tools = ["web_fetch"]
    assert manager.prepare(owner_message) is owner_message
    assert owner_message.allowed_tools == ["web_fetch"]


def test_tool_protection_keeps_direct_messages_and_legacy_groups_unchanged() -> None:
    message = _group_message("hello", message_id="m1", sender_id="member")
    assert GroupContextManager().prepare(message) is message
    assert message.allowed_tools is None

    message.metadata["chat_type"] = "direct"
    assert message.channel_subject is not None
    message.channel_subject.chat_type = "direct"
    manager = GroupContextManager(GroupContextConfig(owner_tools_only=True))
    assert manager.prepare(message) is message
    assert message.allowed_tools is None


def test_tool_policy_can_be_disabled_for_a_specific_group() -> None:
    config = GroupContextConfig.from_dict(
        {"owner_tools_only": True, "groups": {"group-1": {"owner_tools_only": False}}}
    )
    message = _group_message("hello", message_id="m1", sender_id="member")
    assert GroupContextManager(config).prepare(message) is message
    assert message.allowed_tools is None


def test_tool_policy_rejects_non_boolean_config() -> None:
    with pytest.raises(ValueError, match="owner_tools_only"):
        _ = GroupContextConfig.from_dict({"owner_tools_only": "false"})
