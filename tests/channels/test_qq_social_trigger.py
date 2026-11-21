import pytest

from octop_gateway.channels.qq.channel import QQChannel, QQConfig


def test_social_trigger_switch_reads_nested_qq_group_policy() -> None:
    assert QQConfig.from_dict({}).social_trigger_enabled is True
    assert QQConfig.from_dict({"group_context": {"social_trigger_enabled": False}}).social_trigger_enabled is False
    assert QQConfig.from_dict({"group_context": {"social_trigger_enabled": True}}).social_trigger_enabled is True
    with pytest.raises(ValueError, match="social_trigger_enabled"):
        QQConfig.from_dict({"group_context": {"social_trigger_enabled": "false"}})


def test_plugin_group_settings_are_available_without_exposing_mutable_config() -> None:
    config = QQConfig.from_dict({"group_context": {"custom_plugin_switch": False, "history_limit": 10}})
    channel = QQChannel(processor=None, config=config)  # type: ignore[arg-type]
    settings = channel.plugin_group_settings
    assert settings == {"custom_plugin_switch": False}
    settings["custom_plugin_switch"] = True
    assert channel.plugin_group_settings["custom_plugin_switch"] is False
