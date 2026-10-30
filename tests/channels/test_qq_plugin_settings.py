from octop_gateway.channels.qq import QQChannel, QQConfig


def test_plugin_switches_are_scoped_to_group_policy_and_return_a_copy():
    config = QQConfig.from_dict({"group_context": {"enrichment": False, "history_limit": 10}})
    channel = QQChannel(processor=None, config=config)
    settings = channel.plugin_group_settings
    assert settings == {"enrichment": False}
    settings["enrichment"] = True
    assert channel.plugin_group_settings["enrichment"] is False
    assert QQConfig.from_dict({}).plugin_group_settings == {}
