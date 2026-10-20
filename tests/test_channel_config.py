"""Tests for ChannelConfig field-alias / credential-validation contract."""

from __future__ import annotations

import pytest

from octop_gateway.channel import ChannelConfig, ChannelCredentialsError
from octop_gateway.channels import (
    _CHANNEL_MAP,
    SUPPORTED_CHANNEL_KINDS,
    ChannelKind,
)
from octop_gateway.channels.dingtalk import DingTalkConfig
from octop_gateway.channels.feishu import FeishuConfig
from octop_gateway.channels.mqtt import MQTTConfig
from octop_gateway.channels.qq import QQConfig
from octop_gateway.channels.weixin.channel import WeixinConfig


def test_channel_kind_matches_builtin_map() -> None:
    assert {k.value for k in ChannelKind} == set(_CHANNEL_MAP)
    assert frozenset(_CHANNEL_MAP) == SUPPORTED_CHANNEL_KINDS


def test_from_dict_strips_strings_and_ignores_unknown() -> None:
    cfg = FeishuConfig.from_dict({"app_id": "  cli_x  ", "app_secret": "s", "junk": 1})
    assert cfg.app_id == "cli_x"
    assert cfg.app_secret == "s"


def test_qq_alias_client_secret_to_secret() -> None:
    cfg = QQConfig.from_dict({"app_id": "qq", "client_secret": "sec"})
    assert cfg.secret == "sec"


def test_explicit_field_wins_over_alias() -> None:
    cfg = QQConfig.from_dict({"app_id": "qq", "secret": "real", "client_secret": "alias"})
    assert cfg.secret == "real"


def test_dingtalk_aliases() -> None:
    cfg = DingTalkConfig.from_dict({"client_id": "ck", "client_secret": "cs"})
    assert cfg.app_key == "ck"
    assert cfg.app_secret == "cs"


def test_missing_credentials_reports_empty_fields() -> None:
    assert FeishuConfig.from_dict({"app_id": "x"}).missing_credentials() == ["app_secret"]
    assert FeishuConfig.from_dict({"app_id": "x", "app_secret": "y"}).missing_credentials() == []


def test_base_missing_credentials_default_empty() -> None:
    assert ChannelConfig().missing_credentials() == []


def test_weixin_from_dict_flat_token() -> None:
    cfg = WeixinConfig.from_dict({"token": "tok", "bot_uin": "wx-1"})
    assert len(cfg.accounts) == 1
    assert cfg.accounts[0].token == "tok"
    assert cfg.accounts[0].account_id == "wx-1"
    assert cfg.missing_credentials() == []


def test_weixin_from_dict_accounts_array() -> None:
    cfg = WeixinConfig.from_dict({"accounts": [{"account_id": "a", "token": "t"}]})
    assert cfg.accounts[0].account_id == "a"


def test_weixin_skips_tokenless_accounts_and_reports_missing() -> None:
    cfg = WeixinConfig.from_dict({"accounts": [{"account_id": "a"}]})
    assert cfg.accounts == []
    assert cfg.missing_credentials() == ["token"]


def test_weixin_no_token_reports_missing() -> None:
    assert WeixinConfig.from_dict({"bot_uin": "wx"}).missing_credentials() == ["token"]


def test_channel_credentials_error_carries_structured_data() -> None:
    err = ChannelCredentialsError("feishu", ["app_id", "app_secret"])
    assert err.kind == "feishu"
    assert err.missing == ["app_id", "app_secret"]
    with pytest.raises(ChannelCredentialsError):
        raise err


def test_numeric_fields_coerced_from_strings() -> None:
    """Console form values arrive as strings; the declared types must hold."""
    cfg = MQTTConfig.from_dict({"host": "broker", "port": "1883", "qos": "1"})
    assert cfg.port == 1883
    assert isinstance(cfg.port, int)
    assert cfg.qos == 1
    assert isinstance(cfg.qos, int)


def test_bool_fields_coerced_from_strings() -> None:
    assert MQTTConfig.from_dict({"tls_enabled": "false"}).tls_enabled is False
    assert MQTTConfig.from_dict({"tls_enabled": "true"}).tls_enabled is True


def test_string_port_accepted_by_paho() -> None:
    """Regression: a string port used to crash paho's port setter."""
    import paho.mqtt.client as mqtt

    cfg = MQTTConfig.from_dict({"host": "broker", "port": "1883"})
    client = mqtt.Client()
    client.port = cfg.port
    assert client.port == 1883


def test_non_numeric_port_rejected_with_field_name() -> None:
    with pytest.raises(ValueError, match="port"):
        MQTTConfig.from_dict({"host": "broker", "port": "not-a-port"})


def test_string_field_accepts_number() -> None:
    cfg = MQTTConfig.from_dict({"host": "broker", "port": 1883, "username": 42})
    assert cfg.username == "42"
