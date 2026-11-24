"""Unit tests for WhatsAppChannel, WhatsAppConfig, and Self-Hosted QR Bridge helpers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from octop_gateway.channels.whatsapp import (
    WhatsAppChannel,
    WhatsAppConfig,
    check_whatsapp_bridge_status,
    fetch_whatsapp_bridge_qr,
    normalize_whatsapp_identifier,
)
from octop_gateway.models import (
    ChannelSubject,
    FileContent,
    ImageContent,
    InboundMessage,
    MessageEvent,
)


async def _echo_processor(msg: InboundMessage) -> AsyncIterator[MessageEvent]:
    yield MessageEvent.text(f"Echo: {msg.text}")
    yield MessageEvent.completed()


class _FakeResponse:
    def __init__(
        self,
        status: int = 200,
        json_data: dict[str, Any] | None = None,
        body: bytes = b"",
        content_type: str = "application/json",
    ) -> None:
        self.status = status
        self._json = json_data or {}
        self._body = body
        self.content_type = content_type

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self) -> dict[str, Any]:
        return self._json

    async def text(self) -> str:
        return self._body.decode("utf-8", errors="replace") or json.dumps(self._json)

    async def read(self) -> bytes:
        return self._body

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


def test_whatsapp_config_dual_mode_credentials() -> None:
    # Mode 1: Official Meta Cloud API
    cloud_cfg = WhatsAppConfig.from_dict(
        {
            "provider": "cloud",
            "accessToken": "EAAB123",
            "phoneNumberId": "10987654321",
            "verifyToken": "vtok",
            "appSecret": "asec",
        }
    )
    assert cloud_cfg.effective_provider == "cloud"
    assert cloud_cfg.access_token == "EAAB123"
    assert cloud_cfg.phone_number_id == "10987654321"
    assert cloud_cfg.missing_credentials() == []
    assert WhatsAppConfig(provider="cloud").missing_credentials() == ["access_token", "phone_number_id"]

    # Mode 2: Self-hosted WAHA / Evolution QR bridge
    waha_cfg = WhatsAppConfig.from_dict(
        {
            "provider": "waha",
            "bridge_url": "http://127.0.0.1:3000",
            "bridge_api_key": "secret-key",
            "session_name": "personal",
        }
    )
    assert waha_cfg.effective_provider == "waha"
    assert waha_cfg.missing_credentials() == []
    assert WhatsAppConfig(provider="waha").missing_credentials() == ["bridge_url"]

    # Auto-detect bridge mode when bridge_url is provided without phone_number_id
    auto_bridge = WhatsAppConfig.from_dict({"bridge_url": "http://localhost:3000"})
    assert auto_bridge.effective_provider == "waha"
    assert auto_bridge.missing_credentials() == []


def test_normalize_whatsapp_identifier() -> None:
    assert normalize_whatsapp_identifier("+86 138-0013-8000") == "8613800138000"
    assert normalize_whatsapp_identifier("8613800138000@c.us") == "8613800138000"
    assert normalize_whatsapp_identifier("8613800138000:12@s.whatsapp.net") == "8613800138000"
    assert normalize_whatsapp_identifier("1203630123456789@g.us") == "1203630123456789@g.us"


def test_whatsapp_cloud_webhook_verification_and_inbound_parsing() -> None:
    cfg = WhatsAppConfig(
        provider="cloud",
        access_token="EAAB",
        phone_number_id="12345",
        verify_token="my_verify_token",
        app_secret="my_app_secret",
    )
    channel = WhatsAppChannel(_echo_processor, cfg)

    assert channel.verify_webhook_challenge("subscribe", "my_verify_token", "challenge_99") == "challenge_99"
    assert channel.verify_webhook_challenge("subscribe", "wrong_token", "challenge_99") is None

    webhook_payload = {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA_1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messaging_product": "whatsapp",
                            "metadata": {"phone_number_id": "12345"},
                            "contacts": [{"wa_id": "8613800001111", "profile": {"name": "Deshun"}}],
                            "messages": [
                                {
                                    "from": "8613800001111",
                                    "id": "wamid.HBgM1",
                                    "timestamp": "1700000000",
                                    "type": "image",
                                    "image": {
                                        "id": "MEDIA_101",
                                        "mime_type": "image/jpeg",
                                        "caption": "Analyze this receipt",
                                    },
                                    "context": {
                                        "id": "wamid.PARENT",
                                        "quoted_text": "Please send the receipt",
                                    },
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }
    raw_bytes = json.dumps(webhook_payload).encode("utf-8")
    sig = "sha256=" + hmac.new(b"my_app_secret", raw_bytes, hashlib.sha256).hexdigest()
    assert channel.verify_webhook_signature(sig, raw_bytes) is True
    assert channel.verify_webhook_signature("sha256=bad", raw_bytes) is False

    extracted = channel.extract_webhook_messages(webhook_payload)
    assert len(extracted) == 1
    # Duplicate webhook delivery is filtered
    assert channel.extract_webhook_messages(webhook_payload) == []
    assert channel.dropped_duplicate == 1

    msg = channel.parse_inbound(extracted[0])
    assert msg.channel_subject is not None
    assert msg.channel_subject.subject_id == "8613800001111"
    assert msg.channel_subject.chat_type == "direct"
    assert msg.text == "Analyze this receipt"
    assert msg.metadata["sender_name"] == "Deshun"
    assert msg.metadata["reply_to_message_id"] == "wamid.PARENT"
    assert msg.metadata["reply_quote_text"] == "Please send the receipt"
    images = [p for p in msg.content if isinstance(p, ImageContent)]
    assert len(images) == 1
    assert images[0].url == "whatsapp-media://MEDIA_101"


def test_whatsapp_waha_and_evolution_bridge_inbound_parsing() -> None:
    # WAHA group message with mention and quote
    waha_channel = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(provider="waha", bridge_url="http://127.0.0.1:3000"),
    )
    waha_event = {
        "event": "message",
        "session": "default",
        "me": {"id": "8619900009999@c.us", "pushName": "OctopBot"},
        "payload": {
            "id": "false_120363000@g.us_3EB0123",
            "timestamp": 1700000100,
            "from": "120363000@g.us",
            "fromMe": False,
            "participant": "8613811112222@c.us",
            "body": "@8619900009999 summarize the thread",
            "mentionedIds": ["8619900009999@c.us"],
            "_data": {
                "notifyName": "Bob",
                "quotedMsg": {"body": "Long discussion about Q4"},
                "quotedStanzaID": "3EB0000",
            },
        },
    }
    extracted = waha_channel.extract_webhook_messages(waha_event)
    assert len(extracted) == 1
    msg = waha_channel.parse_inbound(extracted[0])
    assert msg.channel_subject is not None
    assert msg.channel_subject.subject_id == "120363000@g.us"
    assert msg.channel_subject.chat_type == "group"
    assert msg.metadata["sender_id"] == "8613811112222@c.us"
    assert msg.metadata["sender_name"] == "Bob"
    assert msg.metadata["bot_mentioned"] is True
    assert msg.metadata["reply_quote_text"] == "Long discussion about Q4"
    assert msg.text == "summarize the thread"

    # Evolution API DM message
    evo_channel = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(provider="evolution", bridge_url="http://127.0.0.1:8080"),
    )
    evo_event = {
        "event": "messages.upsert",
        "instance": "personal",
        "data": {
            "key": {
                "remoteJid": "8613855556666@s.whatsapp.net",
                "fromMe": False,
                "id": "EVO_MSG_1",
            },
            "pushName": "Carol",
            "messageTimestamp": 1700000200,
            "message": {
                "conversation": "Hello from Evolution API",
            },
        },
    }
    evo_extracted = evo_channel.extract_webhook_messages(evo_event)
    assert len(evo_extracted) == 1
    evo_msg = evo_channel.parse_inbound(evo_extracted[0])
    assert evo_msg.channel_subject is not None
    assert evo_msg.channel_subject.subject_id == "8613855556666@s.whatsapp.net"
    assert evo_msg.channel_subject.chat_type == "direct"
    assert evo_msg.text == "Hello from Evolution API"


@pytest.mark.asyncio
async def test_whatsapp_cloud_and_waha_outbound_and_qr_helpers() -> None:
    # 1. Test Cloud API outbound text + media + two-step media download
    cloud_channel = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(provider="cloud", access_token="EAAB_TOK", phone_number_id="998877"),
    )
    requests_log: list[tuple[str, str, Any]] = []

    def _fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        requests_log.append(("GET", url, kwargs))
        if "998877" in url:
            return _FakeResponse(200, {"id": "998877", "display_phone_number": "+86 138 0000 0000"})
        if url.endswith("/MEDIA_101"):
            return _FakeResponse(
                200,
                {
                    "url": "https://lookaside.fbsbx.com/whatsapp_business/attachments/?mid=MEDIA_101",
                    "mime_type": "image/jpeg",
                },
            )
        if "lookaside.fbsbx.com" in url:
            return _FakeResponse(200, json_data=None, body=b"JPEG_BINARY", content_type="image/jpeg")
        if url.endswith("/api/sessions/default"):
            return _FakeResponse(200, {"name": "default", "status": "SCAN_QR_CODE"})
        if "/auth/qr" in url:
            return _FakeResponse(200, {"value": "2@WAHA_QR_RAW_DATA"})
        return _FakeResponse(200, {})

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        requests_log.append(("POST", url, kwargs))
        if url.endswith("/998877/media"):
            return _FakeResponse(200, {"id": "UPLOADED_MEDIA_55"})
        return _FakeResponse(200, {"messages": [{"id": "wamid.OUT1"}]})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.get = MagicMock(side_effect=_fake_get)
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.close = AsyncMock()
    cloud_channel._http = fake_http

    await cloud_channel.start()

    subject = ChannelSubject(
        subject_id="8613800001111",
        chat_type="direct",
        metadata={"chat_id": "8613800001111", "message_id": "wamid.IN1"},
    )
    await cloud_channel._send_text(subject, "Reply via Cloud API")
    post_calls = [item for item in requests_log if item[0] == "POST" and item[1].endswith("/998877/messages")]
    assert len(post_calls) == 1
    assert post_calls[0][2]["json"]["to"] == "8613800001111"
    assert post_calls[0][2]["json"]["context"] == {"message_id": "wamid.IN1"}

    # Outbound media via Cloud API
    b64_pdf = base64.b64encode(b"%PDF-1.4").decode("ascii")
    await cloud_channel._send_media(
        subject,
        FileContent(data=b64_pdf, filename="invoice.pdf", mime_type="application/pdf"),
    )
    media_send_calls = [item for item in requests_log if item[0] == "POST" and item[1].endswith("/998877/messages")]
    assert len(media_send_calls) == 2
    assert media_send_calls[1][2]["json"]["type"] == "document"
    assert media_send_calls[1][2]["json"]["document"] == {"id": "UPLOADED_MEDIA_55", "filename": "invoice.pdf"}

    # Inbound media download via whatsapp-media://MEDIA_101
    raw_img, mime = await cloud_channel.fetch_remote_media("whatsapp-media://MEDIA_101")
    assert raw_img == b"JPEG_BINARY"
    assert mime == "image/jpeg"

    # 2. Test WAHA QR code fetch & status helper + WAHA outbound send
    qr_info = await fetch_whatsapp_bridge_qr(
        "http://127.0.0.1:3000",
        provider="waha",
        session_name="default",
        api_key="k1",
        http=fake_http,
    )
    assert qr_info["status"] == "SCAN_QR_CODE"
    assert qr_info["qrcode"] == "2@WAHA_QR_RAW_DATA"

    status_info = await check_whatsapp_bridge_status(
        "http://127.0.0.1:3000",
        provider="waha",
        session_name="default",
        api_key="k1",
        http=fake_http,
    )
    assert status_info["status"] == "SCAN_QR_CODE"
    assert status_info["connected"] is False

    waha_channel = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(
            provider="waha",
            bridge_url="http://127.0.0.1:3000",
            bridge_api_key="k1",
            use_websocket=False,
        ),
    )
    waha_channel._http = fake_http
    await waha_channel.start()
    waha_subject = ChannelSubject(
        subject_id="8613800001111@c.us",
        chat_type="direct",
        metadata={"chat_id": "8613800001111@c.us", "message_id": "true_123"},
    )
    await waha_channel._send_text(waha_subject, "Reply via WAHA")
    waha_posts = [item for item in requests_log if item[0] == "POST" and item[1].endswith("/api/sendText")]
    assert len(waha_posts) == 1
    assert waha_posts[0][2]["json"] == {
        "session": "default",
        "chatId": "8613800001111@c.us",
        "text": "Reply via WAHA",
        "reply_to": "true_123",
    }

    await cloud_channel.stop()
    await waha_channel.stop()


@pytest.mark.asyncio
async def test_whatsapp_security_policies_and_status_reactions() -> None:
    from octop_gateway.group_context import GroupContextConfig

    cfg = WhatsAppConfig(
        provider="waha",
        bridge_url="http://127.0.0.1:3000",
        use_websocket=False,
        owner_only=False,
        allow_all_groups=False,
        allowed_group_ids=["120363000@g.us"],
        ignore_mention_all=True,
        status_reactions=True,
        group_context=GroupContextConfig(
            enabled=True,
            visibility="all",
            activation="mention",
            owner_commands_only=True,
            owner_tools_only=True,
            owner_ids=["+86 138-1111-2222"],
        ),
    )
    channel = WhatsAppChannel(_echo_processor, cfg)
    channel._self_jid = "8619900009999@c.us"

    # 1. Disallowed group is rejected when allow_all_groups=False
    assert (
        channel.extract_webhook_messages(
            {
                "event": "message",
                "payload": {
                    "id": "m_other_group",
                    "from": "999999@g.us",
                    "fromMe": False,
                    "participant": "8613811112222@c.us",
                    "body": "@8619900009999 hi",
                },
            }
        )
        == []
    )

    # 2. ignore_mention_all ignores @all even if self_jid is in mentionedIds
    mass_events = channel.extract_webhook_messages(
        {
            "event": "message",
            "payload": {
                "id": "m_mass",
                "from": "120363000@g.us",
                "fromMe": False,
                "participant": "8613899990000@c.us",
                "body": "@all hello everyone",
                "mentionedIds": ["8619900009999@c.us"],
            },
        }
    )
    assert len(mass_events) == 1
    mass_msg = channel.parse_inbound(mass_events[0])
    assert mass_msg.metadata["bot_mentioned"] is False

    # 3. GroupContextManager blocks /command from non-owner, allows owner with normalized phone (+86 138-1111-2222 vs 8613811112222@c.us)
    guest_cmd_raw = {
        "_format": "waha",
        "payload": {
            "id": "m_guest_cmd",
            "from": "120363000@g.us",
            "fromMe": False,
            "participant": "8613899990000@c.us",
            "body": "@8619900009999 /new",
            "mentionedIds": ["8619900009999@c.us"],
        },
    }
    assert await channel._prepare_inbound(guest_cmd_raw) is None

    owner_cmd_raw = {
        "_format": "waha",
        "payload": {
            "id": "m_owner_cmd",
            "from": "120363000@g.us",
            "fromMe": False,
            "participant": "8613811112222@c.us",
            "body": "@8619900009999 /new",
            "mentionedIds": ["8619900009999@c.us"],
        },
    }
    prepared_owner_res = await channel._prepare_inbound(owner_cmd_raw)
    assert prepared_owner_res is not None
    prepared_owner, owner_subject = prepared_owner_res
    assert prepared_owner.allowed_tools is None

    # 4. status_reactions sends ⏳ and ✅ via WAHA PUT /api/reaction
    put_calls: list[tuple[str, Any]] = []
    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.post = MagicMock(return_value=_FakeResponse(200, {"ok": True}))
    fake_http.put = MagicMock(
        side_effect=lambda url, **kw: (put_calls.append((url, kw.get("json"))), _FakeResponse(200, {"ok": True}))[1]
    )
    fake_http.close = AsyncMock()
    channel._http = fake_http

    ok = await channel._process_inbound(prepared_owner, owner_subject)
    assert ok is True
    reactions = [p["reaction"] for u, p in put_calls if u.endswith("/api/reaction")]
    assert reactions == ["⏳", "✅"]

    await channel.stop()


@pytest.mark.asyncio
async def test_whatsapp_review_fixes_security_typing_allowlist_and_evolution_mentions() -> None:
    from octop_gateway.group_context import GroupContextConfig

    # 1. Typing keepalive is enabled by default when show_typing=True
    typing_ch = WhatsAppChannel(_echo_processor, WhatsAppConfig(provider="cloud", show_typing=True))
    assert typing_ch.constraints.typing_keepalive_interval > 0
    no_typing_ch = WhatsAppChannel(_echo_processor, WhatsAppConfig(provider="cloud", show_typing=False))
    assert no_typing_ch.constraints.typing_keepalive_interval == 0.0

    # 2. Webhook auth fails closed when require_secret/require_token is True and secrets are unset
    unauth_cloud = WhatsAppChannel(
        _echo_processor, WhatsAppConfig(provider="cloud", access_token="t", phone_number_id="1")
    )
    assert unauth_cloud.verify_webhook_challenge("subscribe", "any", "chal", require_token=True) is None
    assert unauth_cloud.verify_webhook_request({}, b"{}", require_secret=True) is False

    waha_keyed = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(provider="waha", bridge_url="http://127.0.0.1:3000", bridge_api_key="k-secret"),
    )
    assert waha_keyed.verify_webhook_request({}, b"{}", require_secret=True) is False
    assert waha_keyed.verify_webhook_request({"X-Api-Key": "k-secret"}, b"{}", require_secret=True) is True

    # 3. fetch_remote_media does not leak tokens to spoofed query/subdomain URLs
    seen_headers: list[dict[str, str]] = []
    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.get = MagicMock(
        side_effect=lambda url, **kw: (
            seen_headers.append(dict(kw.get("headers") or {})),
            _FakeResponse(200, json_data=None, body=b"ok", content_type="image/png"),
        )[1]
    )
    fake_http.close = AsyncMock()
    media_ch = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(
            provider="waha",
            access_token="META_SECRET",
            bridge_url="http://127.0.0.1:3000",
            bridge_api_key="BRIDGE_SECRET",
        ),
    )
    media_ch._http = fake_http
    await media_ch.fetch_remote_media("https://evil.example/?x=facebook.com")
    await media_ch.fetch_remote_media("http://127.0.0.1:3000.evil.example/media.png")
    await media_ch.fetch_remote_media("http://127.0.0.1:3000/api/files/pic.png")
    assert seen_headers[0] == {}
    assert seen_headers[1] == {}
    assert seen_headers[2].get("X-Api-Key") == "BRIDGE_SECRET"
    await media_ch.stop()

    # 4. allow_all_groups=True ignores stale allowed_group_ids; owner_only + allow_all_dms=False allows owner_ids
    policy_ch = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(
            provider="waha",
            bridge_url="http://127.0.0.1:3000",
            owner_only=True,
            allow_all_dms=False,
            allow_all_groups=True,
            allowed_group_ids=["stale@g.us"],
            group_context=GroupContextConfig(enabled=True, owner_ids=["+86 138-1111-2222"]),
        ),
    )
    assert policy_ch._is_allowed_sender("8613811112222@c.us", "8613811112222@c.us", is_group=False) is True
    assert policy_ch._is_allowed_sender("8613811112222@c.us", "newgroup@g.us", is_group=True) is True
    assert policy_ch._is_allowed_sender("8613999999999@c.us", "8613999999999@c.us", is_group=False) is False

    # 5. Evolution group mention only triggers when the bot itself is mentioned, not another user
    evo_ch = WhatsAppChannel(
        _echo_processor,
        WhatsAppConfig(provider="evolution", bridge_url="http://127.0.0.1:8080", ignore_mention_all=True),
    )
    other_mention = evo_ch.extract_webhook_messages(
        {
            "event": "messages.upsert",
            "instance": "personal",
            "sender": "8619900009999@s.whatsapp.net",
            "data": {
                "key": {
                    "remoteJid": "120363@g.us",
                    "fromMe": False,
                    "id": "E1",
                    "participant": "8613811112222@s.whatsapp.net",
                },
                "message": {
                    "extendedTextMessage": {
                        "text": "@8613800000000 hi",
                        "contextInfo": {"mentionedJid": ["8613800000000@s.whatsapp.net"]},
                    }
                },
            },
        }
    )
    assert len(other_mention) == 1
    assert evo_ch.parse_inbound(other_mention[0]).metadata["bot_mentioned"] is False

    self_mention = evo_ch.extract_webhook_messages(
        {
            "event": "messages.upsert",
            "instance": "personal",
            "sender": "8619900009999@s.whatsapp.net",
            "data": {
                "key": {
                    "remoteJid": "120363@g.us",
                    "fromMe": False,
                    "id": "E2",
                    "participant": "8613811112222@s.whatsapp.net",
                },
                "message": {
                    "extendedTextMessage": {
                        "text": "@all @8619900009999 check this",
                        "contextInfo": {"mentionedJid": ["8619900009999@s.whatsapp.net"]},
                    }
                },
            },
        }
    )
    assert len(self_mention) == 1
    parsed_self = evo_ch.parse_inbound(self_mention[0])
    assert parsed_self.metadata["bot_mentioned"] is True
    assert "@8619900009999" not in parsed_self.text

    # 6. Bridge QR helper rejects non-HTTP schemes
    with pytest.raises(ValueError, match="http:// or https://"):
        await fetch_whatsapp_bridge_qr("file:///etc/passwd")


@pytest.mark.asyncio
async def test_whatsapp_channel_manager_worker_e2e_owner_ids_and_is_connected() -> None:
    import asyncio

    from octop_gateway.group_context import GroupContextConfig
    from octop_gateway.manager import ChannelManager

    delivered = asyncio.Event()
    sent_payloads: list[dict[str, Any]] = []

    def _fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        return _FakeResponse(200, {"status": "WORKING", "me": {"id": "8619900009999@c.us"}})

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        if url.endswith("/api/sendText"):
            sent_payloads.append(kwargs.get("json") or {})
            delivered.set()
            return _FakeResponse(200, {"id": "sent-1"})
        return _FakeResponse(200, {"ok": True})

    fake_http = MagicMock()
    fake_http.closed = False
    fake_http.get = MagicMock(side_effect=_fake_get)
    fake_http.post = MagicMock(side_effect=_fake_post)
    fake_http.close = AsyncMock()

    cfg = WhatsAppConfig(
        provider="waha",
        bridge_url="http://127.0.0.1:3000",
        use_websocket=False,
        allow_all_groups=True,
        group_context=GroupContextConfig(
            enabled=True,
            owner_ids=["+86 138-1111-2222"],
            owner_commands_only=True,
        ),
    )
    ch = WhatsAppChannel(_echo_processor, cfg, channel_id="wa-e2e")
    ch._http = fake_http
    assert ch.is_connected is False

    # 1. parse_inbound accepts already-parsed InboundMessage and normalizes owner_ids immediately
    owner_event = {
        "_format": "waha",
        "event": "message",
        "me": {"id": "8619900009999@c.us"},
        "payload": {
            "id": "M_OWNER",
            "from": "120363@g.us",
            "participant": "8613811112222@c.us",
            "fromMe": False,
            "body": "@8619900009999 /model gpt-5",
            "mentionedIds": ["8619900009999@c.us"],
        },
    }
    parsed_owner = ch.parse_inbound(owner_event)
    assert parsed_owner.metadata["sender_id"] == "+86 138-1111-2222"
    assert ch.parse_inbound(parsed_owner) is parsed_owner
    assert ch.group_context_manager.accepts(parsed_owner) is True

    # Non-owner group command is rejected right after parse_inbound
    guest_event = {
        "_format": "waha",
        "event": "message",
        "me": {"id": "8619900009999@c.us"},
        "payload": {
            "id": "M_GUEST",
            "from": "120363@g.us",
            "participant": "8613999999999@c.us",
            "fromMe": False,
            "body": "@8619900009999 /model gpt-5",
            "mentionedIds": ["8619900009999@c.us"],
        },
    }
    assert ch.group_context_manager.accepts(ch.parse_inbound(guest_event)) is False

    # 2. Full ChannelManager worker loop processes owner command through accepts() + handle_inbound(merged)
    manager = ChannelManager({"wa-e2e": ch})
    await manager.start()
    try:
        assert ch.is_connected is True
        ch.enqueue(owner_event)
        await asyncio.wait_for(delivered.wait(), timeout=2.0)
        assert len(sent_payloads) == 1
        assert sent_payloads[0]["chatId"] == "120363@g.us"
        assert sent_payloads[0]["text"] == "Echo: /model gpt-5"

        # 3. WAHA WebSocket is_connected tracks live websocket state
        fake_ws = MagicMock(closed=False)
        fake_task = MagicMock()
        fake_task.done.return_value = False
        ch._ws = fake_ws
        ch._ws_task = fake_task
        assert ch.is_connected is True
        fake_ws.closed = True
        assert ch.is_connected is False
        ch._ws = None
        ch._ws_task = None
    finally:
        await manager.stop()
    assert ch.is_connected is False
