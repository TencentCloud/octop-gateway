"""WhatsApp channel implementation supporting both Official Meta Cloud API and Self-Hosted QR Bridges.

Dual-mode architecture:
  1. **Official Meta WhatsApp Business Cloud API** (``provider="cloud"``, Graph API v21.0+):
     - Webhook verification (``hub.challenge`` + ``X-Hub-Signature-256`` HMAC-SHA256)
     - Inbound text, media (``whatsapp-media://{media_id}`` two-step resolution), interactive replies,
       and quote context (``context.id``)
     - Outbound text, media upload (``/{phone_number_id}/media``), native read receipts + typing indicators
  2. **Self-Hosted QR Bridge Services** (``provider="waha"`` or ``"evolution"``):
     - Personal WhatsApp account support via QR scan without enterprise Meta qualification
     - WAHA live WebSocket event stream (``/ws?session=...&events=message``) so no public webhook IP is needed,
       plus HTTP webhook ingestion for both WAHA and Evolution API
     - Helper utilities (:func:`fetch_whatsapp_bridge_qr`, :func:`check_whatsapp_bridge_status`) for
       rendering and polling login QR codes in host dashboards
     - Full DM (``@c.us`` / ``@s.whatsapp.net``) and Group (``@g.us``) routing, media send/download,
       quote replies, and typing indicators
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import logging
import re
import time
import uuid
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Self
from urllib.parse import urlencode, urlparse, urlunparse

import aiohttp

from octop_gateway.channel import BaseChannel, ChannelConfig, MessageProcessor, _guess_content_type
from octop_gateway.constraints import ChannelConstraints
from octop_gateway.group_context import GroupContextConfig
from octop_gateway.models import (
    AudioContent,
    ChannelSubject,
    ContentPart,
    FileContent,
    ImageContent,
    InboundMessage,
    TextContent,
    VideoContent,
)
from octop_gateway.push_routing import alias_subject_fields
from octop_gateway.utils import extract_media, extract_text

logger = logging.getLogger(__name__)

_WHATSAPP_MAX_MESSAGE_LEN = 4000
_DEDUP_MAX_SIZE = 1000
_WHATSAPP_MEDIA_SCHEME = "whatsapp-media://"
_FENCE_RE = re.compile(r"^(`{3,}|~{3,})")


def split_whatsapp_text(text: str, limit: int = _WHATSAPP_MAX_MESSAGE_LEN) -> list[str]:
    """Split *text* into chunks of at most *limit* chars, balancing code fences."""
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    remaining = text
    open_fence: str | None = None

    while remaining:
        prefix = f"{open_fence}\n" if open_fence else ""
        budget = limit - len(prefix) - (4 if open_fence else 0)
        if budget < 50:
            budget = max(50, limit - 16)

        if len(remaining) <= limit - len(prefix):
            chunk_body = remaining
            remaining = ""
        else:
            window = remaining[:budget]
            cut = window.rfind("\n")
            if cut < budget // 3:
                cut = window.rfind(" ")
            if cut <= 0:
                cut = budget
            chunk_body = remaining[:cut]
            remaining = remaining[cut:].lstrip("\n")

        for line in chunk_body.splitlines():
            m = _FENCE_RE.match(line.strip())
            if m:
                marker = m.group(1)[:3]
                if open_fence is None:
                    open_fence = line.strip()
                elif open_fence.startswith(marker):
                    open_fence = None

        full_chunk = f"{prefix}{chunk_body}"
        if open_fence and remaining:
            close_marker = "~~~" if open_fence.startswith("~") else "```"
            full_chunk = f"{full_chunk}\n{close_marker}"
        chunks.append(full_chunk)

    return chunks


def normalize_whatsapp_identifier(raw: str) -> str:
    """Normalize a phone number or WhatsApp JID for allowlist comparison.

    Group JIDs (``...@g.us``) and LID JIDs (``...@lid``) keep their domain;
    individual user JIDs (``...@c.us``, ``...@s.whatsapp.net``) and E.164 phone
    numbers are normalized to digits only (e.g. ``+86 138-0000-0000`` -> ``8613800000000``).
    """
    val = str(raw or "").strip()
    if not val:
        return ""
    if val.endswith("@g.us") or val.endswith("@lid"):
        return val
    if "@" in val:
        user_part = val.split("@", 1)[0]
        user_part = user_part.split(":", 1)[0]
        digits = re.sub(r"\D", "", user_part)
        return digits or user_part
    digits = re.sub(r"\D", "", val)
    return digits or val


def _coerce_str_list(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        return [item.strip() for item in raw.split(",") if item.strip()]
    if isinstance(raw, list | tuple | set):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


def _as_dict(val: Any, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(val, dict):
        return val
    return fallback if fallback is not None else {}


def _as_list(val: Any) -> list[Any]:
    return val if isinstance(val, list) else []


@dataclass
class WhatsAppConfig(ChannelConfig):
    """Configuration for WhatsApp channel (Cloud API or WAHA / Evolution QR Bridge)."""

    # Provider selection: "cloud" (Meta Cloud API), "waha" (WAHA QR bridge), or "evolution" (Evolution API)
    provider: str = "cloud"

    # Official Meta Cloud API credentials
    access_token: str = ""
    phone_number_id: str = ""
    business_account_id: str = ""
    verify_token: str = ""
    app_secret: str = ""
    api_version: str = "v21.0"
    api_base: str = "https://graph.facebook.com"

    # Self-hosted QR Bridge credentials (WAHA / Evolution API)
    bridge_url: str = ""
    bridge_api_key: str = ""
    session_name: str = "default"
    use_websocket: bool = True

    # Access & reply policy
    owner_only: bool = False
    allow_all_dms: bool = True
    allowed_user_ids: list[str] = field(default_factory=list)
    allow_all_groups: bool = True
    allowed_group_ids: list[str] = field(default_factory=list)
    ignore_mention_all: bool = False
    reply_to_message: bool = True
    show_typing: bool = True
    status_reactions: bool = False
    http_proxy: str = ""
    probe_mode: str = ""
    group_context: GroupContextConfig = field(
        default_factory=lambda: GroupContextConfig(
            enabled=True,
            visibility="all",
            activation="mention",
        )
    )

    @property
    def effective_provider(self) -> str:
        """Resolve active provider mode (``cloud``, ``waha``, or ``evolution``)."""
        p = (self.provider or "").strip().lower()
        if p in ("waha", "evolution"):
            return p
        if p in ("bridge", "qr", "self_hosted"):
            return "waha"
        if self.bridge_url.strip() and not self.phone_number_id.strip():
            return "waha"
        return "cloud"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        normalized = dict(data)
        if "allowed_user_ids" in normalized:
            normalized["allowed_user_ids"] = _coerce_str_list(normalized["allowed_user_ids"])
        if "allowed_group_ids" in normalized:
            normalized["allowed_group_ids"] = _coerce_str_list(normalized["allowed_group_ids"])
        return super().from_dict(normalized)

    def missing_credentials(self) -> list[str]:
        mode = self.effective_provider
        if mode in ("waha", "evolution"):
            return [] if self.bridge_url.strip() else ["bridge_url"]
        missing: list[str] = []
        if not self.access_token.strip():
            missing.append("access_token")
        if not self.phone_number_id.strip():
            missing.append("phone_number_id")
        return missing


WhatsAppConfig.required_credentials = ("access_token", "phone_number_id")  # type: ignore[attr-defined]
WhatsAppConfig.field_aliases = {  # type: ignore[attr-defined]
    "token": "access_token",
    "accessToken": "access_token",
    "phoneNumberId": "phone_number_id",
    "businessAccountId": "business_account_id",
    "verifyToken": "verify_token",
    "appSecret": "app_secret",
    "bridgeUrl": "bridge_url",
    "bridgeApiKey": "bridge_api_key",
    "api_key": "bridge_api_key",
    "apiKey": "bridge_api_key",
    "sessionName": "session_name",
    "instance_name": "session_name",
    "instanceName": "session_name",
    "proxy": "http_proxy",
}


# =========================================================================
# Self-Hosted Bridge QR Login Helpers (WAHA / Evolution API)
# =========================================================================


_BRIDGE_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15.0)


def _validate_bridge_url(bridge_url: str) -> str:
    """Validate and normalize a WAHA / Evolution API bridge base URL."""
    base = bridge_url.strip().rstrip("/")
    if not base:
        raise ValueError("bridge_url is required")
    parsed = urlparse(base)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("bridge_url must be a valid http:// or https:// URL")
    return base


def _bridge_headers(provider: str, api_key: str) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    key = api_key.strip()
    if not key:
        return headers
    if provider == "evolution":
        headers["apikey"] = key
    else:
        headers["X-Api-Key"] = key
    return headers


async def fetch_whatsapp_bridge_qr(
    bridge_url: str,
    *,
    provider: str = "waha",
    session_name: str = "default",
    api_key: str = "",
    http: aiohttp.ClientSession | None = None,
) -> dict[str, Any]:
    """Fetch or initialize a login QR code from a WAHA or Evolution API bridge.

    Returns a dictionary containing:
      - ``status``: e.g. ``"SCAN_QR_CODE"``, ``"WORKING"``, ``"CONNECTED"``
      - ``qrcode``: raw QR string or ``data:image/png;base64,...`` data URL
      - ``session_name``: active session/instance name
      - ``provider``: ``"waha"`` or ``"evolution"``
    """
    base = _validate_bridge_url(bridge_url)
    mode = "evolution" if provider.strip().lower() == "evolution" else "waha"
    session = session_name.strip() or "default"
    headers = _bridge_headers(mode, api_key)

    owns_session = http is None
    client = http or aiohttp.ClientSession(timeout=_BRIDGE_HTTP_TIMEOUT)
    try:
        if mode == "evolution":
            async with client.get(f"{base}/instance/connect/{session}", headers=headers) as resp:
                if resp.status == 404:
                    # Auto-create Evolution instance
                    create_payload = {"instanceName": session, "qrcode": True, "integration": "WHATSAPP-BAILEYS"}
                    async with client.post(
                        f"{base}/instance/create",
                        headers=headers,
                        json=create_payload,
                    ) as c_resp:
                        c_resp.raise_for_status()
                        c_data: dict[str, Any] = await c_resp.json()
                    qr_obj = _as_dict(c_data.get("qrcode"), c_data)
                    code = str(qr_obj.get("base64") or qr_obj.get("code") or "")
                    return {
                        "status": "SCAN_QR_CODE",
                        "qrcode": code,
                        "session_name": session,
                        "provider": mode,
                    }
                resp.raise_for_status()
                data: dict[str, Any] = await resp.json()
                code = str(data.get("base64") or data.get("code") or "")
                instance_obj = _as_dict(data.get("instance"))
                state = str(instance_obj.get("state") or ("SCAN_QR_CODE" if code else "CONNECTED"))
                return {
                    "status": state.upper(),
                    "qrcode": code,
                    "session_name": session,
                    "provider": mode,
                }

        # WAHA provider
        status = "UNKNOWN"
        async with client.get(f"{base}/api/sessions/{session}", headers=headers) as s_resp:
            if s_resp.status == 404:
                async with client.post(
                    f"{base}/api/sessions",
                    headers=headers,
                    json={"name": session, "start": True},
                ) as c_resp:
                    c_resp.raise_for_status()
                    s_data: dict[str, Any] = await c_resp.json()
                    status = str(s_data.get("status") or "STARTING")
            else:
                s_resp.raise_for_status()
                s_data = await s_resp.json()
                status = str(s_data.get("status") or "UNKNOWN")
                if status == "FAILED":
                    async with client.post(f"{base}/api/sessions/{session}/restart", headers=headers) as rst_resp:
                        if rst_resp.status < 400:
                            rst_data: dict[str, Any] = await rst_resp.json()
                            status = str(rst_data.get("status") or "STARTING")
                        elif rst_resp.status == 404:
                            async with client.post(f"{base}/api/sessions/{session}/start", headers=headers) as st_resp:
                                if st_resp.status < 400:
                                    st_data: dict[str, Any] = await st_resp.json()
                                    status = str(st_data.get("status") or "STARTING")
                elif status == "STOPPED":
                    async with client.post(f"{base}/api/sessions/{session}/start", headers=headers) as st_resp:
                        if st_resp.status < 400:
                            st_data = await st_resp.json()
                            status = str(st_data.get("status") or "STARTING")

        if status.upper() == "WORKING":
            return {
                "status": "WORKING",
                "qrcode": "",
                "session_name": session,
                "provider": mode,
            }

        async with client.get(f"{base}/api/{session}/auth/qr?format=raw", headers=headers) as qr_resp:
            if qr_resp.status == 200:
                ctype = (qr_resp.content_type or "").lower()
                if "json" in ctype:
                    qr_json: dict[str, Any] = await qr_resp.json()
                    qr_val = str(qr_json.get("value") or qr_json.get("data") or "")
                else:
                    raw_bytes = await qr_resp.read()
                    qr_val = f"data:{ctype or 'image/png'};base64,{base64.b64encode(raw_bytes).decode('ascii')}"
                return {
                    "status": "SCAN_QR_CODE",
                    "qrcode": qr_val,
                    "session_name": session,
                    "provider": mode,
                }
        return {
            "status": status.upper(),
            "qrcode": "",
            "session_name": session,
            "provider": mode,
        }
    finally:
        if owns_session:
            await client.close()


async def check_whatsapp_bridge_status(
    bridge_url: str,
    *,
    provider: str = "waha",
    session_name: str = "default",
    api_key: str = "",
    http: aiohttp.ClientSession | None = None,
) -> dict[str, Any]:
    """Poll the connection status of a WAHA or Evolution API WhatsApp session."""
    base = _validate_bridge_url(bridge_url)
    mode = "evolution" if provider.strip().lower() == "evolution" else "waha"
    session = session_name.strip() or "default"
    headers = _bridge_headers(mode, api_key)

    owns_session = http is None
    client = http or aiohttp.ClientSession(timeout=_BRIDGE_HTTP_TIMEOUT)
    try:
        if mode == "evolution":
            async with client.get(f"{base}/instance/connectionState/{session}", headers=headers) as resp:
                resp.raise_for_status()
                data: dict[str, Any] = await resp.json()
            instance_obj = _as_dict(data.get("instance"), data)
            state = str(instance_obj.get("state") or instance_obj.get("status") or "unknown").lower()
            connected = state in ("open", "connected", "working")
            return {
                "connected": connected,
                "status": "WORKING" if connected else state.upper(),
                "session_name": session,
                "provider": mode,
            }

        async with client.get(f"{base}/api/sessions/{session}", headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
        status = str(data.get("status") or "UNKNOWN").upper()
        me = _as_dict(data.get("me"))
        return {
            "connected": status == "WORKING",
            "status": status,
            "session_name": session,
            "provider": mode,
            "account_id": str(me.get("id") or ""),
            "push_name": str(me.get("pushName") or ""),
        }
    finally:
        if owns_session:
            await client.close()


# =========================================================================
# WhatsAppChannel
# =========================================================================


class WhatsAppChannel(BaseChannel):
    """WhatsApp channel adapter supporting Meta Cloud API and WAHA / Evolution QR Bridges."""

    channel_type = "whatsapp"

    def __init__(
        self,
        processor: MessageProcessor,
        config: WhatsAppConfig | None = None,
        *,
        channel_id: str | None = None,
        tenant_id: str | None = None,
        debounce_seconds: float = 0.0,
        constraints: ChannelConstraints | None = None,
    ) -> None:
        self._config = config or WhatsAppConfig()
        super().__init__(
            processor,
            channel_id=channel_id or self._config.channel_id,
            tenant_id=tenant_id or self._config.tenant_id,
            debounce_seconds=debounce_seconds,
            constraints=constraints,
            config=self._config,
        )
        self._display_phone_number: str = ""
        self._self_jid: str = ""
        self._self_lid: str = ""
        self._connection_session: str | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_ready = asyncio.Event()
        self.runtime_error: str | None = None
        self._stopping = False
        self._seen_messages: OrderedDict[str, None] = OrderedDict()
        self._sent_message_ids: OrderedDict[str, None] = OrderedDict()

    def _default_constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            send_rate_limit=(10, 1.0),
            typing_keepalive_interval=5.0 if self._config.show_typing else 0.0,
            show_tool_hints=True,
        )

    def _enrich_push_metadata(self, subject: ChannelSubject, meta: dict[str, Any]) -> dict[str, Any]:
        return alias_subject_fields(meta, subject.subject_id, "chat_id", "recipient_id")

    @property
    def provider(self) -> str:
        """Return effective provider (``cloud``, ``waha``, or ``evolution``)."""
        return self._config.effective_provider

    def _proxy(self) -> str | None:
        return self._config.http_proxy.strip() or None

    def _cloud_url(self, path: str) -> str:
        base = self._config.api_base.rstrip("/")
        version = self._config.api_version.strip().lstrip("/") or "v21.0"
        return f"{base}/{version}/{path.lstrip('/')}"

    def _bridge_url(self, path: str) -> str:
        base = self._config.bridge_url.rstrip("/")
        return f"{base}/{path.lstrip('/')}"

    def _is_duplicate(self, msg_id: str) -> bool:
        if not msg_id:
            return False
        if msg_id in self._seen_messages:
            self._seen_messages.move_to_end(msg_id)
            self.dropped_duplicate += 1
            return True
        self._seen_messages[msg_id] = None
        while len(self._seen_messages) > _DEDUP_MAX_SIZE:
            self._seen_messages.popitem(last=False)
        return False

    def _remember_sent_message_ids(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        candidates: list[str] = []
        raw_id = payload.get("id")
        if isinstance(raw_id, str) and raw_id.strip():
            candidates.append(raw_id.strip())
        elif isinstance(raw_id, dict):
            for k in ("_serialized", "id"):
                v = raw_id.get(k)
                if isinstance(v, str) and v.strip():
                    candidates.append(v.strip())
        key_obj = _as_dict(payload.get("key"))
        if isinstance(key_obj.get("id"), str) and key_obj["id"].strip():
            candidates.append(key_obj["id"].strip())
        data_key = _as_dict(_as_dict(payload.get("_data")).get("key"))
        if isinstance(data_key.get("id"), str) and data_key["id"].strip():
            candidates.append(data_key["id"].strip())
        for cid in candidates:
            self._sent_message_ids[cid] = None
            while len(self._sent_message_ids) > _DEDUP_MAX_SIZE:
                self._sent_message_ids.popitem(last=False)

    def _is_sent_by_self(self, payload: dict[str, Any]) -> bool:
        msg_id = str(payload.get("id") or "").strip()
        if msg_id and msg_id in self._sent_message_ids:
            return True
        data_key = _as_dict(_as_dict(payload.get("_data")).get("key"))
        raw_key_id = str(data_key.get("id") or "").strip()
        return bool(raw_key_id and raw_key_id in self._sent_message_ids)

    def _is_self_chat(self, chat_id: str, *, me: dict[str, Any] | None = None) -> bool:
        target = chat_id.strip().lower()
        if not target or target.endswith("@g.us") or target == "status@broadcast":
            return False
        me_dict = me if isinstance(me, dict) else {}
        self_jids = [
            j.strip().lower()
            for j in (
                self._self_jid,
                str(me_dict.get("id") or ""),
            )
            if j and j.strip()
        ]
        self_lids = [
            lid.strip().lower()
            for lid in (
                self._self_lid,
                str(me_dict.get("lid") or ""),
            )
            if lid and lid.strip()
        ]
        if target in self_jids or target in self_lids:
            return True
        norm_target = normalize_whatsapp_identifier(target)
        if not norm_target:
            return False
        if target.endswith("@lid"):
            return any(normalize_whatsapp_identifier(lid) == norm_target for lid in self_lids)
        return any(normalize_whatsapp_identifier(jid) == norm_target for jid in self_jids)

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Verify credentials/connectivity and start WAHA WebSocket listener if applicable."""
        missing = self._config.missing_credentials()
        if missing:
            raise ValueError(f"WhatsApp channel missing required credentials: {', '.join(missing)}")

        self._stopping = False
        self._ws_ready.clear()
        mode = self.provider

        if mode == "cloud":
            await self._verify_cloud_credentials()
        elif mode == "waha":
            await self._verify_waha_bridge()
            if (
                self._config.use_websocket
                and self._channel_id != "__probe__"
                and self._config.probe_mode != "auth_only"
            ):
                self._ws_task = asyncio.create_task(
                    self._waha_ws_loop(),
                    name=f"whatsapp-waha-ws-{self._channel_id}",
                )
                await self._wait_ws_ready(timeout=5.0)
        elif mode == "evolution":
            await self._verify_evolution_bridge()
        self._connection_session = f"whatsapp-{uuid.uuid4().hex[:12]}"

    async def _wait_ws_ready(self, *, timeout: float) -> None:
        if self._ws_task is None:
            return
        ready_task = asyncio.create_task(self._ws_ready.wait())
        try:
            await asyncio.wait(
                {ready_task, self._ws_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            ready_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ready_task

    async def stop(self) -> None:
        """Stop WebSocket listener and close HTTP session."""
        self._stopping = True
        self._ws_ready.clear()
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._ws_task
        self._ws_task = None
        if self._ws and not self._ws.closed:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._ws = None
        self._connection_session = None
        self.runtime_error = None
        await self._close_http()

    @property
    def is_connected(self) -> bool:
        """Return whether this WhatsApp adapter is currently ready to receive/send events."""
        if self._stopping:
            return False
        if self._ws_task is not None:
            return bool(self._ws is not None and not self._ws.closed and not self._ws_task.done())
        return self._connection_session is not None

    async def _verify_cloud_credentials(self) -> dict[str, Any]:
        """Validate Meta Cloud API ``access_token`` and ``phone_number_id``."""
        http = await self._ensure_http()
        headers = {"Authorization": f"Bearer {self._config.access_token}"}
        url = self._cloud_url(f"{self._config.phone_number_id}?fields=id,display_phone_number,verified_name")
        async with http.get(url, headers=headers, proxy=self._proxy()) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise ValueError(f"WhatsApp Cloud API verification failed (HTTP {resp.status}): {body[:200]}")
            data: dict[str, Any] = await resp.json()
        self._display_phone_number = normalize_whatsapp_identifier(str(data.get("display_phone_number") or ""))
        return data

    async def _verify_waha_bridge(self) -> dict[str, Any]:
        """Validate WAHA bridge reachability and cache self JID if session is active."""
        http = await self._ensure_http()
        headers = _bridge_headers("waha", self._config.bridge_api_key)
        session = self._config.session_name.strip() or "default"
        async with http.get(self._bridge_url(f"api/sessions/{session}"), headers=headers, proxy=self._proxy()) as resp:
            if resp.status == 404:
                # Session may not be created yet; verify /api/sessions endpoint works
                async with http.get(self._bridge_url("api/sessions"), headers=headers, proxy=self._proxy()) as l_resp:
                    l_resp.raise_for_status()
                    return {"status": "STOPPED", "name": session}
            resp.raise_for_status()
            data: dict[str, Any] = await resp.json()
        me = _as_dict(data.get("me"))
        if me.get("id"):
            self._self_jid = str(me["id"])
        if me.get("lid"):
            self._self_lid = str(me["lid"])
        return data

    async def _verify_evolution_bridge(self) -> dict[str, Any]:
        """Validate Evolution API bridge reachability."""
        http = await self._ensure_http()
        headers = _bridge_headers("evolution", self._config.bridge_api_key)
        session = self._config.session_name.strip() or "default"
        async with http.get(
            self._bridge_url(f"instance/connectionState/{session}"),
            headers=headers,
            proxy=self._proxy(),
        ) as resp:
            if resp.status == 404:
                return {"state": "not_created", "instance": session}
            resp.raise_for_status()
            data: dict[str, Any] = await resp.json()
        return data

    def _build_waha_ws_url(self) -> str:
        parsed = urlparse(self._config.bridge_url.rstrip("/"))
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        base_path = parsed.path.rstrip("/")
        ws_path = f"{base_path}/ws"
        query_params: dict[str, str] = {
            "session": self._config.session_name.strip() or "default",
            # Subscribe to message.any so both messages from others (fromMe=false)
            # and user-authored self-chat ("Message Yourself", fromMe=true, source=app)
            # are received once without duplicates; non-self-chat and API echoes are filtered.
            "events": "message.any",
        }
        api_key = self._config.bridge_api_key.strip()
        if api_key:
            # WAHA WebSocketAuth validates x-api-key from query params (not HTTP upgrade headers).
            query_params["x-api-key"] = api_key
        return urlunparse((ws_scheme, parsed.netloc, ws_path, "", urlencode(query_params), ""))

    async def _waha_ws_loop(self) -> None:
        """Listen to WAHA WebSocket event stream with automatic reconnection."""
        backoff = 1.0
        headers = _bridge_headers("waha", self._config.bridge_api_key)
        while not self._stopping:
            close_code: int | None = None
            connected_at: float | None = None
            received_text = False
            try:
                ws_url = self._build_waha_ws_url()
                http = await self._ensure_http()
                async with http.ws_connect(
                    ws_url,
                    headers=headers,
                    heartbeat=30.0,
                    proxy=self._proxy(),
                ) as ws:
                    self._ws = ws
                    self.runtime_error = None
                    self._connection_session = f"whatsapp-{uuid.uuid4().hex[:12]}"
                    connected_at = time.monotonic()
                    self._ws_ready.set()
                    async for msg in ws:
                        if self._stopping:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            received_text = True
                            self._handle_waha_ws_frame(msg.data)
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
                    close_code = getattr(ws, "close_code", None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stopping:
                    break
                err_msg = re.sub(r"(x-api-key=)[^&\s]+", r"\1***", str(exc), flags=re.IGNORECASE)
                self.runtime_error = err_msg or type(exc).__name__
                logger.warning("WAHA WebSocket disconnected (%s); reconnecting in %.1fs", self.runtime_error, backoff)
                self.note_reconnect()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
                continue
            finally:
                self._ws = None
                self._ws_ready.clear()

            if self._stopping:
                break
            if received_text or (connected_at is not None and (time.monotonic() - connected_at) >= 5.0):
                backoff = 1.0
            if close_code == 1008:
                self.runtime_error = "WAHA WebSocket unauthorized (close code 1008); check bridge_api_key"
                logger.warning("%s; reconnecting in %.1fs", self.runtime_error, backoff)
            else:
                logger.warning("WAHA WebSocket closed (code=%s); reconnecting in %.1fs", close_code, backoff)
            self.note_reconnect()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

    def _handle_waha_ws_frame(self, raw_text: str) -> None:
        try:
            frame = json.loads(raw_text)
        except json.JSONDecodeError:
            return
        if not isinstance(frame, dict):
            return
        for event in self.extract_webhook_messages(frame):
            self.note_inbound()
            self.enqueue(event)

    # =========================================================================
    # Webhook Verification & Event Extraction
    # =========================================================================

    def verify_webhook_challenge(
        self,
        mode: str,
        token: str,
        challenge: str,
        *,
        require_token: bool = False,
    ) -> str | None:
        """Verify Meta Cloud API GET webhook subscription challenge."""
        expected = self._config.verify_token.strip()
        if not expected:
            return challenge if (mode == "subscribe" and not require_token) else None
        if mode == "subscribe" and hmac.compare_digest(expected, token.strip()):
            return challenge
        return None

    def verify_webhook_signature(
        self,
        signature_header: str,
        raw_body: bytes | str,
        *,
        require_secret: bool = False,
    ) -> bool:
        """Verify Meta ``X-Hub-Signature-256`` HMAC-SHA256 webhook signature."""
        secret = self._config.app_secret.strip()
        if not secret:
            return not require_secret
        if not signature_header or not signature_header.startswith("sha256="):
            return False
        body_bytes = raw_body.encode("utf-8") if isinstance(raw_body, str) else raw_body
        expected = "sha256=" + hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature_header.strip())

    def verify_webhook_request(
        self,
        headers: Mapping[str, str],
        raw_body: bytes | str,
        *,
        require_secret: bool = True,
    ) -> bool:
        """Verify an inbound HTTP webhook request across Cloud API and bridge modes.

        - If ``app_secret`` is configured, validates ``X-Hub-Signature-256``.
        - For WAHA / Evolution bridge modes without ``app_secret``, accepts a matching
          ``bridge_api_key`` supplied via ``X-Api-Key``, ``apikey``, or ``Authorization: Bearer``.
        - When ``require_secret=True`` (default for unauthenticated HTTP endpoints),
          rejects requests if no webhook verification secret / key is configured.
        """
        lower_headers = {k.lower(): v for k, v in headers.items()}
        if self._config.app_secret.strip():
            return self.verify_webhook_signature(
                lower_headers.get("x-hub-signature-256", ""),
                raw_body,
                require_secret=True,
            )
        if self.provider in ("waha", "evolution"):
            expected_key = self._config.bridge_api_key.strip()
            if expected_key:
                auth_header = lower_headers.get("authorization", "").strip()
                bearer_token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
                candidates = (
                    lower_headers.get("x-api-key", "").strip(),
                    lower_headers.get("apikey", "").strip(),
                    bearer_token,
                )
                return any(c and hmac.compare_digest(expected_key, c) for c in candidates)
        return not require_secret

    def _matches_cloud_phone_number(self, metadata: dict[str, Any]) -> bool:
        expected = self._config.phone_number_id.strip()
        if not expected:
            return False
        incoming = str(metadata.get("phone_number_id") or "").strip()
        return incoming == expected

    def _matches_bridge_account(self, incoming_account: str) -> bool:
        expected = self._config.session_name.strip() or "default"
        return incoming_account.strip() == expected

    def extract_webhook_messages(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract actionable inbound message events from native Cloud API, WAHA, or Evolution envelopes.

        Accepts only native provider envelopes carrying an account identifier that matches this
        channel's configured account (Cloud ``metadata.phone_number_id``, WAHA ``session``, or
        Evolution ``instance``) before updating cached self identities, deduplicating, or returning
        messages. Pre-normalized or raw payloads without a native provider envelope are rejected
        here and should only enter via trusted internal paths (``parse_inbound`` / ``enqueue``).
        """
        if not isinstance(payload, dict):
            return []

        results: list[dict[str, Any]] = []

        # Case 1: Meta Cloud API webhook envelope (object == "whatsapp_business_account" or "entry" list)
        if isinstance(payload.get("entry"), list):
            if self.provider != "cloud":
                return []
            for entry in payload["entry"]:
                if not isinstance(entry, dict):
                    continue
                for change in entry.get("changes") or []:
                    if not isinstance(change, dict):
                        continue
                    val = change.get("value")
                    if not isinstance(val, dict):
                        continue
                    metadata = _as_dict(val.get("metadata"))
                    if not self._matches_cloud_phone_number(metadata):
                        continue
                    contacts = _as_list(val.get("contacts"))
                    contact_map: dict[str, str] = {}
                    for c in contacts:
                        if isinstance(c, dict) and c.get("wa_id"):
                            profile = _as_dict(c.get("profile"))
                            contact_map[str(c["wa_id"])] = str(profile.get("name") or c["wa_id"])
                    for msg in val.get("messages") or []:
                        if not isinstance(msg, dict):
                            continue
                        enriched = {
                            "_format": "cloud",
                            "message": msg,
                            "contact_name": contact_map.get(str(msg.get("from") or ""), ""),
                            "metadata": metadata,
                        }
                        if self._should_accept_normalized(enriched):
                            results.append(enriched)
            return results

        # Case 2: WAHA event envelope (event in ("message", "message.any") and "payload" dict)
        if "payload" in payload and isinstance(payload.get("payload"), dict) and payload.get("event"):
            if self.provider != "waha":
                return []
            ev_name = str(payload.get("event") or "")
            if ev_name not in ("message", "message.any"):
                return []
            raw_session = str(payload.get("session") or "").strip()
            if not self._matches_bridge_account(raw_session):
                return []
            me = _as_dict(payload.get("me"))
            if me.get("id"):
                self._self_jid = str(me["id"])
            if me.get("lid"):
                self._self_lid = str(me["lid"])
            enriched = {
                "_format": "waha",
                "event": ev_name,
                "session": raw_session,
                "me": me,
                "payload": payload["payload"],
            }
            if self._should_accept_normalized(enriched):
                results.append(enriched)
            return results

        # Case 3: Evolution API webhook envelope (event == "messages.upsert" and "data" dict)
        if payload.get("event") == "messages.upsert" and isinstance(payload.get("data"), dict):
            if self.provider != "evolution":
                return []
            raw_instance = str(payload.get("instance") or "").strip()
            if not self._matches_bridge_account(raw_instance):
                return []
            evo_sender = str(payload.get("sender") or "")
            if evo_sender:
                self._self_jid = evo_sender
            enriched = {
                "_format": "evolution",
                "instance": raw_instance,
                "sender": evo_sender,
                "data": payload["data"],
            }
            if self._should_accept_normalized(enriched):
                results.append(enriched)
            return results

        return []

    def _is_allowed_sender(self, sender_id: str, chat_id: str, *, is_group: bool) -> bool:
        norm_sender = normalize_whatsapp_identifier(sender_id)
        if self._config.owner_only:
            owner_pool = {
                normalize_whatsapp_identifier(u)
                for u in (
                    *(self._config.group_context.owner_ids or []),
                    *(self._config.allowed_user_ids or []),
                )
                if str(u).strip()
            }
            if not owner_pool or norm_sender not in owner_pool:
                return False
            if is_group and not self._config.allow_all_groups:
                if not self._config.allowed_group_ids:
                    return False
                allowed_groups = {g.strip() for g in self._config.allowed_group_ids if g.strip()}
                if chat_id not in allowed_groups and normalize_whatsapp_identifier(chat_id) not in allowed_groups:
                    return False
            return True

        if is_group:
            if not self._config.allow_all_groups:
                if not self._config.allowed_group_ids:
                    return False
                allowed_groups = {g.strip() for g in self._config.allowed_group_ids if g.strip()}
                if chat_id not in allowed_groups and normalize_whatsapp_identifier(chat_id) not in allowed_groups:
                    return False
            if self._config.allowed_user_ids:
                allowed_users = {normalize_whatsapp_identifier(u) for u in self._config.allowed_user_ids if u.strip()}
                return norm_sender in allowed_users
            return True

        if not self._config.allow_all_dms:
            if not self._config.allowed_user_ids:
                return False
            allowed_users = {normalize_whatsapp_identifier(u) for u in self._config.allowed_user_ids if u.strip()}
            return norm_sender in allowed_users
        if self._config.allowed_user_ids:
            allowed_users = {normalize_whatsapp_identifier(u) for u in self._config.allowed_user_ids if u.strip()}
            return norm_sender in allowed_users
        return True

    def _normalize_owner_sender(self, message: InboundMessage) -> InboundMessage:
        meta = message.metadata
        if isinstance(meta, dict) and self._config.group_context.owner_ids:
            sender = str(meta.get("sender_id") or "").strip()
            norm_sender = normalize_whatsapp_identifier(sender)
            if norm_sender:
                for raw_owner in self._config.group_context.owner_ids:
                    owner_str = str(raw_owner).strip()
                    if owner_str and normalize_whatsapp_identifier(owner_str) == norm_sender:
                        meta["sender_id"] = owner_str
                        break
        return message

    async def _preprocess_inbound(self, message: InboundMessage) -> None:
        self._normalize_owner_sender(message)
        await super()._preprocess_inbound(message)

    def _should_accept_normalized(self, item: dict[str, Any]) -> bool:
        fmt = item.get("_format")
        if fmt == "cloud" or ("message" in item and isinstance(item.get("message"), dict)):
            msg = _as_dict(item.get("message"), item)
            msg_id = str(msg.get("id") or "")
            sender = str(msg.get("from") or "")
            if not sender:
                return False
            is_group = bool(msg.get("group_id")) or sender.endswith("@g.us")
            chat_id = str(msg.get("group_id") or sender)
            if not self._is_allowed_sender(sender, chat_id, is_group=is_group):
                return False
            return not self._is_duplicate(msg_id)

        if fmt == "waha" or ("from" in item and "fromMe" in item):
            p = _as_dict(item.get("payload"), item)
            me = _as_dict(item.get("me"))
            msg_id = str(p.get("id") or "")
            chat_id = str(p.get("from") or p.get("chatId") or p.get("to") or "")
            if not chat_id or chat_id == "status@broadcast":
                return False
            is_group = chat_id.endswith("@g.us")
            if p.get("fromMe"):
                # Never process bot API echoes or outgoing messages sent to other users/groups.
                # Only allow user-authored self-chat ("Message Yourself") messages.
                source = str(p.get("source") or "").strip().lower()
                if source == "api" or self._is_sent_by_self(p):
                    return False
                if is_group or not self._is_self_chat(chat_id, me=me):
                    return False
            sender = str(p.get("participant") or p.get("author") or chat_id)
            if p.get("fromMe") and self._self_jid:
                sender = self._self_jid
            if not self._is_allowed_sender(sender, chat_id, is_group=is_group):
                return False
            return not self._is_duplicate(msg_id)

        if fmt == "evolution" or ("key" in item and isinstance(item.get("key"), dict)):
            data = _as_dict(item.get("data"), item)
            key = _as_dict(data.get("key"))
            if key.get("fromMe"):
                return False
            msg_id = str(key.get("id") or "")
            chat_id = str(key.get("remoteJid") or "")
            if not chat_id or chat_id == "status@broadcast":
                return False
            is_group = chat_id.endswith("@g.us")
            sender = str(key.get("participant") or data.get("participant") or chat_id)
            if not self._is_allowed_sender(sender, chat_id, is_group=is_group):
                return False
            return not self._is_duplicate(msg_id)

        return True

    # =========================================================================
    # Inbound Parsing
    # =========================================================================

    def parse_inbound(self, raw_payload: Any) -> InboundMessage:
        """Parse a Cloud API, WAHA, or Evolution API message payload into :class:`InboundMessage`."""
        if isinstance(raw_payload, InboundMessage):
            return self._normalize_owner_sender(raw_payload)
        if not isinstance(raw_payload, dict):
            raise TypeError(
                f"Expected dict payload for WhatsAppChannel.parse_inbound, got {type(raw_payload).__name__}"
            )

        # Unwrap outer webhook envelope if caller passed a full webhook payload directly
        if isinstance(raw_payload.get("entry"), list):
            extracted = self.extract_webhook_messages(raw_payload)
            if extracted:
                raw_payload = extracted[0]

        fmt = raw_payload.get("_format")
        if fmt == "waha" or (
            isinstance(raw_payload.get("payload"), dict) and raw_payload.get("event") in ("message", "message.any")
        ):
            return self._normalize_owner_sender(self._parse_waha_inbound(raw_payload))
        if fmt == "evolution" or (
            raw_payload.get("event") == "messages.upsert" and isinstance(raw_payload.get("data"), dict)
        ):
            return self._normalize_owner_sender(self._parse_evolution_inbound(raw_payload))
        if "fromMe" in raw_payload and "from" in raw_payload and "body" in raw_payload:
            return self._normalize_owner_sender(self._parse_waha_inbound({"_format": "waha", "payload": raw_payload}))
        if isinstance(raw_payload.get("key"), dict) and "remoteJid" in raw_payload["key"]:
            return self._normalize_owner_sender(
                self._parse_evolution_inbound({"_format": "evolution", "data": raw_payload})
            )
        return self._normalize_owner_sender(self._parse_cloud_inbound(raw_payload))

    def _parse_cloud_inbound(self, payload: dict[str, Any]) -> InboundMessage:
        msg = _as_dict(payload.get("message"), payload)
        contact_name = str(payload.get("contact_name") or "")
        msg_id = str(msg.get("id") or "")
        sender_id = str(msg.get("from") or "")
        group_id = str(msg.get("group_id") or "")
        is_group = bool(group_id) or sender_id.endswith("@g.us")
        chat_id = group_id if group_id else sender_id
        sender_name = contact_name or sender_id

        try:
            timestamp = float(msg.get("timestamp") or time.time())
        except (TypeError, ValueError):
            timestamp = time.time()

        msg_type = str(msg.get("type") or "text")
        parts: list[ContentPart] = []
        raw_text = ""

        if msg_type == "text":
            text_obj = _as_dict(msg.get("text"))
            raw_text = str(text_obj.get("body") or "")
            if raw_text:
                parts.append(TextContent(text=raw_text))
        elif msg_type == "interactive":
            inter = _as_dict(msg.get("interactive"))
            reply_obj = _as_dict(inter.get("button_reply") or inter.get("list_reply"))
            if reply_obj:
                raw_text = str(reply_obj.get("title") or reply_obj.get("id") or "")
                if raw_text:
                    parts.append(TextContent(text=raw_text))
        elif msg_type == "button":
            btn = _as_dict(msg.get("button"))
            raw_text = str(btn.get("text") or btn.get("payload") or "")
            if raw_text:
                parts.append(TextContent(text=raw_text))
        elif msg_type in ("image", "video", "audio", "document", "sticker"):
            media_obj = _as_dict(msg.get(msg_type))
            media_id = str(media_obj.get("id") or "")
            media_url = str(media_obj.get("url") or (f"{_WHATSAPP_MEDIA_SCHEME}{media_id}" if media_id else ""))
            mime = str(media_obj.get("mime_type") or "application/octet-stream")
            caption = str(media_obj.get("caption") or "").strip()
            if caption:
                raw_text = caption
                parts.append(TextContent(text=caption))
            if media_url:
                if msg_type in ("image", "sticker"):
                    parts.append(ImageContent(url=media_url, mime_type=mime))
                elif msg_type == "video":
                    parts.append(VideoContent(url=media_url, mime_type=mime))
                elif msg_type == "audio":
                    parts.append(AudioContent(url=media_url, mime_type=mime))
                else:
                    fname = str(media_obj.get("filename") or "document")
                    parts.append(FileContent(url=media_url, filename=fname, mime_type=mime))

        if not parts:
            parts.append(TextContent(text=""))

        context_obj = _as_dict(msg.get("context"))
        reply_to_id = str(context_obj.get("id") or "")
        reply_quote_text = str(context_obj.get("quoted_text") or "").strip()

        has_mass_mention = bool(re.search(r"@(all|everyone|here)\b", raw_text, re.IGNORECASE))
        bot_mentioned = not is_group or bool(msg.get("bot_mentioned"))
        if is_group and self._config.ignore_mention_all and has_mass_mention and not msg.get("bot_mentioned"):
            bot_mentioned = False

        metadata: dict[str, Any] = {
            "provider": "cloud",
            "message_id": msg_id,
            "chat_id": chat_id,
            "recipient_id": chat_id,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "is_dm": not is_group,
            "is_group": is_group,
            "bot_mentioned": bot_mentioned,
            "has_mentions": "@" in raw_text,
        }
        if is_group and chat_id:
            metadata["conversation_id"] = chat_id
        if reply_to_id:
            metadata["reply_to_message_id"] = reply_to_id
        if reply_quote_text:
            metadata["reply_quote_text"] = reply_quote_text
            metadata["reply_to_text"] = reply_quote_text

        chat_type: Literal["direct", "group"] = "group" if is_group else "direct"
        subject = ChannelSubject(
            subject_id=chat_id,
            display_name=sender_name if not is_group else chat_id,
            chat_type=chat_type,
            metadata=metadata,
        )

        return InboundMessage(
            channel_id=self._channel_id,
            channel_type=self.channel_type,
            tenant_id=self._tenant_id,
            channel_session_id=self._connection_session or f"{self._channel_id}-unconnected",
            channel_subject=subject,
            content=parts,
            timestamp=timestamp,
            metadata=metadata,
        )

    def _parse_waha_inbound(self, envelope: dict[str, Any]) -> InboundMessage:
        p = _as_dict(envelope.get("payload"), envelope)
        me = _as_dict(envelope.get("me"))
        self_jid = str(me.get("id") or self._self_jid or "")
        self_num = normalize_whatsapp_identifier(self_jid)

        msg_id = str(p.get("id") or "")
        chat_id = str(p.get("from") or p.get("chatId") or "")
        is_group = chat_id.endswith("@g.us")
        sender_id = str(p.get("participant") or p.get("author") or chat_id)
        data_ext = _as_dict(p.get("_data"))
        sender_name = str(
            data_ext.get("notifyName")
            or p.get("notifyName")
            or p.get("pushName")
            or normalize_whatsapp_identifier(sender_id)
        )

        try:
            timestamp = float(p.get("timestamp") or time.time())
        except (TypeError, ValueError):
            timestamp = time.time()

        raw_text = str(p.get("body") or "")
        mentioned_jids: list[str] = []
        if isinstance(p.get("mentionedIds"), list):
            mentioned_jids = [str(x) for x in p["mentionedIds"]]
        elif isinstance(data_ext.get("mentionedJidList"), list):
            mentioned_jids = [str(x) for x in data_ext["mentionedJidList"]]

        has_mentions = bool(mentioned_jids) or ("@" in raw_text)
        has_mass_mention = bool(re.search(r"@(all|everyone|here)\b", raw_text, re.IGNORECASE))
        explicit_self_mention = bool(
            (self_jid and self_jid in mentioned_jids)
            or (self_num and any(normalize_whatsapp_identifier(m) == self_num for m in mentioned_jids))
            or (self_num and f"@{self_num}" in raw_text)
        )
        if self._config.ignore_mention_all and has_mass_mention and not (self_num and f"@{self_num}" in raw_text):
            explicit_self_mention = False
        bot_mentioned = (not is_group) or explicit_self_mention

        cleaned_text = raw_text
        if is_group and self_num:
            cleaned_text = re.sub(rf"@{re.escape(self_num)}\b", "", cleaned_text).strip()

        parts: list[ContentPart] = []
        if cleaned_text:
            parts.append(TextContent(text=cleaned_text))

        media_obj = _as_dict(p.get("media"))
        if media_obj:
            media_url = str(media_obj.get("url") or "")
            media_b64 = str(media_obj.get("data") or "")
            mime = str(media_obj.get("mimetype") or "application/octet-stream")
            fname = str(media_obj.get("filename") or "attachment")
            if media_url or media_b64:
                if mime.startswith("image/"):
                    parts.append(ImageContent(url=media_url, data=media_b64 or None, mime_type=mime))
                elif mime.startswith("video/"):
                    parts.append(VideoContent(url=media_url, data=media_b64 or None, mime_type=mime))
                elif mime.startswith("audio/"):
                    parts.append(AudioContent(url=media_url, data=media_b64 or None, mime_type=mime))
                else:
                    parts.append(FileContent(url=media_url, data=media_b64 or None, filename=fname, mime_type=mime))

        if not parts:
            parts.append(TextContent(text=""))

        quoted_msg = _as_dict(data_ext.get("quotedMsg"))
        reply_quote_text = str(
            quoted_msg.get("body") or quoted_msg.get("caption") or p.get("replyToBody") or ""
        ).strip()
        reply_to_obj = _as_dict(p.get("replyTo"))
        reply_to_id = str(reply_to_obj.get("id") or data_ext.get("quotedStanzaID") or "")
        if not reply_quote_text and reply_to_obj.get("body"):
            reply_quote_text = str(reply_to_obj["body"]).strip()

        metadata: dict[str, Any] = {
            "provider": "waha",
            "session_name": str(envelope.get("session") or self._config.session_name),
            "message_id": msg_id,
            "chat_id": chat_id,
            "recipient_id": chat_id,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "is_dm": not is_group,
            "is_group": is_group,
            "bot_mentioned": bot_mentioned,
            "has_mentions": has_mentions,
        }
        if is_group and chat_id:
            metadata["conversation_id"] = chat_id
        if reply_to_id:
            metadata["reply_to_message_id"] = reply_to_id
        if reply_quote_text:
            metadata["reply_quote_text"] = reply_quote_text
            metadata["reply_to_text"] = reply_quote_text

        chat_type: Literal["direct", "group"] = "group" if is_group else "direct"
        subject = ChannelSubject(
            subject_id=chat_id,
            display_name=sender_name if not is_group else chat_id,
            chat_type=chat_type,
            metadata=metadata,
        )

        return InboundMessage(
            channel_id=self._channel_id,
            channel_type=self.channel_type,
            tenant_id=self._tenant_id,
            channel_session_id=self._connection_session or f"{self._channel_id}-unconnected",
            channel_subject=subject,
            content=parts,
            timestamp=timestamp,
            metadata=metadata,
        )

    def _parse_evolution_inbound(self, envelope: dict[str, Any]) -> InboundMessage:
        data = _as_dict(envelope.get("data"), envelope)
        key = _as_dict(data.get("key"))
        msg_id = str(key.get("id") or "")
        chat_id = str(key.get("remoteJid") or "")
        is_group = chat_id.endswith("@g.us")
        sender_id = str(key.get("participant") or data.get("participant") or chat_id)
        sender_name = str(data.get("pushName") or normalize_whatsapp_identifier(sender_id))

        try:
            timestamp = float(data.get("messageTimestamp") or time.time())
        except (TypeError, ValueError):
            timestamp = time.time()

        msg_obj = _as_dict(data.get("message"))
        ext_text = _as_dict(msg_obj.get("extendedTextMessage"))
        raw_text = str(
            msg_obj.get("conversation")
            or ext_text.get("text")
            or _as_dict(msg_obj.get("imageMessage")).get("caption")
            or _as_dict(msg_obj.get("videoMessage")).get("caption")
            or _as_dict(msg_obj.get("documentMessage")).get("caption")
            or ""
        )

        context_info = _as_dict(
            data.get("contextInfo")
            or ext_text.get("contextInfo")
            or _as_dict(msg_obj.get("imageMessage")).get("contextInfo")
        )

        self_jid = str(envelope.get("sender") or self._self_jid or "")
        self_num = normalize_whatsapp_identifier(self_jid)
        mentioned_jids = [str(x) for x in _as_list(context_info.get("mentionedJid"))]
        has_mentions = bool(mentioned_jids) or ("@" in raw_text)
        has_mass_mention = bool(re.search(r"@(all|everyone|here)\b", raw_text, re.IGNORECASE))
        explicit_self_mention = bool(
            (self_jid and self_jid in mentioned_jids)
            or (self_num and any(normalize_whatsapp_identifier(m) == self_num for m in mentioned_jids))
            or (self_num and f"@{self_num}" in raw_text)
        )
        if self._config.ignore_mention_all and has_mass_mention and not (self_num and f"@{self_num}" in raw_text):
            explicit_self_mention = False
        bot_mentioned = (not is_group) or explicit_self_mention

        cleaned_text = raw_text
        if is_group and self_num:
            cleaned_text = re.sub(rf"@{re.escape(self_num)}\b", "", cleaned_text).strip()

        parts: list[ContentPart] = []
        if cleaned_text:
            parts.append(TextContent(text=cleaned_text))

        media_b64 = str(msg_obj.get("base64") or data.get("base64") or "")
        media_url = str(data.get("mediaUrl") or "")
        if "imageMessage" in msg_obj and isinstance(msg_obj["imageMessage"], dict):
            im = msg_obj["imageMessage"]
            url = media_url or str(im.get("url") or "")
            mime = str(im.get("mimetype") or "image/jpeg")
            if url or media_b64:
                parts.append(ImageContent(url=url, data=media_b64 or None, mime_type=mime))
        elif "videoMessage" in msg_obj and isinstance(msg_obj["videoMessage"], dict):
            vm = msg_obj["videoMessage"]
            url = media_url or str(vm.get("url") or "")
            mime = str(vm.get("mimetype") or "video/mp4")
            if url or media_b64:
                parts.append(VideoContent(url=url, data=media_b64 or None, mime_type=mime))
        elif "audioMessage" in msg_obj and isinstance(msg_obj["audioMessage"], dict):
            am = msg_obj["audioMessage"]
            url = media_url or str(am.get("url") or "")
            mime = str(am.get("mimetype") or "audio/ogg")
            if url or media_b64:
                parts.append(AudioContent(url=url, data=media_b64 or None, mime_type=mime))
        elif "documentMessage" in msg_obj and isinstance(msg_obj["documentMessage"], dict):
            dm = msg_obj["documentMessage"]
            url = media_url or str(dm.get("url") or "")
            mime = str(dm.get("mimetype") or "application/octet-stream")
            fname = str(dm.get("fileName") or "document")
            if url or media_b64:
                parts.append(FileContent(url=url, data=media_b64 or None, filename=fname, mime_type=mime))

        if not parts:
            parts.append(TextContent(text=""))

        quoted = _as_dict(context_info.get("quotedMessage"))
        reply_quote_text = str(
            quoted.get("conversation") or _as_dict(quoted.get("extendedTextMessage")).get("text") or ""
        ).strip()
        reply_to_id = str(context_info.get("stanzaId") or "")

        metadata: dict[str, Any] = {
            "provider": "evolution",
            "session_name": str(envelope.get("instance") or self._config.session_name),
            "message_id": msg_id,
            "chat_id": chat_id,
            "recipient_id": chat_id,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "is_dm": not is_group,
            "is_group": is_group,
            "bot_mentioned": bot_mentioned,
            "has_mentions": has_mentions,
        }
        if is_group and chat_id:
            metadata["conversation_id"] = chat_id
        if reply_to_id:
            metadata["reply_to_message_id"] = reply_to_id
        if reply_quote_text:
            metadata["reply_quote_text"] = reply_quote_text
            metadata["reply_to_text"] = reply_quote_text

        chat_type: Literal["direct", "group"] = "group" if is_group else "direct"
        subject = ChannelSubject(
            subject_id=chat_id,
            display_name=sender_name if not is_group else chat_id,
            chat_type=chat_type,
            metadata=metadata,
        )

        return InboundMessage(
            channel_id=self._channel_id,
            channel_type=self.channel_type,
            tenant_id=self._tenant_id,
            channel_session_id=self._connection_session or f"{self._channel_id}-unconnected",
            channel_subject=subject,
            content=parts,
            timestamp=timestamp,
            metadata=metadata,
        )

    # =========================================================================
    # Outbound API (Cloud API + WAHA + Evolution API)
    # =========================================================================

    def _resolve_target(self, subject: ChannelSubject) -> tuple[str, str | None]:
        """Resolve ``(chat_id, reply_message_id)`` from a :class:`ChannelSubject`."""
        meta = subject.metadata or {}
        chat_id = str(meta.get("chat_id") or meta.get("recipient_id") or subject.subject_id or "").strip()
        if not chat_id:
            raise ValueError("WhatsApp send requires a target chat_id or subject_id")
        reply_msg_id = str(meta.get("message_id") or "").strip() or None if self._config.reply_to_message else None
        return chat_id, reply_msg_id

    @staticmethod
    def _to_waha_chat_id(target: str) -> str:
        if "@" in target:
            return target
        digits = normalize_whatsapp_identifier(target)
        return f"{digits}@c.us"

    async def _send_typing_indicator(self, subject: ChannelSubject) -> None:
        if not self._config.show_typing:
            return
        meta = subject.metadata or {}
        chat_id = str(meta.get("chat_id") or meta.get("recipient_id") or subject.subject_id or "").strip()
        msg_id = str(meta.get("message_id") or "").strip()
        mode = self.provider
        http = await self._ensure_http()

        with contextlib.suppress(Exception):
            if mode == "cloud" and msg_id:
                headers = {
                    "Authorization": f"Bearer {self._config.access_token}",
                    "Content-Type": "application/json",
                }
                payload = {
                    "messaging_product": "whatsapp",
                    "status": "read",
                    "message_id": msg_id,
                    "typing_indicator": {"type": "text"},
                }
                async with http.post(
                    self._cloud_url(f"{self._config.phone_number_id}/messages"),
                    headers=headers,
                    json=payload,
                    proxy=self._proxy(),
                ) as resp:
                    await resp.read()
            elif mode == "waha" and chat_id:
                headers = _bridge_headers("waha", self._config.bridge_api_key)
                payload = {
                    "session": self._config.session_name.strip() or "default",
                    "chatId": self._to_waha_chat_id(chat_id),
                }
                async with http.post(
                    self._bridge_url("api/startTyping"),
                    headers=headers,
                    json=payload,
                    proxy=self._proxy(),
                ) as resp:
                    await resp.read()

    async def _send_reaction(self, subject: ChannelSubject, emoji: str) -> None:
        meta = subject.metadata or {}
        chat_id = str(meta.get("chat_id") or meta.get("recipient_id") or subject.subject_id or "").strip()
        msg_id = str(meta.get("message_id") or "").strip()
        if not chat_id or not msg_id:
            return
        mode = self.provider
        http = await self._ensure_http()
        with contextlib.suppress(Exception):
            if mode == "cloud":
                headers = {
                    "Authorization": f"Bearer {self._config.access_token}",
                    "Content-Type": "application/json",
                }
                to_number = normalize_whatsapp_identifier(chat_id) if not chat_id.endswith("@g.us") else chat_id
                payload: dict[str, Any] = {
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": to_number,
                    "type": "reaction",
                    "reaction": {"message_id": msg_id, "emoji": emoji},
                }
                async with http.post(
                    self._cloud_url(f"{self._config.phone_number_id}/messages"),
                    headers=headers,
                    json=payload,
                    proxy=self._proxy(),
                ) as resp:
                    await resp.read()
            elif mode == "waha":
                headers = _bridge_headers("waha", self._config.bridge_api_key)
                payload = {
                    "session": self._config.session_name.strip() or "default",
                    "messageId": msg_id,
                    "reaction": emoji,
                }
                async with http.put(
                    self._bridge_url("api/reaction"),
                    headers=headers,
                    json=payload,
                    proxy=self._proxy(),
                ) as resp:
                    await resp.read()
            elif mode == "evolution":
                headers = _bridge_headers("evolution", self._config.bridge_api_key)
                session = self._config.session_name.strip() or "default"
                payload = {
                    "key": {
                        "remoteJid": chat_id,
                        "fromMe": False,
                        "id": msg_id,
                    },
                    "reaction": emoji,
                }
                async with http.post(
                    self._bridge_url(f"message/sendReaction/{session}"),
                    headers=headers,
                    json=payload,
                    proxy=self._proxy(),
                ) as resp:
                    await resp.read()

    async def _process_inbound(self, message: InboundMessage, subject: ChannelSubject) -> bool:
        use_reactions = bool(self._config.status_reactions and (subject.metadata or {}).get("message_id"))
        if use_reactions:
            await self._send_reaction(subject, "⏳")
        try:
            ok = await super()._process_inbound(message, subject)
            if use_reactions:
                await self._send_reaction(subject, "✅" if ok else "")
            return ok
        except Exception:
            if use_reactions:
                await self._send_reaction(subject, "")
            raise

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        if not text:
            return
        chat_id, reply_msg_id = self._resolve_target(subject)
        chunks = split_whatsapp_text(text, limit=_WHATSAPP_MAX_MESSAGE_LEN)
        mode = self.provider

        for idx, chunk in enumerate(chunks):
            quote_id = reply_msg_id if idx == 0 else None
            if mode == "waha":
                await self._send_waha_text(chat_id, chunk, reply_to=quote_id)
            elif mode == "evolution":
                await self._send_evolution_text(chat_id, chunk, reply_to=quote_id)
            else:
                await self._send_cloud_text(chat_id, chunk, reply_to=quote_id)
        self.note_outbound()

    async def _send_cloud_text(self, recipient: str, text: str, *, reply_to: str | None = None) -> None:
        http = await self._ensure_http()
        headers = {
            "Authorization": f"Bearer {self._config.access_token}",
            "Content-Type": "application/json",
        }
        to_number = normalize_whatsapp_identifier(recipient) if not recipient.endswith("@g.us") else recipient
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to_number,
            "type": "text",
            "text": {"preview_url": False, "body": text},
        }
        if reply_to:
            payload["context"] = {"message_id": reply_to}
        async with http.post(
            self._cloud_url(f"{self._config.phone_number_id}/messages"),
            headers=headers,
            json=payload,
            proxy=self._proxy(),
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise RuntimeError(f"WhatsApp Cloud API send text failed (HTTP {resp.status}): {body[:200]}")

    async def _send_waha_text(self, chat_id: str, text: str, *, reply_to: str | None = None) -> None:
        http = await self._ensure_http()
        headers = _bridge_headers("waha", self._config.bridge_api_key)
        payload: dict[str, Any] = {
            "session": self._config.session_name.strip() or "default",
            "chatId": self._to_waha_chat_id(chat_id),
            "text": text,
        }
        if reply_to:
            payload["reply_to"] = reply_to
        async with http.post(
            self._bridge_url("api/sendText"),
            headers=headers,
            json=payload,
            proxy=self._proxy(),
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise RuntimeError(f"WAHA sendText failed (HTTP {resp.status}): {body[:200]}")
            with contextlib.suppress(Exception):
                self._remember_sent_message_ids(await resp.json())

    async def _send_evolution_text(self, chat_id: str, text: str, *, reply_to: str | None = None) -> None:
        http = await self._ensure_http()
        headers = _bridge_headers("evolution", self._config.bridge_api_key)
        session = self._config.session_name.strip() or "default"
        payload: dict[str, Any] = {
            "number": chat_id,
            "text": text,
        }
        if reply_to:
            payload["quoted"] = {"key": {"id": reply_to}}
        async with http.post(
            self._bridge_url(f"message/sendText/{session}"),
            headers=headers,
            json=payload,
            proxy=self._proxy(),
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise RuntimeError(f"Evolution API sendText failed (HTTP {resp.status}): {body[:200]}")

    async def _send_content(self, subject: ChannelSubject, parts: list[ContentPart]) -> None:
        text = extract_text(parts)
        media_list = extract_media(parts)
        if text:
            await self._send_text(subject, text)
        for media in media_list:
            await self._send_media(subject, media)

    async def _send_media(self, subject: ChannelSubject, media: ContentPart) -> None:
        chat_id, reply_msg_id = self._resolve_target(subject)
        try:
            raw_bytes, mime_type = await self.load_media_bytes(media)
        except Exception:
            logger.warning("Failed to load media bytes for WhatsApp outbound send", exc_info=True)
            label = self._get_media_label(media)
            await self._send_text(subject, f"[{label}]")
            return

        filename = getattr(media, "filename", None) or self._resolve_media_filename(media, 0, int(time.time()))
        filename = self._safe_media_filename(str(filename))
        mode = self.provider

        try:
            if mode == "waha":
                await self._send_waha_media(chat_id, media, raw_bytes, mime_type, filename, reply_to=reply_msg_id)
            elif mode == "evolution":
                await self._send_evolution_media(chat_id, media, raw_bytes, mime_type, filename)
            else:
                await self._send_cloud_media(chat_id, media, raw_bytes, mime_type, filename, reply_to=reply_msg_id)
            self.note_outbound()
        except Exception:
            logger.warning("Failed to send media via WhatsApp (%s)", mode, exc_info=True)
            label = self._get_media_label(media)
            await self._send_text(subject, f"[{label}]")

    async def _send_cloud_media(
        self,
        recipient: str,
        media: ContentPart,
        raw_bytes: bytes,
        mime_type: str,
        filename: str,
        *,
        reply_to: str | None = None,
    ) -> None:
        http = await self._ensure_http()
        auth_headers = {"Authorization": f"Bearer {self._config.access_token}"}

        # Step 1: Upload media to /{phone_number_id}/media
        form = aiohttp.FormData()
        form.add_field("messaging_product", "whatsapp")
        form.add_field("type", mime_type)
        form.add_field("file", raw_bytes, filename=filename, content_type=mime_type)

        async with http.post(
            self._cloud_url(f"{self._config.phone_number_id}/media"),
            headers=auth_headers,
            data=form,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()
            up_data: dict[str, Any] = await resp.json()
        media_id = str(up_data.get("id") or "")
        if not media_id:
            raise RuntimeError("WhatsApp Cloud API media upload did not return an id")

        # Step 2: Send media message
        if isinstance(media, ImageContent):
            mtype = "image"
            media_obj: dict[str, Any] = {"id": media_id}
        elif isinstance(media, VideoContent):
            mtype = "video"
            media_obj = {"id": media_id}
        elif isinstance(media, AudioContent):
            mtype = "audio"
            media_obj = {"id": media_id}
        else:
            mtype = "document"
            media_obj = {"id": media_id, "filename": filename}

        to_number = normalize_whatsapp_identifier(recipient) if not recipient.endswith("@g.us") else recipient
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to_number,
            "type": mtype,
            mtype: media_obj,
        }
        if reply_to:
            payload["context"] = {"message_id": reply_to}

        json_headers = {
            "Authorization": f"Bearer {self._config.access_token}",
            "Content-Type": "application/json",
        }
        async with http.post(
            self._cloud_url(f"{self._config.phone_number_id}/messages"),
            headers=json_headers,
            json=payload,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()

    async def _send_waha_media(
        self,
        chat_id: str,
        media: ContentPart,
        raw_bytes: bytes,
        mime_type: str,
        filename: str,
        *,
        reply_to: str | None = None,
    ) -> None:
        http = await self._ensure_http()
        headers = _bridge_headers("waha", self._config.bridge_api_key)
        b64_data = base64.b64encode(raw_bytes).decode("ascii")

        if isinstance(media, ImageContent):
            endpoint = "api/sendImage"
        elif isinstance(media, VideoContent):
            endpoint = "api/sendVideo"
        elif isinstance(media, AudioContent):
            endpoint = "api/sendVoice"
        else:
            endpoint = "api/sendFile"

        payload: dict[str, Any] = {
            "session": self._config.session_name.strip() or "default",
            "chatId": self._to_waha_chat_id(chat_id),
            "file": {
                "mimetype": mime_type,
                "filename": filename,
                "data": b64_data,
            },
        }
        if reply_to:
            payload["reply_to"] = reply_to

        async with http.post(
            self._bridge_url(endpoint),
            headers=headers,
            json=payload,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()
            with contextlib.suppress(Exception):
                self._remember_sent_message_ids(await resp.json())

    async def _send_evolution_media(
        self,
        chat_id: str,
        media: ContentPart,
        raw_bytes: bytes,
        mime_type: str,
        filename: str,
    ) -> None:
        http = await self._ensure_http()
        headers = _bridge_headers("evolution", self._config.bridge_api_key)
        session = self._config.session_name.strip() or "default"
        b64_data = base64.b64encode(raw_bytes).decode("ascii")

        if isinstance(media, ImageContent):
            mediatype = "image"
        elif isinstance(media, VideoContent):
            mediatype = "video"
        elif isinstance(media, AudioContent):
            mediatype = "audio"
        else:
            mediatype = "document"

        payload: dict[str, Any] = {
            "number": chat_id,
            "mediatype": mediatype,
            "mimetype": mime_type,
            "media": b64_data,
            "fileName": filename,
        }
        async with http.post(
            self._bridge_url(f"message/sendMedia/{session}"),
            headers=headers,
            json=payload,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()

    async def fetch_remote_media(self, url: str) -> tuple[bytes, str]:
        """Download media from Meta Cloud API (``whatsapp-media://``) or self-hosted bridge."""
        http = await self._ensure_http()

        # Meta Cloud API two-step media download: GET /{media_id} -> url -> GET binary
        if url.startswith(_WHATSAPP_MEDIA_SCHEME):
            media_id = url[len(_WHATSAPP_MEDIA_SCHEME) :].strip()
            auth_headers = {"Authorization": f"Bearer {self._config.access_token}"}
            async with http.get(self._cloud_url(media_id), headers=auth_headers, proxy=self._proxy()) as meta_resp:
                meta_resp.raise_for_status()
                meta_data: dict[str, Any] = await meta_resp.json()
            download_url = str(meta_data.get("url") or "")
            if not download_url:
                raise RuntimeError(f"WhatsApp Cloud API media lookup returned no URL for {media_id}")
            mime_hint = str(meta_data.get("mime_type") or "")
            async with http.get(download_url, headers=auth_headers, proxy=self._proxy()) as bin_resp:
                bin_resp.raise_for_status()
                content_type = mime_hint or bin_resp.content_type or "application/octet-stream"
                data = await bin_resp.read()
                return data, content_type

        # Bridge or external URL download — use strict parsed host/origin checks
        # so credentials are never leaked via substring or prefix confusion.
        headers: dict[str, str] = {}
        parsed_url = urlparse(url)
        url_host = (parsed_url.hostname or "").lower()
        if self._config.bridge_url.strip():
            bridge_parsed = urlparse(self._config.bridge_url.strip().rstrip("/"))
            bridge_path = bridge_parsed.path.rstrip("/")
            same_origin = (
                parsed_url.scheme == bridge_parsed.scheme
                and parsed_url.netloc.lower() == bridge_parsed.netloc.lower()
                and (not bridge_path or parsed_url.path == bridge_path or parsed_url.path.startswith(f"{bridge_path}/"))
            )
            if same_origin:
                headers.update(_bridge_headers(self.provider, self._config.bridge_api_key))
        if (
            not headers
            and self._config.access_token
            and parsed_url.scheme == "https"
            and (
                url_host in ("facebook.com", "fbcdn.net", "fbsbx.com", "whatsapp.net")
                or url_host.endswith((".facebook.com", ".fbcdn.net", ".fbsbx.com", ".whatsapp.net"))
            )
        ):
            headers["Authorization"] = f"Bearer {self._config.access_token}"

        async with http.get(url, headers=headers, proxy=self._proxy()) as resp:
            resp.raise_for_status()
            content_type = resp.content_type or _guess_content_type(url)
            data = await resp.read()
            return data, content_type
