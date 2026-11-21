"""Slack channel implementation via Slack Web API & Socket Mode (or HTTP Events API).

Supports:
  - Socket Mode WebSocket connection (``apps.connections.open`` via ``xapp-...`` app token)
    with zero public IP / webhook requirement and automatic reconnection.
  - HTTP Events API webhook ingestion (``url_verification`` challenge + ``X-Slack-Signature``
    HMAC-SHA256 verification via ``signing_secret``).
  - Rich media inbound (Slack private file download with Bearer auth) & outbound
    (Slack ``files.getUploadURLExternal`` / ``files.completeUploadExternal`` v2 pipeline).
  - Thread-aware replies (``reply_in_thread``) and optional thread session isolation
    (``thread_isolation``).
  - Group / channel mention detection (``<@BOT_ID>``) and shared GroupContextManager policy.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import re
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Literal, Self
from urllib.parse import urlparse

import aiohttp

from octop_gateway.channel import (
    BaseChannel,
    ChannelConfig,
    MessageProcessor,
    _guess_content_type,
    current_inbound_receipt,
)
from octop_gateway.constraints import ChannelConstraints, ReplyTimeoutGuard, tool_hint_message
from octop_gateway.group_context import GroupContextConfig
from octop_gateway.models import (
    AudioContent,
    ChannelSubject,
    ContentPart,
    FileContent,
    ImageContent,
    InboundMessage,
    MessageEventType,
    TextContent,
    VideoContent,
)
from octop_gateway.push_routing import alias_subject_fields
from octop_gateway.utils import close_event_stream, extract_media, extract_text

logger = logging.getLogger(__name__)

_SLACK_MAX_MESSAGE_LEN = 3800
_DEDUP_MAX_SIZE = 1000
_MENTION_RE = re.compile(r"<@([A-Z0-9]+)(?:\|[^>]+)?>")
_FENCE_RE = re.compile(r"^(`{3,}|~{3,})")


def split_slack_text(text: str, limit: int = _SLACK_MAX_MESSAGE_LEN) -> list[str]:
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


def _coerce_id_list(raw: Any) -> list[str]:
    """Normalize a list or comma-separated string of Slack IDs."""
    if not raw:
        return []
    if isinstance(raw, str):
        return [item.strip() for item in raw.split(",") if item.strip()]
    if isinstance(raw, list | tuple | set):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


@dataclass
class SlackConfig(ChannelConfig):
    """Configuration for Slack channel."""

    bot_token: str = ""
    app_token: str = ""
    signing_secret: str = ""
    api_base: str = "https://slack.com/api"
    owner_only: bool = False
    allow_all_channels: bool = True
    allowed_channel_ids: list[str] = field(default_factory=list)
    allow_all_dms: bool = True
    allowed_user_ids: list[str] = field(default_factory=list)
    ignore_mention_all: bool = False
    reply_in_thread: bool = True
    thread_isolation: bool = False
    streaming_mode: str = "off"
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        normalized = dict(data)
        if "allowed_channel_ids" in normalized:
            normalized["allowed_channel_ids"] = _coerce_id_list(normalized["allowed_channel_ids"])
        if "allowed_user_ids" in normalized:
            normalized["allowed_user_ids"] = _coerce_id_list(normalized["allowed_user_ids"])
        return super().from_dict(normalized)


SlackConfig.required_credentials = ("bot_token",)  # type: ignore[attr-defined]
SlackConfig.field_aliases = {  # type: ignore[attr-defined]
    "token": "bot_token",
    "botToken": "bot_token",
    "appToken": "app_token",
    "signingSecret": "signing_secret",
    "proxy": "http_proxy",
}


class SlackChannel(BaseChannel):
    """Slack channel adapter supporting Socket Mode WebSocket and HTTP Events API."""

    channel_type = "slack"

    def __init__(
        self,
        processor: MessageProcessor,
        config: SlackConfig | None = None,
        *,
        channel_id: str | None = None,
        tenant_id: str | None = None,
        debounce_seconds: float = 0.0,
        constraints: ChannelConstraints | None = None,
    ) -> None:
        self._config = config or SlackConfig()
        super().__init__(
            processor,
            channel_id=channel_id or self._config.channel_id,
            tenant_id=tenant_id or self._config.tenant_id,
            debounce_seconds=debounce_seconds,
            constraints=constraints,
            config=self._config,
        )
        self._bot_user_id: str = ""
        self._team_id: str = ""
        self._connection_session: str | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._stopping = False
        self._seen_events: OrderedDict[str, None] = OrderedDict()

    def _default_constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            send_rate_limit=(1, 1.0),
            typing_keepalive_interval=0.0,
            show_tool_hints=True,
        )

    def _enrich_push_metadata(self, subject: ChannelSubject, meta: dict[str, Any]) -> dict[str, Any]:
        raw_target = subject.subject_id.split(":thread:", 1)[0] if subject.subject_id else ""
        return alias_subject_fields(meta, raw_target, "channel_id")

    @property
    def bot_user_id(self) -> str:
        """Resolved Slack bot user ID (e.g. ``U01234567``)."""
        return self._bot_user_id

    def _proxy(self) -> str | None:
        return self._config.http_proxy.strip() or None

    def _api_url(self, method: str) -> str:
        base = self._config.api_base.rstrip("/")
        return f"{base}/{method.lstrip('/')}"

    def _is_duplicate(self, event_key: str) -> bool:
        """Return True if *event_key* was already seen, else record it."""
        if not event_key:
            return False
        if event_key in self._seen_events:
            self._seen_events.move_to_end(event_key)
            self.dropped_duplicate += 1
            return True
        self._seen_events[event_key] = None
        while len(self._seen_events) > _DEDUP_MAX_SIZE:
            self._seen_events.popitem(last=False)
        return False

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Verify Slack credentials via ``auth.test`` and start Socket Mode if ``app_token`` is set."""
        if not self._config.bot_token:
            raise ValueError("Slack bot_token is required")

        self._stopping = False
        await self._verify_auth()
        self._connection_session = f"slack-{uuid.uuid4().hex[:12]}"

        if self._config.app_token:
            ws_url = await self._open_socket_connection()
            if self._channel_id != "__probe__" and self._config.probe_mode != "auth_only":
                self._ws_task = asyncio.create_task(
                    self._ws_loop(initial_url=ws_url),
                    name=f"slack-ws-{self._channel_id}",
                )

    async def stop(self) -> None:
        """Stop Socket Mode loop and close HTTP resources."""
        self._stopping = True
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._ws_task
        self._ws_task = None
        if self._ws and not self._ws.closed:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._ws = None
        await self._close_http()

    async def _verify_auth(self) -> dict[str, Any]:
        """Call ``auth.test`` to validate ``bot_token`` and resolve ``bot_user_id``."""
        http = await self._ensure_http()
        headers = {"Authorization": f"Bearer {self._config.bot_token}"}
        async with http.post(self._api_url("auth.test"), headers=headers, proxy=self._proxy()) as resp:
            resp.raise_for_status()
            data: dict[str, Any] = await resp.json()
        if not data.get("ok"):
            err = data.get("error", "unknown_error")
            raise ValueError(f"Slack auth.test failed: {err}")
        self._bot_user_id = str(data.get("user_id") or "")
        self._team_id = str(data.get("team_id") or "")
        return data

    async def _open_socket_connection(self) -> str:
        """Call ``apps.connections.open`` with ``app_token`` to get a dynamic Socket Mode WSS URL."""
        http = await self._ensure_http()
        headers = {
            "Authorization": f"Bearer {self._config.app_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        async with http.post(
            self._api_url("apps.connections.open"),
            headers=headers,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()
            data: dict[str, Any] = await resp.json()
        if not data.get("ok") or not data.get("url"):
            err = data.get("error", "unknown_error")
            raise ValueError(f"Slack apps.connections.open failed: {err}")
        return str(data["url"])

    async def _ws_loop(self, initial_url: str | None = None) -> None:
        """Maintain Socket Mode WebSocket connection with automatic reconnect."""
        ws_url = initial_url
        backoff = 1.0
        while not self._stopping:
            try:
                if not ws_url:
                    ws_url = await self._open_socket_connection()
                http = await self._ensure_http()
                async with http.ws_connect(
                    ws_url,
                    heartbeat=30.0,
                    proxy=self._proxy(),
                ) as ws:
                    self._ws = ws
                    self._connection_session = f"slack-{uuid.uuid4().hex[:12]}"
                    ws_url = None  # Force fresh URL on next reconnect
                    backoff = 1.0
                    async for msg in ws:
                        if self._stopping:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            should_reconnect = await self._handle_socket_frame(ws, msg.data)
                            if should_reconnect:
                                self.note_reconnect()
                                break
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._stopping:
                    break
                logger.warning("Slack Socket Mode connection error; reconnecting in %.1fs", backoff, exc_info=True)
                self.note_reconnect()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            finally:
                self._ws = None

    async def _handle_socket_frame(self, ws: aiohttp.ClientWebSocketResponse, raw_text: str) -> bool:
        """Process one Socket Mode JSON frame. Returns True if disconnect was requested."""
        try:
            frame = json.loads(raw_text)
        except json.JSONDecodeError:
            return False
        if not isinstance(frame, dict):
            return False

        # Acknowledge envelope immediately (Slack requires ACK within 3s)
        envelope_id = frame.get("envelope_id")
        if envelope_id:
            with contextlib.suppress(Exception):
                await ws.send_json({"envelope_id": envelope_id})

        frame_type = frame.get("type")
        if frame_type == "hello":
            logger.debug("Slack Socket Mode connected: channel_id=%s", self._channel_id)
            return False
        if frame_type == "disconnect":
            logger.info("Slack Socket Mode requested disconnect (%s); refreshing connection", frame.get("reason"))
            return True
        if frame_type == "events_api":
            payload = frame.get("payload")
            if isinstance(payload, dict):
                event = payload.get("event")
                if isinstance(event, dict) and self._should_handle_event(event):
                    self.note_inbound()
                    self.enqueue(event)
        return False

    # =========================================================================
    # HTTP Events API Webhook Support
    # =========================================================================

    def verify_signature(
        self,
        timestamp: str,
        signature: str,
        raw_body: bytes | str,
        *,
        max_skew_seconds: int = 300,
        require_secret: bool = False,
    ) -> bool:
        """Verify ``X-Slack-Signature`` HMAC-SHA256 signature."""
        secret = self._config.signing_secret.strip()
        if not secret:
            return not require_secret
        if not timestamp or not signature:
            return False
        try:
            ts_int = int(timestamp)
        except ValueError:
            return False
        if max_skew_seconds > 0 and abs(int(time.time()) - ts_int) > max_skew_seconds:
            return False

        body_str = raw_body.decode("utf-8", errors="replace") if isinstance(raw_body, bytes) else raw_body
        basestring = f"v0:{timestamp}:{body_str}".encode()
        expected = "v0=" + hmac.new(secret.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)

    def handle_webhook_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle an HTTP Events API payload (``url_verification`` or ``event_callback``)."""
        ptype = payload.get("type")
        if ptype == "url_verification":
            return {"challenge": str(payload.get("challenge") or "")}
        if ptype == "event_callback":
            event = payload.get("event")
            if isinstance(event, dict) and self._should_handle_event(event):
                self.note_inbound()
                self.enqueue(event)
        return {"ok": True}

    # =========================================================================
    # Inbound filtering & parsing
    # =========================================================================

    def _is_dm_event(self, event: dict[str, Any]) -> bool:
        channel_type = str(event.get("channel_type") or "").lower()
        channel_id = str(event.get("channel") or "")
        return channel_type in ("im", "mpim") or channel_id.startswith("D")

    def _is_allowed(self, channel_id: str, user_id: str, *, is_dm: bool) -> bool:
        if self._config.owner_only:
            owners = {
                u.strip() for u in [*self._config.group_context.owner_ids, *self._config.allowed_user_ids] if u.strip()
            }
            if user_id not in owners:
                return False
            if is_dm:
                return True
            if not self._config.allow_all_channels:
                return bool(self._config.allowed_channel_ids) and channel_id in self._config.allowed_channel_ids
            return True
        if is_dm:
            if not self._config.allow_all_dms:
                return bool(self._config.allowed_user_ids) and user_id in self._config.allowed_user_ids
            if self._config.allowed_user_ids:
                return user_id in self._config.allowed_user_ids
            return True
        if self._config.allowed_user_ids and user_id not in self._config.allowed_user_ids:
            return False
        if not self._config.allow_all_channels:
            return bool(self._config.allowed_channel_ids) and channel_id in self._config.allowed_channel_ids
        return True

    def _should_handle_event(self, event: dict[str, Any]) -> bool:
        event_type = str(event.get("type") or "")
        if event_type not in ("message", "app_mention"):
            return False
        subtype = event.get("subtype")
        if subtype not in (None, "", "file_share", "thread_broadcast"):
            return False
        if event.get("bot_id") or event.get("app_id"):
            return False
        user_id = str(event.get("user") or "")
        if not user_id or (self._bot_user_id and user_id == self._bot_user_id):
            return False
        channel_id = str(event.get("channel") or "")
        if not channel_id:
            return False
        is_dm = self._is_dm_event(event)
        if not self._is_allowed(channel_id, user_id, is_dm=is_dm):
            return False
        ts = str(event.get("ts") or event.get("event_ts") or "")
        dedup_key = f"{channel_id}:{ts}" if ts else ""
        return not self._is_duplicate(dedup_key)

    def parse_inbound(self, raw_payload: Any) -> InboundMessage:
        """Convert a Slack event dict into an :class:`InboundMessage`."""
        if not isinstance(raw_payload, dict):
            raise TypeError(f"Expected dict payload for SlackChannel.parse_inbound, got {type(raw_payload).__name__}")

        event = raw_payload.get("event") if raw_payload.get("type") == "event_callback" else raw_payload
        if not isinstance(event, dict):
            event = raw_payload

        channel_id = str(event.get("channel") or "")
        user_id = str(event.get("user") or "")
        sender_name = str(event.get("user_name") or event.get("username") or user_id)
        ts_str = str(event.get("ts") or event.get("event_ts") or "")
        thread_ts = str(event.get("thread_ts") or "")
        raw_text = str(event.get("text") or "")
        event_type = str(event.get("type") or "message")

        is_dm = self._is_dm_event(event)
        is_group = not is_dm

        mentions = _MENTION_RE.findall(raw_text)
        broadcast_mentioned = any(tag in raw_text for tag in ("<!channel>", "<!here>", "<!everyone>"))
        has_mentions = bool(mentions) or broadcast_mentioned
        direct_bot_mention = event_type == "app_mention" or (bool(self._bot_user_id) and self._bot_user_id in mentions)
        bot_mentioned = direct_bot_mention or (is_group and not self._config.ignore_mention_all and broadcast_mentioned)

        # Strip own bot mention from text
        cleaned_text = raw_text
        if self._bot_user_id:
            cleaned_text = re.sub(
                rf"<@{re.escape(self._bot_user_id)}(?:\|[^>]+)?>",
                "",
                cleaned_text,
            ).strip()

        parts: list[ContentPart] = []
        if cleaned_text:
            parts.append(TextContent(text=cleaned_text))

        # Extract Slack files
        files = event.get("files")
        if isinstance(files, list):
            for fobj in files:
                if not isinstance(fobj, dict):
                    continue
                url = str(fobj.get("url_private_download") or fobj.get("url_private") or "")
                if not url:
                    continue
                mime = str(fobj.get("mimetype") or _guess_content_type(url))
                name = str(fobj.get("name") or fobj.get("title") or "attachment")
                size = int(fobj.get("size") or 0) or None
                if mime.startswith("image/"):
                    parts.append(ImageContent(url=url, mime_type=mime, size=size))
                elif mime.startswith("video/"):
                    parts.append(VideoContent(url=url, mime_type=mime, size=size))
                elif mime.startswith("audio/"):
                    parts.append(AudioContent(url=url, mime_type=mime, size=size))
                else:
                    parts.append(FileContent(url=url, filename=name, mime_type=mime, size=size))

        # Extract quoted / shared message attachments
        reply_quote_text = ""
        attachments = event.get("attachments")
        if isinstance(attachments, list):
            quote_chunks: list[str] = []
            for att in attachments:
                if not isinstance(att, dict):
                    continue
                att_text = str(att.get("text") or att.get("fallback") or "").strip()
                if att_text:
                    author = str(att.get("author_name") or att.get("author_subname") or "").strip()
                    quote_chunks.append(f"{author}: {att_text}" if author else att_text)
            if quote_chunks:
                reply_quote_text = "\n".join(quote_chunks)

        if not parts:
            parts.append(TextContent(text=""))

        try:
            timestamp = float(ts_str) if ts_str else time.time()
        except ValueError:
            timestamp = time.time()

        chat_type: Literal["direct", "group"]
        if is_dm:
            subject_id = channel_id or user_id
            chat_type = "direct"
        else:
            effective_thread = thread_ts or ts_str
            if self._config.thread_isolation and effective_thread:
                subject_id = f"{channel_id}:thread:{effective_thread}"
            else:
                subject_id = channel_id
            chat_type = "group"

        metadata: dict[str, Any] = {
            "channel_id": channel_id,
            "conversation_id": channel_id,
            "sender_id": user_id,
            "sender_name": sender_name,
            "ts": ts_str,
            "message_id": ts_str,
            "team_id": str(event.get("team") or self._team_id),
            "is_dm": is_dm,
            "is_group": is_group,
            "bot_mentioned": bot_mentioned,
            "has_mentions": has_mentions,
        }
        if thread_ts:
            metadata["thread_ts"] = thread_ts
        if reply_quote_text:
            metadata["reply_quote_text"] = reply_quote_text
            metadata["reply_to_text"] = reply_quote_text

        subject = ChannelSubject(
            subject_id=subject_id,
            display_name=sender_name if is_dm else channel_id,
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
    # Outbound API (Slack Web API)
    # =========================================================================

    def _resolve_target(self, subject: ChannelSubject) -> tuple[str, str | None]:
        """Resolve ``(channel_id, thread_ts)`` from a :class:`ChannelSubject`."""
        meta = subject.metadata or {}
        raw_subject = subject.subject_id or ""
        isolated_thread: str | None = None
        if ":thread:" in raw_subject:
            raw_channel, isolated_thread = raw_subject.split(":thread:", 1)
        else:
            raw_channel = raw_subject

        channel_id = str(meta.get("channel_id") or raw_channel).strip()
        if not channel_id:
            raise ValueError("Slack send requires a target channel_id or subject_id")

        thread_ts = str(meta.get("thread_ts") or isolated_thread or "").strip() or None
        if not thread_ts and self._config.reply_in_thread and (subject.chat_type == "group" or meta.get("is_group")):
            ts_val = str(meta.get("ts") or "").strip()
            if ts_val:
                thread_ts = ts_val

        return channel_id, thread_ts

    async def _set_reaction(self, channel_id: str, ts: str, name: str, *, remove: bool = False) -> None:
        if not channel_id or not ts or not name:
            return
        http = await self._ensure_http()
        headers = {
            "Authorization": f"Bearer {self._config.bot_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        endpoint = "reactions.remove" if remove else "reactions.add"
        payload = {"channel": channel_id, "timestamp": ts, "name": name}
        with contextlib.suppress(Exception):
            async with http.post(
                self._api_url(endpoint),
                headers=headers,
                json=payload,
                proxy=self._proxy(),
            ) as resp:
                await resp.read()

    async def _post_or_update_stream_text(
        self,
        channel_id: str,
        thread_ts: str | None,
        text: str,
        *,
        message_ts: str | None = None,
    ) -> str | None:
        if not text.strip():
            return message_ts
        http = await self._ensure_http()
        headers = {
            "Authorization": f"Bearer {self._config.bot_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        chunks = split_slack_text(text, limit=_SLACK_MAX_MESSAGE_LEN)
        first_chunk = chunks[0]
        receipt = current_inbound_receipt()
        if message_ts:
            payload: dict[str, Any] = {
                "channel": channel_id,
                "ts": message_ts,
                "text": first_chunk,
            }
            if receipt is not None:
                receipt.attempted = True
            try:
                async with http.post(
                    self._api_url("chat.update"),
                    headers=headers,
                    json=payload,
                    proxy=self._proxy(),
                ) as resp:
                    resp.raise_for_status()
                    data: dict[str, Any] = await resp.json()
                    if data.get("ok"):
                        self.note_outbound()
                        if receipt is not None:
                            receipt.delivered = True
                    else:
                        logger.warning("Slack chat.update failed: %s", data.get("error", "unknown_error"))
            except Exception:
                logger.warning("Slack chat.update request error", exc_info=True)
            return message_ts

        await self._acquire_rate_slot()
        if receipt is not None:
            receipt.attempted = True
        post_payload: dict[str, Any] = {"channel": channel_id, "text": first_chunk}
        if thread_ts:
            post_payload["thread_ts"] = thread_ts
        try:
            async with http.post(
                self._api_url("chat.postMessage"),
                headers=headers,
                json=post_payload,
                proxy=self._proxy(),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if data.get("ok"):
                    self.note_outbound()
                    if receipt is not None:
                        receipt.delivered = True
                    return str(data.get("ts") or "") or None
                logger.warning("Slack chat.postMessage failed: %s", data.get("error", "unknown_error"))
        except Exception:
            logger.warning("Slack chat.postMessage request error", exc_info=True)
        return None

    async def _process_inbound(self, message: InboundMessage, subject: ChannelSubject) -> bool:
        meta = message.metadata or {}
        src_channel = str(meta.get("channel_id") or "")
        src_ts = str(meta.get("ts") or "")
        use_reactions = bool(self._config.status_reactions and src_channel and src_ts)
        receipt = current_inbound_receipt()
        # Fall back to buffered delivery when an inbound plugin installs a text_filter
        # (e.g. [NO_REPLY] suppression) so partial tokens never bypass output filtering.
        use_stream_edit = (self._config.streaming_mode or "").strip().lower() == "edit" and (
            receipt is None or receipt.text_filter is None
        )

        if use_reactions:
            await self._set_reaction(src_channel, src_ts, "hourglass_flowing_sand")

        if not use_stream_edit:
            try:
                ok = await super()._process_inbound(message, subject)
                if use_reactions:
                    await self._set_reaction(src_channel, src_ts, "hourglass_flowing_sand", remove=True)
                    if ok:
                        await self._set_reaction(src_channel, src_ts, "white_check_mark")
                return ok
            except Exception:
                if use_reactions:
                    await self._set_reaction(src_channel, src_ts, "hourglass_flowing_sand", remove=True)
                raise

        channel_id, thread_ts = self._resolve_target(subject)
        stream_ts: str | None = None
        delta_buffer: list[str] = []
        thinking_buffer: list[str] = []
        show_thinking = self.should_show_thinking()
        last_edit_at = 0.0
        succeeded = False
        timeout_guard: ReplyTimeoutGuard | None = None

        async def _flush_thinking() -> None:
            if not thinking_buffer:
                return
            thinking_text = "".join(thinking_buffer).strip()
            thinking_buffer.clear()
            if thinking_text:
                formatted = self._constraints.thinking_template.format(content=thinking_text)
                await self._rate_limited_send(subject, formatted)

        async def _flush_deltas(*, reset_stream: bool = False) -> None:
            nonlocal stream_ts
            if not delta_buffer:
                return
            final_text = self._clean_output("".join(delta_buffer))
            delta_buffer.clear()
            if final_text:
                chunks = split_slack_text(final_text, limit=_SLACK_MAX_MESSAGE_LEN)
                stream_ts = await self._post_or_update_stream_text(
                    channel_id, thread_ts, chunks[0], message_ts=stream_ts
                )
                for extra_chunk in chunks[1:]:
                    await self.reply_text(subject, extra_chunk)
            if reset_stream:
                stream_ts = None

        if self._constraints.reply_timeout > 0:

            async def _on_reply_timeout() -> None:
                if self._constraints.timeout_strategy == "placeholder":
                    await self._rate_limited_send(subject, self._constraints.placeholder_text)

            timeout_guard = ReplyTimeoutGuard(
                timeout=self._constraints.reply_timeout,
                on_timeout=_on_reply_timeout,
            )
            timeout_guard.start()

        events: Any = None
        try:
            events = self._processor(message)
            async for event in events:
                ev_text = extract_text(event.content)
                ev_media = extract_media(event.content)
                if event.type == MessageEventType.DELTA and ev_text:
                    if timeout_guard:
                        timeout_guard.cancel()
                    delta_buffer.append(ev_text)
                    now = time.monotonic()
                    if now - last_edit_at >= 0.5:
                        current_text = self._clean_output("".join(delta_buffer))
                        if current_text:
                            stream_ts = await self._post_or_update_stream_text(
                                channel_id, thread_ts, current_text, message_ts=stream_ts
                            )
                            last_edit_at = now
                elif event.type == MessageEventType.THINKING and show_thinking:
                    await self._on_thinking(subject, event)
                elif event.type == MessageEventType.THINKING_DELTA and show_thinking:
                    for part in event.content:
                        if isinstance(part, TextContent) and part.text:
                            thinking_buffer.append(part.text)
                elif event.type == MessageEventType.FLUSH:
                    await _flush_thinking()
                    await _flush_deltas(reset_stream=True)
                elif event.type == MessageEventType.MESSAGE:
                    if timeout_guard:
                        timeout_guard.cancel()
                    await _flush_thinking()
                    await _flush_deltas(reset_stream=True)
                    cleaned = self._clean_output(ev_text) if ev_text else ""
                    if cleaned:
                        await self._rate_limited_send(subject, cleaned)
                    for m in ev_media:
                        await self.reply_media(subject, m)
                elif event.type == MessageEventType.TOOL_START:
                    if self._constraints.show_tool_hints:
                        hint = tool_hint_message(event.metadata, self._constraints, phase="start")
                        await self._rate_limited_send(subject, hint)
                elif event.type == MessageEventType.TOOL_END:
                    if self._constraints.show_tool_hints:
                        hint = tool_hint_message(event.metadata, self._constraints, phase="end")
                        await self._rate_limited_send(subject, hint)
                elif event.type == MessageEventType.ERROR:
                    if timeout_guard:
                        timeout_guard.cancel()
                    await _flush_deltas(reset_stream=True)
                    error_text = event.error or "An unexpected error occurred."
                    await self._rate_limited_send(subject, error_text)
                    break
                elif event.type == MessageEventType.COMPLETED:
                    if timeout_guard:
                        timeout_guard.cancel()
                    await _flush_thinking()
                    await _flush_deltas()
                    succeeded = True
                    break

            await _flush_thinking()
            await _flush_deltas()
            return succeeded
        except Exception:
            logger.exception(
                "Error processing Slack stream message: channel=%s session=%s",
                self.channel_id,
                message.channel_session_id,
            )
            if timeout_guard:
                timeout_guard.cancel()
            await _flush_deltas(reset_stream=True)
            await self._rate_limited_send(subject, "An error occurred while processing your message.")
            return False
        finally:
            if timeout_guard:
                timeout_guard.cancel()
            if events is not None:
                await close_event_stream(events)
            if use_reactions:
                await self._set_reaction(src_channel, src_ts, "hourglass_flowing_sand", remove=True)
                if succeeded:
                    await self._set_reaction(src_channel, src_ts, "white_check_mark")

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        if not text:
            return
        channel_id, thread_ts = self._resolve_target(subject)
        chunks = split_slack_text(text, limit=_SLACK_MAX_MESSAGE_LEN)
        http = await self._ensure_http()
        headers = {
            "Authorization": f"Bearer {self._config.bot_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        for chunk in chunks:
            payload: dict[str, Any] = {
                "channel": channel_id,
                "text": chunk,
            }
            if thread_ts:
                payload["thread_ts"] = thread_ts
            async with http.post(
                self._api_url("chat.postMessage"),
                headers=headers,
                json=payload,
                proxy=self._proxy(),
            ) as resp:
                resp.raise_for_status()
                data: dict[str, Any] = await resp.json()
                if not data.get("ok"):
                    err = data.get("error", "unknown_error")
                    raise RuntimeError(f"Slack chat.postMessage failed: {err}")
        self.note_outbound()

    async def _send_content(self, subject: ChannelSubject, parts: list[ContentPart]) -> None:
        text = extract_text(parts)
        media_list = extract_media(parts)
        if text:
            await self._send_text(subject, text)
        for media in media_list:
            await self._send_media(subject, media)

    async def _send_media(self, subject: ChannelSubject, media: ContentPart) -> None:
        channel_id, thread_ts = self._resolve_target(subject)
        try:
            raw_bytes, mime_type = await self.load_media_bytes(media)
        except Exception:
            logger.warning("Failed to load media bytes for Slack outbound send", exc_info=True)
            label = self._get_media_label(media)
            await self._send_text(subject, f"[{label}]")
            return

        filename = getattr(media, "filename", None) or self._resolve_media_filename(media, 0, int(time.time()))
        filename = self._safe_media_filename(str(filename))

        try:
            await self._upload_file_v2(
                channel_id=channel_id,
                thread_ts=thread_ts,
                raw_bytes=raw_bytes,
                filename=filename,
                mime_type=mime_type,
            )
            self.note_outbound()
        except Exception:
            logger.warning("Failed to upload media to Slack via files_upload_v2", exc_info=True)
            label = self._get_media_label(media)
            await self._send_text(subject, f"[{label}]")

    async def _upload_file_v2(
        self,
        *,
        channel_id: str,
        thread_ts: str | None,
        raw_bytes: bytes,
        filename: str,
        mime_type: str,
    ) -> str:
        """Execute Slack's 3-step external file upload v2 workflow."""
        http = await self._ensure_http()
        auth_headers = {"Authorization": f"Bearer {self._config.bot_token}"}

        # Step 1: files.getUploadURLExternal
        form_data = {
            "filename": filename,
            "length": str(len(raw_bytes)),
        }
        async with http.post(
            self._api_url("files.getUploadURLExternal"),
            headers=auth_headers,
            data=form_data,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()
            step1: dict[str, Any] = await resp.json()
        if not step1.get("ok") or not step1.get("upload_url") or not step1.get("file_id"):
            raise RuntimeError(f"Slack files.getUploadURLExternal failed: {step1.get('error', 'unknown_error')}")

        upload_url = str(step1["upload_url"])
        file_id = str(step1["file_id"])

        # Step 2: Upload raw bytes to upload_url
        upload_headers = {"Content-Type": mime_type or "application/octet-stream"}
        async with http.post(
            upload_url,
            headers=upload_headers,
            data=raw_bytes,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()

        # Step 3: files.completeUploadExternal
        complete_payload: dict[str, Any] = {
            "files": [{"id": file_id, "title": filename}],
            "channel_id": channel_id,
        }
        if thread_ts:
            complete_payload["thread_ts"] = thread_ts

        json_headers = {
            "Authorization": f"Bearer {self._config.bot_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        async with http.post(
            self._api_url("files.completeUploadExternal"),
            headers=json_headers,
            json=complete_payload,
            proxy=self._proxy(),
        ) as resp:
            resp.raise_for_status()
            step3: dict[str, Any] = await resp.json()
        if not step3.get("ok"):
            raise RuntimeError(f"Slack files.completeUploadExternal failed: {step3.get('error', 'unknown_error')}")
        return file_id

    async def fetch_remote_media(self, url: str) -> tuple[bytes, str]:
        """Download media URL, injecting Bearer token for Slack private file URLs."""
        http = await self._ensure_http()
        host = (urlparse(url).hostname or "").lower()
        headers: dict[str, str] = {}
        if (host == "slack.com" or host.endswith(".slack.com")) and self._config.bot_token:
            headers["Authorization"] = f"Bearer {self._config.bot_token}"
        async with http.get(url, headers=headers, proxy=self._proxy()) as resp:
            resp.raise_for_status()
            content_type = resp.content_type or _guess_content_type(url)
            data = await resp.read()
            return data, content_type
