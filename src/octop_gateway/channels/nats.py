"""NATS channel for IoT devices, robots and service integration.

Unlike the MQTT channel (paho is callback/thread based), ``nats-py`` is
async-first: the client lives on the event loop that created it, so this
channel connects and publishes directly on the running loop and needs no
background thread.
"""

from __future__ import annotations

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from octop_gateway.channel import BaseChannel, ChannelConfig, MessageProcessor
from octop_gateway.constraints import ChannelConstraints
from octop_gateway.models import (
    ChannelSubject,
    ContentPart,
    InboundMessage,
    TextContent,
)

logger = logging.getLogger(__name__)


@dataclass
class NATSConfig(ChannelConfig):
    """Configuration for the NATS channel.

    Attributes:
        servers: Comma-separated NATS server URLs, e.g.
            ``"nats://localhost:4222"`` or
            ``"nats://user:pass@host:4222,nats://host2:4222"``.
        username: Optional username. When only ``password`` is set it is
            used as the token instead.
        password: Optional password / token.
        subscribe_subject: Subject (or wildcard) to subscribe for inbound
            messages, e.g. ``"bots.*.in"``.
        publish_subject: Outbound subject template; ``{client_id}`` is
            substituted with the sender's id.
        tls_enabled: Require a TLS (``tls://``) connection.
        show_thinking: Forward thinking/reasoning content to subscribers.
        show_tool_hints: Show tool-call status messages to subscribers.
    """

    servers: str = "nats://localhost:4222"
    username: str = ""
    password: str = ""
    subscribe_subject: str = "bots.*.in"
    publish_subject: str = "bots.{client_id}.out"
    tls_enabled: bool = False

    required_credentials = ("servers",)


class NATSChannel(BaseChannel):
    """NATS channel built on ``nats-py`` (core NATS pub/sub)."""

    channel_type = "nats"

    def __init__(
        self,
        processor: MessageProcessor,
        *,
        config: NATSConfig,
        channel_id: str | None = None,
        tenant_id: str | None = None,
        debounce_seconds: float = 0.0,
        constraints: ChannelConstraints | None = None,
    ) -> None:
        super().__init__(
            processor,
            channel_id=channel_id,
            tenant_id=tenant_id,
            debounce_seconds=debounce_seconds,
            constraints=constraints,
            config=config,
        )
        self._config = config
        self._nc: Any = None
        self._connected = False
        self._connection_session: str | None = None
        self._recent_msg_ids: OrderedDict[str, None] = OrderedDict()

    def _default_constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            send_rate_limit=(30, 60.0),
            show_thinking=False,
            show_tool_hints=True,
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connect to the NATS servers and subscribe to inbound subjects."""
        if not self._config.servers:
            raise RuntimeError("NATSChannel: servers is required")
        if not self._config.subscribe_subject or not self._config.publish_subject:
            raise RuntimeError("NATSChannel: subscribe_subject and publish_subject are required")

        try:
            import nats
        except ImportError as exc:
            raise ImportError("nats-py is required for NATSChannel. Install with: pip install nats-py") from exc

        servers = [s.strip() for s in self._config.servers.split(",") if s.strip()]
        self._connection_session = f"octop-nats-{self.channel_id}"

        async def _on_error(exc: Exception) -> None:
            logger.warning("NATS error: %s", exc)

        async def _on_disconnected() -> None:
            self._connected = False
            logger.warning("NATS disconnected from %s", servers)

        async def _on_reconnected() -> None:
            self._connected = True
            logger.info("NATS reconnected to %s", servers)

        try:
            self._nc = await nats.connect(
                servers=servers,
                user=self._config.username or None,
                password=self._config.password or None,
                tls_required=self._config.tls_enabled,
                connect_timeout=10,
                reconnect_time_wait=1,
                max_reconnect_attempts=-1,
                error_cb=_on_error,
                disconnected_cb=_on_disconnected,
                reconnected_cb=_on_reconnected,
            )
            await self._nc.subscribe(self._config.subscribe_subject, cb=self._on_message)
        except (OSError, ValueError, TimeoutError):
            logger.exception("NATS connect failed: %s", self._config.servers)
            raise

        self._connected = True
        logger.info(
            "NATSChannel started (servers=%s subject=%s)",
            self._config.servers,
            self._config.subscribe_subject,
        )

    async def stop(self) -> None:
        """Flush pending publishes and disconnect."""
        if self._nc is not None:
            nc, self._nc = self._nc, None
            try:
                await nc.drain()
            except Exception:
                logger.exception("NATS drain failed")
        self._connected = False
        self._connection_session = None
        logger.info("NATSChannel stopped")

    # ------------------------------------------------------------------
    # Inbound
    # NATS subject tokens are separated by ".", MQTT topics by "/".
    # ------------------------------------------------------------------

    async def _on_message(self, msg: Any) -> None:
        """nats-py subscription callback — runs on the channel's event loop."""
        try:
            payload = msg.data.decode("utf-8").strip()
            data: dict[str, Any] = {}
            try:
                data = json.loads(payload)
                content = str(data.get("text", ""))
            except json.JSONDecodeError:
                content = payload

            if not content:
                logger.debug("NATS empty message on %s", msg.subject)
                return

            msg_id = str(data.get("msg_id") or "")
            if msg_id and self._is_duplicate(msg_id):
                logger.debug("NATS duplicate message %s dropped", msg_id)
                return

            # Explicit field wins; otherwise for a 3-token subject like
            # "bots.<client_id>.in" take the middle token.
            client_id = data.get("client_id")
            if not client_id:
                parts = msg.subject.split(".")
                if len(parts) >= 3:
                    client_id = parts[1]
            client_id = str(client_id or "unknown-client")

            native = {
                "topic": msg.subject,
                "client_id": client_id,
                "text": content,
                "raw_payload": payload,
            }

            self.enqueue(native)
        except Exception:  # pylint: disable=broad-except
            # Isolate malformed messages so the subscription stays healthy.
            logger.exception("NATS message handler error")

    def _is_duplicate(self, msg_id: str) -> bool:
        """LRU dedup for publishers that set a stable ``msg_id``."""
        if msg_id in self._recent_msg_ids:
            return True
        self._recent_msg_ids[msg_id] = None
        while len(self._recent_msg_ids) > 1000:
            self._recent_msg_ids.popitem(last=False)
        return False

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        if not self._nc or not self._connected or not text.strip():
            return
        client_id = subject.metadata.get("client_id") or subject.subject_id
        topic = self._config.publish_subject.format(client_id=client_id)
        try:
            await self._nc.publish(topic, text.encode("utf-8"))
            await self._nc.flush()
        except Exception:
            logger.exception("NATS publish failed to %s", topic)
            return
        logger.debug("NATS published to %s (%d chars)", topic, len(text))

    async def _send_content(self, subject: ChannelSubject, parts: list[ContentPart]) -> None:
        for part in parts:
            if isinstance(part, TextContent):
                await self._send_text(subject, part.text)
            else:
                await self._send_media(subject, part)

    async def _send_media(self, subject: ChannelSubject, media: ContentPart) -> None:
        label = self._get_media_label(media)
        url = self._get_media_url(media)
        has_inline = bool(getattr(media, "data", None) or getattr(media, "local_path", None))
        if url:
            await self._send_text(subject, f"[{label}: {url}]")
        elif has_inline:
            await self._send_text(subject, f"[{label} (local file, not deliverable on NATS)]")

    def parse_inbound(self, raw_payload: Any) -> InboundMessage:
        """Parse a NATS native dict into :class:`InboundMessage`."""
        if isinstance(raw_payload, InboundMessage):
            return raw_payload

        data = raw_payload if isinstance(raw_payload, dict) else {}
        client_id = str(data.get("client_id") or "unknown-client")
        text = str(data.get("text") or "")
        topic = str(data.get("topic") or "")
        metadata = {
            "client_id": client_id,
            "topic": topic,
            "raw_payload": data.get("raw_payload", ""),
        }

        return InboundMessage(
            channel_id=self.channel_id,
            channel_type=self.channel_type,
            tenant_id=self._tenant_id,
            channel_subject=ChannelSubject(
                subject_id=client_id,
                chat_type="direct",
                metadata=metadata,
            ),
            channel_session_id=self._connection_session or f"{self.channel_id}-unconnected",
            content=[TextContent(text=text)] if text else [TextContent(text="")],
            metadata=metadata,
        )
