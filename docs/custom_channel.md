# Custom channels

This guide answers two questions from issue #27:

1. **Does the gateway scan for channels?** No. Built-in channels are a fixed,
   lazy-imported map (`octop_gateway/channels/__init__.py` → `_CHANNEL_MAP`).
   Nothing scans packages, entry points, or plugins.
2. **How do I add my own channel (e.g. Synology Chat)?** Subclass
   [`BaseChannel`][octop_gateway.channel.BaseChannel], build it with your own
   config, and pass the instance to `ChannelManager.add_channel()`. Your code
   owns construction — the gateway never discovers classes on its own.

> The normative, complete contract lives in [`AGENTS.md` §8 "Channel
> specification"](../AGENTS.md). This guide is the shorter, example-driven
> path; read AGENTS.md before merging a channel into this repository.

## Minimal working channel

A channel has three jobs: turn platform payloads into
[`InboundMessage`][octop_gateway.models.InboundMessage], deliver agent output
back to the platform, and manage the connection lifecycle. Below is a compact
webhook-style channel (Synology Chat) — the same shape applies to any
HTTP-callback platform:

```python
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from octop_gateway.channel import BaseChannel, ChannelConfig
from octop_gateway.models import ChannelSubject, InboundMessage, TextContent

logger = logging.getLogger(__name__)


@dataclass
class SynologyChatConfig(ChannelConfig):
    base_url: str = ""   # e.g. "https://nas.example.com:5001"
    token: str = ""      # bot token from Synology Chat integration settings


class SynologyChatChannel(BaseChannel):
    channel_type = "synology_chat"

    def __init__(self, processor, config: SynologyChatConfig, **kwargs: Any) -> None:
        super().__init__(processor, config=config, **kwargs)
        self._config = config

    async def start(self) -> None:
        # Webhook-style platforms have no outbound connection to establish.
        # Your host relays Synology's callback POST into
        # channel.enqueue(payload) (thread-safe) or handle_inbound(payload).
        logger.info("SynologyChat channel %s ready", self._channel_id)

    async def stop(self) -> None:
        await self._close_http()

    def parse_inbound(self, raw_payload: object) -> InboundMessage:
        data = raw_payload["payload"] if isinstance(raw_payload, dict) else {}
        meta = dict(data) if isinstance(data, dict) else {}
        user_id = str(meta.get("user_id", ""))
        return InboundMessage(
            channel_id=self._channel_id,
            channel_type=self.channel_type,
            tenant_id=self._tenant_id,
            channel_session_id=f"{self._channel_id}-unconnected",
            content=[TextContent(text=str(meta.get("message", "")))],
            metadata={
                "msg_id": str(meta.get("msg_id", "")),
                "chat_type": "dm",
                "sender_id": user_id,
                "sender_name": str(meta.get("username", "")),
            },
            channel_subject=ChannelSubject(
                subject_id=user_id,
                display_name=str(meta.get("username", "")),
                chat_type="direct",
                metadata=meta,
            ),
        )

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        http = await self._ensure_http()
        url = f"{self._config.base_url}/webapi/entry.cgi"
        payload = {"text": text}
        params = {
            "api": "SYNO.Chat.External", "method": "bot", "version": "2",
            "token": self._config.token,
            "payload": json.dumps({"user_ids": [int(subject.subject_id)], **payload}),
        }
        async with http.post(url, params=params) as resp:
            body = await resp.json()
            if not body.get("success"):
                logger.error("SynologyChat send failed: %s", body)
```

Wire it up (same event loop as the manager):

```python
manager = ChannelManager(processor=..., channel_configs={...})
channel = SynologyChatChannel(processor, SynologyChatConfig(
    base_url="https://nas.example.com:5001", token="xxx", tenant_id="my-agent",
))
await manager.add_channel(channel)
await manager.start()
```

`add_channel` accepts built-ins by name (`channel_type="feishu"` + a config
dict) **or** a constructed `BaseChannel` instance — the instance path is the
custom-channel path. It returns the resolved `channel_id`.

## The contract (what BaseChannel gives you / expects from you)

| Member | Notes |
|---|---|
| `start()` / `stop()` | Lifecycle. Establish/tear down connections; never raise past a log line. |
| `parse_inbound(raw_payload)` | Platform payload → `InboundMessage`. Pure function of the payload. |
| `_send_text(subject, text)` / `_send_content(subject, parts)` / `_send_media(subject, media)` | Outbound primitives. The public `reply`/`push` wrappers add rate limiting — inside your channel always call the `_*` siblings. |
| `handle_inbound()` | **Do not override.** Override `_preprocess_inbound` (normalize/decrypt) or `_process_inbound` (custom delivery) so media persistence, subject tracking, and group-context policy run exactly once. |
| `self.enqueue(payload)` | Thread-safe dispatch into the manager queue — required when a platform SDK calls you from its own threads. |
| `self._ensure_http()` | Lazy shared `aiohttp.ClientSession`; close via `self._close_http()` in `stop()`. |
| Media | `fetch_remote_media()` (auth headers) / `load_media_bytes()` (outbound bytes) — see the media-flow section of AGENTS.md. |

## Metadata keys that drive host behaviour

The processor (Octop) derives session identity and group policy from
`InboundMessage.metadata` / `ChannelSubject`. Set them consistently:

- `channel_subject.subject_id` — **who owns the conversation**: the user id
  for DMs, the conversation/group id for group chats (this is what keeps
  group members on one thread; see QQ/`dingtalk` for reference).
- `metadata["chat_type"]` — `"dm"` or `"group"`.
- `metadata["conversation_id"]` — groups only; keys
  `GroupContextManager` passive-context buffers.
- `metadata["bot_mentioned"]` — groups only; whether the message activates
  the agent (`activation="mention"` policy).
- `metadata["msg_id"]` — enables the host's dedup; also implement an LRU
  `_is_duplicate()` guard in the channel for platform retries.

## Testing your channel

Follow `tests/channels/test_qq.py` style: construct the channel with a fake
processor, call `parse_inbound` on representative payloads, and stub the HTTP
layer (see the `FakeResponse` pattern in `tests/channels/test_dingtalk.py`).
Run the standard gate:

```bash
make all   # ruff + ruff format + mypy --strict + pytest -m "not integration"
```

Feature PRs target the `develop` branch (see CONTRIBUTING.md).
