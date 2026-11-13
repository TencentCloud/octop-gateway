"""QQ group metadata, auth retry and the platform request budget (offline)."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from octop_gateway.channels.qq import QQChannel, QQConfig


class Response:
    def __init__(self, status, body):
        self.status, self.body = status, body

    async def text(self):
        return self.body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class Http:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, headers):
        self.calls.append((url, headers))
        return next(self.responses)


def channel(responses):
    ch = QQChannel(processor=None, config=QQConfig(app_id="test", secret="test"))
    http = Http(responses)
    ch._ensure_http = AsyncMock(return_value=http)
    ch._fresh_auth_headers = AsyncMock(return_value={"Authorization": "QQBot test"})
    return ch, http


@pytest.mark.asyncio
async def test_lookup_encodes_openid_and_refreshes_token_once():
    ch, http = channel([Response(401, '{"code":11244}'), Response(200, '{"group_name":"group"}')])
    assert await ch.get_group_info("a/b?") == {"group_name": "group"}
    assert all(url.endswith("/groups/a%2Fb%3F/info") for url, _ in http.calls)
    assert ch._fresh_auth_headers.await_count == 2
    assert ch._group_info_limiter.available == 28


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body", "error"),
    [
        (403, '{"code":11253,"message":"sensitive detail"}', "11253.*allowlist"),
        (429, '{"code":22009}', "429.*22009"),
        (200, "[]", "invalid response"),
        (200, "not JSON", "invalid JSON"),
    ],
)
async def test_lookup_explains_errors_without_exposing_response_body(status, body, error):
    ch, _ = channel([Response(status, body)])
    with pytest.raises(RuntimeError, match=error) as exc:
        await ch.get_group_info("group")
    assert "sensitive detail" not in str(exc.value)


@pytest.mark.asyncio
async def test_rate_budget_survives_cancelled_waiter_and_recovers(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("octop_gateway.constraints.time", SimpleNamespace(monotonic=lambda: clock[0]))
    ch, http = channel([Response(200, "{}") for _ in range(31)])
    for _ in range(30):
        await ch.get_group_info("group")
    task = asyncio.create_task(ch.get_group_info("group"))
    await asyncio.sleep(0)
    assert len(http.calls) == 30
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    clock[0] += 61
    assert await ch.get_group_info("group") == {}
    assert len(http.calls) == 31


@pytest.mark.asyncio
async def test_empty_openid_never_calls_platform():
    ch, http = channel([])
    with pytest.raises(ValueError, match="empty"):
        await ch.get_group_info(" ")
    assert http.calls == []
