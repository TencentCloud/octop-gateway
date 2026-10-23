"""Table rendering and platform outbound paths."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from octop_gateway.channels.qq.channel import QQChannel
from octop_gateway.channels.weixin.channel import WeixinChannel
from octop_gateway.format_tables import convert_markdown_tables, convert_tables_to_plain, has_gfm_table
from octop_gateway.models import ChannelSubject, TextContent

TABLE = "| 名称 | 数值 |\n|:---|---:|\n| CPU | 80% |\n| 内存占用 | 40% |"
PLAIN = "- 名称=CPU | 数值=80%\n- 名称=内存占用 | 数值=40%"


def test_aligned_table_accounts_for_cjk_width():
    result = convert_markdown_tables(TABLE)
    assert result.startswith("```\n")
    lines = result.splitlines()[1:-1]
    assert len(lines) == 4
    assert "名称     | 数值" in lines[0]
    assert "|:---|" not in result
    assert convert_tables_to_plain(TABLE) == PLAIN


@pytest.mark.parametrize("fence", ["```", "~~~", "````"])
@pytest.mark.parametrize("closed", [False, True])
def test_code_fences_preserved(fence, closed):
    text = f"{fence}\n{TABLE}" + (f"\n{fence}" if closed else "")
    assert convert_markdown_tables(text) == text
    assert convert_tables_to_plain(text) == text
    assert not has_gfm_table(text)


def test_escaped_pipes_short_rows_and_non_tables():
    text = "| a | b |\n|---|---|\n| one\\|two |\n| | |"
    assert convert_tables_to_plain(text) == "- a=one|two"
    for text in ("a || b", "single | pipe", "| a | b |\n| | |"):
        assert convert_markdown_tables(text) == text


@pytest.mark.parametrize("content", [False, True])
async def test_qq_outbound_paths_flatten_tables(content):
    ch = QQChannel.__new__(QQChannel)
    ch._resolve_send_routing = lambda _: ("channel", {})
    ch._send_message = AsyncMock()
    target = ChannelSubject(subject_id="chat")
    if content:
        await ch._send_content(target, [TextContent(text=TABLE)])
    else:
        await ch._send_text(target, TABLE)
    assert ch._send_message.call_args.args[1]["content"] == PLAIN


async def test_weixin_outbound_flattens_before_splitting():
    ch = WeixinChannel.__new__(WeixinChannel)
    ch._user_key = lambda _: "user"
    ch._send_lock = lambda _: asyncio.Lock()
    ch._mark_content = AsyncMock()
    ch._dispatch_text = AsyncMock()
    await ch._send_content(ChannelSubject(subject_id="user"), [TextContent(text=TABLE)])
    assert ch._dispatch_text.call_args.args[1] == PLAIN
