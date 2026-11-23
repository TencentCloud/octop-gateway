"""Unit tests for QQ stream Markdown block buffering."""

from __future__ import annotations

from octop_gateway.channels.qq.stream_blocks import stable_markdown_prefix


class TestStableMarkdownPrefix:
    def test_empty(self) -> None:
        assert stable_markdown_prefix("") == ""

    def test_incomplete_paragraph_is_held(self) -> None:
        assert stable_markdown_prefix("你好，我是助手") == ""
        assert stable_markdown_prefix("你好，我是助手\n还在写") == ""

    def test_paragraph_closes_on_blank_line(self) -> None:
        assert stable_markdown_prefix("导语一段。\n\n") == "导语一段。\n\n"

    def test_heading_then_incomplete_body(self) -> None:
        text = "## 一、通用能力\n\n还在写"
        assert stable_markdown_prefix(text) == "## 一、通用能力\n\n"

    def test_heading_requires_newline(self) -> None:
        assert stable_markdown_prefix("## 一、通") == ""
        assert stable_markdown_prefix("## 一、通用能力\n") == "## 一、通用能力\n"

    def test_list_items_extend_one_by_one(self) -> None:
        assert stable_markdown_prefix("- **写作") == ""
        first = "- **写作**：文章、报告\n"
        assert stable_markdown_prefix(first) == first
        growing = first + "- **总结"
        assert stable_markdown_prefix(growing) == first
        two = first + "- **总结**：长文要点\n"
        assert stable_markdown_prefix(two) == two

    def test_ordered_list_and_blockquote(self) -> None:
        text = "1. 第一项\n> 引用一行\n"
        assert stable_markdown_prefix(text) == text
        assert stable_markdown_prefix("1. 第一项\n> 引用还在") == "1. 第一项\n"

    def test_table_held_until_first_data_row(self) -> None:
        heading = "## 五、专业模块\n\n"
        assert stable_markdown_prefix(heading + "| 能力 | 说明 |\n") == heading
        assert stable_markdown_prefix(heading + "| 能力 | 说明 |\n| --- | --- |\n") == heading
        row = "| 意图路由 | 判断请求类型 |\n"
        full = heading + "| 能力 | 说明 |\n| --- | --- |\n" + row
        assert stable_markdown_prefix(full) == full

    def test_table_holds_incomplete_last_row(self) -> None:
        base = "| 类别 | 可选能力 |\n| --- | --- |\n| 文档办公 | PowerPoint |\n"
        assert stable_markdown_prefix(base + "| 技术 / 开发") == base
        next_row = base + "| 技术 / 开发 | Docker |\n"
        assert stable_markdown_prefix(next_row) == next_row

    def test_compact_separator_and_alignment(self) -> None:
        text = "| a | b |\n|---|---:|\n| 1 | 2 |\n"
        assert stable_markdown_prefix(text) == text
        text_one_dash = "| a | b |\n|-|-|\n| 1 | 2 |\n"
        assert stable_markdown_prefix(text_one_dash) == text_one_dash

    def test_two_tables_second_held(self) -> None:
        first = "| 能力 | 说明 |\n| --- | --- |\n| 意图路由 | 分派流程 |\n\n"
        second_open = first + "## 六、扩展\n\n| 类别 | 可选能力 |\n| --- | --- |\n"
        assert stable_markdown_prefix(second_open) == first + "## 六、扩展\n\n"

    def test_fenced_code_held_until_close(self) -> None:
        assert stable_markdown_prefix("```python\nprint(1)\n") == ""
        closed = "```python\nprint(1)\n```\n"
        assert stable_markdown_prefix(closed) == closed

    def test_hr_and_tilde_fence(self) -> None:
        text = "上文\n\n***\n\n~~~text\nok\n~~~\n"
        assert stable_markdown_prefix(text) == text

    def test_prefix_is_monotonic(self) -> None:
        chunks = [
            "## 五\n\n",
            "## 五\n\n| 能力 | 说明 |\n",
            "## 五\n\n| 能力 | 说明 |\n| --- | --- |\n",
            "## 五\n\n| 能力 | 说明 |\n| --- | --- |\n| 意图路由 | 一 |\n",
            "## 五\n\n| 能力 | 说明 |\n| --- | --- |\n| 意图路由 | 一 |\n| 问答",
        ]
        seen = ""
        for chunk in chunks:
            prefix = stable_markdown_prefix(chunk)
            assert prefix.startswith(seen)
            seen = prefix
        assert seen.startswith("## 五\n\n| 能力 | 说明 |\n| --- | --- |\n| 意图路由 | 一 |\n")
        assert "| 问答" not in seen


class TestPartialParagraphStreaming:
    """Opt-in mode: an open paragraph streams instead of being held."""

    def test_default_mode_is_unchanged(self) -> None:
        text = "你好，我是助手，很高兴为你服务。"
        assert stable_markdown_prefix(text) == ""
        assert stable_markdown_prefix(text, include_partial_paragraph=False) == ""

    def test_single_line_reply_streams(self) -> None:
        text = "你好，我是助手，很高兴为你服务。"
        assert stable_markdown_prefix(text, include_partial_paragraph=True) == text

    def test_single_line_grows_monotonically(self) -> None:
        text = "你好，我是助手，很高兴为你服务。"
        seen = ""
        for index in range(1, len(text) + 1):
            prefix = stable_markdown_prefix(text[:index], include_partial_paragraph=True)
            assert prefix.startswith(seen), f"prefix shrank at {text[:index]!r}"
            seen = prefix
        assert seen == text

    def test_open_paragraph_releases_finished_lines(self) -> None:
        text = "第一行\n第二行还在写"
        assert stable_markdown_prefix(text, include_partial_paragraph=True) == text

    def test_final_paragraph_after_closed_block(self) -> None:
        text = "## 标题\n\n正文还在写"
        assert stable_markdown_prefix(text, include_partial_paragraph=True) == text

    def test_block_heads_are_still_withheld(self) -> None:
        for text in (
            "```python\nprint(1)\n",
            "## 一、通",
            "- **写作",
            "> 引用还在",
            "| 能力 |",
            "    缩进代码",
        ):
            assert stable_markdown_prefix(text, include_partial_paragraph=True) == "", text

    def test_growing_table_releases_row_by_row_monotonically(self) -> None:
        chunks = [
            "你好|",
            "你好|\n---|",
            "你好|\n---|\n| 1",
            "你好|\n---|\n| 1 |\n",
        ]
        seen = ""
        for chunk in chunks:
            prefix = stable_markdown_prefix(chunk, include_partial_paragraph=True)
            assert prefix.startswith(seen), f"{chunk!r} shrank the prefix"
            seen = prefix
        assert seen == "你好|\n---|\n| 1 |\n"

    def test_prose_never_shrinks_when_a_pipe_appears(self) -> None:
        chunks = [
            "你好",
            "你好，我是",
            "你好，我是助手",
            "你好，我是助手 |",
            "你好，我是助手 | 后面还有",
            "你好，我是助手 | 后面还有\n换行了",
        ]
        seen = ""
        for chunk in chunks:
            prefix = stable_markdown_prefix(chunk, include_partial_paragraph=True)
            assert prefix.startswith(seen), f"{chunk!r} shrank the prefix"
            seen = prefix
        assert seen == chunks[-1]

    def test_closed_table_is_released_in_full(self) -> None:
        text = "| a | b |\n| --- | --- |\n| 1 | 2 |\n"
        assert stable_markdown_prefix(text, include_partial_paragraph=True) == text
