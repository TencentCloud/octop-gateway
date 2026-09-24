"""Convert standard Markdown to Telegram-compatible HTML."""

from __future__ import annotations

import re


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def markdown_to_telegram_html(text: str) -> str:
    """Convert Markdown text to Telegram Bot API HTML subset."""
    if not text:
        return text

    placeholders: list[str] = []

    def _ph(html_fragment: str) -> str:
        idx = len(placeholders)
        placeholders.append(html_fragment)
        return f"\x00PH{idx}\x00"

    def _code_block(m: re.Match[str]) -> str:
        lang = (m.group(1) or "").strip()
        code = _escape_html(m.group(2))
        if lang:
            return _ph(f'<pre><code class="language-{_escape_html(lang)}">{code}</code></pre>')
        return _ph(f"<pre>{code}</pre>")

    text = re.sub(r"```(\w*)\n?(.*?)```", _code_block, text, flags=re.DOTALL)
    text = re.sub(r"`([^`\n]+)`", lambda m: _ph(f"<code>{_escape_html(m.group(1))}</code>"), text)
    text = re.sub(
        r"\[([^\]]+)\]\(([^)]+)\)",
        lambda m: _ph(f'<a href="{m.group(2).replace("<", "%3C").replace(">", "%3E")}">{_escape_html(m.group(1))}</a>'),
        text,
    )

    text = _escape_html(text)
    text = re.sub(r"^[\*\-_]{3,}\s*$", "———", text, flags=re.MULTILINE)
    text = re.sub(r"^#{1,6}\s+(.+?)$", r"<b>\1</b>", text, flags=re.MULTILINE)

    lines = text.split("\n")
    result_lines: list[str] = []
    quote_buf: list[str] = []

    def _flush_quote() -> None:
        if quote_buf:
            result_lines.append(f"<blockquote>{chr(10).join(quote_buf)}</blockquote>")
            quote_buf.clear()

    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("&gt; "):
            quote_buf.append(stripped[5:])
        elif stripped == "&gt;":
            quote_buf.append("")
        else:
            _flush_quote()
            result_lines.append(line)
    _flush_quote()
    text = "\n".join(result_lines)

    text = re.sub(r"^(\s*)[\*\-]\s+", r"\1• ", text, flags=re.MULTILINE)
    text = re.sub(r"\|\|(.+?)\|\|", r"<tg-spoiler>\1</tg-spoiler>", text)
    text = re.sub(r"\*{3}(.+?)\*{3}", r"<b><i>\1</i></b>", text)
    text = re.sub(r"\*{2}(.+?)\*{2}", r"<b>\1</b>", text)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\w)\*(.+?)\*(?!\w)", r"<i>\1</i>", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"<i>\1</i>", text)
    text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text)

    for idx, content in enumerate(placeholders):
        text = text.replace(f"\x00PH{idx}\x00", content)
    return text
