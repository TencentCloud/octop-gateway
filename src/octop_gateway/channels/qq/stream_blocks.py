"""Stable Markdown prefixes for QQ C2C replace-mode streaming.

The official stream merge only appends the unsent suffix onto a locked
prefix. A ``\\n`` that lands on that splice is often dropped, which breaks
tables and other block constructs. This module offers the longest prefix
that ends on a complete block so a later frame's suffix does not start
with a stray newline inside a table / fence / list item.

The parser is conservative and monotonic: growing *text* never shrinks
the returned prefix. Incomplete trailing content is omitted until
``finish`` sends the full answer.
"""

from __future__ import annotations

import re

_FENCE_RE = re.compile(r"^( {0,3})(`{3,}|~{3,})(.*)$")
_ATX_RE = re.compile(r"^ {0,3}#{1,6}(?:\s|$)")
_HR_RE = re.compile(r"^ {0,3}(?:(?:\*(?:\s*\*){2,})|(?:-(?:\s*-){2,})|(?:_(?:\s*_){2,}))\s*$")
_LIST_RE = re.compile(r"^ {0,3}(?:[*+-]|\d{1,9}[.)])(?:\s+|$)")
_QUOTE_RE = re.compile(r"^ {0,3}>")
_TABLE_SEP_CELL_RE = re.compile(r"^:?-+:?$")
_INDENT_RE = re.compile(r"^(?: {4,}|\t)")


def stable_markdown_prefix(text: str, *, include_partial_paragraph: bool = False) -> str:
    """Return the longest complete-block prefix of *text*.

    Recognizes ATX headings, fenced code, GFM tables, lists, block quotes,
    thematic breaks, indented code, and paragraphs. A table is held until
    its header, separator, and at least one full data row are present;
    later full rows extend the prefix one row at a time.

    With ``include_partial_paragraph`` a still-open trailing paragraph is
    streamed too: its finished lines are released, and the in-progress last
    line is released as well while it cannot start a block. Replace-mode
    channels need this — a reply that is one long line otherwise stays
    withheld until ``finish``, so the user sees no streaming at all and the
    preview only appears once the whole answer exists.

    The result is monotonic: for growing *text* the returned prefix never
    loses characters it already returned. ``_partial_line_is_safe`` inspects
    only the head of the line for that reason, and an unterminated table is
    released row by row instead of being withheld entirely. Dropping a
    character that a previous frame already carried would make the next
    replace frame not extend the text the platform has accepted, which QQ
    rejects with error 40007.
    """
    if not text:
        return ""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    complete, leftover = _split_complete_lines(normalized)
    end = _stable_line_end(complete, include_partial_paragraph=include_partial_paragraph)
    base = "\n".join(complete[:end]) + "\n" if end > 0 else ""
    if not include_partial_paragraph:
        return base
    if end == len(complete) and _partial_line_is_safe(leftover):
        return base + leftover
    return base


def _split_complete_lines(text: str) -> tuple[list[str], str]:
    if "\n" not in text:
        return [], text
    if text.endswith("\n"):
        return text[:-1].split("\n"), ""
    parts = text.split("\n")
    return parts[:-1], parts[-1]


_LEADING_BLOCK_CHARS = frozenset("|>#`~-_*+0123456789")


def _partial_line_is_safe(line: str) -> bool:
    """Whether an unterminated trailing line may be streamed while it grows.

    Only the head of the line is inspected, so the verdict is stable as the
    line receives more characters: once the head is ordinary prose it stays
    ordinary prose. That monotonicity is the whole point — a verdict that
    could flip back to "unsafe" would make the next frame drop a character
    an earlier frame already sent, and QQ answers those with 40007
    ("already-submitted content cannot be modified").

    A head that could still open a block (table row, fence, heading, list,
    quote, thematic break, indented code, ordered-list digit) is refused
    instead, because releasing it early could commit text that later turns
    out to belong to a block construct.
    """
    if not line or not line.strip():
        return False
    if _INDENT_RE.match(line):
        return False
    head = line.lstrip()[:1]
    return bool(head) and head not in _LEADING_BLOCK_CHARS


def _stable_line_end(lines: list[str], *, include_partial_paragraph: bool = False) -> int:
    index = 0
    stable = 0
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            index += 1
            stable = index
            continue

        fence = _FENCE_RE.match(line)
        if fence:
            closer = _find_fence_close(lines, index, fence)
            if closer is None:
                break
            index = closer + 1
            stable = index
            continue

        if _ATX_RE.match(line):
            index += 1
            stable = index
            continue

        if _is_thematic_break(line):
            index += 1
            stable = index
            continue

        table_end = _try_consume_table(lines, index, allow_partial=include_partial_paragraph)
        if table_end is not None:
            if table_end == 0:
                break
            index = table_end
            stable = index
            continue

        if _LIST_RE.match(line):
            index = _consume_tight_item(lines, index)
            stable = index
            continue

        if _QUOTE_RE.match(line):
            index = _consume_blockquote(lines, index)
            stable = index
            continue

        if _INDENT_RE.match(line):
            index = _consume_indented_code(lines, index)
            stable = index
            continue

        para_end = _consume_paragraph(lines, index)
        if para_end is None:
            # The paragraph is still open. Every line collected so far ends
            # with a newline and cannot change, so it is already stable;
            # ``_consume_paragraph`` also stops before any line that might
            # open a table. Release them when partial streaming was asked for.
            if include_partial_paragraph:
                stable = len(lines)
            break
        index = para_end
        stable = index
    return stable


def _find_fence_close(lines: list[str], start: int, fence: re.Match[str]) -> int | None:
    marker = fence.group(2)[0]
    min_len = len(fence.group(2))
    closer = re.compile(rf"^ {{0,3}}{re.escape(marker)}{{{min_len},}}\s*$")
    for index in range(start + 1, len(lines)):
        if closer.match(lines[index]):
            return index
    return None


def _is_thematic_break(line: str) -> bool:
    if "|" in line:
        return False
    return bool(_HR_RE.match(line))


def _is_table_row(line: str) -> bool:
    stripped = line.strip()
    if "|" not in stripped:
        return False
    if _is_thematic_break(stripped):
        return False
    return stripped.count("|") >= 1 and not _ATX_RE.match(stripped)


def _is_table_separator(line: str) -> bool:
    stripped = line.strip()
    if "|" not in stripped:
        return False
    body = stripped.strip("|")
    cells = [cell.strip() for cell in body.split("|")]
    if not cells or any(not cell for cell in cells):
        return False
    return all(_TABLE_SEP_CELL_RE.match(cell) for cell in cells)


def _try_consume_table(lines: list[str], start: int, *, allow_partial: bool = False) -> int | None:
    """Return exclusive end index, 0 if table started but is incomplete, or None.

    ``allow_partial`` replaces the "started but incomplete" answer with the
    rows seen so far, so a table that is still growing is released one row
    at a time instead of holding the whole prefix back. Replace-mode channels
    only ever resend the full text, so the intermediate one-row-at-a-time
    frames render as a growing table rather than as broken content.
    """
    if not _is_table_row(lines[start]) or _is_table_separator(lines[start]):
        return None
    if start + 1 >= len(lines):
        return start + 1 if allow_partial else 0
    if not _is_table_separator(lines[start + 1]):
        return None
    index = start + 2
    if index >= len(lines) or not _is_table_row(lines[index]) or _is_table_separator(lines[index]):
        return index if allow_partial else 0
    index += 1
    while index < len(lines) and _is_table_row(lines[index]) and not _is_table_separator(lines[index]):
        index += 1
    return index


def _consume_tight_item(lines: list[str], start: int) -> int:
    index = start + 1
    while index < len(lines):
        nxt = lines[index]
        if not nxt.strip():
            break
        if _is_block_start(nxt) and not _INDENT_RE.match(nxt):
            break
        if _LIST_RE.match(nxt):
            break
        if nxt.startswith(" ") or nxt.startswith("\t"):
            index += 1
            continue
        break
    return index


def _consume_blockquote(lines: list[str], start: int) -> int:
    index = start + 1
    while index < len(lines):
        nxt = lines[index]
        if not nxt.strip():
            break
        if _QUOTE_RE.match(nxt) or nxt.startswith("  ") or nxt.startswith("\t"):
            index += 1
            continue
        break
    return index


def _consume_indented_code(lines: list[str], start: int) -> int:
    index = start + 1
    while index < len(lines) and (not lines[index].strip() or _INDENT_RE.match(lines[index])):
        if not lines[index].strip() and index + 1 < len(lines) and not _INDENT_RE.match(lines[index + 1]):
            break
        index += 1
    return index


def _is_block_start(line: str) -> bool:
    if not line.strip():
        return True
    return bool(
        _FENCE_RE.match(line)
        or _ATX_RE.match(line)
        or _is_thematic_break(line)
        or _LIST_RE.match(line)
        or _QUOTE_RE.match(line)
    )


def _consume_paragraph(lines: list[str], start: int) -> int | None:
    index = start + 1
    while index < len(lines) and not _is_block_start(lines[index]):
        if _looks_like_table_open(lines, index):
            break
        index += 1
    if index >= len(lines):
        return None
    if not lines[index].strip():
        return index + 1
    return index


def _looks_like_table_open(lines: list[str], index: int) -> bool:
    if not _is_table_row(lines[index]):
        return False
    if index + 1 < len(lines) and _is_table_separator(lines[index + 1]):
        return True
    return index + 1 >= len(lines)
