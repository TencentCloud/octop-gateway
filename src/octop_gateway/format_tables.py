"""Render Markdown tables for channels without native GFM table support."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable

_ROW_SPLIT = re.compile(r"(?<!\\)\|")
_SEPARATOR = re.compile(r"^:?-{2,}:?$")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


def _display_width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(char) in "WF" else 1 for char in text)


def _pad(text: str, width: int, align: str) -> str:
    gap = max(0, width - _display_width(text))
    if align == "right":
        return " " * gap + text
    if align == "center":
        return " " * (gap // 2) + text + " " * (gap - gap // 2)
    return text + " " * gap


def _split_row(line: str) -> list[str]:
    return [cell.replace(r"\|", "|").strip() for cell in _ROW_SPLIT.split(line.strip()[1:-1])]


def _render_aligned(rows: list[list[str]]) -> str:
    header, separator = rows[:2]
    columns = len(header)
    alignments = [
        "center" if cell.startswith(":") and cell.endswith(":") else "right" if cell.endswith(":") else "left"
        for cell in separator
    ]
    body = [(row + [""] * columns)[:columns] for row in [header, *rows[2:]]]
    widths = [max(_display_width(row[col]) for row in body) for col in range(columns)]
    lines = []
    for index, row in enumerate(body):
        lines.append(" | ".join(_pad(cell, widths[col], alignments[col]) for col, cell in enumerate(row)).rstrip())
        if index == 0:
            lines.append("-+-".join("-" * width for width in widths))
    return "```\n" + "\n".join(lines) + "\n```"


def _render_plain(rows: list[list[str]]) -> str:
    header = rows[0]
    lines = []
    for row in rows[2:]:
        fields = [f"{name}={value}" for name, value in zip(header, row, strict=False) if value]
        if fields:
            lines.append("- " + " | ".join(fields))
    return "\n".join(lines)


def convert_markdown_tables(text: str, *, renderer: Callable[[list[list[str]]], str] = _render_aligned) -> str:
    """Render pipe-delimited tables, preserving complete and open code fences."""
    if "|" not in text:
        return text
    lines = text.split("\n")
    result: list[str] = []
    fence = ""
    index = 0
    while index < len(lines):
        line = lines[index]
        match = _FENCE.match(line)
        if match:
            marker, suffix = match.groups()
            if not fence:
                fence = marker
            elif marker[0] == fence[0] and len(marker) >= len(fence) and not suffix.strip():
                fence = ""
            result.append(line)
            index += 1
            continue
        if fence or not (line.strip().startswith("|") and line.strip().endswith("|")):
            result.append(line)
            index += 1
            continue
        start = index
        rows = []
        while index < len(lines) and lines[index].strip().startswith("|") and lines[index].strip().endswith("|"):
            rows.append(_split_row(lines[index]))
            index += 1
        if (
            len(rows) >= 2
            and len(rows[0]) >= 2
            and len(rows[1]) == len(rows[0])
            and all(_SEPARATOR.fullmatch(cell) for cell in rows[1])
        ):
            result.append(renderer(rows))
        else:
            result.extend(lines[start:index])
    return "\n".join(result)


def convert_tables_to_plain(text: str) -> str:
    """Flatten tables to keyed list rows readable in proportional fonts."""
    return convert_markdown_tables(text, renderer=_render_plain)


def has_gfm_table(text: str) -> bool:
    """Return whether text contains a supported table outside code fences."""
    return convert_markdown_tables(text, renderer=lambda rows: "") != text
