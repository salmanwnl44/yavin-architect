"""Stage 2: a source's bytes into segments with deterministic ids and human locators.

Derived and rebuildable from the object store, not ledger data. Segment ids are a hash of
(source_id, locator), so re-parsing the same bytes gives the same ids.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

MEDIA_PDF = "application/pdf"
MEDIA_MARKDOWN = "text/markdown"
MEDIA_PLAIN = "text/plain"
MEDIA_PYTHON = "text/x-python"
MEDIA_REPO = "application/vnd.yavin.repo-manifest+json"

CODE_WINDOW_LINES = 60


@dataclass(frozen=True)
class Segment:
    segment_id: str
    locator: str
    kind: str  # statement | table | code
    text: str
    position: int


def segment_id(source_id: str, locator: str) -> str:
    return hashlib.sha256(f"{source_id}|{locator}".encode()).hexdigest()[:32]


def _make(source_id: str, position: int, locator: str, kind: str, text: str) -> Segment:
    return Segment(segment_id(source_id, locator), locator, kind, text, position)


def heading_slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "section"


# ---------------------------------------------------------------- plain text and markdown
def parse_plain(source_id: str, text: str, prefix: str = "") -> list[Segment]:
    segments: list[Segment] = []
    for n, paragraph in enumerate(re.split(r"\n\s*\n", text.strip()), start=1):
        if paragraph.strip():
            segments.append(
                _make(source_id, len(segments), f"{prefix}¶{n}", "statement", paragraph.strip())
            )
    return segments


def parse_markdown(source_id: str, text: str, path: str) -> list[Segment]:
    """One segment per heading section: '<path>#<heading-slug> L<a>-L<b>'."""
    lines = text.splitlines()
    sections: list[tuple[str, int]] = []  # (slug, first line index)
    for i, line in enumerate(lines):
        if re.match(r"^#{1,6}\s+\S", line):
            sections.append((heading_slug(line.lstrip("#").strip()), i))
    if not sections or sections[0][1] > 0:
        sections.insert(0, ("_", 0))
    segments: list[Segment] = []
    seen: dict[str, int] = {}
    for n, (slug, start) in enumerate(sections):
        end = sections[n + 1][1] if n + 1 < len(sections) else len(lines)
        body = "\n".join(lines[start:end]).strip()
        if not body:
            continue
        seen[slug] = seen.get(slug, 0) + 1
        unique = slug if seen[slug] == 1 else f"{slug}-{seen[slug]}"
        locator = f"{path}#{unique} L{start + 1}-L{end}"
        segments.append(_make(source_id, len(segments), locator, "statement", body))
    return segments


# ---------------------------------------------------------------- code
def parse_python(source_id: str, text: str, path: str) -> list[Segment]:
    """One segment per top-level function or class (with its docstring):
    '<path>:<symbol> L<a>-L<b>'; the module docstring, when there is one, as '<path>:<module>'."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return parse_code(source_id, text, path)
    lines = text.splitlines()
    segments: list[Segment] = []
    doc = ast.get_docstring(tree)
    if doc:
        first = tree.body[0]
        locator = f"{path}:<module> L{first.lineno}-L{first.end_lineno}"
        segments.append(
            _make(
                source_id, 0, locator, "code", "\n".join(lines[first.lineno - 1 : first.end_lineno])
            )
        )
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start = min([node.lineno, *(d.lineno for d in node.decorator_list)])
            locator = f"{path}:{node.name} L{start}-L{node.end_lineno}"
            body = "\n".join(lines[start - 1 : node.end_lineno])
            segments.append(_make(source_id, len(segments), locator, "code", body))
    return segments or parse_code(source_id, text, path)


def parse_code(source_id: str, text: str, path: str) -> list[Segment]:
    """Fixed-size line windows: '<path> L<a>-L<b>'."""
    lines = text.splitlines()
    segments: list[Segment] = []
    for start in range(0, len(lines), CODE_WINDOW_LINES):
        end = min(start + CODE_WINDOW_LINES, len(lines))
        body = "\n".join(lines[start:end]).strip()
        if body:
            segments.append(
                _make(source_id, len(segments), f"{path} L{start + 1}-L{end}", "code", body)
            )
    return segments


# ---------------------------------------------------------------- pdf
def parse_pdf(source_id: str, data: bytes) -> list[Segment]:
    """Paragraphs 'p.<page> ¶<n>' and, best effort, table rows 'p.<page> table <t> row <r>'."""
    import pymupdf  # heavy; imported only when a PDF is parsed

    segments: list[Segment] = []
    with pymupdf.open(stream=data, filetype="pdf") as document:
        for page_number, page in enumerate(document, start=1):
            table_boxes: list[Any] = []
            tables = []
            try:
                tables = list(page.find_tables().tables)
            except Exception:  # noqa: BLE001 - table detection is best effort
                tables = []
            for t, table in enumerate(tables, start=1):
                table_boxes.append(pymupdf.Rect(table.bbox))
                rows = table.extract()
                header = [" ".join(str(cell or "").split()) for cell in (rows[0] if rows else [])]
                for r, row in enumerate(rows[1:] if len(rows) > 1 else rows, start=1):
                    cells = [" ".join(str(cell or "").split()) for cell in row]
                    record = {
                        header[i] if i < len(header) and header[i] else f"col{i + 1}": _cell(
                            cells[i]
                        )
                        for i in range(len(cells))
                    }
                    locator = f"p.{page_number} table {t} row {r}"
                    segments.append(
                        _make(
                            source_id,
                            len(segments),
                            locator,
                            "table",
                            json.dumps(record, ensure_ascii=False),
                        )
                    )
            paragraph = 0
            for block in page.get_text("blocks"):
                x0, y0, x1, y1, text = block[0], block[1], block[2], block[3], block[4]
                if not str(text).strip() or len(block) > 6 and block[6] != 0:
                    continue
                rect = pymupdf.Rect(x0, y0, x1, y1)
                if any(rect.intersects(box) for box in table_boxes):
                    continue
                paragraph += 1
                locator = f"p.{page_number} ¶{paragraph}"
                segments.append(
                    _make(source_id, len(segments), locator, "statement", str(text).strip())
                )
    return segments


def _cell(text: str) -> Any:
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return text


# ---------------------------------------------------------------- dispatch
def parse_file(source_id: str, data: bytes, media_type: str, path: str = "") -> list[Segment]:
    if media_type == MEDIA_PDF:
        return parse_pdf(source_id, data)
    text = data.decode("utf-8", errors="replace")
    if media_type == MEDIA_MARKDOWN:
        return parse_markdown(source_id, text, path or "document.md")
    if media_type == MEDIA_PYTHON:
        return parse_python(source_id, text, path or "module.py")
    if media_type == MEDIA_PLAIN:
        return parse_plain(source_id, text, f"{path} " if path else "")
    return parse_code(source_id, text, path or "file")


def media_type_for(path: str) -> str:
    lower = path.lower()
    if lower.endswith(".pdf"):
        return MEDIA_PDF
    if lower.endswith((".md", ".markdown")):
        return MEDIA_MARKDOWN
    if lower.endswith(".py"):
        return MEDIA_PYTHON
    if lower.endswith((".txt", ".rst", ".text")) or "." not in lower.rsplit("/", 1)[-1]:
        return MEDIA_PLAIN
    return "text/x-code"
