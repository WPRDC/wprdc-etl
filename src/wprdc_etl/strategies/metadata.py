"""Turn an ArcGIS Hub catalogue entry into CKAN package metadata.

An ArcGIS Hub `data.json` entry carries the authoritative description and
keywords for a layer. Both need reshaping before CKAN can use them:

  * `description` is HTML — and Hub descriptions are pasted out of Word, so
    they arrive wrapped in styled `<span>`/`<font>` noise. CKAN renders
    `notes` as Markdown, so the HTML is converted rather than passed through
    (a raw `style='font-family:&quot;Tahoma&quot;'` attribute renders as
    literal text).
  * `keyword` is free-text and inconsistently cased across a catalogue
    (`Environment` and `environment` both occur), so tags are normalised.

Everything here is pure: no network, no CKAN. `CkanResource` does the writing.
"""

from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser

# Tags CKAN would reject. Its default validator allows alphanumerics, spaces,
# hyphens, underscores and dots, between 2 and 100 characters.
_TAG_STRIP = re.compile(r"[^a-z0-9 ._-]+")
_WS = re.compile(r"[ \t]+")
_BLANKS = re.compile(r"\n{3,}")

# Inline emphasis. `font` and `span` are unwrapped: they only ever carry
# styling in these catalogues.
_BOLD = {"b", "strong"}
_ITALIC = {"i", "em"}
_BLOCK = {"p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "table"}
_HEADING = {"h1": "# ", "h2": "## ", "h3": "### ", "h4": "#### "}


class _Markdownifier(HTMLParser):
    """Minimal HTML -> Markdown for catalogue descriptions.

    Deliberately small. It handles exactly the vocabulary these catalogues
    use — p, div, span, font, b, strong, i, em, a, br, ul, ol, li, h1-h6 —
    and drops everything else to its text. A full HTML-to-Markdown library
    would be a new dependency for this one job.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._href: str | None = None
        self._link_text: list[str] = []
        self._list_depth = 0
        self._emph_depth = 0

    # -- helpers ----------------------------------------------------------
    def _emit(self, text: str) -> None:
        # A newline inside **bold** breaks the emphasis across lines, and if
        # the next line starts with "- " Markdown renders a stray list item.
        # These descriptions are hard-wrapped Word text, so newlines inside an
        # emphasis run carry no meaning — space is the faithful reading.
        if self._emph_depth and "\n" in text:
            text = text.replace("\n", " ")
        if self._href is not None:
            self._link_text.append(text)
        else:
            self.parts.append(text)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "br":
            self._emit("\n")
        elif tag in _BLOCK:
            self._emit("\n\n")
            if tag in _HEADING:
                self._emit(_HEADING[tag])
        elif tag in _BOLD:
            self._emit("**")
            self._emph_depth += 1
        elif tag in _ITALIC:
            self._emit("*")
            self._emph_depth += 1
        elif tag in ("ul", "ol"):
            self._list_depth += 1
            self._emit("\n\n")
        elif tag == "li":
            self._emit("\n" + "  " * (self._list_depth - 1) + "- ")
        elif tag == "a":
            # Nested links don't occur here; if one did, the outer wins.
            if self._href is None:
                self._href = dict(attrs).get("href") or ""
                self._link_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag in _BOLD:
            self._emph_depth = max(0, self._emph_depth - 1)
            self._emit("**")
        elif tag in _ITALIC:
            self._emph_depth = max(0, self._emph_depth - 1)
            self._emit("*")
        elif tag in ("ul", "ol"):
            self._list_depth = max(0, self._list_depth - 1)
            self._emit("\n\n")
        elif tag in _BLOCK:
            self._emit("\n\n")
        elif tag == "a" and self._href is not None:
            text = _WS.sub(" ", "".join(self._link_text)).strip()
            href, self._href = self._href, None
            self._link_text = []
            # A bare link with no text, or a link whose text IS the url, reads
            # better as the url alone than as [url](url).
            if not text:
                self.parts.append(href)
            elif text == href:
                self.parts.append(href)
            else:
                self.parts.append(f"[{text}]({href})" if href else text)

    def handle_data(self, data: str) -> None:
        self._emit(data)


def html_to_markdown(html: str | None) -> str:
    """Convert a catalogue description to Markdown. Empty in, empty out."""
    if not html:
        return ""
    if "<" not in html:
        # Already plain text — normalise whitespace and entities only.
        return _BLANKS.sub("\n\n", _WS.sub(" ", unescape(html))).strip()

    parser = _Markdownifier()
    parser.feed(html)
    parser.close()
    text = "".join(parser.parts)
    # Tidy: collapse runs of spaces, strip trailing space on each line, and
    # cap blank runs at one so paragraphs stay paragraphs.
    text = _WS.sub(" ", text)
    text = "\n".join(line.strip() for line in text.split("\n"))
    return _BLANKS.sub("\n\n", text).strip()


def package_description(
    entry: dict,
    suffix: str | None = None,
    *,
    override: str | None = None,
    fallback: str = "",
) -> str:
    """The CKAN `notes` for a catalogue entry, plus our own Markdown.

    `override` is `ckan.description` — Markdown that REPLACES the publisher's
    text rather than being converted from it. It is already Markdown, so it
    does not go through html_to_markdown.

    `suffix` is `ckan.description_suffix`, appended after a blank line so a
    heading in it starts cleanly. It applies either way, so an override and a
    suffix compose.
    """
    body = (override or "").strip() or html_to_markdown(entry.get("description"))
    body = body or fallback
    extra = (suffix or "").strip()
    if not extra:
        return body
    return f"{body}\n\n{extra}".strip() if body else extra


def normalise_tag(raw: str) -> str | None:
    """One CKAN-safe tag, or None when nothing usable survives."""
    tag = _TAG_STRIP.sub(" ", unescape(raw).strip().lower())
    tag = _WS.sub(" ", tag).strip(" .-_")
    return tag if 2 <= len(tag) <= 100 else None


def package_tags(entry: dict, extra: list[str] | None = None) -> list[str]:
    """Tags for a catalogue entry: its `keyword` list plus any we add.

    Lower-cased because a single catalogue carries both `Environment` and
    `environment`, which CKAN would otherwise keep as two tags. Order is
    preserved and duplicates dropped so the result is stable across runs —
    a churning tag list would make every sync look like a change.
    """
    out: list[str] = []
    for raw in [*(entry.get("keyword") or []), *(extra or [])]:
        tag = normalise_tag(raw)
        if tag and tag not in out:
            out.append(tag)
    return out
