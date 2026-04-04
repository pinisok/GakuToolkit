"""ADV text encoding utilities.

Pure functions for encoding text fields before CSV export:
- Escape sequences (newline, carriage return, tilde)
- Dot normalization (... → …, .... → ……)
- <em> tag word splitting
"""

import re

from .helper import REGEX_DOTS_4_TO_6, REGEX_DOTS_3


def _encode(string: str) -> str:
    """Encode a text string for CSV export.

    Escapes newlines/CR, converts ~ to fullwidth, normalizes dots to ellipsis.
    """
    string = string.replace("\n", "\\n").replace("\r", "\\r").replace("~", "～")
    string = REGEX_DOTS_4_TO_6.sub('……', string)
    string = REGEX_DOTS_3.sub('…', string)
    return string


START_EM_LENGTH = 4
END_EM_LENGTH = 5


def _processEMtag(string: str) -> str:
    """Split multi-word <em> tags into per-word <em> tags.

    Example: <em>hello world</em> → <em>hello</em> <em>world</em>
    """
    if len(string) < 1:
        return string
    start_idx = string.find("<em>")
    if start_idx == -1:
        return string
    end_idx = string[start_idx:].find("</em>")
    if end_idx == -1:
        return string
    result = string[start_idx + START_EM_LENGTH:start_idx + end_idx].replace(" ", "</em> <em>")
    return (
        string[:start_idx]
        + "<em>" + result + "</em>"
        + _processEMtag(string[start_idx + end_idx + END_EM_LENGTH:])
    )
