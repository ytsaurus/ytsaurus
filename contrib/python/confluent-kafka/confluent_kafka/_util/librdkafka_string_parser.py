# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared ``key=value`` string parser with librdkafka-faithful semantics.

Logically equivalent to librdkafka's ``rd_string_split`` (``src/rdstring.c``)
plus ``rd_kafka_conf_kv_split`` (``src/rdkafka_conf.c``), so OAUTHBEARER
config / extension strings tokenize identically to the native client — most
importantly an ``identityPoolId`` that is itself a comma-separated list whose
commas are backslash-quoted.

Semantics (mirroring ``rd_string_split``):

* Fields are separated by a single ``sep`` character.
* ``\\`` escapes the next character: ``\\t`` / ``\\n`` / ``\\r`` / ``\\0`` map to
  TAB / LF / CR / NUL; any other escaped character (including an escaped
  separator or ``\\\\``) is kept literally. A dangling trailing ``\\`` is dropped.
* Leading whitespace is stripped only when *unescaped*; trailing whitespace is
  stripped *unconditionally* (even when it was escaped) — an asymmetry copied
  verbatim from librdkafka. "Whitespace" is the ASCII set (C ``isspace``):
  space, ``\\t``, ``\\n``, ``\\v``, ``\\f``, ``\\r`` — deliberately not Unicode.
* Empty fields are skipped when ``skip_empty`` is set.
* ``key=value`` splits on the FIRST ``=`` (the value may contain further ``=``);
  a field with no ``=`` or an empty key is an error.

This is the cross-language port of .NET's ``LibrdkafkaStringParser``; its tests
(``tests/_util/test_librdkafka_string_parser.py``) port librdkafka's own
``ut_string_split`` vectors verbatim to lock parity.
"""

from typing import List, Optional, Tuple

__all__ = ["split", "parse_key_values"]


def _is_ascii_space(c: str) -> bool:
    """ASCII ``isspace``: space, ``\\t``(0x09)..``\\r``(0x0D).

    Matches librdkafka's C ``isspace`` behaviour; intentionally NOT
    :meth:`str.isspace`, which also matches Unicode whitespace (e.g. U+00A0)
    and would diverge from the native client.
    """
    return c == " " or ("\t" <= c <= "\r")


def split(raw: str, sep: str, skip_empty: bool) -> List[str]:
    """Split ``raw`` into fields on ``sep``, applying librdkafka's ``\\``-escaping
    and whitespace trimming. Logically equivalent to ``rd_string_split``.

    :param raw: The input string to tokenize.
    :param sep: The single field-separator character (only its first character
        is used). ``','`` for comma-separated values, etc.
    :param skip_empty: When true, empty fields (consecutive separators, or
        whitespace-only fields) are omitted from the result.
    :raises TypeError: ``raw`` is ``None``.
    """
    if raw is None:
        raise TypeError("raw must not be None")

    # rd_string_split takes a single separator character.
    sep_char = sep[0] if sep else ""

    fields: List[str] = []
    field: List[str] = []
    next_esc = False
    n = len(raw)
    idx = 0

    while True:
        at_end = idx >= n
        is_esc = next_esc

        if not at_end:
            c = raw[idx]

            # An unescaped backslash is consumed and escapes the next char.
            if not is_esc and c == "\\":
                next_esc = True
                idx += 1
                continue

            next_esc = False

            # Strip leading whitespace (only when unescaped).
            if not is_esc and not field and _is_ascii_space(c):
                idx += 1
                continue

            # Content char: any escaped char, or any non-separator char.
            if is_esc or c != sep_char:
                if is_esc:
                    # Common escape substitutions; an unknown escape (e.g. an
                    # escaped separator or "\\") keeps the character as-is.
                    if c == "t":
                        c = "\t"
                    elif c == "n":
                        c = "\n"
                    elif c == "r":
                        c = "\r"
                    elif c == "0":
                        c = "\0"
                field.append(c)
                idx += 1
                continue

            # Otherwise c is an unescaped separator: fall through to finish
            # the current field.

        # Finish the current field (reached on a separator or end-of-input).
        while field and _is_ascii_space(field[-1]):
            field.pop()  # strip trailing whitespace (unconditional)

        if not field and skip_empty:
            if at_end:
                break
            idx += 1  # advance past the separator
            continue

        fields.append("".join(field))
        field = []

        if at_end:
            break
        idx += 1  # advance past the separator

    return fields


def parse_key_values(
    raw: str,
    sep: str,
    context_label: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """Split ``raw`` via :func:`split` (skipping empty fields) and parse each
    field into a ``(key, value)`` pair on its first ``=``. Logically equivalent
    to applying librdkafka's ``rd_kafka_conf_kv_split`` over every field.

    :param raw: The raw ``key=value`` string to parse.
    :param sep: The single field-separator character (e.g. ``','``).
    :param context_label: Label woven into the error message to identify which
        config a malformed entry came from. When ``None``, the message falls
        back to a generic ``key=value`` phrasing.
    :raises TypeError: ``raw`` is ``None``.
    :raises ValueError: a field has no ``=``, or has an empty key.
    """
    pairs: List[Tuple[str, str]] = []
    for field in split(raw, sep, skip_empty=True):
        # Split on the FIRST '='. eq <= 0 means no '=' or an empty key.
        eq = field.find("=")
        if eq <= 0:
            where = f" in {context_label}" if context_label else ""
            raise ValueError(f"Malformed entry '{field}'{where} (expected key=value).")
        pairs.append((field[:eq], field[eq + 1 :]))
    return pairs
