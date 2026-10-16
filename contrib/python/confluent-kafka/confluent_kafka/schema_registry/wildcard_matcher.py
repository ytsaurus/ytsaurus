#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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
#

import re
from typing import Tuple


def wildcard_match(text: str, matcher: str) -> bool:
    """
    Matches fully-qualified names that use dot (.) as the name boundary.

    A '?' matches a single character.
    A '*' matches one or more characters within a name boundary.
    A '**' matches one or more characters across name boundaries.

    Args:
        text (str): Text to match.
        matcher (str): The wildcard string to match against.

    Returns:
        bool: True if the text matches the pattern, False otherwise.

    Examples:
        >>> wildcardMatch("eve", "eve*")
        True
        >>> wildcardMatch("alice.bob.eve", "a*.bob.eve")
        True
        >>> wildcardMatch("alice.bob.eve", "a*.bob.e*")
        True
        >>> wildcardMatch("alice.bob.eve", "a*")
        False
        >>> wildcardMatch("alice.bob.eve", "a**")
        True
        >>> wildcardMatch("alice.bob.eve", "alice.bob*")
        False
        >>> wildcardMatch("alice.bob.eve", "alice.bob**")
        True
    """
    rex = _wildcard_to_regexp(matcher, '.')
    pattern = re.compile(rex)
    return pattern.fullmatch(text) is not None


def _wildcard_to_regexp(pattern: str, separator: str) -> str:
    dst = ''
    src = pattern.replace('**' + separator + '*', '**')
    i = 0
    size = len(src)
    while i < size:
        c = src[i]
        i += 1
        if c == '*':
            # One char lookahead for **
            if i < size and src[i] == '*':
                dst += '.*'
                i += 1
            else:
                dst += '[^' + separator + ']*'
        elif c == '?':
            dst += '[^' + separator + ']'
        elif c in ('.', '+', '{', '}', '(', ')', '|', '^', '$'):
            # These need to be escaped in regular expressions
            dst += '\\' + c
        elif c == '\\':
            dst, i = _double_slashes(dst, src, i)
        else:
            dst += c
    return dst


def _double_slashes(dst: str, src: str, i: int) -> Tuple[str, int]:
    # Emit the next character with special interpretation
    dst += '\\'
    if i + 1 < len(src):
        dst += '\\' + src[i]
        i += 1
    else:
        # A backslash at the very end is treated like an escaped backslash
        dst += '\\'
    return dst, i
