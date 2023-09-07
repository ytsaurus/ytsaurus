# coding: utf-8

import re
import math

from . import palette

import six
from library.python import strings

import typing as tp  # noqa


MARKUP_PATTERN = r'\[\[([^\[\]]*?)\]\]'
MARKUP_RE = re.compile(MARKUP_PATTERN)
MARKUP_RE_BIN = re.compile(six.ensure_binary(MARKUP_PATTERN))

SUFFIXES = {0: "b", 1: "K", 2: "M", 3: "G", 4: "T"}


class DryFormatter(object):

    def format_status(self, text):
        return "##status##{}".format(text)

    def format_message(self, text):
        return text

    def footer(self):
        return ''

    def header(self):
        return ''


class Formatter(object):
    def __init__(self, support, show_status=True):
        # type: (BaseSupport, bool) -> None
        self._support = support
        self._show_status = show_status

    def decorate(self, text):
        return transform(text, self._support.highlight, self._support.is_marker, MARKUP_RE)

    def format_status(self, text):
        if not self._show_status:
            return ''
        return self._support.clear() + self._support.format(self.decorate(text))

    def format_message(self, text):
        return self._support.clear() + self._support.format(self.decorate(text))

    def footer(self):
        return self._support.footer()

    def header(self):
        return self._support.header()


class BaseSupport(object):

    def __init__(self, scheme=None, profile=None):
        _profile = palette.DEFAULT_PALETTE.copy()
        _profile.update(profile or {})
        scheme = scheme or palette.EMPTY_SCHEME
        for marker, color in _profile.items():
            if color and color not in scheme:
                raise AssertionError("Color '{}' for '{}' marker is not specified in scheme ({})".format(color, marker, scheme))

        self.scheme = scheme
        self.profile = _profile
        self._markers = self.profile.keys() or [marker for marker in palette.Highlight._MARKERS.values()]

    def header(self):
        return ''

    def footer(self):
        return ''

    def clear(self):
        return ''

    def format(self, txt):
        return txt

    def highlight(self, marker, text):
        if not text:
            return text
        if marker.startswith("c:"):
            if marker[len("c:"):] not in self.scheme:
                return text
            return self.colorize(marker[len("c:"):], text)
        return self.decorate(marker, text)

    def decorate(self, marker, text):
        return text

    def colorize(self, color, text):
        return text

    def is_marker(self, marker):
        if marker in self._markers:
            return True
        if marker.startswith("c:"):
            return True


def with_reset(text):
    return text + "[[{}]]".format(palette.Highlight.RESET)


def tags_iter(iter_regex, text, is_tag):
    # type: (re.Pattern[str], str, tp.Callable[[re.Match[str]], bool]) -> tp.Iterable[tuple[str, bool]]
    text_end = 0

    for match in re.finditer(iter_regex, text):
        if is_tag(match.group(1)):
            yield text[text_end:match.start()], True
            yield match.group(1), False
            text_end = match.end()

    yield text[text_end:len(text)], True


def transform(text, transformer, is_marker, iter_regex):
    # type: (str, tp.Callable[[str, str], str], tp.Callable[[str], bool], re.Pattern[str]) -> str
    substr = ''
    res = []
    marker = ''

    for x, is_text in tags_iter(iter_regex, text, is_marker):
        if is_text:
            substr += x
        else:
            res.append(transformer(marker, substr))
            substr = ''
            marker = x

    res.append(transformer(marker, substr))
    return ''.join(res)


def truncate(data, limit, markers=None):
    if len(data) <= limit:
        return data

    markers = markers or palette.Highlight._MARKERS.values()
    for match in MARKUP_RE.finditer(data):
        if match.end() > limit and match.start() < limit and match.group(1) in markers:
            return data[:match.start()]
    return data[:limit]


# Note: the function cannot be replaced by itertools.pairwise()
# [1, 2, 3] => (1, 2), (2, 3), (3, None)
def _pairwise(iterable):
    iterable = iter(iterable)
    prev = next(iterable, None)
    while prev is not None:
        cur = next(iterable, None)
        yield prev, cur
        prev = cur


class _TaggedFragment(object):
    def __init__(self, tag, start, stop):
        self.tag_name = tag
        self.tag = b"[[" + tag + b"]]" if tag else b""
        self.start = start
        self.stop = stop

    def belongs(self, pos):
        # strictly belongs
        return self.start < pos < self.stop

    def belongs_to_tag(self, pos):
        # strictly belongs
        return self.start < pos < self.start + len(self.tag)

    def __str__(self):
        return "<{}>{}:{}".format(self.tag_name, self.start, self.stop)

    def __repr__(self):
        return str(self)

    @classmethod
    def split(cls, text):
        fragments = []
        tags = list(MARKUP_RE_BIN.finditer(text))
        if tags:
            if tags[0].start():
                fragments.append(_TaggedFragment(b"", 0, tags[0].start()))

            for tag, next_tag in _pairwise(tags):
                if next_tag:
                    tag_end = next_tag.start()
                else:
                    tag_end = len(text)
                fragments.append(_TaggedFragment(tag.group(1), tag.start(), tag_end))
        else:
            fragments.append(_TaggedFragment(b"", 0, len(text)))
        return fragments


def truncate_middle(data, limit, ellipsis="...", message=""):
    data = six.ensure_binary(data)
    if len(data) <= limit:
        return six.ensure_str(data)

    ellipsis = six.ensure_binary(ellipsis)
    message = six.ensure_binary(message)

    limit -= (len(ellipsis) + len(message))
    throw_out_interval_start = limit // 2
    throw_out_interval_stop = len(data) - limit + limit // 2

    truncated_fragment = None
    for fragment, next_fragment in _pairwise(_TaggedFragment.split(data)):
        if (fragment.belongs(throw_out_interval_start) and fragment.tag_name == b"path") or fragment.belongs_to_tag(throw_out_interval_start):
            throw_out_interval_start = fragment.start
        elif fragment.belongs(throw_out_interval_start):
            truncated_fragment = fragment

        if fragment.belongs(throw_out_interval_stop) and fragment.tag_name == b"path":
            if next_fragment:
                throw_out_interval_stop = next_fragment.start
            else:
                throw_out_interval_stop = len(data)
        elif fragment.belongs(throw_out_interval_stop):
            if next_fragment:
                next_fragment_distance = next_fragment.start - throw_out_interval_stop
            else:
                next_fragment_distance = len(data) - throw_out_interval_stop
            if len(fragment.tag) < next_fragment_distance:
                throw_out_interval_stop += len(fragment.tag)
                if not truncated_fragment or truncated_fragment.tag_name != fragment.tag_name:
                    # don't want to append the tag if we have truncated the same type of fragment
                    ellipsis += fragment.tag
            else:
                throw_out_interval_stop += next_fragment_distance

    return strings.fix_utf8(data[:throw_out_interval_start] + ellipsis + data[throw_out_interval_stop:] + message)


def format_size(v, suffixes=None):
    suffixes = suffixes or SUFFIXES
    if not v:
        return "0" + suffixes[0]
    log = int(math.log(v, 2))
    k = log // 10
    v = float(v) / 1024 ** k
    if v >= 100:
        return "{}{}".format(int(v), suffixes[k])
    return "{:.1f}{}".format(v, suffixes[k])
