# coding: utf-8

from six.moves import urllib as urllib

import six

if six.PY2:
    from cgi import escape
elif six.PY34:
    from html import escape

from . import palette
from . import formatter


class HtmlSupport(formatter.BaseSupport):
    def __init__(self, scheme=None, profile=None):
        _scheme = palette.DEFAULT_HTML_SCHEME.copy()
        _scheme.update(scheme or {})
        super(HtmlSupport, self).__init__(_scheme, profile)

    def header(self):
        return '<pre style="white-space:pre-wrap;overflow: auto;width:auto;"><code>'

    def footer(self):
        return '</code></pre>\n'

    def decorate(self, marker, text):
        color = self.profile.get(marker)
        if marker == palette.Highlight.PATH:
            return '<a href="%s" target="_blank" style="%s">%s</a>' % (quote_url(text), self.scheme[color], escape(text))
        elif color:
            return '<span style="%s">%s</span>' % (self.scheme[color], escape(text))
        return escape(text)

    def colorize(self, color, text):
        return '<span style="%s">%s</span>' % (self.scheme[color], escape(text))


def quote_url(path):
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(path)
    fullpath = path.lstrip('/')
    if qs:
        fullpath += "&" + qs
    if anchor:
        fullpath += "#" + anchor
    fullpath = urllib.parse.quote(six.ensure_str(fullpath), '/')
    # treat # and & as a part of the path
    return urllib.parse.urlunsplit((scheme, netloc, fullpath, '', ''))
