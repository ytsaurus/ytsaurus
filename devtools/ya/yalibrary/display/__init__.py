# coding: utf-8

from __future__ import print_function
import codecs
import threading

import exts.strings
import exts.windows
import yalibrary.formatter

import six

__all__ = ['CompositeDisplay', 'Display']


class CompositeDisplay(object):
    def __init__(self, *displays):
        self._displays = list(displays)

    def emit_message(self, *args, **kwargs):
        for display in self._displays:
            display.emit_message(*args, **kwargs)

    def emit_status(self, *args, **kwargs):
        for display in self._displays:
            display.emit_status(*args, **kwargs)

    def close(self):
        for display in self._displays:
            display.close()


class Display(object):
    def __init__(self, stream, formatter, text_encoding=None):
        if exts.windows.on_win():
            import colorama
            colorama.init(wrap=False)
            stream = colorama.AnsiToWin32(stream).stream

        self._text_encoding = text_encoding or exts.strings.DEFAULT_ENCODING
        if hasattr(stream, 'encoding'):
            self._stream_encoding = exts.strings.get_stream_encoding(stream)
        else:
            self._stream_encoding = exts.strings.DEFAULT_ENCODING
        if six.PY2:
            writer = codecs.getwriter(self._stream_encoding)
            self._stream = writer(stream, errors=exts.strings.ENCODING_ERRORS_POLICY)
        else:
            self._stream = stream
        self._formatter = formatter
        self._lock = threading.Lock()

        self._write(self._formatter.header())
        self._closed = False

    def close(self):
        self.emit_status('')  # clear status

        self._write(self._formatter.footer())
        self._closed = True

    def _write(self, text):
        if six.PY2:
            text = exts.strings.to_unicode(text, self._text_encoding)

        with self._lock:
            self._stream.write(text)
            self._stream.flush()

    def emit_status(self, *parts):
        if self._closed:
            return

        str = ''.join(parts)
        self._write(self._formatter.format_status(str))

    def emit_message(self, msg=''):
        if self._closed:
            return

        if len(msg) == 0 or msg[-1] != '\n':
            msg += '\n'

        self._write(self._formatter.format_message(msg))


class DevNullDisplay(object):
    def emit_status(self, *args, **kwargs):
        pass

    def emit_message(self, *args, **kwargs):
        pass


def strip_markup(txt):
    return yalibrary.formatter.Formatter(yalibrary.formatter.PlainSupport()).format_message(txt)


def build_term_display(stream, tty):
    import yalibrary.display as yadisplay
    import yalibrary.formatter as yaformatter

    formatter = yaformatter.new_formatter(is_tty=tty)
    return yadisplay.Display(stream, formatter)


if __name__ == '__main__':
    import sys
    import time
    import resource

    d = Display(sys.stdout, yalibrary.formatter.Formatter(yalibrary.formatter.HtmlSupport()))
    d.emit_status('[[imp]]123')
    d.emit_status('[[alt1]]124')
    d.emit_status('[[alt2]]125')
    d.emit_message('[[imp]]abra1')
    d.emit_status('[[alt3]]126')
    d.emit_message('[[unknown]]abra2')

    # 20 MiB
    data = "[[bad]]123[[rst]]312" * 1024 * 1024
    refs = []
    for support in [
        yalibrary.formatter.TermSupport(),
        yalibrary.formatter.HtmlSupport(),
        yalibrary.formatter.PlainSupport()
    ]:
        s, m = time.time(), resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        refs.append(yalibrary.formatter.Formatter(support).format_message(data))
        print("{:13} time:{:0.3f} len:{:<8} mem used: {}".format(
            support.__class__.__name__,
            time.time() - s,
            len(refs[-1]),
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - m))

    # 20 MiB
    data = "\x1b[31;1m123\x1b[0m312123" * 1024 * 1024
    s, m = time.time(), resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    d = yalibrary.formatter.ansi_codes_to_markup(data)
    print("ansi_codes_to_markup time:{:0.3f} len:{:<8} mem used: {}".format(
        time.time() - s,
        len(d),
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - m))
