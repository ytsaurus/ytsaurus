# coding: utf-8

import threading

from . import palette
from . import formatter
from exts import func


CLEAR_TILL_END = '\033[K'


class TermSupport(formatter.BaseSupport):

    def __init__(self, scheme=None, profile=None):
        _scheme = palette.DEFAULT_TERM_SCHEME.copy()
        _scheme.update(scheme or {})
        super(TermSupport, self).__init__(_scheme, profile)
        self._skip_markers = [palette.Highlight.PATH]
        self._cur_marker = self.profile[palette.Highlight.RESET]
        self._lock = threading.Lock()

    @func.memoize()
    def get_color_code(self, name):
        import color
        return color.get_code_by_spec(name)

    @staticmethod
    def escape(txt):
        # fix for tmux <= 2.0ver
        # replace SO (Shift Out) symbol to avoid the corruption of the console.
        # safe for utf-8 - SO cannot be part of code point bytes, except itself
        return txt.replace("\x0e", " ")

    def format(self, txt):
        return TermSupport.escape(txt)

    def clear(self):
        return '\r' + CLEAR_TILL_END

    def decorate(self, marker, text):
        with self._lock:
            # do not change current highlight color for skip markers
            if marker not in self._skip_markers:
                self._cur_marker = marker
            if self.profile.get(self._cur_marker):
                return self.get_color_code(self.profile[self._cur_marker]) + text + self.get_color_code('reset')
            return text

    def colorize(self, color, text):
        return self.get_color_code(color) + text + self.get_color_code('reset')


# XXX
def ansi_codes_to_markup(text):
    # type: (str) -> str

    import color
    color_codes = {code: name for name, code in color.COLORS.items()}
    color_codes['default'] = 'default'
    # there are several specific markers that shouldn't
    # be converted from matching color
    skip_markers = [palette.Highlight.PATH]
    known_markup = {color: word for word, color in palette.DEFAULT_PALETTE.items() if word not in skip_markers}
    reset = [False]

    def transformer(ansi_code, text):
        if ansi_code:
            reset[0] = True
        else:
            # first empty ansi_code should be treated as reset
            if reset[0]:
                reset[0] = False
                ansi_code = "0"
            else:
                return text

        codes = [int(c) for c in ansi_code.split(";")]
        light = color.ATTRIBUTES['light'] in codes
        dark = color.ATTRIBUTES['dark'] in codes
        # strip non-color codes
        codes = [c for c in codes if c in color_codes]
        if not codes:
            # there is a single bold ansi code, interpret it as light-default
            # (light flag is on and prefix will be added below, that's why 'default')
            if light or dark:
                codes = ['default']
            else:
                return text

        # there are several color codes (might be reset + color) - use last one
        term_code = codes[-1]
        if color_codes[term_code] == "reset":
            return "[[%s]]%s" % (palette.Highlight.RESET, text)
        if light:
            prefix = 'light-'
        elif dark:
            prefix = 'dark-'
        else:
            prefix = ''
        color_name = prefix + color_codes[term_code]
        if known_markup.get(color_name):
            return "[[%s]]%s" % (known_markup.get(color_name), text)
        return "[[c:%s]]%s" % (color_name, text)

    from yalibrary.term import console
    return formatter.transform(text, transformer, lambda _: True, console.ecma_48_sgr_regex())
