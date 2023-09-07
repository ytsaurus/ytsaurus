# coding: utf-8

import os
import sys

import six

from exts import func
from exts.os2 import is_tty


ECMA_48_PATTERN = r"\x1b\[([\d;]*?)m"


@func.lazy
def ecma_48_sgr_regex():
    import re
    return re.compile(six.u(ECMA_48_PATTERN))


@func.lazy
def ecma_48_sgr_regex_bytes():
    import re
    return re.compile(six.b(ECMA_48_PATTERN))


@func.memoize()
def ansi_regexs(consider_tty=False, bytes=False):
    if consider_tty and is_tty():
        return []
    if bytes:
        return [ecma_48_sgr_regex_bytes()]
    return [ecma_48_sgr_regex()]


def strip_ansi_codes_wisely(s):
    if len(s) < 100000:
        for rx in ansi_regexs(True, not isinstance(s, six.string_types)):
            s = rx.sub('', s)

    return s


def strip_ansi_codes(s):
    for rx in ansi_regexs(False, not isinstance(s, six.string_types)):
        s = rx.sub('', s)
    return s


def get_term_interface_attrs():
    import termios
    return [termios.tcgetattr(fd) if os.isatty(fd) else None for fd in range(3)]


def set_term_interface_attrs(stdin_attr, stdout_attr, stderr_attr, when):
    import termios
    for fd, attr in zip(range(3), (stdin_attr, stdout_attr, stderr_attr)):
        if attr:
            termios.tcsetattr(fd, when, attr)


def connect_real_tty(device='/dev/tty'):
    import copy

    sys.stdout.flush()
    sys.stderr.flush()

    _, stdout_info, stderr_info = get_term_interface_attrs()
    stdout = os.dup(1)
    stderr = os.dup(2)

    afile = open(device, 'w+')
    os.dup2(afile.fileno(), 1)
    os.dup2(afile.fileno(), 2)

    stdout_attr = copy.deepcopy(stdout_info) if stdout_info else None
    stderr_attr = copy.deepcopy(stderr_info) if stderr_info else None
    return ((stdout, stdout_attr), (stderr, stderr_attr))


def restore_referral(stdout_info, stderr_info):
    import termios
    sys.stdout.flush()
    sys.stderr.flush()

    for fd, attr in zip(range(1, 3), (stdout_info, stderr_info)):
        os.dup2(attr[0], fd)
        os.close(attr[0])
    set_term_interface_attrs(None, stdout_info[1], stderr_info[1], when=termios.TCSANOW)
