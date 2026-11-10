from . import logger_config

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

import functools
import hashlib
import logging
import os
import re
import threading
import time

from collections import defaultdict

LOG_ONCE_BUFF = {}
MAX_BUFF_LEN = 1000
BUFF_CLEANING_LEN = 10


def set_log_level_from_config(logger):
    if not logger_config.LOG_LEVEL:
        logger.setLevel(level=logging.__dict__["INFO"])
    else:
        if logger_config.LOG_LEVEL.upper() == "NOTSET":
            raise Exception("LOG_LEVEL couldn't be defined as NOTSET")
        # Intentionally override trace with debug for compatibility with C++ logging library.
        if logger_config.LOG_LEVEL.upper() == "TRACE":
            logger_config.LOG_LEVEL = "DEBUG"
        logger.setLevel(level=logging.__dict__[logger_config.LOG_LEVEL.upper()])


class SimpleColorizedStreamHandler(logging.StreamHandler):
    C_LCYAN = "\033[96m"
    C_LBLUE = "\033[94m"
    C_LGREEN = "\033[92m"
    C_LYELLOW = "\033[93m"
    C_LGRAY = "\033[37m"
    C_BOLD = "\033[1m"
    C_END = "\033[0m"

    KW = C_LBLUE
    URL = C_LCYAN + C_BOLD
    PARAM = C_LGRAY
    YSON_PARAM = C_LYELLOW

    RE_KW = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"(Perform HTTP \S+ request|Response received)"), r"{}\1{}".format(KW, C_END))
    RE_HTTP = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"(https?://\S+)"), r"{}\1{}".format(URL, C_END))
    RE_JSON = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"(['\"][\w-]+['\"]): ?"), r"{}\1{}: ".format(PARAM, C_END))
    RE_YSON = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"\"([^\";]+?)\"="), "\"{}\\1{}\"=".format(YSON_PARAM, C_END))

    # to disable color output
    DISABLE_COLOR = os.environ.get("YT_LOG_LEVEL") == "Debug"

    terminator = '\n'  # py2 compat

    def _colorize(self, msg):
        msg = SimpleColorizedStreamHandler.RE_KW(msg)
        msg = SimpleColorizedStreamHandler.RE_HTTP(msg)
        msg = SimpleColorizedStreamHandler.RE_JSON(msg)
        msg = SimpleColorizedStreamHandler.RE_YSON(msg)
        return msg

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            if stream.isatty() and record.levelno == logging.DEBUG and not self.DISABLE_COLOR:
                msg = self._colorize(msg)
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


formatter = None


class BeforeFirstLogHandler(logging.StreamHandler):
    """Run registered callbacks before forwarding the first log record."""

    def __init__(self, handler):
        logging.StreamHandler.__init__(self, getattr(handler, "stream", None))
        self.setLevel(handler.level)
        self._handler = handler
        self._callbacks = defaultdict(list)
        self._callbacks_lock = threading.Lock()
        self._callbacks_started = False

    def add_callback(self, log_level, callback):
        with self._callbacks_lock:
            if not self._callbacks_started:
                self._callbacks[log_level].append(callback)
                return

        warning("Cannot add a before-first-log callback after logging has started")

    def setFormatter(self, new_formatter):
        logging.Handler.setFormatter(self, new_formatter)
        self._handler.setFormatter(new_formatter)

    def emit(self, record):
        started = False
        if self._callbacks:
            with self._callbacks_lock:
                if not self._callbacks_started:
                    self._callbacks_started = True
                    started = True
                    callbacks = self._callbacks.get(record.levelno)
                else:
                    callbacks = None

            if callbacks is not None:
                for callback in list(callbacks):
                    try:
                        if callback():
                            callbacks.remove(callback)
                            if not callbacks:
                                del self._callbacks[record.levelno]
                                break
                    except Exception:
                        pass

        try:
            self._handler.handle(record)
        finally:
            if started:
                self._callbacks_started = False

    def flush(self):
        self._handler.flush()

    def close(self):
        try:
            self._handler.close()
        finally:
            logging.Handler.close(self)


def add_before_first_log_callback(log_level, callback):
    """Register a callback to run before the first record emitted by YT logging with same logging level.

    A callback is retried by emiting log records until it returns True.
    """
    if not callable(callback):
        raise TypeError("callback must be callable")

    for handler in LOGGER.handlers:
        if isinstance(handler, BeforeFirstLogHandler):
            handler.add_callback(log_level, callback)
            return

    warning("YT logger has no before-first-log handler")


def set_formatter(new_formatter):
    global formatter
    formatter = new_formatter
    for handler in LOGGER.handlers:
        handler.setFormatter(new_formatter)


logging.getLogger("yt.packages.urllib3").setLevel(logging.WARNING)

LOGGER = logging.getLogger("Yt")

LOGGER.propagate = False

set_log_level_from_config(LOGGER)

if logger_config.LOG_PATH is None:
    _base_handler = SimpleColorizedStreamHandler()
else:
    _base_handler = logging.FileHandler(logger_config.LOG_PATH)

LOGGER.addHandler(BeforeFirstLogHandler(_base_handler))

BASIC_FORMATTER = logging.Formatter(logger_config.LOG_PATTERN)

set_formatter(BASIC_FORMATTER)


def debug(msg, *args, **kwargs):
    LOGGER.debug(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    LOGGER.info(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    LOGGER.warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    LOGGER.error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    LOGGER.exception(msg, *args, **kwargs)


if hasattr(functools, 'lru_cache') and not os.environ.get("YT_LOG_NO_TIP"):
    @functools.lru_cache(maxsize=128)
    def tip(msg):
        LOGGER.debug("[TIP] " + msg)
else:
    # py2
    def tip(msg):
        pass


def log(level, msg, *args, **kwargs):
    LOGGER.log(level, msg, *args, **kwargs)


def log_once(level, msg, *args, **kwargs):
    msg_hash = hashlib.sha256(msg.encode('utf-8')).hexdigest()
    if msg_hash not in LOG_ONCE_BUFF:
        LOGGER.log(level, msg, *args, **kwargs)
    LOG_ONCE_BUFF[msg_hash] = time.time()

    if len(LOG_ONCE_BUFF) >= MAX_BUFF_LEN:
        cleaning_items = sorted(LOG_ONCE_BUFF.items(), key=lambda i: i[1])[:BUFF_CLEANING_LEN]
        for msg, msg_time in cleaning_items:
            del LOG_ONCE_BUFF[msg]
