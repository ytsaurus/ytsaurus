from . import logger_config

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

import logging

logging.getLogger("yt.packages.requests.packages.urllib3").setLevel(logging.WARNING)

try:
    if yatest_common is not None and not int(yatest_common.get_param("inside_arcadia", True)):
        LOGGER = logging.getLogger()
    else:
        LOGGER = logging.getLogger("Yt")
except:
    LOGGER = logging.getLogger("Yt")

LOGGER.propagate = False
LOGGER.setLevel(level=logging.__dict__[logger_config.LOG_LEVEL.upper()])
if logger_config.LOG_PATH is None:
    LOGGER.addHandler(logging.StreamHandler())
else:
    LOGGER.addHandler(logging.FileHandler(logger_config.LOG_PATH))

BASIC_FORMATTER = logging.Formatter(logger_config.LOG_PATTERN)

formatter = None

def set_formatter(new_formatter):
    global formatter
    formatter = new_formatter
    for handler in LOGGER.handlers:
        handler.setFormatter(new_formatter)

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

def log(level, msg, *args, **kwargs):
    LOGGER.log(level, msg, *args, **kwargs)

