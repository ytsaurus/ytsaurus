from . import logger_config

import logging

logging.getLogger("yt.packages.requests.packages.urllib3").setLevel(logging.WARNING)

LOGGER = logging.getLogger("Yt")
LOGGER.propagate = False
LOGGER.setLevel(level=logging.__dict__[logger_config.LOG_LEVEL.upper()])
if logger_config.LOG_PATH is None:
    LOGGER.addHandler(logging.StreamHandler())
else:
    LOGGER.addHandler(logging.FileHandler(logger_config.LOG_PATH))

BASIC_FORMATTER = logging.Formatter(logger_config.LOG_PATTERN)

def set_formatter(formatter):
    for handler in LOGGER.handlers:
        handler.setFormatter(formatter)

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

