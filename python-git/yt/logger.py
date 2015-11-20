import logger_config

import logging

LOGGER = logging.getLogger("Yt")
LOGGER.propagate = False
LOGGER.setLevel(level=logging.__dict__[logger_config.LOG_LEVEL])

BASIC_FORMATTER = logging.Formatter(logger_config.LOG_PATTERN)

def set_formatter(formatter):
    if not LOGGER.handlers:
        LOGGER.addHandler(logging.StreamHandler())
    LOGGER.handlers[0].setFormatter(formatter)

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

