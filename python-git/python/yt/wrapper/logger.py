import logger_config

import logging
from datetime import datetime

class OperationProgressFormatter(logging.Formatter):
    def __init__(self, format="%(asctime)-15s: %(message)s", date_format=None, start_time=None):
        logging.Formatter.__init__(self, format, date_format)
        if start_time is None:
            self._start_time = datetime.now()
        else:
            self._start_time = start_time

    def formatTime(self, record, date_format=None):
        created = datetime.fromtimestamp(record.created)
        if date_format is not None:
            return created.strftime(date_format)
        else:
            def total_minutes(time):
                return time.seconds / 60 + 60 * 24 * time.days
            elapsed = total_minutes(datetime.now() - self._start_time)
            time = datetime.now()
            if time.microsecond > 0:
                time = time.isoformat(" ")[:-3]
            else:
                time = time.isoformat(" ")
            return "{0} ({1:2} min)".format(time, elapsed)

LOGGER = logging.getLogger("YtWrapper")
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

def log(level, msg, *args, **kwargs):
    LOGGER.log(level, msg, *args, **kwargs)

