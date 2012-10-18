import logging

logging.basicConfig(format="%(asctime)-15s, %(levelname)s: %(message)s")
LOGGER = logging.getLogger("YtWrapper")
LOGGER.setLevel(level=logging.INFO)

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
