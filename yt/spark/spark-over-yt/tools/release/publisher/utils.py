import logging
import sys


def configure_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.getLevelName("INFO"))

    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setFormatter(logging.Formatter(fmt='%(asctime)-15s | %(levelname)s | %(filename)s | %(lineno)d: %(message)s'))

    logger.propagate = False
    logger.addHandler(ch)
    return logger
