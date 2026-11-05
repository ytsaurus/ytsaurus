import logging


def configure_logger(orm_logger):
    FORMAT = "%(asctime)s\t%(levelname)s\t%(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = [handler]

    # ORM logger messages are handled by the previously configured root logger.
    orm_logger.handlers = []
