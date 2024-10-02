import logging


def get_dummy_logger() -> logging.Logger:
    logger = logging.getLogger("dummy")
    logger.propagate = False
    logger.setLevel(logging.CRITICAL)  # eat everything
    if not logger.hasHandlers():
        logger.addHandler(logging.NullHandler())
    return logger


def is_valid_collocation_id(collocation_id: str | None) -> bool:
    return collocation_id is not None and collocation_id != "0-0-0-0" and collocation_id != ""
