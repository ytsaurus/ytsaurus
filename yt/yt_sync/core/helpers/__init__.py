import logging


def get_dummy_logger() -> logging.Logger:
    logger = logging.getLogger("dummy")
    logger.propagate = False
    logger.setLevel(logging.CRITICAL)  # eat everything
    if not logger.hasHandlers():
        logger.addHandler(logging.NullHandler())
    return logger


def is_valid_uuid(uuid: str | None) -> bool:
    return bool(uuid) and uuid != "0-0-0-0"


def is_valid_collocation_id(collocation_id: str | None) -> bool:
    return is_valid_uuid(collocation_id)
