import logging

import pytest


@pytest.fixture(scope="session")
def dummy_logger() -> logging.Logger:
    logger = logging.getLogger("dummy")
    logger.addHandler(logging.NullHandler())
    return logger
