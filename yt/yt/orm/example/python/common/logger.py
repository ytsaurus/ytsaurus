import logging
import os


LOG_PATTERN = "%(asctime)-15s\t%(levelname)s\t%(message)s"

logger = logging.getLogger("ExampleClient")
logger.addHandler(logging.StreamHandler())
logger.setLevel(level=logging.__dict__[os.environ.get("EXAMPLE_LOG_LEVEL", "INFO").upper()])

for handler in logger.handlers:
    handler.setFormatter(logging.Formatter(LOG_PATTERN))
