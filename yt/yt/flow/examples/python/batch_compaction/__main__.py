"""Entry point for the Python batch compaction companion process."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .compaction_mapper import EventCompactor
from .total_writer import TotalWriter

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("compactor", EventCompactor())
    pipeline.add("writer", TotalWriter())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")


# [END main]


if __name__ == "__main__":
    main()
