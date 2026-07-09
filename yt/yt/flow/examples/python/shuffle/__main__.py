"""Entry point for the Python shuffle companion process."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .event_mapper import EventMapper
from .event_reducer import EventReducer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("reader", EventMapper(), source=True)
    pipeline.add("reducer", EventReducer())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")
# [END main]


if __name__ == "__main__":
    main()
