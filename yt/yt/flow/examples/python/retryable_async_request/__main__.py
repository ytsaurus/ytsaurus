"""Entry point for the Python retryable-async-request companion process."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .state_keeper_function import StateKeeperFunction
from .request_processor_function import RequestProcessorFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("state", StateKeeperFunction())
    pipeline.add("processor", RequestProcessorFunction())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")
# [END main]


if __name__ == "__main__":
    main()
