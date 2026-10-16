"""Entry point for the Python wait-click-join companion process."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .join_process_function import JoinProcessFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("join", JoinProcessFunction())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")
# [END main]


if __name__ == "__main__":
    main()
