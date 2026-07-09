"""Entry point for the Python wordcount companion process."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .word_count_mapper import WordCountMapper

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("mapper", WordCountMapper())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")
# [END main]


if __name__ == "__main__":
    main()
