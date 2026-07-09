"""Entry point for the Python url-downloader companion process."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .url_download_function import UrlDownloadFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("url_downloader", UrlDownloadFunction())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")
# [END main]


if __name__ == "__main__":
    main()
