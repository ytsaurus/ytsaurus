"""Entry point for the Python noop companion process."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .reader import Reader

logging.basicConfig(level=logging.INFO)


def main():
    pipeline = Pipeline()
    pipeline.add("reader", Reader(), source=True)
    pipeline.run()


if __name__ == "__main__":
    main()
