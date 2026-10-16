"""Entry point for the companion process; the image runs this module as `python3 main.py`."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .text_mapper import TextMapper

logging.basicConfig(level=logging.INFO)


def main():
    pipeline = Pipeline()
    pipeline.add("mapper", TextMapper())
    pipeline.run()


if __name__ == "__main__":
    main()
