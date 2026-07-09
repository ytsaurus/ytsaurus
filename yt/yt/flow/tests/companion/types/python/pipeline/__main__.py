"""Python companion entry point for the type-checking integration test."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .type_mapper import TypeMapper

logging.basicConfig(level=logging.INFO)


def main():
    pipeline = Pipeline()
    pipeline.add("mapper", TypeMapper())
    pipeline.run()


if __name__ == "__main__":
    main()
