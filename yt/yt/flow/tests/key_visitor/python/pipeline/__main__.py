"""Python companion entry point for the key-visitor integration test."""

import logging

from yt.yt.flow.library.python.companion import Pipeline

from .visit_tester import VisitTester

logging.basicConfig(level=logging.INFO)


def main():
    pipeline = Pipeline()
    pipeline.add("tester", VisitTester())
    pipeline.run()


if __name__ == "__main__":
    main()
