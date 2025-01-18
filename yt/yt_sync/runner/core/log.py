from abc import ABC
from abc import abstractmethod
import argparse
import logging
import sys

from coloredlogs import ColoredFormatter
from coloredlogs.converter import convert

_stdout = sys.stdout
_stderr = sys.stderr


class LogSetup(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def add_args(self, parser: argparse.ArgumentParser):
        raise NotImplementedError()

    @abstractmethod
    def setup_logging(self, args: argparse.Namespace):
        raise NotImplementedError()


class DefaultLogSetup(LogSetup):
    def __init__(self) -> None:
        pass

    def add_args(self, parser: argparse.ArgumentParser):
        parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Enable log level DEBUG")
        parser.add_argument("--html", action="store_true", default=False, help="Write log in HTML format")

    def setup_logging(self, args: argparse.Namespace):
        if args.html:
            sys.stdout, sys.stderr = _StreamWrapper(_stdout), _StreamWrapper(_stderr)
        else:
            sys.stdout, sys.stderr = _stdout, _stderr

        log_level = logging.DEBUG if args.verbose else logging.INFO
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredFormatter("%(asctime)s:%(levelname)s: %(message)s"))
        logging.basicConfig(level=log_level, handlers=[handler], force=True)


class _StreamWrapper:
    def __init__(self, underlying):
        self._underlying = underlying

    def write(self, b):
        self._underlying.write(convert(b, code=False))

    def flush(self):
        self._underlying.flush()
