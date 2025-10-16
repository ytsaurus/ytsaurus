import typing
from pathlib import Path
import yt.yson as yson

from .parser import SQLLogicParser
from .executor import Executor
from .suite import SuiteCollector, Suite


class SQLLogicProcessor:
    def __init__(
        self,
        executor: Executor,
        suite_type: typing.Type = Suite,
    ):
        self.executor = executor
        self.suite_type = suite_type

    def processs(self, source_path: Path, destination_path: Path, skip_source_path: typing.Optional[Path] = None):
        parser = SQLLogicParser()
        suite_collector = SuiteCollector(self.executor, self.suite_type)

        for statement in parser.parse(source_path):
            suite_collector.process_statement(statement)

        if skip_source_path and skip_source_path.exists():
            with open(skip_source_path, "rb") as f:
                skip_reason_data = yson.loads(f.read())
                suite_collector.set_skip_reason_data(skip_reason_data)

        suite = suite_collector.get_suite()
        with open(destination_path, "wb") as f:
            f.write(suite.to_yson())
