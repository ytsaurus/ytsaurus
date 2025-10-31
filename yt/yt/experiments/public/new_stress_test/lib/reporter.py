import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
import time
from enum import StrEnum, auto
import logging
from typing import Optional, Any, Callable, List

import yt.wrapper as yt

from yt.wrapper.http_helpers import get_token


class Status(StrEnum):
    UNKNOWN = auto()
    SUCCESS = auto()
    FAILURE = auto()


@dataclass
class TestRunReporterConfig:
    report_cluster: str
    report_path: str
    report_test_run_name: str
    suite_cluster: str
    error_message_formatters: List[Callable[[str], str]] = field(default_factory=list)
    context: Optional[dict[str, Any]] = None


@dataclass
class TestRun:
    start_timestamp: int
    cluster: str
    suite: str
    status: str
    duration: Optional[int] = None
    error_message: Optional[str] = None
    raw_error_message: Optional[str] = None
    context: Optional[dict[str, Any]] = None


class Reporter(ABC):
    @abstractmethod
    def report_success(self):
        pass

    @abstractmethod
    def report_failure(self, error_message: Optional[str]):
        pass


class NoopReporter(Reporter):
    def report_success(self):
        pass

    def report_failure(self, _: Optional[str]):
        pass


class TestRunReporter(Reporter):
    """
    Saving test run execution information to YT for later Datalens processing.
    """

    def __init__(self, config: TestRunReporterConfig):
        self.config = config
        self.start_timestamp = int(time.time())
        self.yt_client = yt.YtClient(proxy=self.config.report_cluster, token=get_token())

    def report_success(self):
        self.report(Status.SUCCESS)

    def report_failure(self, error_message: Optional[str]):
        formatted_message = error_message
        for formatter in self.config.error_message_formatters:
            formatted_message = formatter(formatted_message)

        self.report(Status.FAILURE, formatted_message, error_message)

    def report(self, status: Status, error_message: Optional[str] = None, raw_error_message: Optional[str] = None):
        try:
            now = int(time.time())
            row = TestRun(
                start_timestamp=self.start_timestamp,
                cluster=self.config.suite_cluster,
                suite=self.config.report_test_run_name,
                status=status.value,
                duration=(now - self.start_timestamp),
                error_message=error_message,
                raw_error_message=raw_error_message,
                context=self.config.context,
            )

            logging.debug(f"Row: {row}")

            self.yt_client.insert_rows(self.config.report_path, [asdict(row)], require_sync_replica=False)
        except Exception:
            logging.exception("Unable to save metric")


def create_test_run_reporter(test_run_reporter_config: Optional[TestRunReporterConfig]) -> TestRunReporter:
    if test_run_reporter_config is None:
        return NoopReporter()

    return TestRunReporter(test_run_reporter_config)


def create_test_run_reporter_config(
    iterative_config: dict[str, Any] = None,
    error_message_formatters: list[Callable[[str], str]] = [],
    **kwargs
) -> Optional[TestRunReporterConfig]:
    """
    :param dict[str, Any] iterative_config: Configuration used in iterative runner
    :param list[Callable[[str], str]] error_message_formatters: List of lambdas used to update received error message. Eg. to remove identificators from error message.
    """

    logging.debug(f"Iterative config: {iterative_config}")

    if (
        not iterative_config
        or "check_runner" not in iterative_config
        or "test_run_reporter" not in iterative_config["check_runner"]
    ):
        return None

    test_run_reporter_config = iterative_config["check_runner"]["test_run_reporter"]

    def common_errors_formatter(error_message: str) -> str:
        if "Some operations failed" in error_message:
            return "Some operations failed"
        elif "Timed out while waiting for tablets" in error_message:
            return "Timed out while waiting for tablets"
        elif "Write operation failed" in error_message:
            return "Write operation failed"
        elif re.search(r"Transaction [\d\w\-]+ was aborted or has expired", error_message, re.IGNORECASE):
            return "Transaction was aborted or has expired"
        return error_message

    return TestRunReporterConfig(
        report_cluster=test_run_reporter_config["report_cluster"],
        report_path=test_run_reporter_config["report_path"],
        report_test_run_name=test_run_reporter_config["report_test_run_name"],
        suite_cluster=iterative_config.get("cluster", yt.http_helpers.get_cluster_name()),
        error_message_formatters=[common_errors_formatter, *error_message_formatters],
        context={"iterative_config": iterative_config, **kwargs},
    )
