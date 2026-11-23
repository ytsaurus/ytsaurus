import time
from collections import Counter, defaultdict

import pytest

import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

##################################################################

# The example program and its config live in the parent directory of the test.
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../file_parsing")
PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline.yson")

INPUT_TABLET_COUNT = 2
PARSE_TABLET_COUNT = 2

QUEUE_META_COLUMNS = [
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# The input is required so that a malformed row cannot block a partition of Reader.
INPUT_QUEUE_SCHEMA = [
    {"name": "url", "type": "string", "required": True},
    {"name": "size", "type": "int64", "required": True},
    *QUEUE_META_COLUMNS,
]
PARSE_QUEUE_SCHEMA = [
    {"name": "url", "type": "string"},
    {"name": "size", "type": "int64"},
    {"name": "attempt", "type": "int64"},
    {"name": "first_seen_at", "type": "uint64"},
    *QUEUE_META_COLUMNS,
]
PARSE_OUTPUT_SCHEMA = [
    {"name": "url", "type": "string"},
    {"name": "row_count", "type": "int64"},
    {"name": "attempt", "type": "int64"},
    {"name": "worker", "type": "string"},
    *QUEUE_META_COLUMNS,
]
FAILED_OUTPUT_SCHEMA = [
    {"name": "url", "type": "string"},
    {"name": "attempt", "type": "int64"},
    {"name": "error", "type": "string"},
    *QUEUE_META_COLUMNS,
]

PARSER_COMPUTATION_ID = "Parser"
# The state of Parser that holds the requests in flight; its client name in the example program is "requests".
REQUESTS_STATE_NAME = "/requests"

# The stub parse fails on a negative size.
INVALID_FILE_SIZE = -1

WORKERS_COUNT = 2
WAIT_TIMEOUT = 300
# How long to watch for rows that must not appear after all the expected ones are written.
EXTRA_ROWS_GRACE_PERIOD = 5
# How long the swift timers of accepted requests are given to fire and start the parses.
PARSE_START_DELAY = 2


class FileParsingParameters:
    def __init__(self, slot_count=2, max_queue_wait_ms=5000, parse_time_per_size_unit_ms=100, max_attempts=100):
        self.slot_count = slot_count
        self.max_queue_wait_ms = max_queue_wait_ms
        self.parse_time_per_size_unit_ms = parse_time_per_size_unit_ms
        self.max_attempts = max_attempts


def make_urls(file_count):
    return [f"blob://storage/file-{index}" for index in range(file_count)]


def get_parser_key_hash_expression():
    """Returns the expression of the hash column of the Parser key in the pipeline config."""
    schema = get_yson_config(PIPELINE_CONFIG_PATH)["spec"]["computations"][PARSER_COMPUTATION_ID]["group_by_schema"]
    (expression,) = [column["expression"] for column in schema if "expression" in column]
    return expression


class TestFileParsing(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/input_consumer"
        self.parse_queue = self.work_yt_path + "/parse_queue"
        self.parse_consumer = self.work_yt_path + "/parse_consumer"
        self.parse_output = self.work_yt_path + "/parse_output"
        self.failed_output = self.work_yt_path + "/failed_output"

    def prepare_environment(self, files):
        """Creates the tables and writes the files, given as (url, size) pairs, to the input queue."""
        run_yt_sync(
            self.primary_cluster_name,
            self.work_yt_path,
            tablet_cell_bundle=self.tablet_cell_bundle,
            primary_medium=self.primary_medium,
            output_queues=[
                {"name": "input_queue", "schema": INPUT_QUEUE_SCHEMA, "tablet_count": INPUT_TABLET_COUNT},
                {"name": "parse_queue", "schema": PARSE_QUEUE_SCHEMA, "tablet_count": PARSE_TABLET_COUNT},
                {"name": "parse_output", "schema": PARSE_OUTPUT_SCHEMA},
                {"name": "failed_output", "schema": FAILED_OUTPUT_SCHEMA},
            ],
            extra_consumers={"input_consumer": ["input_queue"], "parse_consumer": ["parse_queue"]},
        )
        rows = [
            {"url": url, "size": size, "$tablet_index": index % INPUT_TABLET_COUNT}
            for index, (url, size) in enumerate(files)
        ]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

    def queue_path(self, path):
        return f"<cluster={self.primary_cluster_name}>{path}"

    def prepare_pipeline_config(self, parameters):
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        computations = config["spec"]["computations"]
        computations["Reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": self.queue_path(self.input_queue),
                "consumer_path": self.queue_path(self.input_consumer),
            }
        )
        computations["Reader"]["sinks"]["queue"]["parameters"]["queue_path"] = self.queue_path(self.parse_queue)
        computations["RequestReader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": self.queue_path(self.parse_queue),
                "consumer_path": self.queue_path(self.parse_consumer),
            }
        )
        parser = computations["Parser"]
        parser["sinks"]["parsed"]["parameters"]["queue_path"] = self.queue_path(self.parse_output)
        parser["sinks"]["retry"]["parameters"]["queue_path"] = self.queue_path(self.parse_queue)
        parser["sinks"]["failed"]["parameters"]["queue_path"] = self.queue_path(self.failed_output)
        parser["processing_function_parameters"]["max_attempts"] = parameters.max_attempts

        executor_parameters = config["spec"]["resources"]["ParseExecutor"].setdefault("parameters", {})
        executor_parameters.update(
            {
                "slot_count": parameters.slot_count,
                "max_queue_wait": parameters.max_queue_wait_ms,
                "parse_time_per_size_unit": parameters.parse_time_per_size_unit_ms,
            }
        )
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def read_parse_queue(self):
        return list(self.client.select_rows(f"url, attempt from [{self.parse_queue}]"))

    def read_parsed(self):
        return list(self.client.select_rows(f"url, row_count, attempt, worker from [{self.parse_output}]"))

    def read_failed(self):
        return list(self.client.select_rows(f"url, attempt, error from [{self.failed_output}]"))

    def read_timers(self):
        return list(
            self.client.select_rows(
                f"computation_id from [{self.pipeline_path}/timers] where computation_id = '{PARSER_COMPUTATION_ID}'"
            )
        )

    def read_requests_states(self):
        return [
            state
            for state in self.read_internal_key_states(PARSER_COMPUTATION_ID)
            if state["name"] == REQUESTS_STATE_NAME
        ]

    def is_drained(self, urls):
        """Returns whether every request of urls has its outcome and Parser has no request in flight."""
        # The queue is read first: a row written to it later is an outcome of a row read here.
        requests = {(row["url"], row["attempt"]) for row in self.read_parse_queue()}
        outputs = {(row["url"], row["attempt"]) for row in self.read_parsed() + self.read_failed()}
        # Reader has moved the whole input.
        if not all((url, 0) in requests for url in urls):
            return False
        # Every request has a result or a retry.
        if not all(request in outputs or (request[0], request[1] + 1) in requests for request in requests):
            return False
        return not self.read_timers() and not self.read_requests_states()

    def wait_for_outputs(self, urls):
        """Waits until the pipeline is drained, then checks that no more rows arrive."""
        wait(
            lambda: self.is_drained(urls),
            timeout=WAIT_TIMEOUT,
            ignore_exceptions=True,
            error_message="Timed out waiting for the parse results",
        )
        time.sleep(EXTRA_ROWS_GRACE_PERIOD)
        parsed = self.read_parsed()
        failed = self.read_failed()
        assert len(parsed) + len(failed) == len(urls), "Extra output rows"
        return parsed, failed

    def check_exactly_once(self, urls, parsed, failed):
        """Checks that every url has exactly one row in the outputs."""
        counts = Counter(row["url"] for row in parsed + failed)
        assert counts == Counter(urls), "Every url must appear exactly once in the outputs"

    def run_pipeline(self, parameters, **kwargs):
        return self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.prepare_pipeline_config(parameters)},
            workers_count=WORKERS_COUNT,
            **kwargs,
        )

    @pytest.mark.authors(["sergeypozdeev"])
    def test_files_are_parsed(self):
        urls = make_urls(40)
        invalid_url = "blob://storage/invalid-file"
        files = [(url, 1) for url in urls] + [(invalid_url, INVALID_FILE_SIZE)]
        self.prepare_environment(files)

        with self.run_pipeline(FileParsingParameters(parse_time_per_size_unit_ms=50)):
            parsed, failed = self.wait_for_outputs(urls + [invalid_url])

        self.check_exactly_once(urls + [invalid_url], parsed, failed)
        assert [(row["url"], row["attempt"]) for row in failed] == [(invalid_url, 0)]
        assert {row["url"] for row in parsed} == set(urls)
        assert all(row["row_count"] == 1 for row in parsed)

    @pytest.mark.authors(["sergeypozdeev"])
    def test_waiting_parses_move_to_other_attempts(self):
        urls = make_urls(30)
        self.prepare_environment([(url, 1) for url in urls])
        parameters = FileParsingParameters(
            slot_count=1,
            max_queue_wait_ms=500,
            parse_time_per_size_unit_ms=300,
            max_attempts=100,
        )

        with self.run_pipeline(parameters):
            parsed, failed = self.wait_for_outputs(urls)

        self.check_exactly_once(urls, parsed, failed)
        assert not failed
        assert all(row["row_count"] == 1 for row in parsed)
        assert any(row["attempt"] > 0 for row in parsed), "No parse was moved to another attempt"
        workers = {row["worker"] for row in parsed}
        assert len(workers) == WORKERS_COUNT, f"Expected the parses to run on all workers, got {workers}"

        # The attempts of a file have different keys, so they may land in different partitions.
        expression = get_parser_key_hash_expression()
        hashes = defaultdict(dict)
        for row in self.client.select_rows(f"url, attempt, {expression} as key_hash from [{self.parse_queue}]"):
            hashes[row["url"]][row["attempt"]] = row["key_hash"]
        retried = {url: hashes_by_attempt for url, hashes_by_attempt in hashes.items() if len(hashes_by_attempt) > 1}
        assert retried, "No file has several attempts in the parse queue"
        for url, hashes_by_attempt in retried.items():
            assert len(set(hashes_by_attempt.values())) == len(hashes_by_attempt), f"Attempts of {url} share a key hash"

    @pytest.mark.authors(["sergeypozdeev"])
    def test_attempts_are_limited(self):
        urls = make_urls(10)
        self.prepare_environment([(url, 10) for url in urls])
        parameters = FileParsingParameters(
            slot_count=1,
            max_queue_wait_ms=300,
            parse_time_per_size_unit_ms=300,
            max_attempts=2,
        )

        with self.run_pipeline(parameters):
            parsed, failed = self.wait_for_outputs(urls)

        self.check_exactly_once(urls, parsed, failed)
        assert failed, "Expected some files to run out of attempts"
        assert all(row["attempt"] == 1 for row in failed), "A file failed before its last attempt"
        assert all(row["row_count"] == 10 for row in parsed)

    @pytest.mark.authors(["sergeypozdeev"])
    def test_parses_survive_worker_restart(self):
        # A parse takes 10 s and a task may wait 60 s for a slot, so no request completes or is rejected
        # before the workers stop.
        urls = make_urls(4)
        self.prepare_environment([(url, 10) for url in urls])
        parameters = FileParsingParameters(slot_count=2, max_queue_wait_ms=60000, parse_time_per_size_unit_ms=1000)

        # The process watcher would take a stopped worker for a crash.
        with self.run_pipeline(parameters, start_watcher_thread=False) as federation:
            # A request state for every file: every request is accepted by Parser, running or waiting for a slot.
            wait(
                lambda: len(self.read_requests_states()) >= len(urls),
                timeout=WAIT_TIMEOUT,
                ignore_exceptions=True,
                error_message="Timed out waiting for the requests to be accepted",
            )
            time.sleep(PARSE_START_DELAY)
            for worker in federation.workers:
                worker.kill()
            # A file without an output row was accepted by Parser, running or waiting for a slot, when its
            # worker stopped.
            written = {row["url"] for row in self.read_parsed() + self.read_failed()}
            interrupted = set(urls) - written
            assert interrupted, "No request was in flight when the workers stopped"
            for worker in federation.workers:
                worker.start()

            parsed, failed = self.wait_for_outputs(urls)

        self.check_exactly_once(urls, parsed, failed)
        assert not failed
        assert interrupted <= {row["url"] for row in parsed}, "An interrupted file is not parsed"
        assert all(row["row_count"] == 10 for row in parsed)
