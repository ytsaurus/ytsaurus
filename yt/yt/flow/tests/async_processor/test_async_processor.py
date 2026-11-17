from collections import Counter
from pathlib import Path

import pytest

import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import INPUT_TABLET_COUNT, run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

# Mirror ResultFactor and FailingEventPeriod of pipeline/main.cpp.
RESULT_FACTOR = 10
FAILING_EVENT_PERIOD = 4

# The computation that runs tasks.
COMPUTATION_ID = "TransformAwaiter"

FETCHES_STATE_NAME = "/fetches"

WAIT_TIMEOUT = 300


def expected_results(event_count):
    return {
        event_id: {
            "result": event_id * RESULT_FACTOR,
            "attempt": 1 if event_id % FAILING_EVENT_PERIOD == 0 else 0,
        }
        for event_id in range(event_count)
    }


##################################################################


class TestAsyncProcessor(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.result_queue = self.work_yt_path + "/transform_output"
        self.task_starts_path = Path(self.path_to_flow_logs) / f"{COMPUTATION_ID}.task_starts"
        self.task_barrier = Path(self.path_to_flow_logs) / "task_barrier"

    def prepare_environment(self, event_count):
        run_yt_sync("primary", self.work_yt_path)
        rows = [
            {"event_id": event_id, "$tablet_index": event_id % INPUT_TABLET_COUNT} for event_id in range(event_count)
        ]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

    def prepare_pipeline_config(self, task_barrier=None, task_barrier_from_attempt=None):
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        computations = config["spec"]["computations"]
        computations["Reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
            }
        )
        computation = computations[COMPUTATION_ID]
        parameters = computation.setdefault("parameters", {})
        parameters["task_starts_path"] = str(self.task_starts_path)
        if task_barrier is not None:
            parameters["task_barrier_path"] = str(task_barrier)
        if task_barrier_from_attempt is not None:
            parameters["task_barrier_from_attempt"] = task_barrier_from_attempt
        computation["sinks"]["queue"]["parameters"]["queue_path"] = f"<cluster=primary>{self.result_queue}"
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def read_results(self):
        return list(self.client.select_rows(f"event_id, result, attempt from [{self.result_queue}]"))

    def read_task_starts(self):
        """Returns the (event id, attempt) pairs whose task has started, once per start."""
        contents = self.task_starts_path.read_text() if self.task_starts_path.exists() else ""
        # A line is complete once its newline is written.
        lines = contents.splitlines() if contents.endswith("\n") else contents.splitlines()[:-1]
        return [tuple(int(value) for value in line.split()) for line in lines]

    def read_timers(self):
        return list(
            self.client.select_rows(
                f"computation_id from [{self.pipeline_path}/timers] where computation_id = '{COMPUTATION_ID}'"
            )
        )

    def read_fetch_states(self):
        return [state for state in self.read_internal_key_states(COMPUTATION_ID) if state["name"] == FETCHES_STATE_NAME]

    def check_results(self, event_count):
        expected_event_ids = set(range(event_count))
        wait(
            lambda: expected_event_ids <= {row["event_id"] for row in self.read_results()},
            timeout=WAIT_TIMEOUT,
            error_message=f"Timed out waiting for the results of {COMPUTATION_ID}",
        )

        # Once the results are written, no timer is left to fire again and the fetch states are cleared.
        wait(lambda: not self.read_timers(), timeout=WAIT_TIMEOUT, ignore_exceptions=True)
        wait(lambda: not self.read_fetch_states(), timeout=WAIT_TIMEOUT, ignore_exceptions=True)

        expected = expected_results(event_count)
        rows = self.read_results()
        event_ids = [row["event_id"] for row in rows]
        assert len(event_ids) == len(set(event_ids)), f"Duplicate results of {COMPUTATION_ID}"
        actual = {row["event_id"]: {"result": row["result"], "attempt": row["attempt"]} for row in rows}
        assert actual == expected, f"Unexpected results of {COMPUTATION_ID}"

    def assert_started_again(self, starts):
        """Checks that every one of the starts happened again and returns the count of every start."""
        start_counts = Counter(self.read_task_starts())
        not_restarted = [start for start in starts if start_counts[start] < 2]
        assert not not_restarted, f"Tasks did not start again: {not_restarted}"
        return start_counts

    def restart_workers_after_tasks_start(self, is_cut_off, task_barrier_from_attempt=None):
        """Restarts the workers once a start matches is_cut_off and checks the results; returns the matching starts."""
        event_count = 300
        self.prepare_environment(event_count)

        with self.start_flow_process_federation(
            pipeline_binary_args={
                "--config": self.prepare_pipeline_config(
                    task_barrier=self.task_barrier, task_barrier_from_attempt=task_barrier_from_attempt
                ),
            },
            workers_count=2,
        ) as federation:
            wait(lambda: any(is_cut_off(start) for start in self.read_task_starts()), timeout=WAIT_TIMEOUT)
            cut_off_starts = {start for start in self.read_task_starts() if is_cut_off(start)}
            for worker in federation.workers:
                worker.restart()
            self.task_barrier.touch()

            self.check_results(event_count)

        return cut_off_starts

    @pytest.mark.authors(["sergeypozdeev"])
    def test_tasks_produce_outputs(self):
        event_count = 100
        self.prepare_environment(event_count)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.prepare_pipeline_config()},
        ):
            self.check_results(event_count)

    @pytest.mark.authors(["sergeypozdeev"])
    def test_tasks_restart_with_job(self):
        # A restart forgets the task in flight and fires its timer again; every event still gets one result.
        # The tasks wait for the barrier, created once the workers restart, so every task that has started is cut off.
        cut_off_starts = self.restart_workers_after_tasks_start(is_cut_off=lambda start: True)
        self.assert_started_again(cut_off_starts)

    @pytest.mark.authors(["sergeypozdeev"])
    def test_retries_restart_with_stored_attempt(self):
        # A restart during a retry fires the timer of the retry again with the attempt kept in the state.
        # Only the retries wait for the barrier: the first attempts run, and the failing ones fail, at once.
        cut_off_retries = self.restart_workers_after_tasks_start(
            is_cut_off=lambda start: start[1] > 0, task_barrier_from_attempt=1
        )
        start_counts = self.assert_started_again(cut_off_retries)
        # The first attempt of a retried event failed before the restart and never ran again.
        from_scratch = [(event_id, 0) for event_id, _ in cut_off_retries if start_counts[(event_id, 0)] != 1]
        assert not from_scratch, f"Retried events started from the first attempt again: {from_scratch}"

    @pytest.mark.authors(["sergeypozdeev"])
    def test_tasks_survive_pipeline_stop_and_start(self):
        # A stop drains the jobs while their tasks run: the roots stay registered and fire again after the start.
        event_count = 100
        self.prepare_environment(event_count)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.prepare_pipeline_config(task_barrier=self.task_barrier)},
        ):
            wait(lambda: len(self.read_task_starts()) >= 10, timeout=WAIT_TIMEOUT)
            cut_off_starts = set(self.read_task_starts())

            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=WAIT_TIMEOUT)
            assert self.read_timers(), "The stopped pipeline lost the held timers"

            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working", timeout=WAIT_TIMEOUT)
            self.task_barrier.touch()

            self.check_results(event_count)

        self.assert_started_again(cut_off_starts)
