import os
import pytest

from collections import defaultdict

import yatest.common
import yt.wrapper

from yt.common import wait, WaitFailed

from yt.yt.flow.library.python.bullied_process import ProblemsConfig, ProcessExitedNormallyException
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows
from yt.yt.flow.tests.transform_ordered_source.pipeline.proto.event_record_pb2 import TEventRecordProto

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
PROTO_PIPELINE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/proto_pipeline.yson"
)
STATE_PIPELINE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/state_pipeline.yson"
)
PROTO_STATE_PIPELINE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/proto_state_pipeline.yson"
)
DISTRIBUTE_PIPELINE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/distribute_pipeline.yson"
)

QUEUE_TABLET_COUNT = 5
PROFILE_COUNT = 10
if yatest.common.context.sanitize:
    EVENT_COUNT = 360
else:
    EVENT_COUNT = 2000

EVENTS_PER_PROFILE = EVENT_COUNT // PROFILE_COUNT
PHASE1_EVENTS = EVENTS_PER_PROFILE // 2

##################################################################


def expected_outputs(event_id):
    if event_id % 7 == 0:
        return []
    copies = 2 if event_id % 5 == 0 else 1
    return [(event_id, copy_index) for copy_index in range(copies)]


def generate_rows(event_begin=0, event_end=EVENTS_PER_PROFILE):
    rows = []
    for reduce_id in range(PROFILE_COUNT):
        for event_id in range(event_begin, event_end):
            rows.append(
                {
                    "reduce_id": reduce_id,
                    "event_id": event_id,
                    "$tablet_index": reduce_id % QUEUE_TABLET_COUNT,
                }
            )
    return rows


def tablet_rows(event_begin, event_end):
    by_tablet = defaultdict(list)
    for row in generate_rows(event_begin, event_end):
        by_tablet[row["$tablet_index"]].append(row["reduce_id"])
    return by_tablet


def offset_from_key_yson(offset_key):
    if offset_key is None or len(offset_key) == 0:
        return 0
    return int(offset_key[0])


def build_expected():
    expected = defaultdict(list)
    for reduce_id in range(PROFILE_COUNT):
        for event_id in range(EVENTS_PER_PROFILE):
            expected[reduce_id].extend(expected_outputs(event_id))
    return expected


def serialize_event(reduce_id, event_id):
    record = TEventRecordProto()
    record.reduce_id = reduce_id
    record.event_id = event_id
    return record.SerializeToString()


def injected_problems_count(federation):
    return sum(process.injected_problems_count for process in federation.controllers + federation.workers)


def read_crash_sentinel(path):
    try:
        with open(path) as sentinel:
            return sentinel.read()
    except FileNotFoundError:
        return ""


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"
        self.proto_events_queue = f"{self.work_yt_path}/proto_events_queue"
        self.counts_queue = f"{self.work_yt_path}/counts_queue"
        self._expected = build_expected()

    def write_input_data(self, event_begin=0, event_end=EVENTS_PER_PROFILE):
        batching_write_rows(
            generate_rows(event_begin, event_end),
            lambda batch: self.client.insert_rows(self.input_queue, batch),
            1000,
        )

    def write_proto_input_data(self, event_begin=0, event_end=EVENTS_PER_PROFILE):
        rows = [
            {
                "data": serialize_event(row["reduce_id"], row["event_id"]),
                "$tablet_index": row["$tablet_index"],
            }
            for row in generate_rows(event_begin, event_end)
        ]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 1000)

    def prepare_config(self, config_path, computation_name, sink_queue, computation_parameters=None):
        pipeline_config = get_yson_config(config_path)
        queue_parameters = {
            "queue_path": f"<cluster=primary>{self.input_queue}",
            "consumer_path": f"<cluster=primary>{self.input_consumer}",
            "finite": True,
        }
        pipeline_config["spec"]["computations"][computation_name]["source_streams"]["queue"]["parameters"].update(
            queue_parameters
        )
        if computation_parameters:
            pipeline_config["spec"]["computations"][computation_name].setdefault("parameters", {}).update(
                computation_parameters
            )
        pipeline_config["spec"]["computations"]["Writer"]["sinks"]["queue"]["parameters"]["queue_path"] = sink_queue

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, os.path.basename(config_path))

    def restart_workers(self, federation):
        for worker in federation.workers:
            try:
                worker.restart()
            except ProcessExitedNormallyException:
                worker.restart()

    def output_row_count(self):
        queue_rows = self.client.select_rows(
            f"* from [{self.output_queue}]", format=yt.wrapper.format.YsonFormat(encoding=None)
        )
        return sum(1 for _ in queue_rows)

    def wait_completed_with_worker_restarts(self, federation, restart_rounds, poll_timeout):
        wait(lambda: self.output_row_count() > 0, timeout=120)
        assert (
            self.client.get_pipeline_state(self.pipeline_path) != "completed"
        ), "Pipeline completed before the first restart, so replay was never exercised"
        self.restart_workers(federation)

        for _ in range(restart_rounds - 1):
            try:
                self.wait_pipeline_state("completed", timeout=poll_timeout)
                break
            except WaitFailed:
                self.restart_workers(federation)

        self.wait_pipeline_state("completed", timeout=240)

    def read_queue_in_order(self, queue_path, columns):
        queue_expr = f"[$tablet_index], [$row_index], {columns} from [{queue_path}]"
        queue_rows = self.client.select_rows(queue_expr, format=yt.wrapper.format.YsonFormat(encoding=None))
        return sorted(queue_rows, key=lambda row: (int(row[b"$tablet_index"]), int(row[b"$row_index"])))

    def check_result(self, expect_multiple_worker_pids=False):
        actual = defaultdict(list)
        worker_pids = set()
        for row in self.read_queue_in_order(self.output_queue, "reduce_id, event_id, copy_index, worker_pid"):
            actual[int(row[b"reduce_id"])].append((int(row[b"event_id"]), int(row[b"copy_index"])))
            worker_pids.add(int(row[b"worker_pid"]))

        diff_path = os.path.join(self.path_to_flow_logs, "diff.json")
        with open(diff_path, "w") as f:
            for reduce_id in sorted(set(actual) | set(self._expected)):
                if actual[reduce_id] != self._expected[reduce_id]:
                    f.write(f"{reduce_id}:\n")
                    f.write(f"    Expected: {self._expected[reduce_id]}\n")
                    f.write(f"    Actual: {actual[reduce_id]}\n")

        assert set(actual) == set(self._expected), f"Look into {diff_path}"
        for reduce_id in self._expected:
            assert (
                actual[reduce_id] == self._expected[reduce_id]
            ), f"Ordered output mismatch in profile {reduce_id}. Look into {diff_path}"

        if expect_multiple_worker_pids:
            assert len(worker_pids) >= 2, "Worker restarts did not produce outputs from multiple process identities"

    def check_proto_result(self):
        actual = defaultdict(list)
        for row in self.read_queue_in_order(self.proto_events_queue, "reduce_id, event_id"):
            actual[int(row[b"reduce_id"])].append(int(row[b"event_id"]))

        expected = {reduce_id: list(range(EVENTS_PER_PROFILE)) for reduce_id in range(PROFILE_COUNT)}
        assert set(actual) == set(expected)
        for reduce_id in expected:
            assert actual[reduce_id] == expected[reduce_id], f"Ordered proto output mismatch in profile {reduce_id}"

    @pytest.mark.authors(["blinkov"])
    def test_exactly_once_under_problems(self):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        self.write_input_data()
        pipeline_config_path = self.prepare_config(PIPELINE_CONFIG_PATH, "EventTransform", self.output_queue)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=2,
            controllers_count=1,
            problems=True,
            controller_problems_config=ProblemsConfig(interval_seconds=5, problems_max_count=3, soft_restarts=True),
            worker_problems_config=ProblemsConfig(interval_seconds=5, problems_max_count=6, soft_restarts=True),
        ) as federation:

            def fault_injected_before_completion():
                if injected_problems_count(federation) > 0:
                    return True
                assert (
                    self.client.get_pipeline_state(self.pipeline_path) != "completed"
                ), "Pipeline completed before the first injected fault, so exactly-once was never stressed"
                return False

            wait(fault_injected_before_completion, timeout=120)
            self.wait_pipeline_state("completed", timeout=240)

        self.check_result()

    @pytest.mark.authors(["blinkov"])
    def test_exactly_once_under_worker_restarts(self):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        self.write_input_data()
        pipeline_config_path = self.prepare_config(PIPELINE_CONFIG_PATH, "EventTransform", self.output_queue)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=2,
            controllers_count=1,
        ) as federation:
            self.wait_completed_with_worker_restarts(federation, restart_rounds=3, poll_timeout=10)

        self.check_result(expect_multiple_worker_pids=True)

    @pytest.mark.authors(["blinkov"])
    def test_proto_transform(self):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        self.write_proto_input_data()
        pipeline_config_path = self.prepare_config(
            PROTO_PIPELINE_CONFIG_PATH, "ProtoEventTransform", self.proto_events_queue
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=2,
            controllers_count=1,
        ):
            self.wait_pipeline_state("completed", timeout=240)

        self.check_proto_result()

    @pytest.mark.authors(["blinkov"])
    def test_distribute_false_messages_are_not_published(self):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        self.write_input_data()
        pipeline_config_path = self.prepare_config(DISTRIBUTE_PIPELINE_CONFIG_PATH, "EventTransform", self.output_queue)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=2,
            controllers_count=1,
        ):
            self.wait_pipeline_state("completed", timeout=240)

        self.check_result()

    def check_state_result(self):
        queue_expr = f"* from [{self.counts_queue}]"
        queue_rows = self.client.select_rows(queue_expr, format=yt.wrapper.format.YsonFormat(encoding=None))

        actual = defaultdict(list)
        for row in queue_rows:
            actual[int(row[b"reduce_id"])].append(int(row[b"count"]))

        expected_counts = list(range(1, EVENTS_PER_PROFILE + 1))
        diff_path = os.path.join(self.path_to_flow_logs, "state_diff.json")
        with open(diff_path, "w") as f:
            for reduce_id in sorted(set(actual) | set(range(PROFILE_COUNT))):
                counts = sorted(actual[reduce_id])
                if counts != expected_counts:
                    f.write(f"{reduce_id}: {counts}\n")

        assert set(actual) == set(range(PROFILE_COUNT)), f"Missing profiles. Look into {diff_path}"
        for reduce_id, counts in actual.items():
            assert (
                sorted(counts) == expected_counts
            ), f"State double-apply/loss for profile {reduce_id}. Look into {diff_path}"

    def check_durable_state_matches_committed_offsets(self, computation_name, phase1_events):
        rows = self.client.select_rows(
            f"* from [{self.pipeline_path}/states]",
            format=yt.wrapper.format.YsonFormat(encoding=None),
        )
        offsets, states = {}, {}
        for row in rows:
            if row[b"computation_id"].decode() != computation_name:
                continue
            name = row[b"name"].decode()
            if name == "/$active_source/v0":
                tablet = int(row[b"key"][-1])
                offsets[tablet] = offset_from_key_yson(row[b"state"].get(b"persisted_offset_exclusive_v2"))
            elif name == "/$state":
                tablet = int(row[b"key"][-1])
                states[tablet] = {
                    int(profile.decode()): int(count) for profile, count in row[b"state"][b"counts"].items()
                }

        assert sum(offsets.values()) > 0, "Sentinel fired but no partition has a committed offset"
        phase1_tablet_rows = tablet_rows(0, phase1_events)
        diff_path = os.path.join(self.path_to_flow_logs, "durable_state_diff.json")
        mismatches = []
        for tablet in sorted(set(offsets) | set(states)):
            expected = defaultdict(int)
            for reduce_id in phase1_tablet_rows[tablet][: offsets.get(tablet, 0)]:
                expected[reduce_id] += 1
            if states.get(tablet, {}) != dict(expected):
                mismatches.append((tablet, offsets.get(tablet, 0), dict(expected), states.get(tablet, {})))
        with open(diff_path, "w") as f:
            for tablet, offset, expected, actual in mismatches:
                f.write(f"tablet {tablet} offset {offset}: expected {expected}, durable {actual}\n")
        assert not mismatches, f"Durable $state inconsistent with committed source offsets. Look into {diff_path}"

    def run_state_pipeline_through_frontier_crash(
        self, pipeline_config_path, crash_sentinel_path, computation_name, write_input
    ):
        write_input(0, PHASE1_EVENTS)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            start_watcher_thread=False,
        ) as federation:
            wait(lambda: read_crash_sentinel(crash_sentinel_path) != "", timeout=120)
            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"
            self.check_durable_state_matches_committed_offsets(computation_name, PHASE1_EVENTS)
            write_input(PHASE1_EVENTS, EVENTS_PER_PROFILE)
            self.restart_workers(federation)
            self.wait_pipeline_state("completed", timeout=240)

        self.check_state_result()

    @pytest.mark.authors(["blinkov"])
    def test_state_exactly_once_under_worker_restarts(self):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        crash_sentinel_path = os.path.join(self.path_to_flow_logs, "counting_crash_sentinel")
        pipeline_config_path = self.prepare_config(
            STATE_PIPELINE_CONFIG_PATH,
            "CountingTransform",
            self.counts_queue,
            computation_parameters={"crash_sentinel_path": crash_sentinel_path},
        )
        self.run_state_pipeline_through_frontier_crash(
            pipeline_config_path, crash_sentinel_path, "CountingTransform", self.write_input_data
        )

    @pytest.mark.authors(["blinkov"])
    def test_proto_state_exactly_once_under_worker_restarts(self):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        crash_sentinel_path = os.path.join(self.path_to_flow_logs, "proto_counting_crash_sentinel")
        pipeline_config_path = self.prepare_config(
            PROTO_STATE_PIPELINE_CONFIG_PATH,
            "ProtoCountingTransform",
            self.counts_queue,
            computation_parameters={"crash_sentinel_path": crash_sentinel_path},
        )
        self.run_state_pipeline_through_frontier_crash(
            pipeline_config_path, crash_sentinel_path, "ProtoCountingTransform", self.write_proto_input_data
        )
