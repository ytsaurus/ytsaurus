import logging
import threading
import time

from collections import Counter, defaultdict

import pytest

import yatest.common

from yt.yt.flow.library.python.bullied_process import ProblemsConfig
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

INPUT_TABLET_COUNT = 3
COLD_KEY_COUNT = 7
HOT_KEY = 0
CHUNK_COUNT = 15
CHUNK_INTERVAL_SECONDS = 2
FILTERED_EVENT_MODULO = 5
SMALL_MAX_ROWS_PER_BATCH = 30

INPUT_QUEUE_SCHEMA = [
    {"name": "event_id", "type": "int64"},
    {"name": "key", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

OUTPUT_QUEUE_SCHEMA = [
    {"name": "key", "type": "uint64"},
    {"name": "count", "type": "int64"},
    {"name": "event_ids", "type": "string"},
    {"name": "message_id", "type": "string"},
    {"name": "group_id", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

GROUP_LOG_SCHEMA = [
    {"name": "group_id", "type": "string"},
    {"name": "last_message_id", "type": "string"},
    {"name": "count", "type": "int64"},
    {"name": "event_ids", "type": "string"},
    {"name": "ids_hash", "type": "uint64"},
]

if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 600
else:
    EVENT_COUNT = 6000


def get_key(event_id):
    # Half of the traffic goes to one hot key.
    return HOT_KEY if event_id % 2 == 0 else 1 + event_id % COLD_KEY_COUNT


INPUT_DATA = [
    {
        "event_id": event_id,
        "key": get_key(event_id),
        # Ids increase only within a source partition, so each key lives in one tablet.
        "$tablet_index": get_key(event_id) % INPUT_TABLET_COUNT,
    }
    for event_id in range(EVENT_COUNT)
]
EXPECTED_EVENT_IDS = set(range(EVENT_COUNT))

##################################################################


class TestSwiftStableBatchSource(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super(TestSwiftStableBatchSource, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.group_log = self.work_yt_path + "/group_log"

    def prepare_environment(self):
        run_yt_sync(
            "primary",
            self.work_yt_path,
            add_input_queue_and_consumer=True,
            input_queue_schema=INPUT_QUEUE_SCHEMA,
            input_queue_tablet_count=INPUT_TABLET_COUNT,
            add_output_queue=True,
            output_queue_schema=OUTPUT_QUEUE_SCHEMA,
        )
        self.client.create("table", self.group_log, attributes={"dynamic": True, "schema": GROUP_LOG_SCHEMA})
        self.client.mount_table(self.group_log, sync=True)

    def prepare_pipeline_config(self, skip_if_expression=None, hold_writer=False):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        reader = pipeline_config["spec"]["computations"]["Reader"]
        reader["processing_function_parameters"][
            "group_log_path"
        ] = f"<cluster={self.primary_cluster_name}>{self.group_log}"
        pipeline_config["spec"]["resources"]["YTClientFactory"]["parameters"]["default_config"] = {
            "proxy_role": self.RPC_PROXY_ROLE,
        }
        if hold_writer:
            pipeline_config["dynamic_spec"]["computations"]["Writer"]["parameters"]["hold_processing"] = True

        pipeline_config["spec"]["computations"]["Reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": False,
            }
        )
        if skip_if_expression is not None:
            pipeline_config["dynamic_spec"]["computations"]["Reader"]["skip_if_expression"] = skip_if_expression

        pipeline_config["spec"]["computations"]["Writer"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
            }
        )

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def write_input(self, rows):
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

    def start_writing(self):
        # Data arrives during the run, so restarts hit read groups that are in flight.
        def write():
            chunk_size = (len(INPUT_DATA) + CHUNK_COUNT - 1) // CHUNK_COUNT
            for begin in range(0, len(INPUT_DATA), chunk_size):
                self.write_input(INPUT_DATA[begin : begin + chunk_size])
                time.sleep(CHUNK_INTERVAL_SECONDS)

        thread = threading.Thread(target=write)
        thread.start()
        return thread

    def select_output(self):
        return list(
            self.client.select_rows(
                f"[$row_index], key, count, event_ids, message_id, group_id from [{self.output_queue}]"
            )
        )

    def select_group_log(self):
        return list(
            self.client.select_rows(f"group_id, last_message_id, count, event_ids, ids_hash from [{self.group_log}]")
        )

    @staticmethod
    def get_event_ids(rows):
        return [int(event_id) for row in rows for event_id in row["event_ids"].split(",") if event_id]

    def check_no_duplicates(self, rows):
        event_ids = self.get_event_ids(rows)
        duplicates = [event_id for event_id, count in Counter(event_ids).items() if count > 1]
        assert not duplicates, f"Event ids counted twice: {duplicates[:10]}"
        assert set(event_ids) <= EXPECTED_EVENT_IDS
        assert sum(row["count"] for row in rows) == len(event_ids)

        message_ids = [row["message_id"] for row in rows]
        assert len(message_ids) == len(set(message_ids)), "An aggregate is delivered twice"

    def check_key_order(self, rows):
        last_message_id = {}
        for row in sorted(rows, key=lambda row: row["$row_index"]):
            key = row["key"]
            if key in last_message_id:
                assert last_message_id[key] < row["message_id"], f"Message ids of key {key} are out of order"
            last_message_id[key] = row["message_id"]

    def check_group_log(self, rows):
        """Checks the groups logged by every processing and returns those processed more than once."""
        copies = defaultdict(list)
        for group in self.select_group_log():
            copies[group["group_id"]].append(group)
        for group_id, group_copies in copies.items():
            assert all(copy == group_copies[0] for copy in group_copies), f"Replays of group {group_id} differ"

        # A group processed in an epoch that never committed is read again under other ids and is
        # never delivered, so only delivered groups must tile the input.
        delivered_group_ids = {row["group_id"] for row in rows}
        assert delivered_group_ids <= copies.keys()
        tablet_event_ids = defaultdict(list)
        for group_id in sorted(delivered_group_ids):
            event_ids = [int(event_id) for event_id in copies[group_id][0]["event_ids"].split(",")]
            tablet_event_ids[get_key(event_ids[0]) % INPUT_TABLET_COUNT] += event_ids
        delivered_event_ids = sorted(self.get_event_ids(rows))
        for tablet_index, event_ids in tablet_event_ids.items():
            expected = [
                event_id for event_id in delivered_event_ids if get_key(event_id) % INPUT_TABLET_COUNT == tablet_index
            ]
            assert event_ids == expected, f"Groups of tablet {tablet_index} overlap or leave gaps"

        replayed = [group_copies[0] for group_copies in copies.values() if len(group_copies) > 1]
        logging.info("Logged groups %d, replayed groups %d", len(copies), len(replayed))
        return replayed

    def set_dynamic_spec(self, computation_id, update):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        update(dynamic_spec["spec"]["computations"][computation_id])
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
        )

    def run_pipeline(self, problems):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        writer = self.start_writing()
        try:
            with self.start_flow_process_federation(
                pipeline_binary_args={"--config": pipeline_config_path},
                workers_count=2,
                problems=problems,
                worker_problems_config=ProblemsConfig(interval_seconds=6, problems_max_count=4),
            ):
                rows = self.wait_output()
        finally:
            writer.join()
        return rows

    def wait_output(self, expected_event_ids=EXPECTED_EVENT_IDS):
        wait(lambda: expected_event_ids <= set(self.get_event_ids(self.select_output())), timeout=300)
        # Let replays after the last restart settle before checking for duplicates.
        time.sleep(10)
        return self.select_output()

    @pytest.mark.authors(["pechatnov"])
    def test_aggregates_exactly_once_with_restarts(self):
        rows = self.run_pipeline(problems=True)

        self.check_no_duplicates(rows)
        self.check_group_log(rows)
        assert set(self.get_event_ids(rows)) == EXPECTED_EVENT_IDS
        self.check_key_order(rows)

        counts = defaultdict(int)
        hot_rows = 0
        for row in rows:
            counts[row["key"]] += row["count"]
            hot_rows += row["key"] == HOT_KEY
        assert counts == Counter(get_key(event_id) for event_id in range(EVENT_COUNT))
        # Aggregation reduces the load of the hot key.
        assert hot_rows < counts[HOT_KEY] // 2

    @pytest.mark.authors(["pechatnov"])
    def test_replay_keeps_filter_of_first_read(self):
        self.prepare_environment()
        half = len(INPUT_DATA) // 2
        self.write_input(INPUT_DATA[:half])
        pipeline_config_path = self.prepare_pipeline_config(
            skip_if_expression=f"event_id % {FILTERED_EVENT_MODULO} = 0", hold_writer=True
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=2,
        ) as federation:
            # The held writer delivers nothing, so every group read now stays unpersisted.
            wait(lambda: len(self.select_group_log()) >= INPUT_TABLET_COUNT, timeout=120)
            # Lets the reader commit the groups it has processed.
            time.sleep(5)

            # Workers are frozen while the filter is dropped, so the groups read under it are replayed
            # after the change.
            for worker in federation.workers:
                worker.send_sigstop()
            filtered_group_ids = {group["group_id"] for group in self.select_group_log()}
            self.set_dynamic_spec("Reader", lambda reader: reader.pop("skip_if_expression"))
            self.set_dynamic_spec("Writer", lambda writer: writer["parameters"].update({"hold_processing": False}))
            self.write_input(INPUT_DATA[half:])
            for worker in federation.workers:
                worker.restart()

            first_half_kept_event_ids = {
                row["event_id"] for row in INPUT_DATA[:half] if row["event_id"] % FILTERED_EVENT_MODULO != 0
            }
            # Groups first read after the change are not filtered.
            second_half_event_ids = {row["event_id"] for row in INPUT_DATA[half:]}
            rows = self.wait_output(first_half_kept_event_ids | second_half_event_ids)

        self.check_no_duplicates(rows)
        self.check_key_order(rows)
        replayed = [group for group in self.check_group_log(rows) if group["group_id"] in filtered_group_ids]
        assert replayed, "No group read under the filter was replayed"

        output_event_ids = set(self.get_event_ids(rows))
        for group in replayed:
            event_ids = [int(event_id) for event_id in group["event_ids"].split(",")]
            tablet_index = get_key(event_ids[0]) % INPUT_TABLET_COUNT
            filtered_event_ids = {
                event_id
                for event_id in range(min(event_ids), max(event_ids) + 1)
                if event_id % FILTERED_EVENT_MODULO == 0 and get_key(event_id) % INPUT_TABLET_COUNT == tablet_index
            }
            assert not filtered_event_ids & output_event_ids, f"Replay of group {group['group_id']} dropped the filter"

    @pytest.mark.authors(["pechatnov"])
    def test_replay_reads_groups_larger_than_batch_limit(self):
        self.prepare_environment()
        # A backlog makes every first read full.
        self.write_input(INPUT_DATA)
        pipeline_config_path = self.prepare_pipeline_config(hold_writer=True)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=2,
        ) as federation:
            # The held writer delivers nothing, so every group read now stays unpersisted.
            wait(
                lambda: sum(group["count"] > SMALL_MAX_ROWS_PER_BATCH for group in self.select_group_log())
                >= INPUT_TABLET_COUNT,
                timeout=120,
            )
            # Lets the reader commit the groups it has processed.
            time.sleep(5)

            # Workers are frozen while the spec changes, so the groups are replayed under the new limit.
            for worker in federation.workers:
                worker.send_sigstop()
            self.set_dynamic_spec(
                "Reader", lambda reader: reader.update({"max_rows_per_batch": SMALL_MAX_ROWS_PER_BATCH})
            )
            self.set_dynamic_spec("Writer", lambda writer: writer["parameters"].update({"hold_processing": False}))
            for worker in federation.workers:
                worker.restart()

            rows = self.wait_output()

        self.check_no_duplicates(rows)
        assert set(self.get_event_ids(rows)) == EXPECTED_EVENT_IDS
        self.check_key_order(rows)
        replayed = self.check_group_log(rows)
        assert replayed, "No group was replayed"
        assert any(
            group["count"] > SMALL_MAX_ROWS_PER_BATCH for group in replayed
        ), "No replayed group exceeds the batch limit"
