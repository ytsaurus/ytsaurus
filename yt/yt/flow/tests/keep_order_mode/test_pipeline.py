import logging
import os
import pytest
import random

from collections import defaultdict

import library.python.codecs

import yatest.common
import yt.wrapper

from yt.common import wait, WaitFailed
from yt.wrapper import yson

from yt.yt.flow.library.python.bullied_process import ProblemsConfig
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

QUEUE_TABLET_COUNT = 7
PROFILE_COUNT = 10
if yatest.common.context.sanitize:
    EVENT_COUNT = 360
else:
    EVENT_COUNT = 2500

##################################################################


def generate_row(value):
    return {
        "value": library.python.codecs.dumps("zstd_6", yson.dumps(value)),
        "codec": "zstd_6",
        "$tablet_index": value["reduce_id"] % QUEUE_TABLET_COUNT,
    }


def generate_test_data():
    result = []
    expected_result = defaultdict(list)
    for i in range(1000000000):
        for p in range(PROFILE_COUNT):
            if i % (p + 10) == 0:
                if len(result) >= EVENT_COUNT:
                    return result, expected_result
                if i * 147 % (p + 11) == 0:
                    result.append(generate_row({"reduce_id": p, "event_id": -1, "event_time": 1000000}))
                else:
                    event_id = len(expected_result[p])
                    expected_result[p].append(event_id)
                    result.append(
                        generate_row(
                            {"reduce_id": p, "event_id": event_id, "event_time": 1000000 + random.randint(0, 1000)}
                        )
                    )


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"
        self._data, self._expected_result = generate_test_data()
        self._data_written_offset = 0

    def write_input_data(self, share=1.0):
        final_offset = min(len(self._data), self._data_written_offset + int(len(self._data) * share))
        batching_write_rows(
            self._data[0:final_offset], lambda batch: self.client.insert_rows(self.input_queue, batch), 1000
        )

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["Reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["QueueReducer"]["sinks"]["queue"]["parameters"][
            "queue_path"
        ] = self.output_queue

        for computation in pipeline_config["spec"]["computations"].values():
            # Set relaxed_ordering parameter. It can be changed to True if you want to check that test really can find ordering problems.
            computation["relaxed_ordering"] = False

            # Explicitly set input_ordering parameter to event time.
            if len(computation["input_stream_ids"]) > 0:
                computation["input_ordering"] = {
                    "time_type": "event_time",
                }

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def check_result(self):
        queue_expr = f"* from [{self.output_queue}]"
        queue_rows = self.client.select_rows(queue_expr, format=yt.wrapper.format.YsonFormat(encoding=None))

        queue_result = defaultdict(list)
        for row in queue_rows:
            queue_result[int(row[b"reduce_id"])].append(int(row[b"event_id"]))
        assert set(queue_result) == set(self._expected_result)

        diff_path = os.path.join(self.path_to_flow_logs, "diff.json")
        with open(diff_path, "w") as f:
            for reduce_id, events in queue_result.items():
                expected_events = self._expected_result[reduce_id]
                if events != expected_events:
                    f.write(f"{reduce_id}:\n")
                    f.write(f"    Expected: {expected_events}\n")
                    f.write(f"    Actual: {events}\n")

        for reduce_id, events in queue_result.items():
            events_set = set(events)
            assert len(events) == len(events_set), f"Duplicate event in profile {reduce_id}. Look into {diff_path}"
            for e in self._expected_result[reduce_id]:
                assert e in events_set, f"Missing event {e} in profile {reduce_id}. Look into {diff_path}"

        assert self._expected_result == queue_result, f"Look into {diff_path}"

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            pytest.param(2, 1, True, id="1c_2w_unstable"),
        ],
    )
    def test_basic(self, workers_count, controllers_count, problems):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        self.write_input_data()
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
            controller_problems_config=ProblemsConfig(interval_seconds=20, problems_max_count=3, soft_restarts=True),
            worker_problems_config=ProblemsConfig(interval_seconds=20, problems_max_count=6, soft_restarts=True),
        ):
            self.wait_pipeline_state("completed", timeout=240)

        self.check_result()

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(2, 1, id="1c_2w"),
        ],
    )
    def test_repartitioning_and_killing(self, workers_count, controllers_count):
        run_yt_sync("primary", self.work_yt_path, QUEUE_TABLET_COUNT)

        self.write_input_data()
        pipeline_config_path = self.prepare_pipeline_config()

        modified_computations = {"Shuffle_1", "Shuffle_2", "QueueReducer"}

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
        ) as federation:
            for i in range(3):
                try:
                    self.wait_pipeline_state("completed", timeout=10)
                except WaitFailed:
                    # It is normal case that waiting failed. It is intentional "sleep" logic.
                    pass

                # At the same moment change desired partition count.
                dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
                for computation_name, computation in dynamic_spec["spec"]["computations"].items():
                    if computation_name in modified_computations:
                        logging.info(f"Changing desired partition count for {computation_name}")
                        computation["parameters"]["desired_partition_count"] += 1  # Just change value.
                old_partitions = self.client.get_flow_view(
                    self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
                )
                self.client.set_pipeline_dynamic_spec(
                    self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
                )

                # Wait for changing partitions set.
                def is_new_partitions_created():
                    if self.client.get_pipeline_state(self.pipeline_path) == "completed":
                        # Do not wait forever for partitions change is pipeline is completed.
                        return True
                    new_partitions = self.client.get_flow_view(
                        self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
                    )
                    return len(set(new_partitions.keys()) - set(old_partitions.keys())) > 0

                wait(is_new_partitions_created, sleep_backoff=0.05, timeout=30)

                # Kill workers to drop all messages in memory in message distributor.
                for worker in federation.workers:
                    worker.restart()

            self.wait_pipeline_state("completed", timeout=240)

        self.check_result()
