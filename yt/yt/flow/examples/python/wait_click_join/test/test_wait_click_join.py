"""E2E test for the Python wait-click-join companion."""

import logging

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    "yt/yt/flow/examples/python/wait_click_join/test/pipeline.yson"
)

if yatest.common.context.sanitize is not None:
    TOTAL_ACTIONS = 100
else:
    TOTAL_ACTIONS = 2000

SHUFFLE_WINDOW = 10

#################################################################


def generate_data(count):
    start_time = 1714215000
    delta_time = 500
    output = []
    for i in range(count):
        # 20 percent hits has no show
        has_show = hash(str(i) + "show") % 5 > 0
        # 75 percent hits has no click
        # some clicks may not have show
        has_click = hash(str(i) + "click") % 4 == 0

        hit_time = start_time + (hash(str(i) + "hit") % delta_time)
        record = {
            "hit_id": str(hash(i)),
            "hit_payload": "payload_{}".format(str(hash(i))),
            "hit_time": hit_time,
            "click_time": 0,
            "show_time": 0,
        }

        if has_click:
            record["click_time"] = hit_time + (hash(str(i) + "click_time") % 10)
        else:
            record["click_time"] = 0

        if has_show:
            record["show_time"] = hit_time
        else:
            record["show_time"] = 0
        output.append(record)
    return output


def generate_expected_output(data):
    output = []
    for row in data:
        # we want only hits that lead at least to show
        if row["show_time"] == 0:
            continue
        output.append(
            {
                "hit_id": row["hit_id"],
                "hit_time": row["hit_time"],
                "is_click": row["click_time"] != 0,
                "show_time": row["show_time"],
                "click_time": row["click_time"],
                "hit_payload": row["hit_payload"],
            }
        )
    return output


DATA = generate_data(TOTAL_ACTIONS)

EXPECTED_OUTPUT = generate_expected_output(DATA)


def generate_hit_log(tablet_count):
    result = []
    for record in DATA:
        result.append(
            {
                "$tablet_index": hash(record["hit_id"] + "salt_hit") % tablet_count,
                "hit_id": record["hit_id"],
                "hit_time": record["hit_time"],
                "hit_payload": record["hit_payload"],
            }
        )

    result.sort(key=lambda x: (x["hit_time"] + (int(hash(x["hit_id"] + "shuffle_hit")) % SHUFFLE_WINDOW)))
    return result


def generate_action_log(tablet_count):
    result = []
    for record in DATA:
        if record["show_time"] != 0:
            result.append(
                {
                    "$tablet_index": hash(record["hit_id"] + "salt_show") % tablet_count,
                    "hit_id": record["hit_id"],
                    "hit_time": record["hit_time"],
                    "action_time": record["show_time"],
                    "is_click": False,
                }
            )
        if record["click_time"] != 0:
            result.append(
                {
                    "$tablet_index": hash(record["hit_id"] + "salt_click") % tablet_count,
                    "hit_id": record["hit_id"],
                    "hit_time": record["hit_time"],
                    "action_time": record["click_time"],
                    "is_click": True,
                }
            )

    result.sort(key=lambda x: (x["action_time"] + (int(hash(x["hit_id"] + "shuffle_action")) % SHUFFLE_WINDOW)))
    return result


class TestWaitClickJoin(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(
        "yt/yt/flow/examples/python/wait_click_join/wait_click_join"
    )

    def setup_method(self, method):
        super(TestWaitClickJoin, self).setup_method(method)
        self.replica_cluster_name = self.remote_cluster_names[0]
        self.action_queue = self.work_yt_path + "/action_queue"
        self.hit_queue = self.work_yt_path + "/hit_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.output_producer = self.work_yt_path + "/producer"
        self.join_state = self.work_yt_path + "/join_state"

    def prepare_environment(self):
        tablet_count = 5
        run_yt_sync("primary", self.work_yt_path, tablet_count)

        batching_write_rows(generate_action_log(tablet_count), lambda batch: self.client.insert_rows(self.action_queue, batch), 100)

        batching_write_rows(generate_hit_log(tablet_count), lambda batch: self.client.insert_rows(self.hit_queue, batch), 100)

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["action_reader"]["source_streams"]["action_queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.action_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["hit_reader"]["source_streams"]["hit_queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.hit_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": True,
            }
        )

        pipeline_config["spec"]["computations"]["join"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
                "producer_path": f"<cluster=primary>{self.output_producer}",
            }
        )
        pipeline_config["spec"]["computations"]["join"]["external_state_managers"]["/join-state"]["parameters"][
            "path"
        ] = self.join_state

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(1, 1, id="1c_1w"),
        ],
    )
    def test_basic(self, workers_count, controllers_count):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()
        with self.start_flow_process_federation(
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=workers_count,
            controllers_count=controllers_count,
        ):
            self.wait_pipeline_state("completed")

            expr = f"hit_id, hit_time, is_click, show_time, click_time, hit_payload from [{self.output_queue}]"
            rows = list(self.client.select_rows(expr))
            assert len(EXPECTED_OUTPUT) == len(rows)
            expected = {row["hit_id"]: row for row in EXPECTED_OUTPUT}
            got = {row["hit_id"]: row for row in rows}

            # full comparison may be very slow
            keys = sorted(expected.keys())[:100]
            for key in keys:
                logging.info("Expected item: %s", expected[key])
                logging.info("     Got item: %s", got[key])
                assert expected[key] == got[key]

            for key, expected_value in expected.items():
                assert expected_value == got.get(key, {})
            expected_len = len(expected)
            got_len = len(got)
            assert expected_len == got_len

            expr = f"* from [{self.join_state}]"
            key_states = list(self.client.select_rows(expr))
            assert len(key_states) == 0

            expr = f"* from [{self.pipeline_path}/partition_states]"
            partition_states = list(self.client.select_rows(expr))
            assert len(partition_states) == 0
