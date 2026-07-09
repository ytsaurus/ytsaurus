"""E2E test for the Python shuffle companion."""

import json
import logging

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    "yt/yt/flow/examples/python/shuffle/test/pipeline.yson"
)

if yatest.common.context.sanitize is not None:
    TOTAL_EVENTS = 100
else:
    TOTAL_EVENTS = 1500

#################################################################


def generate_log(tablet_count):
    result = []
    for i in range(TOTAL_EVENTS):
        value = f"data_{i}"
        row = {
            "value": value,
            "key_a": hash(value + "_a") % 10,
            "key_b": hash(value + "_b") % 10,
            "key_c": hash(value + "_c") % 10,
            "key_d": hash(value + "_d") % 10,
        }
        result.append(
            {
                "data": json.dumps(row),
                "$tablet_index": i % tablet_count,
            }
        )
    return result


class Test(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(
        "yt/yt/flow/examples/python/shuffle/shuffle"
    )

    def prepare_environment(self, input_queue):
        tablet_count = 5
        run_yt_sync("primary", self.work_yt_path, tablet_count)
        batching_write_rows(generate_log(tablet_count), lambda batch: self.client.insert_rows(input_queue, batch), 1000)

    def prepare_pipeline_config(self, input_queue, input_consumer, data_state):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{input_queue}",
                "consumer_path": f"<cluster=primary>{input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["reducer"]["external_state_managers"]["/shuffle-state"]["parameters"][
            "path"
        ] = data_state
        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(1, 1, id="1c_1w"),
            pytest.param(4, 1, id="1c_4w"),
        ],
    )
    def test_basic(self, workers_count, controllers_count):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        data_state = self.work_yt_path + "/data_state"
        self.prepare_environment(input_queue)
        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer, data_state)

        with self.start_flow_process_federation(
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=workers_count,
            controllers_count=controllers_count,
        ):
            self.wait_pipeline_state("completed")
            logging.info("pipeline completed")

            expr = f"* from [{data_state}]"
            rows = list(self.client.select_rows(expr))
            logging.info("Rows: %s", rows)
            got = dict()
            for row in rows:
                got[row["value"]] = row["count"]
            expected = dict((f"data_{i}", 4) for i in range(TOTAL_EVENTS))
            logging.info("Expected: %s", expected)
            logging.info("     Got: %s", got)

            for key, expected_value in expected.items():
                assert expected_value == got.get(key, ())
            expected_len = len(expected)
            got_len = len(got)
            assert expected_len == got_len
            logging.info("check completed")
