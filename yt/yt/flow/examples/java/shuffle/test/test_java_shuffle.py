import json
import logging
import pytest

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_java_base import FlowTestJavaBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows
from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/../shuffle/src/main/resources/pipeline.yson"
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


class Test(FlowTestJavaBase):
    JAVA_RUNNER_BINARY_DIR = yatest.common.binary_path(f"{yatest.common.context.project_path}/../shuffle/")
    JAVA_RUNNER_MAIN_CLASS = "tech.ytsaurus.flow.examples.shuffle.RunnerMain"

    def prepare_environment(self, input_queue, input_consumer):
        tablet_count = 5
        run_yt_sync("primary", self.work_yt_path, tablet_count)
        batching_write_rows(generate_log(tablet_count), lambda batch: self.client.insert_rows(input_queue, batch), 1000)

    def prepare_pipeline_config(self, input_queue, input_consumer, data_state, run_vanilla=False, persist_ratio=0.0):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{input_queue}",
                "consumer_path": f"<cluster=primary>{input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["reader"]["parameters"].update(
            {
                "persist_ratio": persist_ratio,
            }
        )
        pipeline_config["spec"]["computations"]["reducer"]["external_state_managers"]["/shuffle-state"]["parameters"][
            "path"
        ] = data_state
        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def _get_partitions_count(self):
        return len(self.client.get_flow_view(self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False))

    def _change_partition_count(self):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        for computation in ["shuffle_a", "shuffle_b", "shuffle_c", "shuffle_d", "reducer"]:
            dynamic_spec["spec"]["computations"][computation]["parameters"]["desired_partition_count"] += 1
        self.client.set_pipeline_dynamic_spec(self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"])

    def _wait_epoch_sync(self):
        def epoch_is_sync():
            united_epoch = self.client.get_flow_view(self.pipeline_path, view_path="/state/traverse_data/united_stream/epoch", cache=False)
            epoch = self.client.get_flow_view(self.pipeline_path, view_path="/state/epoch", cache=False)
            return epoch == united_epoch and epoch > 0

        wait(epoch_is_sync, timeout=180, ignore_exceptions=True)

    def _test_basic(self, federation):
        # Path to internal yson state.
        data_state = self.work_yt_path + "/data_state"
        with federation:
            self.wait_pipeline_state("completed", timeout=180)
            logging.info("pipeline completed")

            expr = f"* from [{data_state}]"
            rows = list(self.client.select_rows(expr))
            logging.info("Rows:     %s", rows)
            got = dict()
            for row in rows:
                got[row["value"]] = row["count"]
            expected = dict((f"data_{i}", 4) for i in range(TOTAL_EVENTS))
            logging.info("prepared got and expected")
            logging.info("Expected: %s", expected)
            logging.info("     Got: %s", got)

            for key, expected_value in expected.items():
                assert expected_value == got.get(key, ())
            expected_len = len(expected)
            got_len = len(got)
            assert expected_len == got_len
            logging.info("check completed")

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            pytest.param(1, 1, False, id="1c_1w_stable"),
            pytest.param(4, 1, False, id="1c_4w_stable"),
            pytest.param(4, 2, True, id="2c_4w_unstable"),
        ],
    )
    def test_basic(self, workers_count, controllers_count, problems):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        data_state = self.work_yt_path + "/data_state"
        self.prepare_environment(input_queue, input_consumer)
        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer, data_state)

        self._test_basic(
            federation=self.start_flow_process_federation(
                pipeline_binary_args={
                    "--config": pipeline_config_path,
                },
                workers_count=workers_count,
                controllers_count=controllers_count,
                problems=problems,
            ),
        )

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "persist_ratio", "problems"),
        [
            pytest.param(4, 1, 0.5, False, id="1c_4w_half_persist_stable"),
        ],
    )
    def test_persist(self, workers_count, controllers_count, persist_ratio, problems):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        data_state = self.work_yt_path + "/data_state"
        self.prepare_environment(input_queue, input_consumer)
        pipeline_config_path = self.prepare_pipeline_config(
            input_queue, input_consumer, data_state, persist_ratio=persist_ratio
        )

        self._test_basic(
            federation=self.start_flow_process_federation(
                pipeline_binary_args={
                    "--config": pipeline_config_path,
                },
                workers_count=workers_count,
                controllers_count=controllers_count,
                problems=problems,
            ),
        )
