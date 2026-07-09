"""E2E test for the Python wordcount companion."""

import logging
import random
import string

import pytest
import yatest.common
import yt.yson as yson

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    "yt/yt/flow/examples/python/word_count/test/pipeline.yson"
)

if yatest.common.context.sanitize is not None:
    TOTAL_EVENTS = 100
else:
    TOTAL_EVENTS = 1500


def generate_log(tablet_count):
    expected_counts = {}
    result = []
    for i in range(TOTAL_EVENTS):
        word = ''.join(random.choices(string.ascii_lowercase, k=(random.randint(1, 4))))
        expected_counts[word] = expected_counts.get(word, 0) + 1
        result.append(
            {
                "word": word,
                "$tablet_index": i % tablet_count,
            }
        )
    return result, expected_counts


# [BEGIN test_setup]
class Test(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(
        "yt/yt/flow/examples/python/word_count/word_count"
    )
# [END test_setup]

    def prepare_environment(self, input_queue):
        tablet_count = 5
        run_yt_sync("primary", self.work_yt_path, tablet_count)
        logs, expected_counts = generate_log(tablet_count)
        batching_write_rows(logs, lambda batch: self.client.insert_rows(input_queue, batch), 1000)
        self._expected_counts = expected_counts

    def prepare_pipeline_config(self, input_queue, input_consumer, run_process=False):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{input_queue}",
                "consumer_path": f"<cluster=primary>{input_consumer}",
                "finite": True,
            }
        )
        if run_process:
            pipeline_config["spec"]["resources"]["CompanionManager"]["parameters"]["entrypoint"] = {
                "executable": self.PYTHON_COMPANION_BINARY,
            }
            pipeline_config["spec"]["resources"]["CompanionManager"]["parameters"]["run_process"] = run_process

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
        self.prepare_environment(input_queue)
        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer)

        with self.start_flow_process_federation(
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=workers_count,
            controllers_count=controllers_count,
        ):
            self.wait_pipeline_state("completed")
            logging.info("pipeline completed")
            got_counts = {}
            expr = f"* from [{self.pipeline_path}/states]"
            key_states = list(self.client.select_rows(expr))
            for state in key_states:
                yson_payload = yson.loads(state['state']['payload'].encode())
                got_counts[yson_payload['word']] = yson_payload['count']
            logging.info("Got counts: %s", got_counts)
            logging.info("Expected counts: %s", self._expected_counts)
            assert self._expected_counts == got_counts

            logging.info("check completed")

    @pytest.mark.authors(["mikari"])
    def test_vanilla_jobs(self):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        self.prepare_environment(input_queue)
        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            use_vanilla_jobs=True,
        ):
            # Vanilla startup is slow: heavy binary upload + cold flow_server boot.
            self.wait_pipeline_state("completed", timeout=600)
            got_counts = {}
            expr = f"* from [{self.pipeline_path}/states]"
            for state in self.client.select_rows(expr):
                yson_payload = yson.loads(state['state']['payload'].encode())
                got_counts[yson_payload['word']] = yson_payload['count']
            assert self._expected_counts == got_counts

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(4, 1, id="1c_4w"),
        ],
    )
    def test_managed(self, workers_count, controllers_count):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        self.prepare_environment(input_queue)
        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer, run_process=True)

        with self.start_flow_process_federation(
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=workers_count,
            controllers_count=controllers_count,
        ):
            self.wait_pipeline_state("completed")
            logging.info("pipeline completed")
            got_counts = {}
            expr = f"* from [{self.pipeline_path}/states]"
            key_states = list(self.client.select_rows(expr))
            for state in key_states:
                yson_payload = yson.loads(state['state']['payload'].encode())
                got_counts[yson_payload['word']] = yson_payload['count']
            logging.info("Got counts: %s", got_counts)
            logging.info("Expected counts: %s", self._expected_counts)
            assert self._expected_counts == got_counts

            logging.info("check completed")
