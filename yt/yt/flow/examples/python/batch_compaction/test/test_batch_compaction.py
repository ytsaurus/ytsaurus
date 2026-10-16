"""E2E test for the Python batch compaction companion (Swift + explicit lineage)."""

import logging
import random
import string

import pytest
import yatest.common
import yt.yson as yson

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path("yt/yt/flow/examples/python/batch_compaction/test/pipeline.yson")

if yatest.common.context.sanitize is not None:
    TOTAL_EVENTS = 100
else:
    TOTAL_EVENTS = 1500


def generate_log(tablet_count):
    expected_totals = {}
    result = []
    for i in range(TOTAL_EVENTS):
        word = ''.join(random.choices(string.ascii_lowercase, k=(random.randint(1, 3))))
        count = random.randint(1, 5)
        expected_totals[word] = expected_totals.get(word, 0) + count
        result.append(
            {
                "word": word,
                "count": count,
                "$tablet_index": i % tablet_count,
            }
        )
    return result, expected_totals


class Test(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path("yt/yt/flow/examples/python/batch_compaction/batch_compaction")

    def prepare_environment(self, input_queue):
        tablet_count = 5
        run_yt_sync("primary", self.work_yt_path, tablet_count)
        logs, expected_totals = generate_log(tablet_count)
        self.client.insert_rows(input_queue, logs)
        self._expected_totals = expected_totals

    def prepare_pipeline_config(self, input_queue, input_consumer):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{input_queue}",
                "consumer_path": f"<cluster=primary>{input_consumer}",
                "finite": True,
            }
        )

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["timoninmaxim"])
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
            got_totals = {}
            expr = f"* from [{self.pipeline_path}/states]"
            for state in self.client.select_rows(expr):
                yson_payload = yson.loads(state['state']['payload'].encode())
                got_totals[yson_payload['word']] = yson_payload['count']
            logging.info("Got totals: %s", got_totals)
            logging.info("Expected totals: %s", self._expected_totals)
            assert self._expected_totals == got_totals
