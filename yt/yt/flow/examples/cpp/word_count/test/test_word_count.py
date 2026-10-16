import logging

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline.yson")

if yatest.common.context.sanitize:
    TOTAL_EVENTS = 100
else:
    TOTAL_EVENTS = 1000

##################################################################


def build_expected_counts(sentences):
    counts = {}
    for sentence in sentences:
        for word in sentence.split():
            counts[word] = counts.get(word, 0) + 1
    return counts


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../word_count")

    def prepare_environment(self, input_queue, input_consumer, word_counts):
        from .yt_sync import run_yt_sync

        run_yt_sync("primary", self.work_yt_path, queue_tablet_count=5)

    def prepare_pipeline_config(self, input_queue, input_consumer, word_counts):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{input_queue}",
                "consumer_path": f"<cluster=primary>{input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["counter"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = word_counts

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(1, 1, id="1c_1w"),
            pytest.param(4, 2, id="2c_4w"),
        ],
    )
    def test_basic(self, workers_count, controllers_count):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        word_counts = self.work_yt_path + "/word_counts"

        self.prepare_environment(input_queue, input_consumer, word_counts)

        sentences = [f"hello world sentence {i}" for i in range(TOTAL_EVENTS)]
        rows = [{"text": s, "$tablet_index": i % 5} for i, s in enumerate(sentences)]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(input_queue, batch), 100)

        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer, word_counts)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
        ):
            self.wait_pipeline_state("completed", timeout=180)

        result = list(self.client.select_rows(f"* from [{word_counts}]"))
        logging.info("word_count result rows: %d", len(result))
        assert len(result) > 0
