import logging

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

# min_word_length in the spec is 4, so words shorter than 4 characters are skipped.
SENTENCES = [
    "hello to a world",
    "flow is on it",
]
EXPECTED_COUNTS = {"hello": 1, "world": 1, "flow": 1}
EXPECTED_SKIPPED = {"to": 2, "a": 1, "is": 2, "on": 2, "it": 2}

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def prepare_pipeline_config(self, input_queue, input_consumer, word_counts, skipped_words):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{input_queue}",
                "consumer_path": f"<cluster=primary>{input_consumer}",
                "finite": True,
            }
        )
        counter = pipeline_config["spec"]["computations"]["counter"]
        counter["processing_function_parameters"]["skipped_words_table_path"] = skipped_words
        counter["external_state_managers"]["/state"]["parameters"]["path"] = word_counts

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    def test_skipped_words_written_in_sync(self):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        word_counts = self.work_yt_path + "/word_counts"
        skipped_words = self.work_yt_path + "/skipped_words"

        run_yt_sync("primary", self.work_yt_path, queue_tablet_count=1)

        rows = [{"text": sentence, "$tablet_index": 0} for sentence in SENTENCES]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(input_queue, batch), 100)

        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer, word_counts, skipped_words)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=180)

        counts = {row["word"]: row["count"] for row in self.client.select_rows(f"word, count from [{word_counts}]")}
        skipped = {
            row["word"]: row["length"] for row in self.client.select_rows(f"word, length from [{skipped_words}]")
        }
        logging.info("counts=%s skipped=%s", counts, skipped)

        # Long words are counted; short words are skipped and written into the skipped table by Sync.
        assert counts == EXPECTED_COUNTS
        assert skipped == EXPECTED_SKIPPED
