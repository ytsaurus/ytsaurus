import logging

import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

# Enough sync-recorded epochs to guarantee the pipeline ran well past the single input
# batch: with batch_duration = 100 most of them are epochs with an empty batch, which is
# exactly the path where a stale or absent epoch seqno would reach the sync phase.
MIN_RECORDED_EPOCHS = 20

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def prepare_pipeline_config(self, input_queue, input_consumer, output_queue, seq_nos):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        probe = pipeline_config["spec"]["computations"]["probe"]
        probe["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{input_queue}",
                "consumer_path": f"<cluster=primary>{input_consumer}",
            }
        )
        probe["sinks"]["queue"]["parameters"]["queue_path"] = output_queue
        probe["processing_function_parameters"]["seq_nos_table_path"] = seq_nos

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    def test_sync_sees_fresh_epoch_seqno_every_epoch(self):
        input_queue = self.work_yt_path + "/input_queue"
        input_consumer = self.work_yt_path + "/consumer"
        output_queue = self.work_yt_path + "/output_queue"
        seq_nos = self.work_yt_path + "/seq_nos"

        run_yt_sync("primary", self.work_yt_path, queue_tablet_count=1)

        pipeline_config_path = self.prepare_pipeline_config(input_queue, input_consumer, output_queue, seq_nos)

        def recorded_epochs():
            return list(self.client.select_rows(f"seq_no from [{seq_nos}]"))

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            rows = [{"text": "hello flow world", "$tablet_index": 0}]
            batching_write_rows(rows, lambda batch: self.client.insert_rows(input_queue, batch), 100)

            # The probe's sync phase throws (crash-looping the epoch) unless every epoch —
            # including the many idle ones after the single batch above — observes a fresh,
            # strictly increasing seqno, so the recorded-epoch count keeps growing only while
            # that invariant holds.
            wait(
                lambda: len(recorded_epochs()) >= MIN_RECORDED_EPOCHS,
                timeout=120,
                error_message="the pipeline stopped recording epoch seqnos from the sync phase",
            )

            words = sorted(row["word"] for row in self.client.select_rows(f"word from [{output_queue}]"))

        recorded = recorded_epochs()
        logging.info("recorded_epochs=%s words=%s", len(recorded), words)

        assert len(recorded) >= MIN_RECORDED_EPOCHS
        assert words == ["flow", "hello", "world"]
