import pytest
import time

from yt.yt.flow.tests.computation_cycles_and_buffers.lib.test_base import TestBase, EVENT_COUNT

from yt.common import wait

##################################################################


class Test(TestBase):
    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize(
        ("cut_buffers", "processing_mode"),
        [
            pytest.param(False, "exactly_once", id="exactly_once"),
            pytest.param(True, "exactly_once", id="exactly_once_cut_buffers"),
            pytest.param(False, "at_least_once_consistent", id="at_least_once_consistent"),
            # pytest.param(False, "at_least_once_relaxed", id="at_least_once_relaxed"),
        ],
    )
    def test_work(self, cut_buffers, processing_mode):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(processing_mode=processing_mode)
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            if cut_buffers:
                time.sleep(30)
                assert (
                    self.client.get_pipeline_state(self.pipeline_path) != "completed"
                ), "Can't complete so fast with current throttling options"

                self.client.pause_pipeline(self.pipeline_path)
                wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "paused")

                # Buffers are automatically cut here because of restart and zeroying of stream demands.

                self.client.start_pipeline(self.pipeline_path)

            self.wait_pipeline_state("completed", timeout=180)

            expr = f"* from [{self.state}]"
            rows = list(self.client.select_rows(expr))

            assert len(rows) == 1
            row = rows[0]
            assert row["data"] == "payload"
            if processing_mode == "exactly_once":
                assert row["count"] == EVENT_COUNT
            else:
                assert row["count"] >= EVENT_COUNT
