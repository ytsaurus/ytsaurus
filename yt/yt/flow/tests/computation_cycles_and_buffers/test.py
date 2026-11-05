import pytest

from yt.yt.flow.tests.computation_cycles_and_buffers.lib.test_base import TestBase, EVENT_COUNT

from yt.common import wait

##################################################################


class Test(TestBase):
    WARM_OUTPUT_BUFFER_LIMIT = "64Ki"
    WARM_OUTPUT_BUFFER_LIMIT_BYTES = 64 * 1024
    WARM_OUTPUT_BUFFER_USED_THRESHOLD_BYTES = 48 * 1024
    CUT_OUTPUT_BUFFER_LIMIT = "32Ki"
    CUT_OUTPUT_BUFFER_LIMIT_BYTES = 32 * 1024

    def _get_transform_a_output_buffers(self):
        view = self.client.get_flow_view(self.pipeline_path, cache=False)
        partitions = view["state"]["execution_spec"]["layout"]["partitions"]
        statuses = view["feedback"]["partition_job_statuses"]
        for partition_id, partition in partitions.items():
            if partition["computation_id"] != "transform_a":
                continue
            job_status = statuses.get(partition_id, {}).get("current_job_status", {})
            output_limits = job_status.get("output_limits", {}).get("output_buffer_bytes", {})
            if "ta1" in output_limits and "ta2" in output_limits:
                return output_limits
        return {}

    def _wait_output_buffers_warm(self):
        def output_buffers_are_warm():
            buffers = self._get_transform_a_output_buffers()
            return (
                buffers
                and buffers["ta1"].get("limit", 0) >= self.WARM_OUTPUT_BUFFER_LIMIT_BYTES
                and buffers["ta1"].get("used", 0) >= self.WARM_OUTPUT_BUFFER_USED_THRESHOLD_BYTES
                and buffers["ta2"].get("limit", 0) >= self.WARM_OUTPUT_BUFFER_LIMIT_BYTES
                and buffers["ta2"].get("used", 0) <= self.CUT_OUTPUT_BUFFER_LIMIT_BYTES
            )

        wait(output_buffers_are_warm, timeout=180)

    def _wait_output_buffers_cut(self):
        def output_buffers_are_cut():
            buffers = self._get_transform_a_output_buffers()
            return (
                buffers
                and buffers["ta1"].get("limit", 0) <= self.CUT_OUTPUT_BUFFER_LIMIT_BYTES
                and buffers["ta1"].get("used", 0) > buffers["ta1"].get("limit", 0)
                and buffers["ta2"].get("limit", 0) <= self.CUT_OUTPUT_BUFFER_LIMIT_BYTES
                and buffers["ta2"].get("used", 0) <= buffers["ta2"].get("limit", 0)
            )

        wait(output_buffers_are_cut, timeout=180)

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
        pipeline_config_path = self.prepare_pipeline_config(
            processing_mode=processing_mode,
            throttled_computation="swift_map_b" if cut_buffers else None,
            output_buffer_limit=self.WARM_OUTPUT_BUFFER_LIMIT if cut_buffers else None,
        )
        expr = f"* from [{self.state}]"
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            if cut_buffers:
                # Blocking the cycle tail fills ta1 while ta2 remains independently drainable.
                self._wait_output_buffers_warm()

                # Reproduce the old shared-output-limit deadlock: ta1 stays above the
                # new limit, while the free ta2 stream must remain usable by sb1.
                self.set_output_buffer_limit(self.CUT_OUTPUT_BUFFER_LIMIT)
                self._wait_output_buffers_cut()

                # Releasing swift_map_b sends sb1 back into transform_a. Per-stream
                # limits let it leave through ta2 even while ta1 is still over limit.
                self.release_input_throttler("swift_map_b")

            self.wait_pipeline_state("completed", timeout=180)

            rows = list(self.client.select_rows(expr))

            assert len(rows) == 1
            row = rows[0]
            assert row["data"] == "payload"
            if processing_mode == "exactly_once":
                assert row["count"] == EVENT_COUNT
            else:
                assert row["count"] >= EVENT_COUNT
