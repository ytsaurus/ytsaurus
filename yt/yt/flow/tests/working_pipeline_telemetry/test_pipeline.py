import os

import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

FAIL_KEY = "42"
FAIL_COMMENT = "TELEMETRY_TEST_INTENTIONAL_FAIL"

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["parameters"].update({
            "fail_key": FAIL_KEY,
            "fail_comment": FAIL_COMMENT,
        })

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["pechatnov"])
    def test_telemetry(self):
        run_yt_sync("primary", self.work_yt_path)
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_pipeline_state("working")

            def check_job_fail_error():
                description = self.client.flow_execute(self.pipeline_path, "describe-pipeline")
                assert FAIL_COMMENT in str(description["computations"]["reader"]["messages"])
                return True

            wait(lambda: check_job_fail_error(), timeout=180, ignore_exceptions=True)

            def find_job_status(computation_id, filter_func):
                flow_view = self.client.get_flow_view(self.pipeline_path, cache=False)
                partitions = flow_view["state"]["execution_spec"]["layout"]["partitions"]
                partition_job_statuses = flow_view["feedback"]["partition_job_statuses"]
                for partition_id, partition in partitions.items():
                    if partition["computation_id"] != computation_id:
                        continue
                    job_status = partition_job_statuses.get(partition_id, {}).get("current_job_status")
                    if not job_status:
                        continue
                    if filter_func(job_status):
                        return job_status
                return None

            def check_epoch_part_times(job_status):
                return sum(job_status.get("epoch_part_times", {}).values()) > 0

            wait(lambda: find_job_status("reader", check_epoch_part_times), timeout=180)

            def check_input_limits(job_status):
                input_buffer = job_status.get("input_limits", {}).get("input_buffer_bytes", {})
                return sum(v.get("used", 0) for v in input_buffer.values()) > 0

            wait(lambda: find_job_status("processor", check_input_limits), timeout=180)

            def get_output_limits_checker(name):
                def checker(job_status, name=name):
                    return sum(v.get("used", 0) for v in job_status.get("output_limits", {}).get(name, {}).values()) > 0
                return checker

            wait(lambda: find_job_status("reader", get_output_limits_checker("output_buffer_bytes")), timeout=180)
            wait(lambda: find_job_status("reader", get_output_limits_checker("output_store_bytes")), timeout=180)
            wait(lambda: find_job_status("reader", get_output_limits_checker("output_store_count")), timeout=180)

            # Test get-worker-backtraces.
            if yatest.common.context.sanitize is None:
                workers = self.client.flow_execute(self.pipeline_path, "describe-workers")
                assert len(workers["workers"]) > 0
                worker_address = workers["workers"][0]["address"]
                res = self.client.flow_execute(self.pipeline_path, "get-worker-backtraces", {"worker": worker_address})
                with open(os.path.join(self.path_to_flow_logs, "get_worker_backtraces.txt"), "w") as f:
                    f.write(res["text"])
                assert isinstance(res["text"], str) and len(res["text"]) > 0

            # TODO: Test computation retryable errors.
            # TODO: Test metrics.
