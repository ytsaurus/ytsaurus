import os
import re

import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

FAIL_COMMENT = "TELEMETRY_TEST_INTENTIONAL_FAIL"

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)

    def prepare_pipeline_config(
        self, fail_comment=None, commit_gate=None, worker_lease_timeout=None, reader_class=None
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        if reader_class is not None:
            pipeline_config["spec"]["computations"]["reader"]["computation_class_name"] = reader_class
        if fail_comment is not None:
            pipeline_config["spec"]["computations"]["reader"]["parameters"]["fail_comment"] = fail_comment
        if commit_gate is not None:
            ready_path, release_path = commit_gate
            reader_parameters = pipeline_config["spec"]["computations"]["reader"]["parameters"]
            reader_parameters["commit_gate_ready_path"] = ready_path
            reader_parameters["commit_gate_release_path"] = release_path
            reader_parameters["lineage_commit_path"] = ready_path + ".reader"
            pipeline_config["spec"]["computations"]["processor"]["parameters"]["lineage_commit_path"] = (
                ready_path + ".processor"
            )
        self.patch_config(pipeline_config)
        if worker_lease_timeout is not None:
            pipeline_config["dynamic_spec"]["job_manager"]["lost_job_timeout"] = worker_lease_timeout

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["mikari"])
    def test_fully_filtered_input_lineage(self):
        run_yt_sync("primary", self.work_yt_path)
        observation_path = os.path.join(self.path_to_flow_logs, "filtered_lineage_commit")
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        config["spec"]["computations"]["processor"]["parameters"]["lineage_commit_path"] = observation_path
        processor = config["dynamic_spec"]["computations"]["processor"]
        processor["skip_if_expression"] = "true"
        processor["parameters"]["desired_partition_count"] = 1
        self.patch_config(config)
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")

        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            # Observe the actual committed delta, without waiting for the lineage EMA to mature.
            wait(lambda: os.path.exists(observation_path), timeout=120)
            with open(observation_path) as observation:
                output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
            assert input_count > 0
            assert input_bytes > 0
            assert output_count == 0
            assert output_bytes == 0

    @pytest.mark.authors(["pechatnov"])
    def test_telemetry(self):
        run_yt_sync("primary", self.work_yt_path)
        commit_gate_ready_path = os.path.join(self.path_to_flow_logs, "lineage_commit_gate_ready")
        commit_gate_release_path = os.path.join(self.path_to_flow_logs, "lineage_commit_gate_release")
        pipeline_config_path = self.prepare_pipeline_config(
            commit_gate=(commit_gate_ready_path, commit_gate_release_path)
        )

        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_pipeline_state("working")

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

            wait(lambda: os.path.exists(commit_gate_ready_path), timeout=60)

            def get_lineage_rates():
                flow_view = self.client.get_flow_view(self.pipeline_path, cache=False)
                return flow_view.get("ephemeral_state", {}).get("lineage_rates", {})

            def has_lineage_edge(output_stream, parent_stream):
                return parent_stream in get_lineage_rates().get(output_stream, {})

            # The first source epoch has prepared output but has not committed it yet.
            assert not has_lineage_edge("data", "reader/random")
            assert not os.path.exists(commit_gate_ready_path + ".reader")

            with open(commit_gate_release_path, "w"):
                pass

            wait(lambda: find_job_status("reader", check_epoch_part_times), timeout=180)

            # Read the real job commit deltas without waiting for the lineage EMA to mature.
            for computation in ("reader", "processor"):
                observation_path = commit_gate_ready_path + "." + computation
                wait(lambda: os.path.exists(observation_path), timeout=180)
                with open(observation_path) as observation:
                    output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
                assert output_count == input_count > 0
                assert output_bytes > 0
                assert input_bytes > 0

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

            # Lineage statistics are sent independently from regular job status heartbeats.
            wait(lambda: has_lineage_edge("data", "reader/random"), timeout=180)
            wait(lambda: has_lineage_edge("processed_data", "data"), timeout=180)

            flow_view = self.client.get_flow_view(self.pipeline_path, cache=False)
            assert all(
                not worker_status.get("statistics")
                for worker_status in flow_view.get("feedback", {}).get("worker_statuses", {}).values()
            )

            # TODO: Test computation retryable errors.
            # TODO: Test metrics.

    @pytest.mark.authors(["pechatnov"])
    def test_source_replay_lineage(self):
        run_yt_sync("primary", self.work_yt_path)
        ready_path = os.path.join(self.path_to_flow_logs, "replay_ready")
        release_path = os.path.join(self.path_to_flow_logs, "replay_release")
        pipeline_config_path = self.prepare_pipeline_config(
            commit_gate=(ready_path, release_path), reader_class="TReplayReader"
        )
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: os.path.exists(ready_path), timeout=60)
            assert not os.path.exists(ready_path + ".reader")
            with open(release_path, "w"):
                pass
            wait(lambda: os.path.exists(ready_path + ".reader"), timeout=60)
            with open(ready_path + ".reader") as observation:
                output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
            assert output_count == input_count > 0
            assert output_bytes > 0 and input_bytes > 0
            with open(os.path.join(self.path_to_flow_logs, "Worker_0_FilteredDebug.log")) as worker_log:
                publications = re.findall(
                    r"Publishing batch \([^\n]*SourceBatches: ([0-9]+), Parsed: [0-9]+, Outputs: ([0-9]+)",
                    worker_log.read(),
                )
            assert any(int(inputs) > 0 for inputs, _ in publications)
            assert all(int(outputs) == 0 for _, outputs in publications)

    @pytest.mark.authors(["pechatnov"])
    def test_worker_backtraces(self):
        if yatest.common.context.sanitize is not None:
            pytest.skip("Backtrace introspection is not supported with sanitizers")

        run_yt_sync("primary", self.work_yt_path)
        # Symbolization may block heartbeats beyond the default five-second test lease.
        pipeline_config_path = self.prepare_pipeline_config(worker_lease_timeout="30s")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: self.client.flow_execute(self.pipeline_path, "describe-workers")["workers"])
            workers = self.client.flow_execute(self.pipeline_path, "describe-workers")
            assert len(workers["workers"]) > 0
            worker_address = workers["workers"][0]["address"]
            res = self.client.flow_execute(self.pipeline_path, "get-worker-backtraces", {"worker": worker_address})
            with open(os.path.join(self.path_to_flow_logs, "get_worker_backtraces.txt"), "w") as f:
                f.write(res["text"])
            assert isinstance(res["text"], str) and len(res["text"]) > 0

    @pytest.mark.authors(["timoninmaxim"])
    def test_job_failure(self):
        run_yt_sync("primary", self.work_yt_path)
        pipeline_config_path = self.prepare_pipeline_config(fail_comment=FAIL_COMMENT)

        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_pipeline_state("working")

            def check_job_fail_error():
                description = self.client.flow_execute(self.pipeline_path, "describe-pipeline")
                # Only a real job failure counts; the "Spec" message echoes fail_comment unconditionally.
                job_fail_messages = [
                    message
                    for message in description["computations"]["reader"]["messages"]
                    if str(message.get("text", "")).startswith("Job failed")
                ]
                assert any(FAIL_COMMENT in str(message) for message in job_fail_messages)
                return True

            wait(lambda: check_job_fail_error(), timeout=180, ignore_exceptions=True)
