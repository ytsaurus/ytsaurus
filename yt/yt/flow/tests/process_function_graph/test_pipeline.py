import copy
from pathlib import Path

import pytest
import yt.wrapper as yt
import yatest.common

from yt.common import wait
from yt.yt.flow.library.python.bullied_process import ProcessExitedNormallyException
from yt.yt.flow.library.python.client.state_unpack import unpack_state
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync


class TestProcessFunctionGraph(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    @property
    def config_path(self):
        return yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

    def prepare_pipeline_config(self, configure=None) -> str:
        config = get_yson_config(self.config_path)
        for name in ("left", "right"):
            source = config["spec"]["computations"][f"{name}_reader"]["source_streams"]["queue"]
            source["parameters"].update(
                {
                    "queue_path": f"<cluster=primary>{self.work_yt_path}/{name}_queue",
                    "consumer_path": f"<cluster=primary>{self.work_yt_path}/{name}_consumer",
                    "finite": True,
                }
            )

        sink = config["spec"]["computations"]["graph"]["sinks"]["queue"]
        sink["parameters"]["queue_path"] = f"{self.work_yt_path}/output_queue"
        if configure is not None:
            configure(config)
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    def test_state_and_timers_after_fan_in(self) -> None:
        run_yt_sync("primary", self.work_yt_path)
        for name in ("left", "right"):
            path = f"{self.work_yt_path}/{name}_queue"
            batching_write_rows(
                [{"key": 7, "$tablet_index": 0}],
                lambda rows: self.client.insert_rows(path, rows),
                100,
            )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.prepare_pipeline_config()},
        ):
            self.wait_pipeline_state("completed", timeout=180)

        self.check_result()

    def output_rows(self):
        return list(self.client.select_rows(f"key, count, branch_count, kind from [{self.work_yt_path}/output_queue]"))

    def graph_states(self):
        rows = self.client.select_rows(f'* from [{self.pipeline_path}/states] where computation_id = "graph"')
        return {
            row["name"].lstrip("/"): unpack_state(row)
            for row in rows
            if row["name"].lstrip("/").startswith("functions/")
        }

    def check_result(self):
        rows = self.output_rows()
        assert len(rows) == 4
        assert {row["key"] for row in rows} == {7}
        assert sorted(row["count"] for row in rows) == [1, 2, 3, 4]
        assert sorted(row["branch_count"] for row in rows) == [2, 2, 3, 3]
        assert sorted(row["kind"] for row in rows) == [0, 0, 1, 1]
        assert self.graph_states() == {
            "functions/left/count": 2,
            "functions/right/count": 3,
            "functions/merge/count": 4,
        }

    def prepare_recovery(self, mode, marker, delivered_ids):
        run_yt_sync("primary", self.work_yt_path)
        for name in ("left", "right"):
            self.client.insert_rows(f"{self.work_yt_path}/{name}_queue", [{"key": 7, "$tablet_index": 0}])

        def configure(config):
            graph = config["spec"]["computations"]["graph"]
            # A separate computation exercises recovery of materialized graph outputs and input dedup.
            config["spec"]["computations"]["delivery"] = {
                "computation_class_name": "NYT::NFlow::TProcessFunctionComputation",
                "processing_function": "NYT::NFlow::NProcessFunctionGraphTest::TGraphDeliveryFunction",
                "processing_function_parameters": {"ids_path": str(delivered_ids)},
                "group_by_schema": copy.deepcopy(graph["group_by_schema"]),
                "input_stream_ids": ["result"],
                "output_stream_ids": ["delivered"],
                "sinks": graph.pop("sinks"),
            }
            config["spec"]["computations"]["delivery"]["sinks"]["queue"]["input_stream_ids"] = ["delivered"]
            config["dynamic_spec"]["computations"]["delivery"] = {
                "batch_duration": 100,
                "parameters": {"desired_partition_count": 1},
            }
            if mode in ("throw", "kill", "freeze"):
                graph["process_function_graph"]["nodes"]["after_outputs"] = {
                    "processing_function": "NYT::NFlow::NProcessFunctionGraphTest::TGraphRecoveryFunction",
                    "processing_function_parameters": {"marker_path": str(marker), "throw_once": mode == "throw"},
                    "input_stream_ids": ["result"],
                }
            else:
                graph["sinks"] = {
                    "fault": {
                        "sink_class_name": "NYT::NFlow::NProcessFunctionGraphTest::TGraphCommitFaultSink",
                        "input_stream_ids": ["result"],
                        "parameters": {"marker_path": str(marker), "exit_after_commit": mode == "post_commit_exit"},
                    }
                }

        return self.prepare_pipeline_config(configure)

    @staticmethod
    def marker_lines(path):
        contents = path.read_text() if path.exists() else ""
        # A marker is created before its contents are written; wait for complete lines.
        return contents.splitlines() if contents.endswith("\n") else []

    def current_job(self, partition_id):
        partitions = self.client.get_flow_view(
            self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
        )
        return partitions.get(partition_id, {}).get("current_job_id")

    def check_uncommitted(self):
        assert self.graph_states() == {}
        assert self.output_rows() == []
        assert (
            list(self.client.select_rows(f'* from [{self.pipeline_path}/timers] where computation_id = "graph"')) == []
        )

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize("mode", ["throw", "kill"])
    def test_recovers_all_nodes_and_timers_before_commit(self, mode):
        marker = Path(self.path_to_flow_logs) / "before_commit"
        delivered_ids = Path(self.path_to_flow_logs) / "delivered_ids"
        config = self.prepare_recovery(mode, marker, delivered_ids)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config}, start_watcher_thread=False
        ) as federation:
            wait(lambda: len(self.marker_lines(marker)) == 1, timeout=120)
            (partition_id,) = self.marker_lines(marker)
            old_job = self.current_job(partition_id)
            assert old_job
            self.check_uncommitted()
            if mode == "throw":
                Path(str(marker) + ".throw").touch()
            else:
                federation.workers[0].restart()
            wait(lambda: len(self.marker_lines(Path(str(marker) + ".attempts"))) >= 2, timeout=120)
            wait(lambda: self.current_job(partition_id) not in (None, old_job), timeout=120)
            self.check_uncommitted()
            Path(str(marker) + ".release").touch()
            self.wait_pipeline_state("completed", timeout=180)
        self.check_result()

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize("mode", ["lost_commit_response", "post_commit_exit"])
    def test_recovers_committed_graph_output(self, mode):
        marker = Path(self.path_to_flow_logs) / "after_commit"
        delivered_ids = Path(self.path_to_flow_logs) / "delivered_ids"
        config = self.prepare_recovery(mode, marker, delivered_ids)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config}, start_watcher_thread=False
        ) as federation:
            wait(lambda: self.marker_lines(marker), timeout=120)
            if mode == "post_commit_exit":
                wait(lambda: not federation.workers[0].is_running(), timeout=30)
                assert self.graph_states()
                assert self.output_rows() == []
                try:
                    federation.workers[0].restart()
                except ProcessExitedNormallyException:
                    federation.workers[0].restart()
            self.wait_pipeline_state("completed", timeout=180)
            if mode == "lost_commit_response":
                wait(
                    lambda: any(
                        "Commit returned error, but transaction was committed" in path.read_text(errors="replace")
                        for path in Path(self.path_to_flow_logs).glob("Worker*.log")
                    ),
                    timeout=30,
                )
        self.check_result()
        assert not Path(str(marker) + ".replayed").exists()
        committed_ids = set(self.marker_lines(marker))
        assert committed_ids
        assert committed_ids <= set(self.marker_lines(delivered_ids))

    @pytest.mark.authors(["sergeypozdeev"])
    def test_old_worker_cannot_commit_after_reassignment(self):
        marker = Path(self.path_to_flow_logs) / "before_handover"
        delivered_ids = Path(self.path_to_flow_logs) / "delivered_ids"
        config = self.prepare_recovery("freeze", marker, delivered_ids)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config}, workers_count=2, start_watcher_thread=False
        ) as federation:
            wait(lambda: len(self.marker_lines(marker)) == 1, timeout=120)
            (partition_id,) = self.marker_lines(marker)
            old_job = self.current_job(partition_id)
            assert old_job
            jobs = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/layout/jobs", cache=False
            )
            worker_port = int(jobs[old_job]["worker_address"].rsplit(":", 1)[1])
            old_worker = next(worker for worker in federation.workers if worker.rpc_port == worker_port)
            old_worker.send_sigstop()
            try:
                wait(lambda: self.current_job(partition_id) not in (None, old_job), timeout=120)
                wait(lambda: len(self.marker_lines(Path(str(marker) + ".attempts"))) >= 2, timeout=120)
                self.check_uncommitted()
                Path(str(marker) + ".release").touch()
            finally:
                old_worker.send_sigcont()
            self.wait_pipeline_state("completed", timeout=180)
        self.check_result()

    @pytest.mark.authors(["sergeypozdeev"])
    def test_dynamic_parameters_validation(self) -> None:
        run_yt_sync("primary", self.work_yt_path)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.prepare_pipeline_config()},
        ):
            self.wait_pipeline_state("completed", timeout=180)
            original = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            for parameters in (
                {"nodes": {"missing": {}}},
                {"nodes": {"left": {"multiplier": "invalid"}}},
                {"nodes": {"left": {"typo": 2}}},
                {"typo": 1},
            ):
                invalid = copy.deepcopy(original["spec"])
                invalid["computations"]["graph"]["processing_function_parameters"] = parameters
                with pytest.raises(yt.YtError):
                    self.client.set_pipeline_dynamic_spec(self.pipeline_path, invalid)
                # The combined API checks node parameters even without strict validation, and before
                # it refuses any update of a completed pipeline.
                with pytest.raises(yt.YtError, match="process function graph"):
                    self.client.flow_execute(
                        self.pipeline_path,
                        flow_command="set-pipeline-specs",
                        flow_argument={"dynamic_spec": invalid, "validate_strict": False},
                    )
                assert self.client.get_pipeline_dynamic_spec(self.pipeline_path) == original

            valid = copy.deepcopy(original["spec"])
            valid["computations"]["graph"]["processing_function_parameters"] = {
                "nodes": {"left": {"multiplier": 2}},
            }
            self.client.set_pipeline_dynamic_spec(self.pipeline_path, valid)
            assert self.client.get_pipeline_dynamic_spec(self.pipeline_path)["spec"] == valid

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize("invalid_dynamic_node", [False, True])
    def test_validate_only_after_prepare(self, invalid_dynamic_node: bool) -> None:
        config = get_yson_config(self.config_path)
        self.patch_config(config)
        # A valid graph has no parseability errors; an unknown node fails the hard validation.
        config["abort_on_specs_parseability_error"] = not invalid_dynamic_node
        if invalid_dynamic_node:
            config["dynamic_spec"]["computations"]["graph"]["processing_function_parameters"] = {
                "nodes": {"missing_node": {}}
            }
        config_path = self.dump_config_to_log_dir(config, "validate_only.yson")

        result = yatest.common.execute(
            [self.FLOW_BINARY_PATH, "--config", config_path, "--validate-only"],
            check_exit_code=False,
            collect_cores=False,
            timeout=60,
        )
        stderr = (
            result.std_err.decode("utf-8", errors="replace") if isinstance(result.std_err, bytes) else result.std_err
        )
        if invalid_dynamic_node:
            assert result.exit_code != 0, stderr
            assert "Unknown process function graph node" in stderr, stderr
            assert "missing_node" in stderr, stderr
        else:
            assert result.exit_code == 0, stderr
        assert not self.client.exists(self.pipeline_path)
