import os

import pytest
import yatest.common

from yt.yt.flow.tests.computation_cycles_and_buffers.lib.test_base import TestBase

from yt.common import wait

##################################################################

JOB_INVESTIGATION_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/tools/job_investigation/job_investigation")
DRAW_PIPELINE_GRAPH_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/tools/draw_pipeline_graph/draw_pipeline_graph")

##################################################################


class Test(TestBase):
    def _get_any_job_id(self):
        jobs = self.client.get_flow_view(self.pipeline_path, view_path="/state/execution_spec/layout/jobs", cache=False)
        return list(jobs.keys())[0]

    @pytest.mark.authors(["pechatnov"])
    def test_tools(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            expr = f"* from [{self.state}]"
            wait(lambda: len(list(self.client.select_rows(expr))) > 0)
            assert (
                self.client.get_pipeline_state(self.pipeline_path) != "completed"
            ), "Can't complete so fast with current throttling options"

            pipeline_full_path = f"primary:{self.pipeline_path}"

            job_investigation_output_path = os.path.join(self.path_to_flow_logs, "job_investigation.out")
            yatest.common.execute(
                [JOB_INVESTIGATION_BINARY_PATH, "--input", pipeline_full_path, "--check-is-full"],
                wait=True,
                stdout=job_investigation_output_path,
                stderr=os.path.join(self.path_to_flow_logs, "job_investigation.err"),
            )

            with open(job_investigation_output_path, "r") as f:
                assert self._get_any_job_id() in f.read()

            draw_pipeline_graph_dot_output_path = os.path.join(self.path_to_flow_logs, "pipeline_graph.dot")
            yatest.common.execute(
                [
                    DRAW_PIPELINE_GRAPH_BINARY_PATH,
                    "--input",
                    pipeline_full_path,
                    "--check-is-full",
                    "--dot-output",
                    draw_pipeline_graph_dot_output_path,
                    "--output",
                    os.path.join(self.path_to_flow_logs, "pipeline_graph.svg"),
                    "--use-embedded-dot",
                ],
                wait=True,
                stdout=os.path.join(self.path_to_flow_logs, "draw_pipeline_graph.out"),
                stderr=os.path.join(self.path_to_flow_logs, "draw_pipeline_graph.err"),
            )
            with open(draw_pipeline_graph_dot_output_path, "r") as f:
                text = f.read()
                assert "transform_a" in text
                assert self.input_queue in text
