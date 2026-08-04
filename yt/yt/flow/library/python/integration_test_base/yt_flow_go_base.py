"""
Test base class for Go-based Flow companion tests.
Similar to FlowTestPythonBase but for Go companions.
"""

import logging
import os
from contextlib import contextmanager

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import (
    get_yson_config,
    dump_yson_config,
)

log = logging.getLogger(__name__)

_COMPANION_MANAGER_CLASS = "NYT::NFlow::NCompanion::TCompanionManager"


class FlowTestGoBase(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")
    GO_COMPANION_BINARY: str  # Path to GO_PROGRAM binary.
    VANILLA_WORKER_PORT_COUNT = 3

    @contextmanager
    def start_flow_process_federation(
        self,
        pipeline_binary_args=None,
        use_vanilla_jobs=False,
        **kwargs,
    ):
        pipeline_binary_args = dict(pipeline_binary_args or {})
        config_path = pipeline_binary_args.get("--config")
        if config_path is not None:
            pipeline_binary_args["--config"] = self._prepare_launch_config(config_path, use_vanilla_jobs)
        pipeline_binary_args["--flow-bin"] = self.FLOW_BINARY_PATH

        with super().start_flow_process_federation(
            runner_binary_path=self.GO_COMPANION_BINARY,
            pipeline_binary_args=pipeline_binary_args,
            use_vanilla_jobs=use_vanilla_jobs,
            run_pipeline=True,
            **kwargs,
        ) as federation:
            yield federation

    def _prepare_launch_config(self, config_path, use_vanilla_jobs):
        """Rewrite the pipeline config the runner sets the spec from."""
        pipeline_config = get_yson_config(config_path)
        pipeline_config["abort_on_specs_parseability_error"] = False
        if not use_vanilla_jobs:
            for resource_id, resource_def in pipeline_config.get("spec", {}).get("resources", {}).items():
                if resource_def.get("resource_class_name") != _COMPANION_MANAGER_CLASS:
                    continue
                parameters = resource_def.setdefault("parameters", {})
                parameters["entrypoint"] = {"executable": self.GO_COMPANION_BINARY}
                log.info("Patched companion resource %s to spawn the local binary", resource_id)
        patched_path = os.path.join(self.path_to_flow_logs, "pipeline_launch.yson")
        dump_yson_config(pipeline_config, patched_path)
        return patched_path
