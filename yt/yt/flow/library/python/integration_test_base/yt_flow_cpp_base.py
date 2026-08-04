"""
Test base class for C++ companion Flow tests: the standard flow_server is the
runner, and the worker spawns the C++ companion binary via the generic
TCompanionManager entrypoint.
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


class FlowTestCppCompanionBase(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")
    CPP_COMPANION_BINARY: str  # Path to the companion PROGRAM binary.
    # rpc + monitoring + companion: the worker spawns the companion on a third YT port.
    VANILLA_WORKER_PORT_COUNT = 3

    @contextmanager
    def start_flow_process_federation(
        self,
        pipeline_binary_args=None,
        **kwargs,
    ):
        pipeline_binary_args = dict(pipeline_binary_args or {})
        config_path = pipeline_binary_args.get("--config")
        if config_path is not None:
            pipeline_binary_args["--config"] = self._prepare_launch_config(config_path)

        with super().start_flow_process_federation(
            pipeline_binary_args=pipeline_binary_args,
            **kwargs,
        ) as federation:
            yield federation

    def _prepare_launch_config(self, config_path):
        """Point every TCompanionManager resource at the local companion binary."""
        pipeline_config = get_yson_config(config_path)
        # The companion's process functions are not registered in the runner binary, so the
        # runner must not abort on an unknown ``processing_function``.
        pipeline_config["abort_on_specs_parseability_error"] = False
        for resource_id, resource_def in pipeline_config.get("spec", {}).get("resources", {}).items():
            if resource_def.get("resource_class_name") != _COMPANION_MANAGER_CLASS:
                continue
            parameters = resource_def.setdefault("parameters", {})
            parameters["entrypoint"] = {"executable": self.CPP_COMPANION_BINARY}
            log.info("Patched companion resource %s to spawn the local binary", resource_id)
        patched_path = os.path.join(self.path_to_flow_logs, "pipeline_launch.yson")
        dump_yson_config(pipeline_config, patched_path)
        return patched_path
