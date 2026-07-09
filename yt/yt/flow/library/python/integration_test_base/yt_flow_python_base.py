"""
Test base class for Python-based Flow companion tests.
Similar to FlowTestJavaBase but for Python companions.
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


class FlowTestPythonBase(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")
    PYTHON_COMPANION_BINARY: str  # Path to PY3_PROGRAM binary.
    # rpc + monitoring + companion: the worker spawns the python companion on a third YT port.
    VANILLA_WORKER_PORT_COUNT = 3

    @contextmanager
    def start_flow_process_federation(
        self,
        pipeline_binary_args=None,
        use_vanilla_jobs=False,
        **kwargs,
    ):
        # The python companion binary is the runner: launched as
        # ``./companion --config pipeline.yson --flow-bin flow_server`` it enriches the spec and
        # hands off to flow_server, which sets the pipeline spec (and, for vanilla, submits the
        # operation). The runner — not the test — always sets the spec; the worker spawns the
        # companion itself via run_process, matching production.
        pipeline_binary_args = dict(pipeline_binary_args or {})
        config_path = pipeline_binary_args.get("--config")
        if config_path is not None:
            pipeline_binary_args["--config"] = self._prepare_launch_config(config_path, use_vanilla_jobs)
        pipeline_binary_args["--flow-bin"] = self.FLOW_BINARY_PATH

        with super().start_flow_process_federation(
            runner_binary_path=self.PYTHON_COMPANION_BINARY,
            pipeline_binary_args=pipeline_binary_args,
            use_vanilla_jobs=use_vanilla_jobs,
            run_pipeline=True,
            **kwargs,
        ) as federation:
            yield federation

    def _prepare_launch_config(self, config_path, use_vanilla_jobs):
        """Rewrite the pipeline config the runner sets the spec from."""
        pipeline_config = get_yson_config(config_path)
        # Companion computations carry parameters opaque to the C++ spec parser, so the runner must
        # not abort on "unrecognized" spec fields.
        pipeline_config["abort_on_specs_parseability_error"] = False
        if not use_vanilla_jobs:
            # Local federation: the worker spawns the companion from its on-disk path. Vanilla
            # instead ships ./py_companion into the job, which the runner patches in itself.
            for resource_id, resource_def in pipeline_config.get("spec", {}).get("resources", {}).items():
                if resource_def.get("resource_class_name") != _COMPANION_MANAGER_CLASS:
                    continue
                parameters = resource_def.setdefault("parameters", {})
                parameters["entrypoint"] = {"executable": self.PYTHON_COMPANION_BINARY}
                parameters["run_process"] = True
                log.info("Patched companion resource %s to spawn the local binary", resource_id)
        patched_path = os.path.join(self.path_to_flow_logs, "pipeline_launch.yson")
        dump_yson_config(pipeline_config, patched_path)
        return patched_path
