import os
import logging
import subprocess
import dataclasses
from typing import Optional

from contextlib import contextmanager

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import (
    get_yson_config,
    dump_yson_config,
)

log = logging.getLogger("yt_flow_java_base")

# Querying a JVM property launches a full JVM via run.sh. Under ASAN the cold
# start is slow on loaded build hosts, so the budget is generous there.
_JAVA_PROPERTY_QUERY_TIMEOUT = 60 if yatest.common.context.sanitize is not None else 15


@dataclasses.dataclass
class FlowJavaPipeline:
    runner_main_class: str
    runner_binary_dir: str


class FlowTestJavaBase(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")
    # Includes both JAVA_RUNNER_MAIN_CLASS and JAVA_NODE_COMPANION_MAIN_CLASS.
    JAVA_RUNNER_BINARY_DIR: str
    JAVA_RUNNER_MAIN_CLASS: str
    # Optional: Override this in subclass to specify companion main class.
    JAVA_NODE_COMPANION_MAIN_CLASS: Optional[str] = None
    # Extra port for the companion the worker spawns.
    VANILLA_WORKER_PORT_COUNT = 3
    # Cached JVM properties; populated lazily on first query.
    _java_properties_cache: Optional[dict] = None

    @classmethod
    def setup_class(cls):
        super(FlowTestJavaBase, cls).setup_class()
        cls.FLOW_JAVA_PIPELINE = FlowJavaPipeline(
            runner_main_class=cls.JAVA_RUNNER_MAIN_CLASS, runner_binary_dir=cls.JAVA_RUNNER_BINARY_DIR
        )

    def patch_config(self, pipeline_config: dict):
        # Apply for pipeline spec with run_process=true only.
        if "spec" in pipeline_config and pipeline_config["spec"]["resources"]["CompanionManager"]["parameters"].get(
            "run_process"
        ):
            pipeline_config["spec"]["resources"]["CompanionManager"]["parameters"].update(
                {
                    "jdk_bin_path": self.jdk_bin_path(),
                    "classpath": self.classpath(),
                }
            )
        super().patch_config(pipeline_config)

    def runner_binary_path(self) -> str:
        return os.path.join(self.JAVA_RUNNER_BINARY_DIR, "run.sh")

    def jdk_bin_path(self) -> str:
        return os.path.join(self.java_property("java.home"), "bin", "java")

    def classpath(self) -> str:
        return os.path.join(self.JAVA_RUNNER_BINARY_DIR, self.java_property("java.library.path"), "*")

    def java_property(self, javaPropName: str) -> str:
        """
        Retrieves a JAVA_PROGRAM environment property value using built-in run.sh script.
        """
        properties = self._java_properties()
        if javaPropName not in properties:
            raise RuntimeError(f"Java property {javaPropName} not found")
        propVal = properties[javaPropName]
        log.info(f"Queried Java property from run.sh {javaPropName}: {propVal}")
        return propVal

    def _java_properties(self) -> dict:
        """Queries all JVM properties via run.sh once and caches the result."""
        if self._java_properties_cache is not None:
            return self._java_properties_cache

        try:
            result = subprocess.run(
                [self.runner_binary_path(), "-XshowSettings:properties", "-version"],
                cwd=self.JAVA_RUNNER_BINARY_DIR,
                capture_output=True,
                text=True,
                check=True,
                timeout=_JAVA_PROPERTY_QUERY_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("Java properties query timed out")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Java properties failed to query: {e.stderr}")

        properties = {}
        for line in result.stderr.splitlines():
            if "=" not in line:
                continue
            name, value = line.split("=", 1)
            properties[name.strip()] = value.strip()

        self._java_properties_cache = properties
        return properties

    @contextmanager
    def start_flow_process_federation(
        self,
        pipeline_binary_args: dict[str, str] = {},
        runner_binary_path: str | None = None,
        run_companion_externally: bool = False,
        use_vanilla_jobs: bool = False,
        additional_env: dict[str, str] | None = None,
        **kwargs,
    ):
        if runner_binary_path is None:
            runner_binary_path = self.runner_binary_path()

        enriched_pipeline_binary_args = dict()
        enriched_pipeline_binary_args[self.FLOW_JAVA_PIPELINE.runner_main_class] = None
        enriched_pipeline_binary_args.update(pipeline_binary_args)

        # The runner always enriches the spec and hands the launch to flow_server, which sets the spec.
        enriched_pipeline_binary_args["--flow-bin"] = self.FLOW_BINARY_PATH
        config_path = enriched_pipeline_binary_args.get("--config")
        if config_path is not None:
            enriched_pipeline_binary_args["--config"] = self._prepare_launch_config(config_path)

        # Configure companion process for Java if run_companion_externally is enabled.
        companion_binary_args = None

        if run_companion_externally:
            companion_binary_args = [
                self.JAVA_NODE_COMPANION_MAIN_CLASS,
            ]

        env = dict(additional_env) if additional_env is not None else {}
        if use_vanilla_jobs:
            # Local test YT has no porto layers: run the companion from the host JDK with no layers.
            env["YT_FLOW_JDK_BIN_PATH"] = self.jdk_bin_path()
            env["YT_FLOW_JDK_LAYERS"] = "[]"
            # The Java vanilla op is slower to publish a leader; allow a larger window.
            kwargs.setdefault("leader_wait_timeout", 120)

        with super().start_flow_process_federation(
            pipeline_binary_args=enriched_pipeline_binary_args,
            runner_binary_path=runner_binary_path,
            run_companion_externally=run_companion_externally,
            use_vanilla_jobs=use_vanilla_jobs,
            run_pipeline=True,
            additional_env=env,
            companion_binary_path=runner_binary_path,
            companion_binary_args=companion_binary_args,
            **kwargs,
        ) as federation:
            yield federation

    def _prepare_launch_config(self, config_path: str) -> str:
        """Rewrite the pipeline config the runner sets the spec from."""
        pipeline_config = get_yson_config(config_path)
        # Java companion specs carry fields the C++ parser does not recognize; do not abort on them.
        pipeline_config["abort_on_specs_parseability_error"] = False
        patched_path = os.path.join(self.path_to_flow_logs, "pipeline_launch.yson")
        dump_yson_config(pipeline_config, patched_path)
        return patched_path
