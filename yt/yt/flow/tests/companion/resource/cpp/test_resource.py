"""End-to-end companion-resource test for the C++ companion.

A pipeline-spec resource of class ``NYT::NFlow::NCompanion::TCompanionResource``
proxies companion-hosted greeting and dependency resources registered via
``TPipeline::AddResource``. The greeting consumes its dependency through a
resource-local alias, while the process function uses a distinct job alias.
The output makes the dependency value, dynamic greeting suffix, and serving
process observable. The test asserts that:

1. dependencies initialize before the greeting and aliases resolve correctly;
2. a dynamic-spec update reaches the dependency and dependent;
3. after the companion process is killed the complete graph and job bindings
   recover in-band with the latest configuration.
"""

import logging
import os
import signal

import pytest
import yatest.common

from yt.common import wait
from yt.wrapper import yson

from yt.yt.flow.library.python.integration_test_base.yt_flow_cpp_base import (
    FlowTestCppCompanionBase,
)
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

INPUT_QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

OUTPUT_QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "greeting", "type": "string"},
    {"name": "suffix", "type": "string"},
    {"name": "dependency_value", "type": "string"},
    {"name": "pid", "type": "int64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


def find_companion_pid(companion_binary, companion_port):
    """PID of this test worker's companion, identified by its unique configured port."""
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as f:
                cmdline = f.read()
        except OSError:
            continue
        if companion_binary.encode("utf-8") not in cmdline:
            continue
        try:
            with open(f"/proc/{entry}/environ", "rb") as f:
                environment = f.read().split(b"\0")
        except OSError:
            continue
        config_entries = [
            value.split(b"=", 1)[1] for value in environment if value.startswith(b"YT_FLOW_COMPANION_CONFIG=")
        ]
        if not config_entries:
            continue
        config = yson.loads(config_entries[0])
        port = config.get("port", config.get(b"port"))
        if port == companion_port:
            pids.append(int(entry))
    assert len(pids) == 1, f"expected one companion on port {companion_port}, found {pids}"
    return pids[0]


def _to_str(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


class TestCompanionResource(FlowTestCppCompanionBase):
    CPP_COMPANION_BINARY = yatest.common.binary_path("yt/yt/flow/tests/companion/resource/cpp/companion/companion")

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": False,
            }
        )

        sink_params = pipeline_config["spec"]["computations"]["mapper"]["sinks"]["queue"]["parameters"]
        sink_params.update({"queue_path": f"<cluster=primary>{self.output_queue}"})

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_output(self, key_prefix):
        rows = self.client.select_rows(
            f"`key`, `greeting`, `suffix`, `dependency_value`, `pid` from [{self.output_queue}]"
        )
        result = []
        for row in rows:
            row = {name: _to_str(value) for name, value in row.items()}
            if row["key"].startswith(key_prefix):
                result.append(row)
        return result

    def insert_until_output(self, key_prefix, predicate, timeout):
        """Insert one fresh row per poll under |key_prefix| until an output row satisfies |predicate|.

        Continuous insertion is required because the asserted condition (an applied reconfigure, a
        healed companion process) becomes observable only through rows processed after it happens.
        """
        state = {"next_index": 0, "matched": None}

        def check():
            self.client.insert_rows(self.input_queue, [{"key": f"{key_prefix}-{state['next_index']}"}])
            state["next_index"] += 1
            for row in self.get_output(key_prefix):
                if predicate(row):
                    state["matched"] = row
                    return True
            return False

        wait(check, timeout=timeout, sleep_backoff=2)
        return state["matched"]

    def set_dynamic_parameters(self, suffix, dependency_value):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        spec = dynamic_spec["spec"]
        spec.setdefault("resources", {}).setdefault("greeting", {}).setdefault("parameters", {})["suffix"] = suffix
        (spec.setdefault("resources", {}).setdefault("greeting_dependency", {}).setdefault("parameters", {}))[
            "value"
        ] = dependency_value
        self.client.set_pipeline_dynamic_spec(self.pipeline_path, spec, expected_version=dynamic_spec["version"])

    @pytest.mark.authors(["sergeypozdeev"])
    def test_resource_lifecycle(self):
        run_yt_sync(
            self.primary_cluster_name,
            self.work_yt_path,
            tablet_cell_bundle=self.tablet_cell_bundle,
            primary_medium=self.primary_medium,
            add_input_queue_and_consumer=True,
            input_queue_schema=INPUT_QUEUE_SCHEMA,
            add_output_queue=True,
            output_queue_schema=OUTPUT_QUEUE_SCHEMA,
        )

        pipeline_config_path = self.prepare_pipeline_config()
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ) as federation:
            # Phase 1: init delivers the static and dynamic parameters to the resource.
            row = self.insert_until_output("phase1", lambda r: True, timeout=180)
            assert row["greeting"] == "hello"
            assert row["suffix"] == "v1"
            assert row["dependency_value"] == "dependency-v1"
            assert row["pid"] > 0
            logging.info("Phase 1 passed: resource initialized (row=%s)", row)

            # Phase 2: a dynamic-spec update reaches the companion resource via reconfigure.
            self.set_dynamic_parameters("v2", "dependency-v2")
            row = self.insert_until_output(
                "phase2",
                lambda r: r["suffix"] == "v2" and r["dependency_value"] == "dependency-v2",
                timeout=180,
            )
            assert row["greeting"] == "hello"
            logging.info("Phase 2 passed: reconfigure applied (row=%s)", row)

            # Phase 3: kill the companion process; the worker respawns it and the resource is
            # re-initialized in-band (RS_RESOURCE_NOT_INITIALIZED healing), observable as a fresh
            # pid in the output still carrying the up-to-date parameters.
            old_pid = find_companion_pid(
                self.CPP_COMPANION_BINARY,
                federation.workers[0].companion_port,
            )
            logging.info("Killing companion process (pid=%s)", old_pid)
            os.kill(old_pid, signal.SIGKILL)

            row = self.insert_until_output(
                "phase3",
                lambda r: r["pid"] != old_pid,
                timeout=240,
            )
            assert row["greeting"] == "hello"
            assert row["suffix"] == "v2"
            assert row["dependency_value"] == "dependency-v2"
            logging.info("Phase 3 passed: resource re-initialized after companion kill (row=%s)", row)
