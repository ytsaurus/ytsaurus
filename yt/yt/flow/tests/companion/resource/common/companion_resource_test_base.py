"""Shared body of the per-language companion-resource end-to-end tests.

Every companion SDK (C++, Java, Python, Go) hosts the same two resources: a
greeting resource that consumes a dependency resource through a resource-local
alias, and a mapper that copies the resource state into every output row, so
the dependency value, the dynamic greeting suffix and the serving process are
observable from the output queue. The scenario asserts that:

1. dependencies initialize before the greeting and aliases resolve correctly;
2. a dynamic-spec update reaches the dependency and dependent;
3. after the companion processes are killed the complete graph and job bindings
   recover in-band with the latest configuration.

A subclass mixes this in ahead of its language's ``FlowTest*Base`` and supplies
``PIPELINE_CONFIG_PATH`` plus ``COMPANION_CMDLINE_MARKER`` (the binary path or
main class that identifies the companion process on the command line).
"""

import logging
import os
import signal

import pytest

from yt.common import wait
from yt.wrapper import yson

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

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


# The companion's own drain grace plus the supervisor's margin: a process outliving it is
# one the supervisor had to kill.
GRACEFUL_STOP_TIMEOUT_SECONDS = 40


def find_companion_pids(cmdline_marker, companion_port, expected_count=1):
    """PIDs of this test worker's companion processes, identified by |cmdline_marker|
    and the unique configured port.

    |expected_count| is asserted when set; pass None for a companion that serves
    one port from several processes (the Python companion pre-forks).
    """
    marker = cmdline_marker.encode("utf-8") if isinstance(cmdline_marker, str) else cmdline_marker
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as f:
                cmdline = f.read()
        except OSError:
            continue
        if marker not in cmdline:
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
    assert pids, f"expected at least one companion on port {companion_port}"
    if expected_count is not None:
        assert (
            len(pids) == expected_count
        ), f"expected {expected_count} companion(s) on port {companion_port}, found {pids}"
    return pids


def alive_pids(pids):
    """Subset of |pids| whose processes still exist."""
    alive = []
    for pid in pids:
        try:
            os.kill(pid, 0)
        except OSError:
            continue
        alive.append(pid)
    return alive


def _to_str(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


class CompanionResourceTestBase:
    """Language-independent part of the companion-resource test."""

    # Path of the pipeline spec, and the marker identifying the companion
    # process on the command line; both are set by the language subclass.
    PIPELINE_CONFIG_PATH: str
    COMPANION_CMDLINE_MARKER: str
    # Companion processes expected to serve the port; None for a companion that
    # fans one port out across pre-forked processes.
    EXPECTED_COMPANION_PROCESSES = 1

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(self.PIPELINE_CONFIG_PATH)

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
            row = self.insert_until_output("phase1", lambda r: True, timeout=240)
            assert row["greeting"] == "hello"
            assert row["suffix"] == "v1"
            assert row["dependency_value"] == "dependency-v1"
            assert row["pid"] > 0
            logging.info("Phase 1 passed: resource initialized (row=%s)", row)

            # Phase 2: a dynamic-spec update reaches the companion resource via reconfigure;
            # the dependency value propagates through the dependent's re-init with the
            # advanced dependency reference.
            self.set_dynamic_parameters("v2", "dependency-v2")
            row = self.insert_until_output(
                "phase2",
                lambda r: r["suffix"] == "v2" and r["dependency_value"] == "dependency-v2",
                timeout=180,
            )
            assert row["greeting"] == "hello"
            logging.info("Phase 2 passed: reconfigure applied (row=%s)", row)

            # Phase 3: kill every companion process; the worker respawns them and the
            # resources are re-initialized in-band (RS_RESOURCE_NOT_INITIALIZED healing),
            # observable as a fresh pid in the output still carrying up-to-date parameters.
            old_pids = find_companion_pids(
                self.COMPANION_CMDLINE_MARKER,
                federation.workers[0].companion_port,
                self.EXPECTED_COMPANION_PROCESSES,
            )
            logging.info("Killing companion processes (pids=%s)", old_pids)
            for pid in old_pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

            row = self.insert_until_output(
                "phase3",
                lambda r: r["pid"] not in old_pids,
                timeout=240,
            )
            assert row["greeting"] == "hello"
            assert row["suffix"] == "v2"
            assert row["dependency_value"] == "dependency-v2"
            logging.info("Phase 3 passed: resource re-initialized after companion kill (row=%s)", row)

            # Phase 4: stop every companion gracefully. A SIGKILL never reaches the shutdown
            # path, so only this phase proves that a stopping companion drains its batches,
            # releases its resources and exits on its own — instead of hanging past its
            # budget and being killed — and that the worker heals the pipeline afterwards
            # exactly as it does after a kill.
            old_pids = find_companion_pids(
                self.COMPANION_CMDLINE_MARKER,
                federation.workers[0].companion_port,
                self.EXPECTED_COMPANION_PROCESSES,
            )
            logging.info("Stopping companion processes gracefully (pids=%s)", old_pids)
            for pid in old_pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

            # A companion still alive after its own drain grace plus the supervisor margin is
            # one that had to be killed rather than one that stopped.
            wait(
                lambda: not alive_pids(old_pids),
                timeout=GRACEFUL_STOP_TIMEOUT_SECONDS,
                sleep_backoff=1,
            )

            row = self.insert_until_output(
                "phase4",
                lambda r: r["pid"] not in old_pids,
                timeout=240,
            )
            assert row["greeting"] == "hello"
            assert row["suffix"] == "v2"
            assert row["dependency_value"] == "dependency-v2"
            logging.info("Phase 4 passed: resource re-initialized after graceful stop (row=%s)", row)
