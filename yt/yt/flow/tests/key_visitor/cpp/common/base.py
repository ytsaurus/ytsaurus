"""Shared fixture of the cpp key-visitor integration suites: table bootstrap, pipeline config
patching and output-table readers."""

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.yt.flow.tests.key_visitor.cpp.common.yt_sync import run_yt_sync

KEY_VISITOR_CPP = "yt/yt/flow/tests/key_visitor/cpp"


def pipeline_binary(pipeline_dir):
    """Path of the pipeline binary built from `<KEY_VISITOR_CPP>/<pipeline_dir>` (DEPENDS)."""
    return yatest.common.binary_path(f"{KEY_VISITOR_CPP}/{pipeline_dir}/pipeline")


def pipeline_config(pipeline_dir, name="pipeline.yson"):
    """Source path of a pipeline spec under `<KEY_VISITOR_CPP>/<pipeline_dir>` (DATA)."""
    return yatest.common.source_path(f"{KEY_VISITOR_CPP}/{pipeline_dir}/{name}")


class KeyVisitorTestBase(FlowTestBase):
    # Tests pass binary_path explicitly.
    FLOW_BINARY_PATH = None

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.user_state = self.work_yt_path + "/user_state"
        self.swift_state = self.work_yt_path + "/state"

    def run_yt_sync(self, **kwargs):
        run_yt_sync("primary", self.work_yt_path, **kwargs)

    # --- tables ------------------------------------------------------------------------------

    def get_output(self):
        return list(self.client.select_rows(f"`key`, `payload`, `visit_index` from [{self.output_queue}]"))

    def send_keys(self, entries):
        rows = [{"key": k, "payload": p} for k, p in entries]
        self.client.insert_rows(self.input_queue, rows)

    def seed_user_state(self, entries):
        rows = [{"key": k, "payload": p, "visit_index": 0} for k, p in entries]
        self.client.insert_rows(self.user_state, rows)

    def max_visit_index_per_key(self):
        result = {}
        for row in self.get_output():
            result[row["key"]] = max(result.get(row["key"], 0), row["visit_index"])
        return result

    def latest_payload_per_key(self):
        """Payload carried by the highest-indexed visit of every key."""
        latest = {}
        for row in self.get_output():
            idx = row["visit_index"]
            if idx > latest.get(row["key"], (-1, None))[0]:
                latest[row["key"]] = (idx, row["payload"])
        return {key: payload for key, (_, payload) in latest.items()}

    def assert_latest_payloads(self, expected_latest, what="seeded keys"):
        """Every key was visited and its latest visit carries the expected payload."""
        latest = self.latest_payload_per_key()
        missing = set(expected_latest) - set(latest)
        assert not missing, (
            f"pipeline reached `completed` but {len(missing)} {what} were never visited: "
            f"{sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}"
        )
        for key, expected_payload in expected_latest.items():
            assert latest[key] == expected_payload, (
                f"key={key!r}: latest visit had payload {latest[key]!r}, expected {expected_payload!r} "
                "— the final pass did not run after the input completed"
            )

    def partition_states(self, computation_id):
        partitions = self.client.get_flow_view(
            self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
        )
        return {
            pid: partition.get("state")
            for pid, partition in partitions.items()
            if partition.get("computation_id") == computation_id
        }

    # --- pipeline configs ----------------------------------------------------------------------

    def _patch_queue_source(self, computation, finite):
        computation["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": finite,
            }
        )

    def _patch_queue_sink(self, computation):
        computation["sinks"]["queue"]["parameters"].update({"queue_path": f"<cluster=primary>{self.output_queue}"})

    def prepare_pipeline_config(self, period_ms=20000, finite=True, desired_partition_count=None):
        """`pipeline`: internal per-key state, messages from a queue source."""
        config = get_yson_config(pipeline_config("pipeline"))

        self._patch_queue_source(config["spec"]["computations"]["key_reader"], finite)
        self._patch_queue_sink(config["spec"]["computations"]["tester"])

        tester_dynamic = config["dynamic_spec"]["computations"]["tester"]
        tester_dynamic["key_visitor_streams"]["visit_iter"]["period"] = period_ms
        if desired_partition_count is not None:
            tester_dynamic.setdefault("parameters", {})["desired_partition_count"] = desired_partition_count

        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def prepare_pipeline_external_config(self, period_ms=1000, finite=True, config_name="pipeline.yson"):
        """`pipeline_external`: the same over a TSimpleExternalStateManager (auto or manual preload)."""
        config = get_yson_config(pipeline_config("pipeline_external", config_name))

        self._patch_queue_source(config["spec"]["computations"]["key_reader"], finite)
        self._patch_queue_sink(config["spec"]["computations"]["tester"])
        config["spec"]["computations"]["tester"]["external_state_managers"]["/user-state-external"]["parameters"][
            "path"
        ] = self.user_state
        config["dynamic_spec"]["computations"]["tester"]["key_visitor_streams"]["visit_iter"]["period"] = period_ms

        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline_external.yson")

    def prepare_pipeline_swift_config(self, period_ms=20000, finite=True):
        """`pipeline_swift`: key-visitor stream in a TSwiftMapComputation over external state."""
        config = get_yson_config(pipeline_config("pipeline_swift"))

        self._patch_queue_source(config["spec"]["computations"]["key_reader"], finite)
        config["spec"]["computations"]["tester"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = self.swift_state
        config["dynamic_spec"]["computations"]["tester"]["key_visitor_streams"]["visit_iter"]["period"] = period_ms

        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline_swift.yson")

    def prepare_pipeline_keyvisitor_only_config(self, period_ms=1000, desired_partition_count=4, finite=True):
        """`pipeline_keyvisitor_only`: a computation whose only work source is the visit stream."""
        config = get_yson_config(pipeline_config("pipeline_keyvisitor_only"))

        computation = config["spec"]["computations"]["reviser_like"]
        computation["external_state_managers"]["/user-state"]["parameters"]["path"] = self.user_state
        self._patch_queue_sink(computation)

        dynamic = config["dynamic_spec"]["computations"]["reviser_like"]
        dynamic["key_visitor_streams"]["visit_iter"]["period"] = period_ms
        dynamic["key_visitor_streams"]["visit_iter"]["finite"] = finite
        dynamic.setdefault("parameters", {})["desired_partition_count"] = desired_partition_count

        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline_keyvisitor_only.yson")

    def prepare_pipeline_visitor_loop_config(self, period_ms=1000, upstream_streams=None):
        """`pipeline_visitor_loop`: every visit pings a downstream computation whose answer comes back."""
        config = get_yson_config(pipeline_config("pipeline_visitor_loop"))

        computations = config["spec"]["computations"]
        self._patch_queue_source(computations["key_reader"], finite=True)
        self._patch_queue_sink(computations["tester"])
        if upstream_streams is not None:
            computations["tester"]["key_visitor_streams"]["visit_iter"]["upstream_streams"] = upstream_streams
        config["dynamic_spec"]["computations"]["tester"]["key_visitor_streams"]["visit_iter"]["period"] = period_ms

        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline_visitor_loop.yson")
