import json
import os
import pytest
import re

from copy import deepcopy

import yatest.common

import requests

import yt.wrapper as yt_wrapper
import yt.yson as yson
from yt.wrapper.errors import YtError, YtResponseError
from yt.wrapper.http_helpers import get_proxy_address_url, get_http_api_version, get_token

from yt.yt.flow.library.python.bullied_process import ProcessDiedException
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

# The pipeline binary and its config live in the original test directory regardless of which
# per-package variant is running this shared body, so address them by a fixed arcadia path
# instead of the per-target project_path.
PIPELINE_DIR = "yt/yt/flow/tests/flow_execute/pipeline"
PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{PIPELINE_DIR}/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{PIPELINE_DIR}/pipeline")

if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 200
else:
    EVENT_COUNT = 1000


def generate_data(event_count, tablet_count):
    result = []
    for i in range(event_count):
        result.append({"data": f"payload_{i}", "$tablet_index": i % tablet_count})
    return result


TABLET_COUNT = 5
INPUT_DATA = generate_data(EVENT_COUNT, TABLET_COUNT)
EXPECTED_DATA = [row["data"] for row in INPUT_DATA]
EXPECTED_DATA.sort()

##################################################################


def _env_flag(name: str) -> bool:
    """Read a 0/1 feature toggle from the environment."""
    return bool(int(os.environ[name]))


def _make_ace(action, subjects, permissions, inheritance_mode="object_and_descendants"):
    def _to_list(x):
        if isinstance(x, str):
            return [x]
        return x

    return {
        "action": action,
        "subjects": _to_list(subjects),
        "permissions": _to_list(permissions),
        "inheritance_mode": inheritance_mode,
    }


class FlowExecuteTestBase(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH

    # Feature toggles set per-package; defaults live in ya.make.inc.
    ENABLE_PROXY_SIGNATURES = _env_flag("FLOW_EXECUTE_ENABLE_PROXY_SIGNATURES")
    CHECK_FLOW_EXECUTE_PLAINTEXT = _env_flag("FLOW_EXECUTE_CHECK_PLAINTEXT")
    CHECK_KNOWN_COMMANDS = _env_flag("FLOW_EXECUTE_CHECK_KNOWN_COMMANDS")
    CHECK_SPEC_VERSIONS = _env_flag("FLOW_EXECUTE_CHECK_SPEC_VERSIONS")
    TEST_FLOW_CORE_TARGET = _env_flag("FLOW_EXECUTE_TEST_FLOW_CORE_TARGET")

    def setup_method(self, method):
        super(FlowExecuteTestBase, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.producer = self.work_yt_path + "/producer"
        self.external_state = self.work_yt_path + "/external_state"

    def prepare_environment(self):
        run_yt_sync("primary", self.work_yt_path, TABLET_COUNT)

        batching_write_rows(INPUT_DATA, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

    def prepare_pipeline_config(self, finite=True):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": finite,
            }
        )

        pipeline_config["spec"]["computations"]["reader"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
                "producer_path": f"<cluster=primary>{self.producer}",
            }
        )

        pipeline_config["spec"]["computations"]["state_writer"]["external_state_managers"]["/external_state"][
            "parameters"
        ]["path"] = f"<cluster=primary>{self.external_state}"

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def _get_partitions_count(self):
        return len(
            self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
            )
        )

    def _wait_epoch_sync(self):
        def epoch_is_sync():
            united_epoch = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/traverse_data/united_stream/epoch", cache=False
            )
            epoch = self.client.get_flow_view(self.pipeline_path, view_path="/state/epoch", cache=False)
            return epoch == united_epoch and epoch > 0

        wait(epoch_is_sync, timeout=180)

    @pytest.mark.authors(["thenewone"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            pytest.param(
                1,
                1,
                False,
                id="1c_1w_stable",
            ),
        ],
    )
    def test_basics(self, workers_count, controllers_count, problems):
        self.prepare_environment()
        # Run the pipeline non-finite so the source stream does not drain and the pipeline does not
        # auto-complete. This keeps every pause/wait-for-paused sequence below stable. The spec is
        # flipped to finite at the very end so the pipeline completes deterministically.
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        with pytest.raises(YtResponseError, match="Cannot discover pipeline controller"):
            self.client.flow_execute(self.pipeline_path, flow_command="list")

        # Require the controller to validate the proxy signature on every
        # incoming RPC. Combined with `flow_proxy_signature_enabled` on the
        # RPC proxy side this exercises the full sign/validate path end-to-end.
        # Skipped on packages without proxy-signature support (see ENABLE_PROXY_SIGNATURES).
        node_config = {}
        if self.ENABLE_PROXY_SIGNATURES:
            node_config["authenticator"] = {
                "require_proxy_signature": True,
            }

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            node_config=node_config,
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
        ):
            # Wait flow to be started.
            wait(lambda: self._get_partitions_count() != 0)
            self._wait_epoch_sync()

            known_commands = [
                "list",
                "get-command-required-permission",
                "get-flow-view",
                "get-flow-view-v2",
                "get-pipeline-spec",
                "set-pipeline-spec",
                "get-pipeline-dynamic-spec",
                "set-pipeline-dynamic-spec",
                "set-pipeline-specs",
                "get-pipeline-state",
                "set-target-pipeline-state",
                "describe-pipeline",
                "describe-computation",
                "describe-computations",
                "describe-partition",
                "describe-worker",
                "describe-workers",
                "get-worker-orchid",
                "get-controller-orchid",
                "kill-worker",
                "update-worker",  # No test logic for this stub right now.
                "get-worker-backtraces",  # Tested in working_pipeline_telemetry test.
                "read-states",  # Tested in test_read_states.
                "delete-states",  # Tested in test_delete_states.
                "get-flow-core-target",
                "set-flow-core-target",
            ]

            # Test normal scenario in python flow_execute.
            # Test list.
            res = self.client.flow_execute(self.pipeline_path, flow_command="list")
            if self.CHECK_KNOWN_COMMANDS:
                assert res == known_commands

            # Test get-command-required-permission.
            res = self.client.flow_execute(
                self.pipeline_path, flow_command="get-command-required-permission", flow_argument={"command": "list"}
            )
            assert res["permission"] == ["read"]
            res = self.client.flow_execute(
                self.pipeline_path,
                flow_command="get-command-required-permission",
                flow_argument={"command": "set-pipeline-spec"},
            )
            assert res["permission"] == ["write"]
            with pytest.raises(YtResponseError, match="No such command"):
                self.client.flow_execute(
                    self.pipeline_path,
                    flow_command="get-command-required-permission",
                    flow_argument={"command": "magic"},
                )

            # Test normal scenario in python flow_execute with get-flow-view.
            def exec_get_flow_view(*args, **kwargs):
                return self.client.flow_execute(self.pipeline_path, flow_command="get-flow-view", *args, **kwargs)

            def json_dump_bin(obj):
                return json.dumps(obj, separators=(",", ":")).encode("utf-8")

            res = exec_get_flow_view(flow_argument={})
            assert "state" in res and "current_spec" in res and "current_dynamic_spec" in res

            res = exec_get_flow_view(flow_argument={"path": "/state/job_manager_state/computations/reader"})
            # The source state is nested under the source identity (a hash of the identifying
            # params, here the queue cluster and path), so derive the actual name instead of pinning it.
            (source_state_name,) = [name for name in res if name.startswith("/sources/")]
            assert re.fullmatch(r"/sources/queue/[0-9a-f]{32}/queue_info/v0", source_state_name), source_state_name
            expected_reader_states = {
                source_state_name: {"cached_partition_count": 5},
                "/sinks/queue/queue_info/v0": {"cached_partition_count": 5},
            }
            assert res == expected_reader_states

            res = exec_get_flow_view(
                flow_argument={"path": "/state/job_manager_state/computations/reader", "cache": False}
            )
            assert res == expected_reader_states

            res = exec_get_flow_view(
                flow_argument=json.dumps({"path": "/state/job_manager_state/computations/reader"}), input_format="json"
            )
            assert res == expected_reader_states

            path_arg = yson.dumps({"path": "/state/job_manager_state/computations/reader"}, "text").decode("utf-8")
            res = exec_get_flow_view(flow_argument=path_arg, input_format="yson")
            assert res == expected_reader_states

            path_arg = yson.dumps({"path": "/state/job_manager_state/computations/reader"}, "binary")
            res = exec_get_flow_view(flow_argument=path_arg, input_format="yson")
            assert res == expected_reader_states

            source_state_ypath = "/state/job_manager_state/computations/reader/" + source_state_name.replace("/", "\\/")
            res = exec_get_flow_view(
                flow_argument={"path": source_state_ypath},
                output_format="json",
            )
            assert res == json_dump_bin({"cached_partition_count": 5})

            res = exec_get_flow_view(
                flow_argument={"path": source_state_ypath},
                output_format="yson",
            )
            assert res == yson.dumps({"cached_partition_count": 5}, "binary")

            yson_text_format = yson.to_yson_type("yson", attributes={"format": "text"})
            res = exec_get_flow_view(
                flow_argument={"path": source_state_ypath},
                output_format=yson_text_format,
            )
            assert res == yson.dumps({"cached_partition_count": 5}, "text")

            # Test normal scenario in python flow_execute with set-pipeline-dynamic-spec and set-pipeline-dynamic-spec.
            def exec_set_dyn_spec(*args, **kwargs):
                return self.client.flow_execute(
                    self.pipeline_path, flow_command="set-pipeline-dynamic-spec", *args, **kwargs
                )

            def exec_get_dyn_spec(*args, **kwargs):
                return self.client.flow_execute(
                    self.pipeline_path, flow_command="get-pipeline-dynamic-spec", *args, **kwargs
                )

            res = exec_get_dyn_spec(flow_argument={})
            assert "spec" in res and "version" in res
            if self.CHECK_SPEC_VERSIONS:
                assert res["version"] == 2
            base_version = res["version"]
            spec = res["spec"]
            assert "computations" in spec and "job_manager" in spec and "job_tracker" in spec

            spec["job_tracker"]["load_throughput_throttler"]["period"] = 1001
            res = exec_set_dyn_spec(flow_argument={"spec": spec})
            assert res["version"] == base_version + 1

            res = exec_get_dyn_spec(flow_argument={})
            assert res == {"spec": spec, "version": base_version + 1}

            spec["job_tracker"]["load_throughput_throttler"]["period"] = 1002
            res = exec_set_dyn_spec(flow_argument={"spec": spec, "expected_version": base_version + 1})
            assert res["version"] == base_version + 2

            res = exec_get_dyn_spec(flow_argument={})
            assert res == {"spec": spec, "version": base_version + 2}

            test_path = "/job_tracker/load_throughput_throttler/period"

            res = exec_get_dyn_spec(flow_argument={"path": test_path})
            assert res == {"spec": 1002, "version": base_version + 2}

            res = exec_set_dyn_spec(flow_argument={"spec": 1003, "path": test_path})
            assert res == {"version": base_version + 3}

            res = exec_get_dyn_spec(flow_argument={"path": test_path})
            assert res == {"spec": 1003, "version": base_version + 3}

            res = exec_set_dyn_spec(flow_argument=json.dumps({"spec": 1004, "path": test_path}), input_format="json")
            assert res == {"version": base_version + 4}

            res = exec_get_dyn_spec(flow_argument={"path": test_path}, output_format="yson")
            assert res == yson.dumps({"spec": 1004, "version": base_version + 4}, "binary")

            spec_arg = yson.dumps({"spec": 1005, "path": test_path}, "binary")
            res = exec_set_dyn_spec(flow_argument=spec_arg, input_format="yson", output_format="json")
            assert res == json_dump_bin({"version": base_version + 5})

            res = exec_get_dyn_spec(
                flow_argument='{"path"="' + test_path + '"}', input_format="yson", output_format="json"
            )
            assert res == json_dump_bin({"spec": 1005, "version": base_version + 5})

            spec_arg = yson.dumps({"spec": 1006, "path": test_path}, "text").decode("utf-8")
            res = exec_set_dyn_spec(flow_argument=spec_arg, input_format="yson", output_format="yson")
            assert res == yson.dumps({"version": base_version + 6}, "binary")

            res = exec_get_dyn_spec(
                flow_argument='{"path"="' + test_path + '"}', input_format="yson", output_format=yson_text_format
            )
            assert res == yson.dumps({"spec": 1006, "version": base_version + 6}, "text")

            # Test get-pipeline-spec.
            res = self.client.flow_execute(self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={})
            assert "computations" in res["spec"]
            assert res["version"] > 0
            res = self.client.flow_execute(
                self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={"path": "/computations"}
            )
            assert "reader" in res["spec"]

            # Test set-pipeline-spec.
            self.client.pause_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "paused")
            old_static_spec = self.client.flow_execute(
                self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={}
            )
            static_spec = deepcopy(old_static_spec["spec"])
            static_spec["computations"]["reader"]["watermark_strategy"] = {"watermark_percentile": {"value": 0.5}}
            with pytest.raises(YtResponseError, match="stop the pipeline"):
                self.client.flow_execute(
                    self.pipeline_path,
                    flow_command="set-pipeline-spec",
                    flow_argument={
                        # No force flag.
                        "expected_version": old_static_spec["version"],
                        "spec": static_spec,
                    },
                )
            with pytest.raises(YtResponseError, match="Must increase pipeline spec version"):
                self.client.flow_execute(
                    self.pipeline_path,
                    flow_command="set-pipeline-spec",
                    flow_argument={
                        "expected_version": old_static_spec["version"] + 1,  # Bad expected version.
                        "spec": static_spec,
                        "force": True,
                    },
                )
            new_static_spec_version = self.client.flow_execute(
                self.pipeline_path,
                flow_command="set-pipeline-spec",
                flow_argument={
                    "force": True,
                    "expected_version": old_static_spec["version"],
                    "spec": static_spec,
                },
            )["version"]
            assert old_static_spec["version"] + 1 == new_static_spec_version
            new_static_spec = self.client.flow_execute(
                self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={}
            )
            assert new_static_spec["version"] == new_static_spec_version
            assert (
                new_static_spec["spec"]["computations"]["reader"]["watermark_strategy"]["watermark_percentile"]["value"]
                == 0.5
            )

            self.client.flow_execute(
                self.pipeline_path,
                flow_command="set-target-pipeline-state",
                flow_argument={"target_pipeline_state": "completed"},
            )

            def check_working_pipeline_state():
                state = self.client.flow_execute(
                    self.pipeline_path, flow_command="get-pipeline-state", flow_argument={}
                )["pipeline_state"]
                return state == "working" or state == "completed"

            wait(check_working_pipeline_state)

            # Test set-pipeline-specs.
            self.run_set_pipeline_specs_test()

            # Test normal scenario in python flow_execute with describe-pipeline.
            res = self.client.flow_execute(self.pipeline_path, flow_command="describe-pipeline")
            assert "status" in res
            assert res["computations"]["reader"]["partitions_stats"]["count"] > 0
            assert len(res["computations"]["reader"]["partitions_stats"]["count_by_state"]) > 0
            assert len(res["messages"]) > 0
            assert any(
                "OAUTH" in str(message) and "root" in str(message) for message in res["messages"]
            ), f"Auth info is not found. Messages: {res['messages']}"

            # Test invalid arguments in python flow_execute.
            with pytest.raises(YtResponseError, match="No such command: magic"):
                res = self.client.flow_execute(self.pipeline_path, flow_command="magic")
            with pytest.raises(TypeError, match="Serialized flow_argument must be str, bytes or bytearray"):
                exec_get_flow_view(flow_argument={}, input_format="json")

            with pytest.raises(YtError, match="Incorrect format xml"):
                exec_set_dyn_spec(flow_argument="<spec></spec>", input_format="xml")
            with pytest.raises(YtError, match="Incorrect format proto"):
                exec_get_dyn_spec(flow_argument=b"a:b", input_format="proto")
            with pytest.raises(YtError, match="Incorrect format csv"):
                exec_get_dyn_spec(flow_argument={}, output_format="csv")
            with pytest.raises(YtError, match="Incorrect format proto"):
                exec_set_dyn_spec(flow_argument={"spec": spec}, output_format="proto")

            with pytest.raises(YtResponseError, match='Cannot parse "map"; expected "begin_map", actual "begin_list"'):
                exec_get_flow_view(flow_argument=[1, 2, 3])
            with pytest.raises(
                YtResponseError,
                match="Error reading parameter /path\n\\s*"
                'Cannot parse "string"; expected "string_value", actual "int64_value"',
            ):
                exec_get_flow_view(flow_argument={"path": 42})
            with pytest.raises(YtResponseError, match='Unrecognized field "/use_magic" has been encountered'):
                exec_get_flow_view(flow_argument={"use_magic": True})
            # Due to specific parsing of bool values the check below fails with 'expected "string"' message.
            with pytest.raises(
                YtResponseError,
                match="Error reading parameter /cache\n\\s*"
                'Cannot parse "bool"; expected one of the tokens {"boolean_value", "string_value", }, actual "double_value"',
            ):
                exec_get_flow_view(flow_argument={"cache": 0.5})

            with pytest.raises(YtResponseError, match='Cannot parse "map"; expected "begin_map", actual "begin_list"'):
                exec_get_dyn_spec(flow_argument=[1, 2, 3])
            with pytest.raises(
                YtResponseError,
                match="Error reading parameter /path\n\\s*"
                'Cannot parse "string"; expected "string_value", actual "int64_value"',
            ):
                exec_get_dyn_spec(flow_argument={"path": 42})
            with pytest.raises(YtResponseError, match='Unrecognized field "/use_magic" has been encountered'):
                exec_get_dyn_spec(flow_argument={"use_magic": True})

            with pytest.raises(YtResponseError, match='Cannot parse "map"; expected "begin_map", actual "begin_list"'):
                exec_set_dyn_spec(flow_argument=[1, 2, 3])
            with pytest.raises(
                YtResponseError,
                match="Error reading parameter /path\n\\s*"
                'Cannot parse "string"; expected "string_value", actual "int64_value"',
            ):
                exec_set_dyn_spec(flow_argument={"path": 42})
            with pytest.raises(
                YtResponseError,
                match="Error reading parameter /expected_version\n\\s*"
                'Cannot parse "long"; expected one of the tokens {"int64_value", "uint64_value", }, actual "boolean_value"',
            ):
                exec_set_dyn_spec(flow_argument={"spec": {}, "expected_version": True})
            with pytest.raises(YtResponseError, match="Missing required parameter /spec"):
                exec_set_dyn_spec(flow_argument={"expected_version": 0})
            with pytest.raises(YtResponseError, match='Unrecognized field "/use_magic" has been encountered'):
                exec_set_dyn_spec(flow_argument={"spec": {}, "use_magic": True})

            # Test normal scenario in command line utility.
            yt_path = yatest.common.binary_path("yt/python/yt/wrapper/bin/yt_make/yt")

            def console_flow_execute(*args):
                res = yatest.common.execute(
                    [yt_path, "--proxy", "primary", "flow", "execute", self.pipeline_path, *args]
                )
                assert res.stdout == "" or res.stdout.endswith(b"\n")
                return res.stdout.rstrip(b"\n")

            def console_flow_describe(*args):
                res = yatest.common.execute(
                    [yt_path, "--proxy", "primary", "flow", "describe-pipeline", self.pipeline_path, *args]
                )
                assert res.stdout == "" or res.stdout.endswith(b"\n")
                return res.stdout.rstrip(b"\n")

            res = yson.loads(console_flow_describe())
            assert "status" in res
            assert res["computations"]["reader"]["partitions_stats"]["count"] > 0

            res = yson.loads(console_flow_execute("describe-pipeline", "{}", "--input-format=json"))
            assert "status" in res
            assert res["computations"]["reader"]["partitions_stats"]["count"] > 0

            res = console_flow_execute(
                "get-flow-view",
                json.dumps({"path": "/state/job_manager_state/computations/reader"}),
                "--input-format=json",
            )
            assert yson.loads(res) == expected_reader_states

            res = console_flow_execute(
                "get-flow-view",
                json.dumps({"path": "/state/job_manager_state/computations/reader"}),
                "--input-format=json",
                "--output-format=json",
            )
            assert json.loads(res) == expected_reader_states

            old_dynamic_spec_version = yson.loads(
                console_flow_execute("get-pipeline-dynamic-spec", "{}", "--input-format=json")
            )["version"]
            spec["job_tracker"]["load_throughput_throttler"]["period"] = 1010
            res = console_flow_execute("set-pipeline-dynamic-spec", yson.dumps({"spec": spec}), "--input-format=yson")
            assert yson.loads(res) == {"version": old_dynamic_spec_version + 1}

            res = console_flow_execute("get-pipeline-dynamic-spec", "{}", "--input-format=json")
            assert yson.loads(res) == {"spec": spec, "version": old_dynamic_spec_version + 1}

            test_path = "/job_tracker/load_throughput_throttler/period"
            arg = {"spec": 1011, "path": test_path}
            res = console_flow_execute("set-pipeline-dynamic-spec", json.dumps(arg), "--input-format=json")
            assert yson.loads(res) == {"version": old_dynamic_spec_version + 2}

            arg = {"path": test_path}
            res = console_flow_execute(
                "get-pipeline-dynamic-spec", json.dumps(arg), "--input-format=json", "--output-format=json"
            )
            assert json.loads(res) == {"spec": 1011, "version": old_dynamic_spec_version + 2}

            # Test invalid arguments in command line utility.
            with pytest.raises(Exception, match="Incorrect format xml"):
                res = console_flow_execute("get-flow-view", "--input-format=xml")

            with pytest.raises(Exception, match="Incorrect format proto"):
                res = console_flow_execute("get-flow-view", "--input-format=proto")

            with pytest.raises(Exception, match="Incorrect format csv"):
                res = console_flow_execute("get-flow-view", "--output-format=csv")

            with pytest.raises(Exception, match="Incorrect format proto"):
                res = console_flow_execute("get-flow-view", "--output-format=proto")

            # Test describe-pipeline-status.
            res = self.client.flow_execute(
                self.pipeline_path, flow_command="describe-pipeline", flow_argument={"status_only": True}
            )
            assert len(res["messages"]) > 0

            # Test describe-computations.
            res = self.client.flow_execute(self.pipeline_path, flow_command="describe-computations")
            assert len(res["computations"]) > 0
            assert any(c["class_name"] == "TReader" for c in res["computations"])

            # Test describe-computation.
            res = self.client.flow_execute(
                self.pipeline_path, flow_command="describe-computation", flow_argument={"computation_id": "reader"}
            )
            assert res["class_name"] == "TReader"
            assert len(res["partitions"]) > 0
            partition_id = res["partitions"][0]["partition_id"]

            # Test plaintext output via direct HTTP request to HTTP proxy.
            def flow_execute_via_http(flow_command, *, flow_argument=None, field=None):
                proxy_url = get_proxy_address_url(client=self.client)
                api_version = get_http_api_version(client=self.client)
                token = get_token(client=self.client)
                url = f"{proxy_url}/api/{api_version}/flow_execute_plaintext"
                query_params = {
                    "pipeline_path": str(self.pipeline_path),
                    "flow_command": flow_command,
                    "input_format": "yson",
                }
                if flow_argument is not None:
                    query_params["flow_argument"] = yson.dumps(flow_argument, yson_format="text").decode()
                if field is not None:
                    query_params["field"] = field
                headers = {
                    "Authorization": f"OAuth {token}",
                }
                response = requests.get(url, params=query_params, headers=headers)
                response.raise_for_status()
                return response.content

            # The flow_execute_plaintext HTTP-proxy command is absent in older packages
            # (see CHECK_FLOW_EXECUTE_PLAINTEXT).
            if self.CHECK_FLOW_EXECUTE_PLAINTEXT:
                res = flow_execute_via_http(
                    "describe-computation",
                    flow_argument={"computation_id": "reader"},
                )
                assert isinstance(res, bytes) and res.startswith(b"{")
                res = flow_execute_via_http(
                    "describe-computation", flow_argument={"computation_id": "reader"}, field="class_name"
                )
                assert res == b"TReader"
                assert yson.loads(
                    flow_execute_via_http("get-pipeline-spec", flow_argument={})
                ) == self.client.flow_execute(self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={})

            # Test describe-partition.
            res = self.client.flow_execute(
                self.pipeline_path, flow_command="describe-partition", flow_argument={"partition_id": partition_id}
            )
            assert res["computation_id"] == "reader"

            # Test describe-workers.
            res = self.client.flow_execute(self.pipeline_path, flow_command="describe-workers")
            assert len(res["workers"]) > 0
            worker_address = res["workers"][0]["address"]
            worker_incarnation_id = res["workers"][0]["incarnation_id"]

            # Test describe-worker.
            res = self.client.flow_execute(
                self.pipeline_path, flow_command="describe-worker", flow_argument={"worker": worker_address}
            )
            assert res["incarnation_id"] == worker_incarnation_id

            # Test get-worker-orchid.
            res = exec_get_flow_view(flow_argument={"path": "/state/workers"})
            worker = list(res.keys())[0]

            res = self.client.flow_execute(
                self.pipeline_path, flow_command="get-worker-orchid", flow_argument={"worker": worker}
            )
            assert "job_tracker" in res["value"]

            res = self.client.flow_execute(
                self.pipeline_path,
                flow_command="get-worker-orchid",
                flow_argument={"worker": worker, "path": "/build_info"},
            )
            assert "binary_version" in res["value"]

            with pytest.raises(YtResponseError):
                self.client.flow_execute(
                    self.pipeline_path, flow_command="get-worker-orchid", flow_argument={"worker": "_"}
                )

            # Test get-controller-orchid.
            res = self.client.flow_execute(
                self.pipeline_path,
                flow_command="get-controller-orchid",
                flow_argument={"path": "/node_info/incarnation_id"},
            )
            assert len(res["value"]) > 0

            # Test ACL.
            # No access.
            self.client.create("user", attributes={"name": "u1"})
            self.client.set(f"{self.pipeline_path}/@inherit_acl", False)
            assert self.client.check_permission("u1", "read", self.pipeline_path)["action"] == "deny"
            # Create a client acting as user u1 using impersonation_user config option.
            u1_client = yt_wrapper.YtClient(
                proxy=self.client.config["proxy"]["url"],
                token="",
                config={"impersonation_user": "u1"},
            )
            with pytest.raises(YtResponseError):  # Sanity check, must fail.
                u1_client.get(f"{self.pipeline_path}/@pipeline_format_version")
            with pytest.raises(YtResponseError):  # Pipeline read request must fail.
                u1_client.get_flow_view(self.pipeline_path)
            # Add read access.
            self.client.set(f"{self.pipeline_path}/@acl", [_make_ace("allow", "u1", "read")])
            u1_client.get_flow_view(self.pipeline_path)  # Pipeline read request must succeed.
            self.client.flow_execute(
                self.pipeline_path, flow_command="describe-workers"
            )  # Describe workers read request must succeed.
            with pytest.raises(YtResponseError):  # Pipeline write request must fail.
                u1_client.start_pipeline(self.pipeline_path)
            with pytest.raises(YtResponseError):  # Pipeline flow-execute write request must fail.
                u1_client.flow_execute(self.pipeline_path, flow_command="set-pipeline-dynamic-spec", flow_argument={})
            # Add write access.
            self.client.set(
                f"{self.pipeline_path}/@acl", [_make_ace("allow", "u1", "read"), _make_ace("allow", "u1", "write")]
            )
            u1_client.start_pipeline(self.pipeline_path)  # Pipeline write request must succeed.

            # Flip the source stream to finite so the pipeline drains and completes deterministically.
            # The pipeline ran non-finite until now to keep the pause/wait-for-paused checks stable.
            self.client.pause_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "paused")
            cur = self.client.flow_execute(self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={})
            finite_spec = deepcopy(cur["spec"])
            finite_spec["computations"]["reader"]["source_streams"]["queue"]["parameters"]["finite"] = True
            self.client.flow_execute(
                self.pipeline_path,
                flow_command="set-pipeline-spec",
                flow_argument={
                    "force": True,
                    "expected_version": cur["version"],
                    "spec": finite_spec,
                },
            )
            self.client.start_pipeline(self.pipeline_path)

            # Wait for complete and done.
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "completed", timeout=180)
            expr = f"data from [{self.output_queue}]"
            data = [row["data"] for row in list(self.client.select_rows(expr))]
            data.sort()
            assert data == EXPECTED_DATA

        with pytest.raises(YtResponseError, match="Cannot connect to pipeline controller leader"):
            self.client.flow_execute(self.pipeline_path, flow_command="list")

    def run_set_pipeline_specs_test(self):
        unique_test_value = 41291

        # Stop pipeline before updating specs.
        self.client.pause_pipeline(self.pipeline_path)
        wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "paused")

        # Get current specs.
        old_static_spec = self.client.flow_execute(
            self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={}
        )
        old_dynamic_spec = self.client.flow_execute(
            self.pipeline_path, flow_command="get-pipeline-dynamic-spec", flow_argument={}
        )

        # Modify both specs.
        static_spec = deepcopy(old_static_spec["spec"])
        static_spec["computations"]["reader"]["watermark_strategy"] = {
            "watermark_percentile": {"delay": unique_test_value}
        }

        dynamic_spec = deepcopy(old_dynamic_spec["spec"])
        dynamic_spec["job_tracker"]["load_throughput_throttler"]["period"] = unique_test_value

        # Create invalid spec.
        invalid_dynamic_spec = deepcopy(dynamic_spec)
        invalid_dynamic_spec["computations"]["strange_computation_not_in_static_spec"] = {}

        # Try to update with invalid spec - should fail validation.
        with pytest.raises(YtResponseError):
            self.client.flow_execute(
                self.pipeline_path,
                flow_command="set-pipeline-specs",
                flow_argument={
                    "spec": static_spec,
                    "dynamic_spec": invalid_dynamic_spec,
                    "allow_spec_update_on_pause": True,
                    "validate_strict": True,
                },
            )

        result = self.client.flow_execute(
            self.pipeline_path,
            flow_command="set-pipeline-specs",
            flow_argument={
                "spec": static_spec,
                "dynamic_spec": dynamic_spec,
                "allow_spec_update_on_pause": True,
                "validate_strict": True,
            },
        )

        assert result["spec_version"] == old_static_spec["version"] + 1
        assert result["dynamic_spec_version"] == old_dynamic_spec["version"] + 1

        # Verify both specs were updated.
        new_static_spec = self.client.flow_execute(
            self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={}
        )
        new_dynamic_spec = self.client.flow_execute(
            self.pipeline_path, flow_command="get-pipeline-dynamic-spec", flow_argument={}
        )

        assert new_static_spec["version"] == result["spec_version"]
        assert new_dynamic_spec["version"] == result["dynamic_spec_version"]
        assert (
            new_static_spec["spec"]["computations"]["reader"]["watermark_strategy"]["watermark_percentile"]["delay"]
            == unique_test_value
        )
        assert new_dynamic_spec["spec"]["job_tracker"]["load_throughput_throttler"]["period"] == unique_test_value

        # Start pipeline again.
        self.client.start_pipeline(self.pipeline_path)

    @pytest.mark.authors(["mikari"])
    def test_read_states(self):
        # state_writer increments per-key, per-partition and external_state counters for every input
        # event. Modes covered: (1) computation_id, (2) computation_id+key, (3) partition_id, plus
        # the external_state section (read + delete + re-read).
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        expected_total = len(INPUT_DATA)
        sample_key = INPUT_DATA[0]["data"]

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):

            def totals():
                res = self.client.read_states(
                    self.pipeline_path, computation_id="state_writer", limit=expected_total * 2
                )
                key_total = sum(e["states"]["/key_state"]["count"] for e in res["key_states"])
                partition_total = sum(
                    e["states"].get("/partition_state", {"count": 0})["count"] for e in res["partition_states"]
                )
                return key_total, partition_total

            # Mode 1: aggregate counters converge to the input size.
            wait(lambda: totals() == (expected_total, expected_total), timeout=180)

            # Pause so live state is stable for the rest of the test.
            self.client.pause_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "paused", timeout=180)

            # Mode 2: exact key lookup returns a single row with count=1.
            res = self.client.read_states(
                self.pipeline_path,
                computation_id="state_writer",
                key={"data": sample_key},
                name="/key_state",
                target="key_state",
            )
            assert len(res["key_states"]) == 1
            assert res["key_states"][0]["states"] == {"/key_state": {"count": 1}}
            assert res["partition_states"] == []

            # Mode 3: partition counters across all state_writer partitions sum to the input size.
            comp = self.client.flow_execute(
                self.pipeline_path,
                flow_command="describe-computation",
                flow_argument={"computation_id": "state_writer"},
            )
            partition_total = sum(
                entry["states"]["/partition_state"]["count"]
                for p in comp["partitions"]
                for entry in self.client.read_states(
                    self.pipeline_path,
                    partition_id=p["partition_id"],
                    name="/partition_state",
                    target="partition_state",
                )["partition_states"]
                if "/partition_state" in entry["states"]
            )
            assert partition_total == expected_total

            # Mode 3 + target filter on a reader partition (has both source-key state and partition state).
            reader_partition = self.client.flow_execute(
                self.pipeline_path, flow_command="describe-computation", flow_argument={"computation_id": "reader"}
            )["partitions"][0]["partition_id"]
            both = self.client.read_states(self.pipeline_path, partition_id=reader_partition)
            assert len(both["key_states"]) == 1
            only_keys = self.client.read_states(self.pipeline_path, partition_id=reader_partition, target="key_state")
            assert len(only_keys["key_states"]) == 1 and only_keys["partition_states"] == []
            only_partitions = self.client.read_states(
                self.pipeline_path, partition_id=reader_partition, target="partition_state"
            )
            assert only_partitions["key_states"] == []

            # external_state section: Mode 3 lists every manager row; joiner section is empty
            # (no readers wired on state_writer).
            res = self.client.read_states(
                self.pipeline_path, computation_id="state_writer", target="external_key_state", limit=expected_total * 2
            )
            manager_rows = res["external_key_states"]
            assert len(manager_rows) > 0
            assert all(r["states"] == {"/external_state": {"count": 1}} for r in manager_rows)
            assert res["joined_external_key_states"] == []

            # external_state Mode 1: exact-key lookup hits the manager only.
            res = self.client.read_states(
                self.pipeline_path, computation_id="state_writer", target="external_key_state", key={"data": sample_key}
            )
            assert len(res["external_key_states"]) == 1
            assert res["external_key_states"][0]["states"] == {"/external_state": {"count": 1}}
            assert res["joined_external_key_states"] == []

            # delete-states erases every row in the manager's table.
            result = self.client.delete_states(
                self.pipeline_path, computation_id="state_writer", target="external_key_state", commit=True, force=True
            )
            assert result["committed"]
            bucket = result["matched_states"]["external_key_states"]
            assert bucket["total"] == expected_total
            assert bucket["details"] == {"state_writer": {"/external_state": expected_total}}

            # After delete: manager rows are gone.
            res = self.client.read_states(
                self.pipeline_path, computation_id="state_writer", target="external_key_state", limit=expected_total * 2
            )
            assert res["external_key_states"] == []
            assert res["joined_external_key_states"] == []

            # Argument validation.
            for kwargs, match in [
                ({}, "Exactly one"),
                ({"computation_id": "state_writer", "partition_id": reader_partition}, "mutually exclusive"),
                ({"key": {"data": sample_key}, "partition_id": reader_partition}, "key requires computation_id"),
                (
                    {"computation_id": "state_writer", "key": {"data": sample_key}, "target": "partition_state"},
                    "target=partition_state cannot be combined with key",
                ),
                ({"computation_id": "magic"}, "Unknown computation"),
            ]:
                with pytest.raises(YtResponseError, match=match):
                    self.client.read_states(self.pipeline_path, **kwargs)

    @pytest.mark.authors(["mikari"])
    def test_delete_states(self):
        # delete-states erases rows in chunks. Modes mirror read-states; dry-run is the default.
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        states_path = self.pipeline_path + "/states"
        name_a, name_b = "/test_delete_alpha", "/test_delete_beta"

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: self._get_partitions_count() != 0)
            self._wait_epoch_sync()
            self.client.stop_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "stopped", timeout=180)

            self.client.insert_rows(
                states_path,
                [
                    {"computation_id": "reader", "key": [1], "name": name_a, "state": {"v": 1}},
                    {"computation_id": "reader", "key": [1], "name": name_b, "state": {"v": 2}},
                    {"computation_id": "reader", "key": [2], "name": name_a, "state": {"v": 3}},
                ],
            )

            # The pipeline run injects its own bookkeeping rows ($watermark, ...); restrict to ours.
            def synthetic():
                res = self.client.read_states(self.pipeline_path, computation_id="reader", limit=1000)
                return sorted(
                    (list(e["key"]), n) for e in res["key_states"] for n in e["states"] if n in (name_a, name_b)
                )

            initial = [([1], name_a), ([1], name_b), ([2], name_a)]
            assert synthetic() == initial

            # Dry-run preview: rows survive, count reflects the two alpha rows.
            preview = self.client.delete_states(self.pipeline_path, computation_id="reader", name=name_a)
            assert not preview["committed"]
            assert preview["matched_states"]["key_states"]["total"] == 2
            assert synthetic() == initial

            # Commit by name: alpha rows gone, beta survives.
            commit = self.client.delete_states(self.pipeline_path, computation_id="reader", name=name_a, commit=True)
            assert commit["committed"]
            assert commit["matched_states"]["key_states"]["total"] == 2
            assert synthetic() == [([1], name_b)]

            # Mode 2 (computation_id + key): scoped delete leaves nothing behind.
            self.client.delete_states(self.pipeline_path, computation_id="reader", key=[1], name=name_b, commit=True)
            assert synthetic() == []

            # Refusal to delete while the pipeline is running.
            self.client.start_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "working", timeout=180)
            with pytest.raises(YtResponseError, match="Cannot delete states while pipeline is"):
                self.client.delete_states(self.pipeline_path, computation_id="reader", name=name_a, commit=True)

    @pytest.mark.authors(["pechatnov"])
    def test_kill_worker(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        with pytest.raises(ProcessDiedException):
            with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
                # Wait flow to be started.
                wait(lambda: self._get_partitions_count() != 0)
                self._wait_epoch_sync()

                workers = self.client.flow_execute(self.pipeline_path, flow_command="describe-workers")
                assert len(workers["workers"]) > 0
                worker_address = workers["workers"][0]["address"]

                self.client.flow_execute(
                    self.pipeline_path, flow_command="kill-worker", flow_argument={"worker": worker_address}
                )

                wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "completed", timeout=180)

    @pytest.mark.authors(["timoninmaxim"])
    def test_flow_core_target_version(self):
        # set-/get-flow-core-target are handled by the YT-side pipeline node; older packaged YT
        # releases predate these commands. Variants bound to such packages disable this test via
        # FLOW_EXECUTE_TEST_FLOW_CORE_TARGET=0.
        if not self.TEST_FLOW_CORE_TARGET:
            pytest.skip("set-/get-flow-core-target not supported by the pinned YT package")

        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            run_pipeline=False,
            workers_count=1,
            controllers_count=1,
        ) as federation:
            # Test set flow core target.
            result = self.client.flow_execute(self.pipeline_path, "get-flow-core-target", {})
            assert result["flow_core_target"] == ""

            result = self.client.flow_execute(
                self.pipeline_path, "set-flow-core-target", {"flow_core_target": "v2.0.0"}
            )
            assert result["version"] == 1

            result = self.client.flow_execute(self.pipeline_path, "get-flow-core-target", {})
            assert result["flow_core_target"] == "v2.0.0"
            assert result["version"] == 1

            self.client.flow_execute(self.pipeline_path, "set-flow-core-target", {"flow_core_target": "v3.0.0"})
            result = self.client.flow_execute(self.pipeline_path, "get-flow-core-target", {})
            assert result["flow_core_target"] == "v3.0.0"
            assert result["version"] == 2

            result = self.client.flow_execute(self.pipeline_path, "set-flow-core-target", {"flow_core_target": ""})
            assert result["version"] == 3
            version_after_set = result["version"]

            result = self.client.flow_execute(self.pipeline_path, "get-flow-core-target", {})
            assert result["flow_core_target"] == ""
            assert result["version"] == 3

            result = self.client.flow_execute(
                self.pipeline_path,
                "set-flow-core-target",
                {"flow_core_target": "v3.0.0", "expected_version": version_after_set},
            )
            assert result["flow_core_target"] == "v3.0.0" if "flow_core_target" in result else True

            with pytest.raises(YtResponseError, match="FlowCoreTarget version mismatch"):
                self.client.flow_execute(
                    self.pipeline_path,
                    "set-flow-core-target",
                    {"flow_core_target": "v4.0.0", "expected_version": version_after_set},
                )

            self.client.flow_execute(self.pipeline_path, "set-flow-core-target", {"flow_core_target": "v2.0.0"})

            # Test spec update is rejected when flow core target mismatch.
            def flow_core_target_is_loaded(expect):
                flow_view = self.client.get_flow_view(
                    self.pipeline_path, "/state/execution_spec/flow_core_target/value", cache=False
                )
                return flow_view == expect

            wait(lambda: flow_core_target_is_loaded("v2.0.0"))

            with pytest.raises(
                YtResponseError,
                match="Flow core target mismatches",
            ):
                self.client.flow_execute(
                    self.pipeline_path,
                    "set-pipeline-specs",
                    {
                        "spec": self.client.flow_execute(self.pipeline_path, "get-pipeline-spec", {})["spec"],
                        "dynamic_spec": self.client.flow_execute(self.pipeline_path, "get-pipeline-dynamic-spec", {})[
                            "spec"
                        ],
                    },
                )

            self.client.flow_execute(self.pipeline_path, "set-flow-core-target", {"flow_core_target": ""})
            wait(lambda: flow_core_target_is_loaded(""))

            # Test set flow core target is rejected while pipeline is working.
            federation.start()

            self.wait_pipeline_state("working")

            with pytest.raises(
                YtResponseError,
                # Wording differs across YT versions (trunk: "cannot update FlowCoreTarget in this
                # state"; packaged: 'Cannot update FlowCoreTarget in state "Working": ...'), so match
                # the stable common fragment.
                match="update FlowCoreTarget in",
            ):
                self.client.flow_execute(self.pipeline_path, "set-flow-core-target", {"flow_core_target": "v2.0.0"})

            # Test set flow core target on pause requires flag.
            self.client.pause_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "paused")

            with pytest.raises(
                YtResponseError,
                # Wording differs across YT versions (trunk: "please stop pipeline before updating
                # FlowCoreTarget or use 'allow_update_on_pause=true'"; packaged: 'Cannot update
                # FlowCoreTarget in state "Paused": ... pass \'allow_update_on_pause=true\' ...'), so
                # match the stable common fragment naming the bypass flag.
                match="allow_update_on_pause=true",
            ):
                self.client.flow_execute(self.pipeline_path, "set-flow-core-target", {"flow_core_target": "v2.0.0"})

            self.client.flow_execute(
                self.pipeline_path,
                "set-flow-core-target",
                {"flow_core_target": "v2.0.0", "allow_update_on_pause": True},
            )
            result = self.client.flow_execute(self.pipeline_path, "get-flow-core-target", {})
            assert result["flow_core_target"] == "v2.0.0"

        # Test flow core target is recovered after restart.
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            run_pipeline=False,
            workers_count=1,
            controllers_count=1,
        ):

            def get_flow_core_target():
                try:
                    result = self.client.flow_execute(self.pipeline_path, "get-flow-core-target", {})
                    return result["flow_core_target"]
                except YtResponseError:
                    return None

            # Need wait while new Controller becomes leader.
            wait(lambda: get_flow_core_target() == "v2.0.0")

            # Test that using actual flow core target ublock pipeline operations.
            actual_target = self.client.flow_execute(
                self.pipeline_path,
                "get-controller-orchid",
                {"path": "/node_info/flow_core_version"},
            )["value"]

            self.client.flow_execute(
                self.pipeline_path,
                "set-flow-core-target",
                {"flow_core_target": actual_target, "allow_update_on_pause": True},
            )
            wait(lambda: flow_core_target_is_loaded(actual_target))

            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working")

    @pytest.mark.authors(["timoninmaxim"])
    def test_mutating_commands_rejected_when_completed(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            # Finite source drains the pipeline; it self-completes deterministically.
            self.wait_pipeline_state("completed")

            # Read-only commands keep working; the gate only fires on mutations.
            assert (
                self.client.flow_execute(self.pipeline_path, flow_command="get-pipeline-state", flow_argument={})[
                    "pipeline_state"
                ]
                == "completed"
            )
            self.client.flow_execute(self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={})
            self.client.flow_execute(self.pipeline_path, flow_command="get-flow-view", flow_argument={})
            self.client.flow_execute(self.pipeline_path, flow_command="describe-pipeline", flow_argument={})
            if self.TEST_FLOW_CORE_TARGET:
                self.client.flow_execute(self.pipeline_path, flow_command="get-flow-core-target", flow_argument={})

            completed_final_msg = "Recreate the pipeline to recover"

            for target in ("stopped", "paused", "completed"):
                with pytest.raises(YtResponseError, match=completed_final_msg):
                    self.client.flow_execute(
                        self.pipeline_path,
                        flow_command="set-target-pipeline-state",
                        flow_argument={"target_pipeline_state": target},
                    )

            # Spec updates that go through CheckPipelineSafeForSpecUpdate are rejected on Completed.
            # set-pipeline-spec delegates to set-pipeline-specs and so inherits the gate.
            current_spec = self.client.flow_execute(
                self.pipeline_path, flow_command="get-pipeline-spec", flow_argument={}
            )
            current_dynamic_spec = self.client.flow_execute(
                self.pipeline_path, flow_command="get-pipeline-dynamic-spec", flow_argument={}
            )
            new_spec = deepcopy(current_spec["spec"])
            new_spec["computations"]["reader"]["watermark_strategy"] = {"watermark_percentile": {"delay": 12345}}
            with pytest.raises(YtResponseError, match=completed_final_msg):
                self.client.flow_execute(
                    self.pipeline_path,
                    flow_command="set-pipeline-spec",
                    flow_argument={
                        "force": True,
                        "expected_version": current_spec["version"],
                        "spec": new_spec,
                    },
                )
            with pytest.raises(YtResponseError, match=completed_final_msg):
                self.client.flow_execute(
                    self.pipeline_path,
                    flow_command="set-pipeline-specs",
                    flow_argument={
                        "spec": new_spec,
                        "dynamic_spec": current_dynamic_spec["spec"],
                        "allow_spec_update_on_pause": True,
                    },
                )

            if self.TEST_FLOW_CORE_TARGET:
                with pytest.raises(
                    YtResponseError,
                    match=r'Cannot update FlowCoreTarget in state "Completed"',
                ):
                    self.client.flow_execute(
                        self.pipeline_path,
                        flow_command="set-flow-core-target",
                        flow_argument={"flow_core_target": "v2.0.0"},
                    )

            # Pipeline state stays Completed — none of the rejections accidentally mutated it.
            assert (
                self.client.flow_execute(self.pipeline_path, flow_command="get-pipeline-state", flow_argument={})[
                    "pipeline_state"
                ]
                == "completed"
            )
