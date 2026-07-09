import pytest

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

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


class TestComputation(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super(TestComputation, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_queue_alt = self.work_yt_path + "/input_queue_alt"
        self.consumer = self.work_yt_path + "/consumer"
        self.consumer_alt = self.work_yt_path + "/consumer_alt"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.producer = self.work_yt_path + "/producer"

    def prepare_environment(self):
        run_yt_sync("primary", self.work_yt_path, TABLET_COUNT)

        batching_write_rows(INPUT_DATA, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)
        batching_write_rows(INPUT_DATA, lambda batch: self.client.insert_rows(self.input_queue_alt, batch), 100)

    def prepare_pipeline_config(self, cpu_aware, finite=True):
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

        pipeline_config["dynamic_spec"]["job_manager"]["use_cpu_aware_balancer"] = cpu_aware

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
        ("workers_count", "controllers_count", "update_type", "cpu_aware", "rename_target"),
        [
            pytest.param(1, 1, "pause", False, "computation", id="1c_1w_pause_greedy"),
            pytest.param(1, 1, "stop", False, "computation", id="1c_1w_stop_greedy"),
            pytest.param(1, 1, "pause", True, "computation", id="1c_1w_pause_cpu_aware"),
            pytest.param(1, 1, "stop", True, "computation", id="1c_1w_stop_cpu_aware"),
            pytest.param(1, 1, "stop", False, "source_stream", id="1c_1w_stop_source_stream"),
            pytest.param(1, 1, "stop", False, "sink", id="1c_1w_stop_sink"),
            pytest.param(1, 1, "stop", False, "pipeline_stream", id="1c_1w_stop_pipeline_stream"),
        ],
    )
    def test_rename(self, workers_count, controllers_count, update_type, cpu_aware, rename_target):
        assert update_type == "pause" or update_type == "stop"
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(cpu_aware)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=False,
        ):
            # Wait flow to be started.
            wait(lambda: self._get_partitions_count() != 0)
            self._wait_epoch_sync()

            # Pause/stop.
            if update_type == "pause":
                self.client.pause_pipeline(self.pipeline_path)
                self.wait_pipeline_state("paused", timeout=180)
            else:
                self.client.stop_pipeline(self.pipeline_path)
                self.wait_pipeline_state("stopped", timeout=180)

            # Update spec, renaming a pipeline object selected by rename_target.
            static_spec = self.client.get_pipeline_spec(self.pipeline_path)["spec"]
            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)["spec"]

            def rename_in_streams_dependency(comp, old_id, new_id):
                deps = comp.get("streams_dependency")
                if not deps:
                    return
                if old_id in deps:
                    deps[new_id] = deps.pop(old_id)
                for parents in deps.values():
                    if old_id in parents:
                        parents.remove(old_id)
                        parents.append(new_id)

            if rename_target == "computation":
                static_spec["computations"]["reader_renamed"] = static_spec["computations"].pop("reader")
                dynamic_spec["computations"]["reader_renamed"] = dynamic_spec["computations"].pop("reader")
            elif rename_target == "source_stream":
                reader = static_spec["computations"]["reader"]
                reader["source_streams"]["queue_renamed"] = reader["source_streams"].pop("queue")
                rename_in_streams_dependency(reader, "queue", "queue_renamed")
            elif rename_target == "sink":
                reader = static_spec["computations"]["reader"]
                reader["sinks"]["queue_renamed"] = reader["sinks"].pop("queue")
            elif rename_target == "pipeline_stream":
                static_spec["streams"]["data_renamed"] = static_spec["streams"].pop("data")
                reader = static_spec["computations"]["reader"]
                reader["output_stream_ids"] = ["data_renamed"]
                reader["sinks"]["queue"]["input_stream_ids"] = ["data_renamed"]
                rename_in_streams_dependency(reader, "data", "data_renamed")
            else:
                raise AssertionError(f"unknown rename_target {rename_target}")

            if update_type == "pause":
                self.client.set_pipeline_spec(self.pipeline_path, static_spec, force=True)
            else:
                self.client.set_pipeline_spec(self.pipeline_path, static_spec)
            self.client.set_pipeline_dynamic_spec(self.pipeline_path, dynamic_spec)

            # start again.
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state(["working", "completed"], timeout=180)

            # Wait for complete and done.
            self.wait_pipeline_state("completed", timeout=180)
            expr = f"data from [{self.output_queue}]"
            data = [row["data"] for row in self.client.select_rows(expr)]
            data.sort()
            if update_type == "stop" and rename_target == "computation":
                # Other rename targets discard state by design; only assert no crash + completion.
                assert data == EXPECTED_DATA

    def _collect_source_keys(self):
        partitions = self.client.get_flow_view(
            self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
        )
        result = []
        for partition in partitions.values():
            source_key = partition.get("source_key")
            if source_key:
                result.append(source_key)
        return result

    @pytest.mark.authors(["mikari"])
    def test_change_source_path(self):
        # YTFLOW-525: changing the queue_path in the source spec must produce a fresh set of partitions so
        # the new physical source is read from offset 0 (state of the previous source is discarded by
        # design).
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(cpu_aware=False)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: self._get_partitions_count() != 0)
            self._wait_epoch_sync()

            original_identities = self._layout_identities()
            assert original_identities, "expected at least one source partition"

            self.client.stop_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "stopped", timeout=180)

            static_spec = self.client.get_pipeline_spec(self.pipeline_path)["spec"]
            reader_params = static_spec["computations"]["reader"]["source_streams"]["queue"]["parameters"]
            reader_params["queue_path"] = f"<cluster=primary>{self.input_queue_alt}"
            reader_params["consumer_path"] = f"<cluster=primary>{self.consumer_alt}"
            self.client.set_pipeline_spec(self.pipeline_path, static_spec)

            self.client.start_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) in ("working", "completed"), timeout=180)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "completed", timeout=180)

            # The changed queue_path yields a different source identity, so every partition key is new.
            new_identities = self._layout_identities()
            assert new_identities, "expected at least one source partition after restart"
            assert not (new_identities & original_identities)

    # A source key is [stream id, source identity (an opaque hash of the identifying params),
    # partition coordinates...], so the identity is the second column.

    def _layout_identities(self):
        return {key[1] for key in self._collect_source_keys()}

    def _reader_state_identities(self):
        states = self.client.read_states(self.pipeline_path, computation_id="reader", limit=1000)
        return {entry["key"][1] for entry in states["key_states"]}

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize("restart_federation", [False, True], ids=["live_controller", "restarted_controller"])
    def test_source_change_erases_old_state(self, restart_federation):
        # YTFLOW-525: when the source identity changes, the partitions of the old source vanish from
        # ListKeys() while still executing. They must be completed (which erases their per-source-key
        # state), not interrupted (which would leave the old source-key state behind as garbage).
        # An infinite source keeps the original partitions executing until the identity change.
        # The restarted_controller variant mirrors a release rollout: the controller processes restart
        # between the spec change and the retirement, wiping the ephemeral dynamic partition specs,
        # and the vanished keys must still complete (YTFLOWSUPPORT-128).
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(cpu_aware=False, finite=False)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: self._get_partitions_count() != 0)
            self._wait_epoch_sync()

            # The original source read its partitions and persisted their per-source-key state.
            original_identities = self._layout_identities()
            assert original_identities, "expected at least one source partition"
            wait(lambda: self._reader_state_identities() & original_identities, timeout=180)

            self.client.stop_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "stopped", timeout=180)

            static_spec = self.client.get_pipeline_spec(self.pipeline_path)["spec"]
            reader_params = static_spec["computations"]["reader"]["source_streams"]["queue"]["parameters"]
            reader_params["queue_path"] = f"<cluster=primary>{self.input_queue_alt}"
            reader_params["consumer_path"] = f"<cluster=primary>{self.consumer_alt}"
            self.client.set_pipeline_spec(self.pipeline_path, static_spec)

            if not restart_federation:
                self._restart_pipeline_and_check_erasure(original_identities)

        if restart_federation:
            with self.start_flow_process_federation(
                pipeline_binary_args={"--config": pipeline_config_path},
                workers_count=1,
                controllers_count=1,
                problems=False,
                run_pipeline=False,
            ):
                self._restart_pipeline_and_check_erasure(original_identities)

    def _restart_pipeline_and_check_erasure(self, original_identities):
        self.client.start_pipeline(self.pipeline_path)
        wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "working", timeout=180)

        # The identity changed, so fresh partitions appear and the old ones are retired
        # (completed) and removed.
        wait(lambda: self._layout_identities() - original_identities, timeout=180)
        wait(lambda: not (self._layout_identities() & original_identities), timeout=180)
        self._wait_epoch_sync()

        # The new source persisted its own state, while the old identity's state was erased on
        # completion (interruption would have left it behind).
        wait(
            lambda: self._reader_state_identities() and not (self._reader_state_identities() & original_identities),
            timeout=180,
        )
