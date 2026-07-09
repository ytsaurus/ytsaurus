import copy
import json
import logging
import time

import pytest
import yatest

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.common import wait

from .yt_sync import run_yt_sync, INPUT_QUEUE_SCHEMA

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline.yson")
NODE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../node_config.yson")

##################################################################


class TestTableInjectorBase(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../table_injector")
    USE_REMOTE_INPUT_QUEUE = False

    def setup_method(self, method):
        super(TestTableInjectorBase, self).setup_method(method)

        self.input_queue_cluster_name = self.remote_cluster_names[0] if self.USE_REMOTE_INPUT_QUEUE else "primary"
        self.input_queue_path = self.work_yt_path + "/t_input"
        self.input_consumer_path = self.work_yt_path + "/t_input_consumer"
        self.output_queue_path = self.work_yt_path + "/t_output"
        self.output_producer_path = self.work_yt_path + "/t_output_producer"

        run_yt_sync("primary", self.work_yt_path, skip_input_queue=self.USE_REMOTE_INPUT_QUEUE)

        if self.USE_REMOTE_INPUT_QUEUE:
            self._setup_remote_input_queue()

    def _setup_remote_input_queue(self):
        remote_client = self.cluster_name_to_client[self.input_queue_cluster_name]

        remote_client.create(
            "table",
            self.input_queue_path,
            attributes={
                "dynamic": True,
                "schema": INPUT_QUEUE_SCHEMA,
            },
        )
        remote_client.mount_table(self.input_queue_path, sync=True)

        self.client.register_queue_consumer(
            queue_path=f"{self.input_queue_cluster_name}:{self.input_queue_path}",
            consumer_path=f"primary:{self.input_consumer_path}",
            vital=True,
        )

    def prepare_pipeline_config(self, select_limit: int, finite: bool = True, use_compact_input_messages=None):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["dynamic_spec"]["computations"]["First"]["max_rows_per_batch"] = select_limit

        pipeline_config["spec"]["computations"]["First"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster={self.input_queue_cluster_name}>{self.input_queue_path}",
                "consumer_path": f"<cluster=primary>{self.input_consumer_path}",
                "finite": finite,
            }
        )
        pipeline_config["spec"]["computations"]["Second"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue_path}",
                "producer_path": f"<cluster=primary>{self.output_producer_path}",
            }
        )

        # "Second" has a uint64 hash key, so compact input messages are enabled by default.
        # Tests that inspect the "input_messages" table directly opt out to keep using the full table.
        if use_compact_input_messages is not None:
            pipeline_config["spec"]["computations"]["Second"]["use_compact_input_messages"] = use_compact_input_messages

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")


class TestTableInjector(TestTableInjectorBase):
    @pytest.mark.authors(["gryzlov-ad"])
    @pytest.mark.parametrize(
        ("fresh_queue",),
        [
            pytest.param(True, id="fresh_queue"),
            pytest.param(False, id="old_queue"),
        ],
    )
    def test_simple(self, fresh_queue):
        # This test inspects the "input_messages" table key column, so it needs the full table.
        pipeline_config_path = self.prepare_pipeline_config(select_limit=5, use_compact_input_messages=False)

        data = [{"data": f"hello_{str(i) * (i + 1)}"} for i in range(10)]

        if not fresh_queue:
            trash_data = [{"data": "trash_hello"}]
            self.client.insert_rows(self.input_queue_path, trash_data)
            self.client.advance_consumer(
                consumer_path=self.input_consumer_path,
                queue_path=self.input_queue_path,
                partition_index=0,
                old_offset=0,
                new_offset=len(trash_data),
            )

        with self.start_flow_process_federation(
            node_config=get_yson_config(NODE_CONFIG_PATH),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.client.insert_rows(self.input_queue_path, data)
            self.wait_pipeline_state("completed", timeout=180)

            expr = f"data from [{self.output_queue_path}] ORDER BY data LIMIT 100"
            wait(lambda: len(list(self.client.select_rows(expr))) == len(data), timeout=60)
            assert list(self.client.select_rows(expr)) == data

            # Check that 'key' field with type 'any' works correctly.
            keys_expr = f"key from [{self.pipeline_path}/input_messages] ORDER BY key LIMIT 100"
            keys = [row["key"] for row in self.client.select_rows(keys_expr)]

            assert sorted(keys) == keys
            for key in keys:
                assert isinstance(key, list)
                assert len(key) == 3

            middle_hash1 = keys[len(keys) // 2][0]
            middle_hash2 = keys[len(keys) // 2][1]
            half_keys = list(self.client.select_rows(f"""
                key
                FROM [{self.pipeline_path}/input_messages]
                WHERE
                    key >= yson_string_to_any('[{middle_hash1}u; {middle_hash2}u]')
                ORDER BY key LIMIT 100"""))
            assert len(half_keys) == (len(keys) + 1) // 2

            states = list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))
            assert len(states) == 0

    def _get_partition_job(self, partition_id):
        layout = self.client.get_flow_view(self.pipeline_path, view_path="/state/execution_spec/layout", cache=False)
        job = layout["partitions"][partition_id]["current_job_id"]
        return layout["jobs"][job]

    def _get_any_partition(self, computation_id):
        injector_partition_spec = None
        layout = self.client.get_flow_view(self.pipeline_path, view_path="/state/execution_spec/layout", cache=False)
        for partition_spec in layout["partitions"].values():
            if partition_spec["computation_id"] == computation_id:
                injector_partition_spec = partition_spec
                break
        assert injector_partition_spec is not None
        return injector_partition_spec

    @pytest.mark.authors(["gryzlov-ad"])
    def test_lease_recreated(self):
        pipeline_config_path = self.prepare_pipeline_config(select_limit=5, finite=False)

        data = [{"data": f"world_{i}"} for i in range(10)]

        with self.start_flow_process_federation(
            node_config=get_yson_config(NODE_CONFIG_PATH),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.client.insert_rows(self.input_queue_path, data[:5])
            expr = f"data from [{self.output_queue_path}] ORDER BY data LIMIT 100"
            wait(lambda: list(self.client.select_rows(expr)) == data[:5])

            injector_partition = self._get_any_partition("First")
            injector_job = self._get_partition_job(injector_partition["partition_id"])
            lease_id = injector_job["lease_id"]

            self.client.abort_transaction(lease_id)
            self.client.insert_rows(self.input_queue_path, data[5:])
            wait(lambda: len(list(self.client.select_rows(expr))) == len(data), timeout=60)
            assert lease_id not in str(
                self.client.get_flow_view(self.pipeline_path, cache=False)["state"]["execution_spec"]
            )

            new_injector_job = self._get_partition_job(injector_partition["partition_id"])
            assert new_injector_job["job_id"] != injector_job["job_id"]
            assert new_injector_job["lease_id"] != injector_job["lease_id"]

            # Check that lease is correctly recreated even without job failure due to unsuccessful commit.
            sink_partition = self._get_any_partition("Second")
            sink_job = self._get_partition_job(sink_partition["partition_id"])
            self.client.abort_transaction(sink_job["lease_id"])
            wait(lambda: self._get_partition_job(sink_partition["partition_id"])["lease_id"] != sink_job["lease_id"])

    @pytest.mark.authors(["gryzlov-ad"])
    def test_worker_dies(self):
        pipeline_config_path = self.prepare_pipeline_config(select_limit=42, finite=False)

        data = [{"data": f"hello_{i}"} for i in range(1000)]
        self.client.insert_rows(self.input_queue_path, data)

        with self.start_flow_process_federation(
            node_config=get_yson_config(NODE_CONFIG_PATH),
            workers_count=2,
            pipeline_binary_args={"--config": pipeline_config_path},
        ) as federation:

            expr = f"data from [{self.output_queue_path}]"
            wait(lambda: len(list(self.client.select_rows(expr))) >= 200, timeout=60)

            federation.workers[0].stop()

            wait(lambda: len(list(self.client.select_rows(expr))) == 1000, timeout=120)

            wait(lambda: len(list(self.client.select_rows(f"data from [{self.pipeline_path}/controller_logs]"))) > 0)

    def _get_pipeline_spec(self, spec_type, expected_version=None, **kwargs):
        if spec_type == "spec":
            get_spec_result = self.client.get_pipeline_spec(self.pipeline_path, **kwargs)
        else:
            get_spec_result = self.client.get_pipeline_dynamic_spec(self.pipeline_path, **kwargs)

        if expected_version is not None:
            assert get_spec_result["version"] == expected_version

        return get_spec_result["spec"]

    def _set_pipeline_spec(self, spec_type, spec, **kwargs):
        if spec_type == "spec":
            result = self.client.set_pipeline_spec(self.pipeline_path, spec, **kwargs)
        else:
            result = self.client.set_pipeline_dynamic_spec(self.pipeline_path, spec, **kwargs)
        if isinstance(result, bytes):
            result = json.loads(result)
        return result

    def _remove_pipeline_spec(self, spec_type, **kwargs):
        if spec_type == "spec":
            return self.client.remove_pipeline_spec(self.pipeline_path, **kwargs)
        else:
            return self.client.remove_pipeline_dynamic_spec(self.pipeline_path, **kwargs)

    def _wait_epoch_sync(self):
        def epoch_is_sync():
            global_epoch = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/traverse_data/global/epoch", cache=False
            )
            epoch = self.client.get_flow_view(self.pipeline_path, view_path="/state/epoch", cache=False)
            return epoch == global_epoch and epoch > 0

        wait(epoch_is_sync)

    # TODO(mikari): move to flow api
    def _wait_spec_sync(self):
        def spec_is_sync():
            spec = self.client.get_pipeline_spec(self.pipeline_path)
            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            execution_spec = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec", cache=False
            )
            return (
                execution_spec["pipeline_spec"]["version"] == spec["version"]
                and execution_spec["dynamic_pipeline_spec"]["version"] == dynamic_spec["version"]
            )

        wait(spec_is_sync)

    @pytest.mark.authors(["gryzlov-ad"])
    @pytest.mark.skip(reason="Broken due to schema is unfriendly to is_subdict:-(")
    @pytest.mark.parametrize("spec_type", ["spec", "dynamic_spec"])
    def test_get_set_pipeline_specs(self, spec_type):
        pass

    @pytest.mark.authors(["gryzlov-ad"])
    def test_start_stop_pipeline(self):
        with self.start_flow_process_federation(
            node_config=get_yson_config(NODE_CONFIG_PATH),
            run_pipeline=False,
        ):
            # This test compacts and inspects the "input_messages" table, so it needs the full table.
            pipeline_config = get_yson_config(
                self.prepare_pipeline_config(select_limit=5, finite=False, use_compact_input_messages=False)
            )
            for spec_type in ["spec", "dynamic_spec"]:
                new_version = self._set_pipeline_spec(spec_type, pipeline_config[spec_type], expected_version=0)[
                    "version"
                ]
                assert new_version == 1

            self.client.start_pipeline(self.pipeline_path)

            data = [{"data": f"world_{i}"} for i in range(10)]
            self.client.insert_rows(self.input_queue_path, data[:5])

            expr = f"data from [{self.output_queue_path}] ORDER BY data LIMIT 100"
            wait(lambda: list(self.client.select_rows(expr)) == data[:5], timeout=60)

            self.client.pause_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "paused")

            with pytest.raises(Exception):
                self._set_pipeline_spec("spec", pipeline_config[spec_type], force=False)

            spec_copy = copy.deepcopy(pipeline_config["spec"])
            spec_copy["computations"]["Second"]["parameters"]["key"] = "value"
            new_version = self._set_pipeline_spec("spec", spec_copy, force=True)["version"]
            assert new_version == 2
            # epoch could not became synced because pipeline is not executing
            self._wait_spec_sync()

            new_version = self._set_pipeline_spec("spec", pipeline_config["spec"], force=True)["version"]
            assert new_version == 3

            self.client.start_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "working")

            self.client.insert_rows(self.input_queue_path, data[5:])
            wait(lambda: len(list(self.client.select_rows(expr))) == len(data), timeout=60)
            assert list(self.client.select_rows(expr)) == data

            self.client.stop_pipeline(self.pipeline_path)
            wait(lambda: self.client.get_pipeline_state(self.pipeline_path) == "stopped", timeout=60)

            input_messages_table_path = f"{self.pipeline_path}/input_messages"
            self.client.set(f"{input_messages_table_path}/@mount_config/max_data_ttl", 0)
            self.client.remount_table(input_messages_table_path)
            # Flush: freeze + unfreeze
            self.client.freeze_table(input_messages_table_path, sync=True)
            self.client.unfreeze_table(input_messages_table_path, sync=True)
            # Compact: unmount, force compaction, remount
            chunk_ids = set(self.client.get(f"{input_messages_table_path}/@chunk_ids"))
            self.client.set(f"{input_messages_table_path}/@forced_compaction_revision", 1)
            self.client.unmount_table(input_messages_table_path, sync=True)
            self.client.mount_table(input_messages_table_path, sync=True)
            wait(
                lambda: len(chunk_ids.intersection(set(self.client.get(f"{input_messages_table_path}/@chunk_ids"))))
                == 0
            )
            assert list(self.client.select_rows(f"* from [{input_messages_table_path}]")) == []

    @pytest.mark.authors(["pechatnov"])
    def test_watermark_advancing_when_source_is_unavailable(self):
        pipeline_config_path = self.prepare_pipeline_config(select_limit=5)

        with self.start_flow_process_federation(
            node_config=get_yson_config(NODE_CONFIG_PATH),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):

            def get_times():
                streams = self.client.get_flow_view(
                    self.pipeline_path, view_path="/state/traverse_data/computations/First/streams", cache=False
                )
                return {
                    "event_watermark": int(streams["first"]["event_watermark"]),
                    "time": int(streams["queue"]["system_watermark"]),
                }

            def write_row_and_check():
                self.client.insert_rows(self.input_queue_path, [{"data": "hello_world"}])
                times = get_times()
                now = int(time.time())
                time_lag = now - times["time"]
                time_threshold = 4
                event_time_lag = now - times["event_watermark"]
                event_time_threshold = 9
                logging.info(
                    "Row is written (Now: %d, "
                    "Time: %d, TimeLag: %d, TimeThreshold: %d, "
                    "EventWatermark: %d, EventTimeLag: %d, EventTimeThreshold: %d)",
                    now,
                    times["time"],
                    time_lag,
                    time_threshold,
                    times["event_watermark"],
                    event_time_lag,
                    event_time_threshold,
                )
                # Check that time lag and watermark lag is small enough.
                return time_lag <= time_threshold and event_time_lag <= event_time_threshold

            def check_watermark_advancing():
                times = get_times()
                now = int(time.time())
                time_lag = now - times["time"]
                time_lower_threshold = 5
                event_time_lag = now - times["event_watermark"]
                event_time_lower_threshold = 5
                event_time_upper_threshold = 9
                logging.info(
                    "Check watermark advancing (Now: %d, "
                    "TimeWatermark: %d, TimeLag: %d, TimeLowerThreshold: %d, "
                    "EventWatermark: %d, EventTimeLag: %d, EventTimeLowerThreshold: %d, EventTimeUpperThreshold: %d)",
                    now,
                    times["time"],
                    time_lag,
                    time_lower_threshold,
                    times["event_watermark"],
                    event_time_lag,
                    event_time_lower_threshold,
                    event_time_upper_threshold,
                )
                # Check that time lag is high and event time lag is small enough.
                return event_time_lower_threshold <= event_time_lag <= event_time_upper_threshold
                # TODO: how to check that no reading has place?
                # return time_lag >= time_lower_threshold and event_time_lower_threshold <= event_time_lag <= event_time_upper_threshold

            # Ensure pipeline is in normal mode.
            wait(write_row_and_check, timeout=60)

            self.client.unmount_table(self.input_queue_path, sync=True)

            # Ensure watermark is increased.
            wait(check_watermark_advancing, timeout=60)
            # Wait to increase watermark lag to high value if watermark advancing doesn't work.
            time.sleep(5)
            # Ensure watermark is advancing.
            wait(check_watermark_advancing, timeout=60)

            injector_partition = self._get_any_partition("First")
            retryable_errors_when_unmounted = self.client.get_flow_view(
                self.pipeline_path,
                view_path=f"/feedback/partition_job_statuses/{injector_partition['partition_id']}/current_job_status/retryable_errors",
                cache=False,
            )

            self.client.mount_table(self.input_queue_path, sync=True)

            # Ensure pipeline is recovered.
            wait(write_row_and_check, timeout=60)

            # Intentionally do minor check after major checks.
            assert len(retryable_errors_when_unmounted) > 0


class TestTableInjectorWithRemoteQueue(TestTableInjectorBase):
    USE_REMOTE_INPUT_QUEUE = True

    @pytest.mark.authors(["pechatnov"])
    def test_simple(self):
        data = [{"data": f"hello_{str(i) * (i + 1)}"} for i in range(10)]
        pipeline_config_path = self.prepare_pipeline_config(select_limit=len(data))

        with self.start_flow_process_federation(
            node_config=get_yson_config(NODE_CONFIG_PATH),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.cluster_name_to_client[self.input_queue_cluster_name].insert_rows(self.input_queue_path, data)
            self.wait_pipeline_state("completed", timeout=180)

            expr = f"data from [{self.output_queue_path}] ORDER BY data LIMIT 100"
            wait(lambda: len(list(self.client.select_rows(expr))) == len(data), timeout=60)
            assert list(self.client.select_rows(expr)) == data

            expr = f"* from [{self.input_consumer_path}] LIMIT 100"
            offsets = list(self.client.select_rows(expr))
            assert len(offsets) == 1
            assert offsets[0]["queue_cluster"] == self.input_queue_cluster_name
            assert offsets[0]["queue_path"] == self.input_queue_path
            assert offsets[0]["partition_index"] == 0
            assert offsets[0]["offset"] == len(data)
