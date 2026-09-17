import copy
import math
import os
import re

import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

FAIL_COMMENT = "TELEMETRY_TEST_INTENTIONAL_FAIL"

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)

    def prepare_pipeline_config(
        self,
        fail_comment=None,
        commit_gate=None,
        worker_lease_timeout=None,
        reader_class=None,
        consumer_class=None,
        fixed_input=False,
        fail_before_commit=False,
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        if reader_class is not None:
            pipeline_config["spec"]["computations"]["reader"]["computation_class_name"] = reader_class
        if consumer_class is not None:
            pipeline_config["spec"]["computations"]["consumer"]["computation_class_name"] = consumer_class
        if fail_comment is not None:
            pipeline_config["spec"]["computations"]["reader"]["parameters"]["fail_comment"] = fail_comment
        pipeline_config["spec"]["computations"]["reader"]["parameters"]["fail_before_commit"] = fail_before_commit
        if fixed_input:
            parameters = pipeline_config["dynamic_spec"]["computations"]["reader"]["source_streams"]["random"][
                "parameters"
            ]
            parameters["message_size_mean"] = 0
            parameters["message_key_range"] = 0
        if commit_gate is not None:
            ready_path, release_path = commit_gate
            reader_parameters = pipeline_config["spec"]["computations"]["reader"]["parameters"]
            reader_parameters["commit_gate_ready_path"] = ready_path
            reader_parameters["commit_gate_release_path"] = release_path
            reader_parameters["lineage_observation_path"] = ready_path + ".reader"
            pipeline_config["spec"]["computations"]["processor"]["parameters"]["lineage_observation_path"] = (
                ready_path + ".processor"
            )
            if consumer_class is not None:
                processor_parameters = pipeline_config["spec"]["computations"]["processor"]["parameters"]
                processor_parameters["commit_gate_ready_path"] = ready_path + ".service"
                processor_parameters["commit_gate_release_path"] = release_path + ".service"
        self.patch_config(pipeline_config)
        if worker_lease_timeout is not None:
            pipeline_config["dynamic_spec"]["job_manager"]["lost_job_timeout"] = worker_lease_timeout

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["mikari"])
    def test_fully_filtered_input_lineage(self):
        run_yt_sync("primary", self.work_yt_path)
        observation_path = os.path.join(self.path_to_flow_logs, "filtered_lineage_observation")
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        config["spec"]["computations"]["processor"]["parameters"]["lineage_observation_path"] = observation_path
        processor = config["dynamic_spec"]["computations"]["processor"]
        processor["skip_if_expression"] = "true"
        processor["parameters"]["desired_partition_count"] = 1
        self.patch_config(config)
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")

        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            # Observe the actual processing delta, without waiting for the lineage EMA to mature.
            wait(lambda: os.path.exists(observation_path), timeout=120)
            with open(observation_path) as observation:
                output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
            assert input_count > 0
            assert input_bytes > 0
            assert output_count == 0
            assert output_bytes == 0

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("consumer_class", ["TConsumer", "TSwiftConsumer"])
    def test_telemetry(self, consumer_class):
        run_yt_sync("primary", self.work_yt_path)
        commit_gate_ready_path = os.path.join(self.path_to_flow_logs, "lineage_commit_gate_ready")
        commit_gate_release_path = os.path.join(self.path_to_flow_logs, "lineage_commit_gate_release")
        pipeline_config_path = self.prepare_pipeline_config(
            commit_gate=(commit_gate_ready_path, commit_gate_release_path),
            consumer_class=consumer_class,
            fixed_input=True,
        )

        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_pipeline_state("working")

            selected_partitions = {}

            def find_job_status(computation_id, filter_func):
                flow_view = self.client.get_flow_view(self.pipeline_path, cache=False)
                partitions = flow_view["state"]["execution_spec"]["layout"]["partitions"]
                partition_job_statuses = flow_view["feedback"]["partition_job_statuses"]
                for partition_id, partition in partitions.items():
                    if partition["computation_id"] != computation_id:
                        continue
                    if computation_id in selected_partitions and selected_partitions[computation_id] != str(
                        partition_id
                    ):
                        continue
                    job_status = partition_job_statuses.get(partition_id, {}).get("current_job_status")
                    if not job_status:
                        continue
                    node = job_status.get("from_partition_traverse_data", {}).get("node", {})
                    if node.get("processing_rates"):
                        assert node["iteration_cycle"] > 0
                    if filter_func(job_status):
                        return job_status
                return None

            def check_epoch_part_times(job_status):
                return sum(job_status.get("epoch_part_times", {}).values()) > 0

            wait(lambda: os.path.exists(commit_gate_ready_path), timeout=60)

            def get_lineage_ratios():
                flow_view = self.client.get_flow_view(self.pipeline_path, cache=False)
                return flow_view.get("ephemeral_state", {}).get("lineage_ratios", {})

            def has_lineage_edge(output_stream, parent_stream):
                rate = get_lineage_ratios().get(output_stream, {}).get(parent_stream, {})
                return rate.get("count", {}).get("weight", 0) > 0 and rate["count"]["ratio"] > 0

            # Lineage is visible before the first source commit; processing rates are not.
            wait(lambda: os.path.exists(commit_gate_ready_path + ".reader"), timeout=60)
            wait(lambda: has_lineage_edge("data", "reader/random"), timeout=120)
            wait(lambda: find_job_status("reader", lambda status: status.get("inited_time")), timeout=60)
            pending_status = find_job_status("reader", lambda status: status.get("inited_time"))
            assert not pending_status.get("from_partition_traverse_data", {}).get("node", {}).get("processing_rates")

            with open(commit_gate_release_path, "w"):
                pass

            wait(lambda: find_job_status("reader", check_epoch_part_times), timeout=180)

            # The processor has selected inputs and completed DoProcess, but its epoch is not committed.
            wait(lambda: os.path.exists(commit_gate_ready_path + ".service"), timeout=60)
            with open(commit_gate_ready_path + ".service.cycle") as gate:
                selected_partitions["processor"], _ = gate.read().split()
            wait(lambda: find_job_status("processor", lambda status: status.get("inited_time")), timeout=60)
            pending_processor = find_job_status("processor", lambda status: status.get("inited_time"))
            # Empty epochs may have warmed up while waiting for the source heartbeat.
            pending_rates = (
                pending_processor.get("from_partition_traverse_data", {}).get("node", {}).get("processing_rates", {})
            )
            for rate in pending_rates.values():
                assert float(rate["processed"]["processed_messages_per_second"]) == 0
                assert float(rate["processed"]["processed_bytes_per_second"]) == 0
                assert not rate.get("capacity")
            with open(commit_gate_release_path + ".service", "w"):
                pass

            # Read the real processing deltas without waiting for the lineage EMA to mature.
            for computation in ("reader", "processor"):
                observation_path = commit_gate_ready_path + "." + computation
                wait(lambda: os.path.exists(observation_path), timeout=180)
                with open(observation_path) as observation:
                    output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
                assert output_count == input_count > 0
                assert output_bytes > 0
                assert input_bytes > 0

            def check_input_limits(job_status):
                input_buffer = job_status.get("input_limits", {}).get("input_buffer_bytes", {})
                return sum(v.get("used", 0) for v in input_buffer.values()) > 0

            wait(lambda: find_job_status("processor", check_input_limits), timeout=180)

            def get_output_limits_checker(name):
                def checker(job_status, name=name):
                    return sum(v.get("used", 0) for v in job_status.get("output_limits", {}).get(name, {}).values()) > 0

                return checker

            wait(lambda: find_job_status("reader", get_output_limits_checker("output_buffer_bytes")), timeout=180)
            wait(lambda: find_job_status("reader", get_output_limits_checker("output_store_bytes")), timeout=180)
            wait(lambda: find_job_status("reader", get_output_limits_checker("output_store_count")), timeout=180)

            def get_processing_rates(job_status):
                return job_status.get("from_partition_traverse_data", {}).get("node", {}).get("processing_rates")

            def get_cycle(job_status):
                return job_status["from_partition_traverse_data"]["node"]["iteration_cycle"]

            def check_processing_rates(job_status):
                processing_rates = get_processing_rates(job_status)
                rate = processing_rates.get("rate_1m") if processing_rates else None
                return (
                    processing_rates
                    and get_cycle(job_status) > 0
                    and rate
                    and all(
                        rate.get(kind) and math.isfinite(rate[kind][name]) and rate[kind][name] > 0
                        for kind in ("processed", "capacity")
                        for name in ("processed_messages_per_second", "processed_bytes_per_second")
                    )
                )

            # Verify the runtime-to-heartbeat path, including a consumer that emits nothing.
            for computation in ("reader", "processor", "consumer"):
                wait(lambda: find_job_status(computation, check_processing_rates), timeout=180)
                processing_rates = get_processing_rates(find_job_status(computation, check_processing_rates))
                if computation == "processor":
                    # Message IDs grow even for fixed payloads. The paired EMA ratio must
                    # stay within the observed processing batch averages, not the first epoch alone.
                    with open(commit_gate_ready_path + "." + computation + ".input_bytes") as bounds:
                        minimum, maximum = map(float, bounds.read().split())
                    processed = processing_rates["rate_1m"]["processed"]
                    ratio = processed["processed_bytes_per_second"] / processed["processed_messages_per_second"]
                    assert minimum - 1e-6 <= ratio <= maximum + 1e-6, (computation, ratio, minimum, maximum)
                if computation == "consumer":
                    assert processing_rates["rate_1m"]["capacity"]["processed_messages_per_second"] <= 1100
                previous = get_cycle(find_job_status(computation, check_processing_rates))
                wait(
                    lambda: find_job_status(
                        computation,
                        lambda status: check_processing_rates(status) and get_cycle(status) > previous,
                    ),
                    timeout=180,
                )

            service_ready_path = commit_gate_ready_path + ".service"
            service_release_path = commit_gate_release_path + ".service"
            # Hold a later epoch before commit; live Traverse must retain the last completed sample.
            before_gate = find_job_status("processor", check_processing_rates)
            previous_cycle = get_cycle(before_gate)
            os.remove(service_ready_path)
            os.remove(service_release_path)
            wait(lambda: os.path.exists(service_ready_path), timeout=60)
            with open(service_ready_path + ".cycle") as gate:
                partition_id, cycle = gate.read().split()
                assert partition_id == selected_partitions["processor"]
                published_cycle = int(cycle)
            assert published_cycle >= previous_cycle

            def is_pending_epoch(status):
                node = status.get("from_partition_traverse_data", {}).get("node", {})
                return node.get("iteration_cycle") == published_cycle

            wait(lambda: find_job_status("processor", is_pending_epoch), timeout=60)
            pending = find_job_status("processor", is_pending_epoch)
            retained = get_processing_rates(pending)
            assert retained
            assert get_cycle(pending) == published_cycle
            wait(
                lambda: find_job_status("processor", lambda status: status["update_time"] > pending["update_time"]),
                timeout=60,
            )
            assert get_processing_rates(find_job_status("processor", is_pending_epoch)) == retained
            with open(service_release_path, "w"):
                pass
            wait(
                lambda: find_job_status(
                    "processor",
                    lambda status: check_processing_rates(status) and get_cycle(status) > published_cycle,
                ),
                timeout=180,
            )

            # Lineage statistics are sent independently from regular job status heartbeats.
            wait(lambda: has_lineage_edge("data", "reader/random"), timeout=180)
            wait(lambda: has_lineage_edge("processed_data", "data"), timeout=180)

            flow_view = self.client.get_flow_view(self.pipeline_path, cache=False)
            assert all(
                not worker_status.get("statistics")
                for worker_status in flow_view.get("feedback", {}).get("worker_statuses", {}).values()
            )

            # TODO: Test computation retryable errors.
            # TODO: Test metrics.

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize(
        "computation_class", ["TProcessor", "TFilteringSwiftProcessor", "TReader", "TTransformReader"]
    )
    @pytest.mark.parametrize("skip_all", [False, True])
    def test_filter_processing_rates(self, computation_class, skip_all):
        run_yt_sync("primary", self.work_yt_path)
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        computation = "reader" if computation_class in ("TReader", "TTransformReader") else "processor"
        if computation == "reader":
            config["spec"]["computations"]["reader"]["source_streams"]["random"][
                "source_class_name"
            ] = "TFilteringTelemetrySource"
        else:
            config["spec"]["computations"]["reader"]["computation_class_name"] = "TFilteringTestReader"
        config["spec"]["computations"][computation]["computation_class_name"] = computation_class
        config["dynamic_spec"]["computations"][computation]["skip_if_expression"] = (
            "true" if skip_all else 'key = "drop"'
        )
        observation_path = os.path.join(self.path_to_flow_logs, "filter_lineage")
        config["spec"]["computations"][computation]["parameters"]["lineage_observation_path"] = observation_path
        self.patch_config(config)
        for downstream in ("processor", "consumer"):
            config["dynamic_spec"]["computations"][downstream]["parameters"]["desired_partition_count"] = 1
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):

            def check_rates():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                if not os.path.exists(observation_path + ".totals"):
                    return False
                with open(observation_path + ".totals") as observation:
                    output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
                if input_count < 1000:
                    return False
                count_ratio = output_count / input_count
                byte_ratio = output_bytes / input_bytes
                if skip_all:
                    assert count_ratio == byte_ratio == 0
                elif not (0.095 < count_ratio < 0.105 and 0 < byte_ratio < 0.02):
                    return False
                for partition_id, partition in view["state"]["execution_spec"]["layout"]["partitions"].items():
                    if partition["computation_id"] != computation:
                        continue
                    status = (
                        view["feedback"]["partition_job_statuses"].get(partition_id, {}).get("current_job_status", {})
                    )
                    rate = (
                        status.get("from_partition_traverse_data", {})
                        .get("node", {})
                        .get("processing_rates", {})
                        .get("rate_1m")
                    )
                    if not rate or not rate.get("capacity"):
                        return False
                    for kind in ("processed", "capacity"):
                        count = rate[kind]["processed_messages_per_second"]
                        byte_size = rate[kind]["processed_bytes_per_second"]
                        if count <= 0 or not math.isfinite(count) or not math.isfinite(byte_size):
                            return False
                        # Most input bytes belong to dropped rows, not the small kept payloads.
                        if byte_size / count < 3000:
                            return False
                    return True
                return False

            wait(check_rates, timeout=240)

            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            dynamic_spec["spec"]["computations"][computation]["skip_if_expression"] = "false"
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path,
                dynamic_spec["spec"],
                expected_version=dynamic_spec["version"],
            )
            with open(observation_path + ".totals") as observation:
                before = list(map(float, observation.read().split()))

            def check_reconfigured_filter():
                with open(observation_path + ".totals") as observation:
                    after = list(map(float, observation.read().split()))
                output_count, input_count, output_bytes, input_bytes = [
                    current - previous for current, previous in zip(after, before)
                ]
                return (
                    input_count >= 1000
                    and output_count / input_count > 0.9
                    and input_bytes > 0
                    and output_bytes / input_bytes > 0.9
                )

            wait(check_reconfigured_filter, timeout=120)

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("reader_class", ["TTransformReader", "TDelayedReader"])
    def test_source_processing_rates(self, reader_class):
        run_yt_sync("primary", self.work_yt_path)
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        reader = config["spec"]["computations"]["reader"]
        reader["computation_class_name"] = reader_class
        observation_path = os.path.join(self.path_to_flow_logs, "source_processed_lineage")
        if reader_class == "TDelayedReader":
            clock = copy.deepcopy(reader)
            clock["computation_class_name"] = "TTransformReader"
            clock["output_stream_ids"] = ["clock"]
            config["spec"]["computations"]["clock"] = clock
            config["spec"]["streams"]["clock"] = copy.deepcopy(config["spec"]["streams"]["data"])
            config["dynamic_spec"]["computations"]["clock"] = copy.deepcopy(
                config["dynamic_spec"]["computations"]["reader"]
            )
            reader["watermark_strategy"] = {"watermark_alignment": {"read_delays": {"clock": "1h"}}}
            reader["parameters"]["lineage_observation_path"] = observation_path
        self.patch_config(config)
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):

            def get_rates():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                for partition_id, partition in view["state"]["execution_spec"]["layout"]["partitions"].items():
                    if partition["computation_id"] != "reader":
                        continue
                    status = (
                        view["feedback"]["partition_job_statuses"].get(partition_id, {}).get("current_job_status", {})
                    )
                    node = status.get("from_partition_traverse_data", {}).get("node", {})
                    rate = node.get("processing_rates", {}).get("rate_1m")
                    if rate:
                        return rate
                return None

            wait(get_rates, timeout=180)
            rate = get_rates()
            for kind in ("processed", "capacity"):
                for name in ("processed_messages_per_second", "processed_bytes_per_second"):
                    assert math.isfinite(rate[kind][name]) and rate[kind][name] > 0
            if reader_class == "TDelayedReader":
                wait(lambda: os.path.exists(observation_path), timeout=60)
                with open(observation_path) as observation:
                    output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
                assert output_count == input_count > 0
                assert output_bytes > 0 and input_bytes > 0
                # Read delays block publication, not logical lineage.
                with open(os.path.join(self.path_to_flow_logs, "Worker_0_FilteredDebug.log")) as worker_log:
                    publications = re.findall(
                        r"Publishing batch \([^\n]*ComputationId: reader,[^\n]*SourceBatches: ([0-9]+)",
                        worker_log.read(),
                    )
                assert publications and all(int(count) == 0 for count in publications)

    @pytest.mark.authors(["mikari"])
    def test_delayed_publication_does_not_recount_lineage(self):
        run_yt_sync("primary", self.work_yt_path)
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        reader = config["spec"]["computations"]["reader"]
        clock = copy.deepcopy(reader)
        clock["computation_class_name"] = "TPublicationClockReader"
        clock["output_stream_ids"] = ["clock"]
        release_path = os.path.join(self.path_to_flow_logs, "release_publication")
        clock["parameters"]["publication_release_path"] = release_path
        config["spec"]["computations"]["clock"] = clock
        config["spec"]["streams"]["clock"] = copy.deepcopy(config["spec"]["streams"]["data"])
        config["dynamic_spec"]["computations"]["clock"] = copy.deepcopy(
            config["dynamic_spec"]["computations"]["reader"]
        )
        reader["computation_class_name"] = "TDelayedReader"
        reader["source_streams"]["random"]["source_class_name"] = "TSingleBatchTelemetrySource"
        reader["parameters"]["output_event_timestamp"] = 10000
        reader["watermark_strategy"] = {"watermark_alignment": {"read_delays": {"clock": "1h"}}}
        observation_path = os.path.join(self.path_to_flow_logs, "single_batch_lineage")
        reader["parameters"]["lineage_observation_path"] = observation_path
        downstream_path = observation_path + ".downstream"
        config["spec"]["computations"]["processor"]["parameters"]["lineage_observation_path"] = downstream_path
        self.patch_config(config)
        config["dynamic_spec"]["computations"]["processor"]["parameters"]["desired_partition_count"] = 1
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):

            def reader_cycle():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                for partition_id, partition in view["state"]["execution_spec"]["layout"]["partitions"].items():
                    if partition["computation_id"] == "reader":
                        status = (
                            view["feedback"]["partition_job_statuses"]
                            .get(partition_id, {})
                            .get("current_job_status", {})
                        )
                        return status.get("from_partition_traverse_data", {}).get("node", {}).get("iteration_cycle", 0)
                return 0

            wait(lambda: os.path.exists(observation_path), timeout=60)
            with open(observation_path) as observation:
                before = list(map(float, observation.read().split()))
            assert before[0] == before[1] == 1
            assert before[2] > 0 and before[3] > 0
            cycle = reader_cycle()
            wait(lambda: reader_cycle() > cycle + 1, timeout=60)
            assert not os.path.exists(downstream_path)

            with open(release_path, "w"):
                pass
            wait(lambda: os.path.exists(downstream_path), timeout=120)

            cycle = reader_cycle()
            wait(lambda: reader_cycle() > cycle, timeout=60)
            with open(observation_path + ".totals") as observation:
                after = list(map(float, observation.read().split()))
            assert after == before

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize("reader_class", ["TReader", "TTransformReader"])
    @pytest.mark.parametrize("slow_phase", ["fetch", "user_processing"])
    def test_source_work_limits_capacity(self, reader_class, slow_phase):
        run_yt_sync("primary", self.work_yt_path)
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        reader = config["spec"]["computations"]["reader"]
        reader["computation_class_name"] = reader_class
        if slow_phase == "fetch":
            reader["source_streams"]["random"]["source_class_name"] = "TSlowTelemetrySource"
        else:
            reader["parameters"]["processing_delay"] = "200ms"
            config["dynamic_spec"]["computations"]["reader"]["source_streams"]["random"]["parameters"][
                "message_count_mean"
            ] = 1
        self.patch_config(config)
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):

            def get_rate():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                for partition_id, partition in view["state"]["execution_spec"]["layout"]["partitions"].items():
                    if partition["computation_id"] != "reader":
                        continue
                    status = (
                        view["feedback"]["partition_job_statuses"].get(partition_id, {}).get("current_job_status", {})
                    )
                    return (
                        status.get("from_partition_traverse_data", {})
                        .get("node", {})
                        .get("processing_rates", {})
                        .get("rate_1m")
                    )
                return None

            wait(get_rate, timeout=180)
            rate = get_rate()
            capacity = rate["capacity"]["processed_messages_per_second"]
            processed = rate["processed"]["processed_messages_per_second"]
            # Each input requires 200 ms of the selected phase, not merely incidental commit work.
            assert 0 < processed <= capacity <= 5.01, rate

    @pytest.mark.authors(["pechatnov"])
    def test_source_replay_lineage(self):
        run_yt_sync("primary", self.work_yt_path)
        ready_path = os.path.join(self.path_to_flow_logs, "replay_ready")
        release_path = os.path.join(self.path_to_flow_logs, "replay_release")
        pipeline_config_path = self.prepare_pipeline_config(
            commit_gate=(ready_path, release_path), reader_class="TReplayReader"
        )
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: os.path.exists(ready_path), timeout=60)
            assert os.path.exists(ready_path + ".reader")
            with open(release_path, "w"):
                pass
            wait(lambda: os.path.exists(ready_path + ".reader"), timeout=60)
            with open(ready_path + ".reader") as observation:
                output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
            assert output_count == input_count > 0
            assert output_bytes > 0 and input_bytes > 0

            def has_replay_publication():
                with open(os.path.join(self.path_to_flow_logs, "Worker_0_FilteredDebug.log")) as worker_log:
                    publications = re.findall(
                        r"Publishing batch \([^\n]*ComputationId: reader,[^\n]*SourceBatches: ([0-9]+), Parsed: [0-9]+, Outputs: ([0-9]+)",
                        worker_log.read(),
                    )
                assert all(int(outputs) == 0 for _, outputs in publications)
                return any(int(inputs) > 0 for inputs, _ in publications)

            wait(has_replay_publication, timeout=60)

    @pytest.mark.authors(["pechatnov"])
    def test_worker_backtraces(self):
        if yatest.common.context.sanitize is not None:
            pytest.skip("Backtrace introspection is not supported with sanitizers")

        run_yt_sync("primary", self.work_yt_path)
        # Symbolization may block heartbeats beyond the default five-second test lease.
        pipeline_config_path = self.prepare_pipeline_config(worker_lease_timeout="30s")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: self.client.flow_execute(self.pipeline_path, "describe-workers")["workers"])
            workers = self.client.flow_execute(self.pipeline_path, "describe-workers")
            assert len(workers["workers"]) > 0
            worker_address = workers["workers"][0]["address"]
            res = self.client.flow_execute(self.pipeline_path, "get-worker-backtraces", {"worker": worker_address})
            with open(os.path.join(self.path_to_flow_logs, "get_worker_backtraces.txt"), "w") as f:
                f.write(res["text"])
            assert isinstance(res["text"], str) and len(res["text"]) > 0

    @pytest.mark.authors(["timoninmaxim"])
    @pytest.mark.parametrize("fail_before_commit", [False, True])
    @pytest.mark.parametrize("reader_class", ["TReader", "TTransformReader"])
    def test_job_failure(self, fail_before_commit, reader_class):
        run_yt_sync("primary", self.work_yt_path)
        ready_path = os.path.join(self.path_to_flow_logs, "failed_commit_ready")
        release_path = os.path.join(self.path_to_flow_logs, "failed_commit_release")
        pipeline_config_path = self.prepare_pipeline_config(
            fail_comment=FAIL_COMMENT,
            reader_class=reader_class,
            fail_before_commit=fail_before_commit,
            commit_gate=(ready_path, release_path) if fail_before_commit else None,
        )

        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_pipeline_state("working")

            if fail_before_commit:
                wait(lambda: os.path.exists(ready_path), timeout=60)
                assert os.path.exists(ready_path + ".reader")
                with open(release_path, "w"):
                    pass

            def check_job_fail_error():
                description = self.client.flow_execute(self.pipeline_path, "describe-pipeline")
                # Only a real job failure counts; the "Spec" message echoes fail_comment unconditionally.
                job_fail_messages = [
                    message
                    for message in description["computations"]["reader"]["messages"]
                    if str(message.get("text", "")).startswith("Job failed")
                ]
                assert any(FAIL_COMMENT in str(message) for message in job_fail_messages)
                return True

            wait(lambda: check_job_fail_error(), timeout=180, ignore_exceptions=True)
            assert os.path.exists(ready_path + ".reader") == fail_before_commit
            flow_view = self.client.get_flow_view(self.pipeline_path, cache=False)
            for partition_id, partition in flow_view["state"]["execution_spec"]["layout"]["partitions"].items():
                if partition["computation_id"] == "reader":
                    status = flow_view["feedback"]["partition_job_statuses"].get(partition_id, {})
                    job = status.get("current_job_status", {})
                    assert not job.get("from_partition_traverse_data", {}).get("node", {}).get("processing_rates")

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        "computation_class", ["TReader", "TTransformReader", "TFilteringSwiftProcessor", "TProcessor"]
    )
    @pytest.mark.parametrize("fail_commit", [False, True])
    def test_processing_rates_commit_boundary(self, computation_class, fail_commit):
        run_yt_sync("primary", self.work_yt_path)
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        computation = "reader" if computation_class in ("TReader", "TTransformReader") else "processor"
        ready_path = os.path.join(self.path_to_flow_logs, "processing_ready")
        release_path = os.path.join(self.path_to_flow_logs, "processing_release")
        observation_path = os.path.join(self.path_to_flow_logs, "processing_lineage")
        target = config["spec"]["computations"][computation]
        target["computation_class_name"] = computation_class
        target["parameters"].update(
            {
                "commit_gate_ready_path": ready_path,
                "commit_gate_release_path": release_path,
                "lineage_observation_path": observation_path,
            }
        )
        if fail_commit:
            target["parameters"]["fail_comment"] = FAIL_COMMENT
            if computation == "reader":
                target["parameters"]["fail_before_commit"] = True
        self.patch_config(config)
        config["dynamic_spec"]["computations"]["processor"]["parameters"]["desired_partition_count"] = 1
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: os.path.exists(ready_path), timeout=90)
            with open(ready_path + ".cycle") as gate:
                partition_id, _ = gate.read().split()

            def get_status():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                return view["feedback"]["partition_job_statuses"].get(partition_id, {}).get("current_job_status")

            wait(get_status, timeout=60)
            status = get_status()
            assert not status.get("from_partition_traverse_data", {}).get("node", {}).get("processing_rates")
            with open(observation_path) as observation:
                output_count, input_count, output_bytes, input_bytes = map(float, observation.read().split())
            assert output_count == input_count > 0
            assert output_bytes > 0 and input_bytes > 0
            with open(release_path, "w"):
                pass
            if fail_commit:

                def has_failure():
                    description = self.client.flow_execute(self.pipeline_path, "describe-pipeline")
                    return any(
                        str(message.get("text", "")).startswith("Job failed") and FAIL_COMMENT in str(message)
                        for message in description["computations"][computation]["messages"]
                    )

                wait(has_failure, timeout=180)
                assert os.path.exists(observation_path)
                status = get_status()
                if status:
                    assert not status.get("from_partition_traverse_data", {}).get("node", {}).get("processing_rates")
            else:
                wait(lambda: os.path.exists(observation_path), timeout=90)

                def has_rates():
                    status = get_status()
                    if not status:
                        return False
                    rate = (
                        status.get("from_partition_traverse_data", {})
                        .get("node", {})
                        .get("processing_rates", {})
                        .get("rate_1m")
                    )
                    return rate and rate["processed"]["processed_messages_per_second"] > 0 and rate.get("capacity")

                wait(has_rates, timeout=180)

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        "computation_class", ["TReader", "TTransformReader", "TProcessor", "TFilteringSwiftProcessor"]
    )
    def test_input_throttle_limits_capacity(self, computation_class):
        run_yt_sync("primary", self.work_yt_path)
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        computation = "reader" if computation_class in ("TReader", "TTransformReader") else "processor"
        config["spec"]["computations"][computation]["computation_class_name"] = computation_class
        config["spec"]["computations"][computation]["parameters"]["lineage_observation_path"] = os.path.join(
            self.path_to_flow_logs, "throttled_lineage"
        )
        self.patch_config(config)
        dynamic = config["dynamic_spec"]["computations"][computation]
        dynamic["input_rows_throttler_id"] = "input_quota"
        dynamic["max_rows_per_batch"] = 1
        config["dynamic_spec"]["computations"]["processor"]["parameters"]["desired_partition_count"] = 1
        config["dynamic_spec"]["throttlers"] = {
            "input_quota": {"limit": 10.0, "period": 1000, "request_period": 100, "max_grant_amount": 1}
        }
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):

            def get_rate():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                for partition_id, partition in view["state"]["execution_spec"]["layout"]["partitions"].items():
                    if partition["computation_id"] != computation:
                        continue
                    status = (
                        view["feedback"]["partition_job_statuses"].get(partition_id, {}).get("current_job_status", {})
                    )
                    if status.get("epoch_part_times", {}).get("Input.Throttle", 0) <= 0:
                        continue
                    return (
                        status.get("from_partition_traverse_data", {})
                        .get("node", {})
                        .get("processing_rates", {})
                        .get("rate_1m")
                    )
                return None

            wait(get_rate, timeout=180)
            rate = get_rate()
            # Quota waits are processing time in the current effective-capacity contract.
            assert (
                0
                < rate["processed"]["processed_messages_per_second"]
                <= rate["capacity"]["processed_messages_per_second"]
                <= 15
            ), rate
