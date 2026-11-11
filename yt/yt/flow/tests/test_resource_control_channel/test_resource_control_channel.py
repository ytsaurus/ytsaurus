import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

WORKERS_COUNT = 2

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_published_value(self):
        """The `value` field of the spec payload the controller published for Counter."""
        return self.client.get_flow_view(
            self.pipeline_path,
            view_path="/state/execution_spec/resource_target_revisions/value/Counter/spec/value",
            cache=False,
        )

    def all_instances_applied(self, value):
        """Every worker and the controller-side instance decoded a spec-payload value at least
        `value`. The reported number is read out of the delivered payload, so this asserts the
        payload itself -- not just its revision id -- reached the instances. Monotone in time,
        so frequently published revisions cannot make it flap."""
        view = self.client.get_flow_view(
            self.pipeline_path,
            view_path="/ephemeral_state/resource_controller_views/Counter",
            cache=False,
        )
        applied = sum(count for decoded, count in view["workers_per_value"].items() if int(decoded) >= value)
        return applied >= WORKERS_COUNT and view.get("controller_value", 0) >= value

    @pytest.mark.authors(["mikari"])
    def test_target_revision_reaches_workers(self):
        """The spec payload published by the resource controller reaches every worker, and the
        values the workers decode from it keep catching up with the published one."""
        run_yt_sync("primary", self.work_yt_path)
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=WORKERS_COUNT,
            controllers_count=1,
        ):
            self.wait_pipeline_state("working")

            wait(lambda: self.get_published_value() >= 1, timeout=60, ignore_exceptions=True)
            first = self.get_published_value()
            wait(lambda: self.all_instances_applied(first), timeout=60, ignore_exceptions=True)

            # The counter keeps publishing new payloads and the workers keep catching up.
            wait(lambda: self.get_published_value() > first, timeout=60)
            second = self.get_published_value()
            wait(lambda: self.all_instances_applied(second), timeout=60, ignore_exceptions=True)

    @pytest.mark.authors(["mikari"])
    def test_pause_and_stop_remove_resource_assignments(self):
        run_yt_sync("primary", self.work_yt_path)
        pipeline_config_path = self.prepare_pipeline_config()

        def view():
            return self.client.get_flow_view(self.pipeline_path, cache=False)

        def loaded_instance_ids():
            current = view()
            instances = current["state"]["execution_spec"]["layout"]["resource_instances"]
            statuses = current["feedback"]["worker_statuses"]
            if len(instances) != WORKERS_COUNT:
                return set()
            for instance_id, instance in instances.items():
                status = (
                    statuses.get(instance["worker_address"], {}).get("resource_instance_statuses", {}).get(instance_id)
                )
                if not status or status["load_state"] != "loaded":
                    return set()
            return set(instances)

        def assert_assignments_removed():
            current = view()
            layout = current["state"]["execution_spec"]["layout"]
            assert not layout["jobs"]
            assert not layout["resource_instances"]

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=WORKERS_COUNT,
            controllers_count=1,
        ):
            self.wait_pipeline_state("working")
            wait(lambda: loaded_instance_ids(), timeout=60, ignore_exceptions=True)
            before_pause = loaded_instance_ids()
            self.client.pause_pipeline(self.pipeline_path)
            self.wait_pipeline_state("paused", timeout=120)
            assert_assignments_removed()

            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working")
            wait(lambda: loaded_instance_ids(), timeout=60, ignore_exceptions=True)
            assert before_pause.isdisjoint(loaded_instance_ids())

            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=120)
            assert_assignments_removed()

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize("preload,always_on", [(True, False), (False, False), (False, True)])
    def test_failed_load_retries_with_new_instance(self, preload, always_on):
        run_yt_sync("primary", self.work_yt_path)
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        counter = config["spec"]["resources"]["Counter"]
        counter["always_on"] = always_on
        counter["preload_required"] = preload
        counter["parameters"]["fail_first_load"] = True
        config["spec"]["computations"]["Consumer"]["required_resource_ids"]["Counter"]["controller"] = False
        config["dynamic_spec"]["job_manager"] = {"balancer_type": "resource_queue", "async_balancing": False}
        self.patch_config(config)
        config["dynamic_spec"]["job_manager"].pop("use_cpu_aware_balancer", None)
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")

        def consumer_assigned_after_load():
            view = self.client.get_flow_view(self.pipeline_path, cache=False)
            layout = view["state"]["execution_spec"]["layout"]
            for job in layout["jobs"].values():
                if layout["partitions"][job["partition_id"]]["computation_id"] != "Consumer":
                    continue
                for instance_id, instance in layout["resource_instances"].items():
                    if (
                        instance["resource_id"] == "Counter"
                        and instance["worker_address"] == job["worker_address"]
                        and instance["worker_incarnation_id"] == job["worker_incarnation_id"]
                    ):
                        status = view["feedback"]["worker_statuses"][job["worker_address"]]
                        return status["resource_instance_statuses"][instance_id]["load_state"] == "loaded"
            return False

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.wait_pipeline_state("working")
            wait(consumer_assigned_after_load, timeout=90, ignore_exceptions=True)
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=120)
