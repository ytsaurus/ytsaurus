from .conftest import (
    DEFAULT_POD_SET_SPEC,
    YpOrchidClient,
    create_nodes,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
)

from yp.common import wait, WaitFailed

import yt.yson as yson

from yt.common import update

from yt.packages.six.moves import xrange

import datetime
import pytest
import time


@pytest.mark.usefixtures("yp_env_configurable")
class TestConfig(object):
    CONFIG_UPDATE_PERIOD = 1 * 1000

    YP_MASTER_CONFIG = dict(
        config_update_period=CONFIG_UPDATE_PERIOD,
    )

    def _get_config(self):
        instance_address = self._orchid.get_instances()[0]
        config = self._orchid.get(instance_address, "/config")
        config_update_time = self._orchid.get(instance_address, "/config/@update_time")
        return dict(config), datetime.datetime.strptime(config_update_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    def _get_initial_config(self):
        instance_address = self._orchid.get_instances()[0]
        return dict(self._orchid.get(instance_address, "/initial_config"))

    def _set_and_validate_config_patch(self, yp_env_configurable, value, type="document"):
        initial_config = self._get_initial_config()
        expected_config = update(initial_config, value)
        yp_env_configurable.set_cypress_config_patch(value, type)
        def is_patch_applied():
            config, _ = self._get_config()
            return expected_config == config
        wait(is_patch_applied)

    def _validate_config_stability(self):
        initial_config, initial_update_time = self._get_config()
        assert initial_config == self._get_initial_config()
        for i in xrange(100):
            config, update_time = self._get_config()
            assert initial_config == config
            assert initial_update_time == update_time
            time.sleep(0.05)

    def _set_and_validate_config_stability(self, yp_env_configurable, *args, **kwargs):
        yp_env_configurable.set_cypress_config_patch(*args, **kwargs)
        self._validate_config_stability()

    def _prepare_scheduler_validation(self, yp_client):
        create_nodes(yp_client, 1)
        pod_set_id = yp_client.create_object("pod_set", attributes=dict(spec=DEFAULT_POD_SET_SPEC))
        return create_pod_with_boilerplate(yp_client, pod_set_id, dict(
            resource_requests=dict(
                vcpu_guarantee=100,
            ),
            enable_scheduling=True,
        ))

    def _validate_scheduler_liveness(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        pod_id = self._prepare_scheduler_validation(yp_client)
        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

    def _validate_scheduler_lifelessness(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        pod_id = self._prepare_scheduler_validation(yp_client)
        with pytest.raises(WaitFailed):
            wait(
                lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)),
                iter=20,
                sleep_backoff=1.0,
            )

    def test_orchid(self, yp_env_configurable):
        self._orchid = yp_env_configurable.create_orchid_client()

        def test_default_fields(config):
            assert config["config_update_period"] == self.CONFIG_UPDATE_PERIOD
            assert "scheduler" in config
            assert "disable_stage" in config["scheduler"]
            assert config["scheduler"]["disable_stage"]["remove_orphaned_allocations"] == False
            assert "access_control_manager" in config
            assert "global_resource_allocator" in config["scheduler"]

        config, _ = self._get_config()
        test_default_fields(config)

        initial_config = self._get_initial_config()
        test_default_fields(initial_config)
        assert initial_config == config

        loop_period = config["scheduler"]["loop_period"]

        yp_env_configurable.set_cypress_config_patch(dict(scheduler=dict(loop_period=loop_period + 1)))
        def is_config_updated():
            new_config, _ = self._get_config()
            return config != new_config and new_config["scheduler"]["loop_period"] == loop_period + 1
        wait(is_config_updated)

        assert initial_config == self._get_initial_config()

        self._validate_scheduler_liveness(yp_env_configurable)

    def test_reconfiguration_stability(self, yp_env_configurable):
        self._orchid = yp_env_configurable.create_orchid_client()

        initial_config, initial_update_time = self._get_config()
        assert initial_update_time > datetime.datetime.fromtimestamp(0)

        # Without changes.
        self._validate_config_stability()

        # With incorrect types.
        self._set_and_validate_config_stability(yp_env_configurable, 42, "int64_node")

        self._set_and_validate_config_stability(yp_env_configurable, "abracadabra", "string_node")

        # With trivial changes of map type.
        self._set_and_validate_config_stability(yp_env_configurable, dict(), "map_node")

        # With trivial changes of document type.
        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict()))

        # With violated constraints.
        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=42))

        self._set_and_validate_config_stability(yp_env_configurable, dict(worker_thread_pool_size=-1))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict(
            global_resource_allocator=42,
        )))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict(
            global_resource_allocator=dict(
                every_node_selection_strategy=42,
            ),
        )))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict(
            global_resource_allocator=dict(
                every_node_selection_strategy=dict(
                    iteration_period="abracadabra",
                )
            ),
        )))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict(
            global_resource_allocator=dict(
                every_node_selection_strategy=dict(
                    iteration_period="abracadabra",
                ),
            ),
            failed_allocation_backoff=dict(
                start=42,
                max=42,
            ),
        )))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=yson.YsonEntity()))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict(
            global_resource_allocator=yson.YsonEntity(),
        )))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict(
            global_resource_allocator=dict(pod_node_score=dict(type="abracadbar")),
        )))

        self._set_and_validate_config_stability(yp_env_configurable, dict(scheduler=dict(
            disable_stage=dict(abracadabra="123"),
        )))

        self._set_and_validate_config_stability(yp_env_configurable, dict(worker_thread_pool_size=yson.YsonEntity()))

        self._validate_scheduler_liveness(yp_env_configurable)

    def test_reconfiguration(self, yp_env_configurable):
        self._orchid = yp_env_configurable.create_orchid_client()

        self._set_and_validate_config_patch(
            yp_env_configurable,
            dict(scheduler=dict(unknown_field="abracadabra")),
        )

        self._set_and_validate_config_patch(yp_env_configurable, dict(config_update_period=100500))

        self._set_and_validate_config_patch(yp_env_configurable, dict(worker_thread_pool_size=1))

        self._set_and_validate_config_patch(yp_env_configurable, dict(scheduler=dict(
            allocation_commit_concurrency=100,
            loop_period=10 * 1000,
            global_resource_allocator=dict(
                pod_node_score=dict(
                    type="node_random_hash",
                    parameters=dict(),
                ),
            ),
        )))

        self._set_and_validate_config_patch(yp_env_configurable, dict(
            unknown_field=42,
            unknown_field2="xxx",
            node_tracker=dict(),
            access_control_manager=dict(),
            accounting_manager=dict(),
            yt_connector=dict(
                user="xxx",
                root_path="////",
                instance_tag=42,
            ),
            transaction_manager=dict(
                input_row_limit=100500,
            ),
        ))

        self._validate_scheduler_liveness(yp_env_configurable)

    def test_scheduler_reconfiguration(self, yp_env_configurable):
        self._orchid = yp_env_configurable.create_orchid_client()

        # Update different parameters without easily visible side effects.
        self._set_and_validate_config_patch(yp_env_configurable, dict(scheduler=dict(
            loop_period=2 * 1000,
            failed_allocation_backoff=dict(
                start=5 * 1000,
                max=5 * 1000
            ),
            allocation_commit_concurrency=10,
            global_resource_allocator=dict(
                every_node_selection_strategy=dict(
                    enable=True,
                    iteration_period=5,
                    iteration_splay=3,
                ),
                pod_node_score=dict(
                    type="free_cpu_memory_share_variance",
                    parameters=dict(),
                ),
            ),
        )))
        self._validate_scheduler_liveness(yp_env_configurable)

        # Waits for the scheduler to apply config patch.
        def sync_scheduler_config():
            config, _ = self._get_config()
            time.sleep((config["config_update_period"] + config["scheduler"]["loop_period"]) * 2 / 1000.0)

        def test_incorrect_pod_node_score(pod_node_score):
            self._set_and_validate_config_patch(yp_env_configurable, dict(scheduler=dict(
                global_resource_allocator=dict(
                    pod_node_score=pod_node_score,
                ),
            )))
            sync_scheduler_config()
            self._validate_scheduler_lifelessness(yp_env_configurable)

        # Update pod node score incorrectly.
        yp_env_configurable.reset_cypress_config_patch()
        test_incorrect_pod_node_score(dict(type="node_random_hash", parameters=dict(seed="abracadabra")))

        # Disable the scheduler.
        self._set_and_validate_config_patch(yp_env_configurable, dict(scheduler=dict(disabled=True)))
        sync_scheduler_config()
        self._validate_scheduler_lifelessness(yp_env_configurable)

        # Disable the scheduler differently.
        self._set_and_validate_config_patch(yp_env_configurable, dict(scheduler=dict(disable_stage=dict(
            revoke_pods_with_acknowledged_eviction=True,
            remove_orphaned_allocations=True,
            schedule_pods=True,
        ))))
        sync_scheduler_config()
        self._validate_scheduler_lifelessness(yp_env_configurable)

        # Finally make sure we can recover the scheduler after all reconfigurations.
        yp_env_configurable.reset_cypress_config_patch()
        self._validate_scheduler_liveness(yp_env_configurable)
