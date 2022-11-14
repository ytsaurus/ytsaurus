from .test_tablet_actions import TabletBalancerBase

from yt_commands import (
    authors, set, get, ls, update, wait, sync_mount_table, sync_reshard_table,
    insert_rows, sync_create_cells, sync_flush_table)

from yt.common import update_inplace

import pytest

##################################################################


class TestStandaloneTabletBalancer(TabletBalancerBase):
    NUM_TABLET_BALANCERS = 3
    ENABLE_STANDALONE_TABLET_BALANCER = True
    NUM_TEST_PARTITIONS = 5

    @classmethod
    def modify_tablet_balancer_config(cls, config):
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 100,
                "tablet_action_polling_period": 100,
            },
        })
        for rule in config["logging"]["rules"]:
            rule.pop("exclude_categories", None)

    @classmethod
    def setup_class(cls):
        super(TestStandaloneTabletBalancer, cls).setup_class()

        tablet_balancer_config = cls.Env._cluster_configuration["tablet_balancer"][0]
        cls.root_path = tablet_balancer_config.get("root", "//sys/tablet_balancer")
        cls.config_path = tablet_balancer_config.get("dynamic_config_path", cls.root_path + "/config")

    @classmethod
    def _apply_dynamic_config_patch(cls, patch):
        config = get(cls.config_path)
        update_inplace(config, patch)
        set(cls.config_path, config)

        instances = ls(cls.root_path + "/instances")

        def config_updated_on_all_instances():
            for instance in instances:
                effective_config = get(
                    "{}/instances/{}/orchid/dynamic_config_manager/effective_config".format(cls.root_path, instance))
                if update(effective_config, config) != effective_config:
                    return False
            return True

        wait(config_updated_on_all_instances)

    def _set_enable_tablet_balancer(self, value):
        self._apply_dynamic_config_patch({
            "enable": value
        })

    def _set_default_schedule_formula(self, value):
        self._apply_dynamic_config_patch({
            "schedule": value
        })

    def _get_enable_tablet_balancer(self):
        return get("//sys/tablet_balancer/config/enable")

    @authors("alexelexa")
    def test_builtin_tablet_balancer_disabled(self):
        assert not get("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer")

    @authors("alexelexa")
    def test_standalone_tablet_balancer_on(self):
        assert self._get_enable_tablet_balancer()
        assert get("//sys/tablet_balancer/config/enable_everywhere")

    @authors("alexelexa")
    @pytest.mark.parametrize(
        "parameterized_balancing_metric",
        [
            "double([/performance_counters/dynamic_row_write_count])",
            "double([/statistics/uncompressed_data_size])"
        ],
    )
    def test_parameterized_balancing(self, parameterized_balancing_metric):
        self._configure_bundle("default")
        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        set(
            "//sys/tablet_cell_bundles/default/@tablet_balancer_config/parameterized_balancing_metric",
            parameterized_balancing_metric
        )

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)

        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        }
        set("//tmp/t/@tablet_balancer_config", config)

        sync_reshard_table("//tmp/t", [[], [10], [20], [30]])
        sync_mount_table("//tmp/t", cell_id=cells[0])

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(3)])  # 3 rows
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(10, 11)])  # 1 row
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(20, 22)])  # 2 rows
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(30, 32)])  # 2 rows
        sync_flush_table("//tmp/t")

        set("//tmp/t/@enable_parameterized_balancing", True)

        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

        tablets = get("//tmp/t/@tablets")
        assert(tablets[0]["cell_id"] == tablets[1]["cell_id"])
        assert(tablets[2]["cell_id"] == tablets[3]["cell_id"])


##################################################################


class TestStandaloneTabletBalancerMulticell(TestStandaloneTabletBalancer):
    NUM_SECONDARY_MASTER_CELLS = 2
