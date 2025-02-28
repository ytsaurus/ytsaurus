from yt_dynamic_tables_base import DynamicTablesBase
from .test_tablet_actions import TabletActionsBase, TabletBalancerBase

from yt_commands import (
    authors, set, get, ls, update, wait, sync_mount_table, sync_reshard_table,
    insert_rows, sync_create_cells, sync_flush_table, remove,
    sync_compact_table, wait_for_tablet_state, create_tablet_cell_bundle, sync_unmount_table)

from yt.common import update_inplace

import pytest

from time import sleep

##################################################################


class TestStandaloneTabletBalancerBase:
    NUM_TABLET_BALANCERS = 3
    ENABLE_STANDALONE_TABLET_BALANCER = True

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

    def _turn_off_pivot_keys_picking(self):
        self._apply_dynamic_config_patch({
            "pick_reshard_pivot_keys": False,
        })

    def _get_instances_orchid(self, instances):
        for instance in instances:
            yield get(f"{self.root_path}/instances/{instance}/orchid/tablet_balancer")

    def _get_last_iteration_instance_orchid(self, instances=None):
        if instances is None:
            instances = ls(self.root_path + "/instances")

        start_times = list()
        for orchid in self._get_instances_orchid(instances):
            start_time = orchid.get("last_iteration_start_time")
            if start_time:
                start_times.append((start_time, orchid))
        return max(start_times, key=lambda pair: pair[0])[1]

    def _get_last_iteration_start_time(self, instances=None):
        return self._get_last_iteration_instance_orchid(instances=instances).get("last_iteration_start_time")

    def _wait_full_iteration(self):
        instances = ls(self.root_path + "/instances")
        first_iteration_start_time = self._get_last_iteration_start_time(instances)
        wait(lambda: first_iteration_start_time < self._get_last_iteration_start_time(instances))

    @classmethod
    def modify_tablet_balancer_config(cls, multidaemon_config, config):
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 100,
                "parameterized_timeout_on_start": 0,
            },
            "election_manager": {
                "transaction_ping_period": 100,
                "leader_cache_update_period": 100,
            }
        })
        if "logging" in config:
            for rule in config["logging"]["rules"]:
                rule.pop("exclude_categories", None)
        for rule in multidaemon_config["logging"]["rules"]:
            rule.pop("exclude_categories", None)

    @classmethod
    def setup_class(cls):
        super(TestStandaloneTabletBalancerBase, cls).setup_class()

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


@pytest.mark.enabled_multidaemon
class TestStandaloneTabletBalancer(TestStandaloneTabletBalancerBase, TabletBalancerBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 5

    def _test_simple_reshard(self):
        self._configure_bundle("default")
        sync_create_cells(2)
        self._create_sorted_table("//tmp/t2")
        sync_reshard_table("//tmp/t2", [[], [1]])
        sync_mount_table("//tmp/t2")
        wait(lambda: get("//tmp/t2/@tablet_count") == 1)

    @authors("alexelexa")
    def test_builtin_tablet_balancer_disabled(self):
        assert not get("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer")

    @authors("alexelexa")
    def test_standalone_tablet_balancer_on(self):
        assert self._get_enable_tablet_balancer()
        assert get("//sys/tablet_balancer/config/enable_everywhere")

    @authors("alexelexa")
    def test_non_existent_group_config(self):
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_balancer_config/group", "non-existent")
        sleep(1)
        assert get("//tmp/t/@tablet_balancer_config/group") == "non-existent"

    @authors("alexelexa")
    def test_fetch_cell_only_from_secondary_in_multicell(self):
        self._apply_dynamic_config_patch({
            "fetch_tablet_cells_from_secondary_masters": True
        })
        self._test_simple_reshard()

    @authors("alexelexa")
    def test_pick_pivot_keys_merge(self):
        self._apply_dynamic_config_patch({
            "pick_reshard_pivot_keys": True,
        })

        self._test_simple_reshard()

    @authors("alexelexa")
    @pytest.mark.parametrize("in_memory_mode", ["none", "uncompressed"])
    @pytest.mark.parametrize("with_hunks", [True, False])
    def test_pick_pivot_keys_split(self, in_memory_mode, with_hunks):
        self._apply_dynamic_config_patch({
            "pick_reshard_pivot_keys": True,
            "enable": False,
        })

        self._test_tablet_split(
            in_memory_mode=in_memory_mode,
            with_hunks=with_hunks,
            with_slicing=True)

    @authors("alexelexa")
    def test_by_bundle_errors(self):
        instances = get("//sys/tablet_balancer/instances")

        self._configure_bundle("default")
        sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])

        set(
            "//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups",
            "string instead of map. Bazinga!"
        )

        def _get_last_iteration_instance_retryable_errors(instances):
            orchid = self._get_last_iteration_instance_orchid(instances)
            return orchid.get("retryable_bundle_errors")

        def _check_parsing_error():
            errors = _get_last_iteration_instance_retryable_errors(instances)
            return len(errors) > 0 and str(errors).find("Bundle has unparsable tablet balancer config") > 0

        wait(lambda: _check_parsing_error())

        self._apply_dynamic_config_patch({
            "bundle_errors_ttl": 100,
        })

        remove("//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups")

        wait(lambda: len(_get_last_iteration_instance_retryable_errors(instances)) == 0)

    @authors("alexelexa")
    def test_move_table_between_bundles(self):
        create_tablet_cell_bundle("another")
        self._configure_bundle("default")
        self._configure_bundle("another")

        sync_create_cells(1, tablet_cell_bundle="another")
        sync_create_cells(1, tablet_cell_bundle="default")

        self._create_sorted_table("//tmp/t1", tablet_cell_bundle="another")
        self._create_sorted_table("//tmp/t2", tablet_cell_bundle="default")
        self._create_sorted_table("//tmp/t3", tablet_cell_bundle="default")

        for index in range(1, 4):
            sync_reshard_table(f"//tmp/t{index}", [[], [1]])
            sync_mount_table(f"//tmp/t{index}")

        for index in range(1, 4):
            wait(lambda: get(f"//tmp/t{index}/@tablet_count") == 1)

        sync_unmount_table("//tmp/t3")
        set("//tmp/t3/@tablet_cell_bundle", "another")

        self._create_sorted_table("//tmp/t4", tablet_cell_bundle="default")

        sync_reshard_table("//tmp/t3", [[], [1]])
        sync_reshard_table("//tmp/t4", [[], [1]])

        sync_mount_table("//tmp/t3")
        sync_mount_table("//tmp/t4")

        wait(lambda: get("//tmp/t3/@tablet_count") == 1)
        wait(lambda: get("//tmp/t4/@tablet_count") == 1)


@pytest.mark.enabled_multidaemon
class TestStandaloneTabletBalancerSlow(TestStandaloneTabletBalancerBase, TabletActionsBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def modify_tablet_balancer_config(cls, multidaemon_config, config):
        super(TestStandaloneTabletBalancerSlow, cls).modify_tablet_balancer_config(multidaemon_config, config)
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 5000,
            },
        })

    @authors("alexelexa")
    def test_action_hard_limit(self):
        self._set_enable_tablet_balancer(False)
        self._apply_dynamic_config_patch({
            "max_actions_per_group": 1
        })

        self._configure_bundle("default")
        sync_create_cells(2)

        self._create_sorted_table("//tmp/t")

        set("//tmp/t/@max_partition_data_size", 320)
        set("//tmp/t/@desired_partition_data_size", 256)
        set("//tmp/t/@min_partition_data_size", 240)
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@chunk_writer", {"block_size": 64})
        set("//tmp/t/@enable_verbose_logging", True)

        # Create four chunks expelled from eden
        sync_reshard_table("//tmp/t", [[], [1], [2], [3]])
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "A" * 256} for i in range(4)])
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        wait_for_tablet_state("//tmp/t", "mounted")
        set("//tmp/t/@tablet_balancer_config/min_tablet_size", 500)
        set("//tmp/t/@tablet_balancer_config/max_tablet_size", 1000)
        set("//tmp/t/@tablet_balancer_config/desired_tablet_size", 750)

        self._set_enable_tablet_balancer(True)

        wait(lambda: get("//tmp/t/@tablet_count") == 3)
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 3
        assert [[], [2], [3]] == [tablet["pivot_key"] for tablet in tablets]

        wait(lambda: get("//tmp/t/@tablet_count") == 2)
        self._apply_dynamic_config_patch({
            "max_actions_per_group": 100
        })

        self._apply_dynamic_config_patch({
            "max_actions_per_group": 100
        })


@pytest.mark.enabled_multidaemon
class TestParameterizedBalancing(TestStandaloneTabletBalancerBase, DynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def modify_tablet_balancer_config(cls, multidaemon_config, config):
        super(TestParameterizedBalancing, cls).modify_tablet_balancer_config(multidaemon_config, config)
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 5000,
            },
        })

    def _set_default_metric(self, metric):
        set(
            "//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups",
            {
                "default": {"parameterized": {"metric": metric}}
            }
        )

    def _enable_parameterized_reshard(self, group):
        set(
            f"//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/{group}/parameterized",
            {
                "enable_reshard": True,
            }
        )

    def _set_group_config(self, group, config):
        set(
            f"//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/{group}",
            config
        )

    @authors("alexelexa")
    def test_auto_move(self):
        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        }
        set("//tmp/t/@tablet_balancer_config", config)

        self._set_default_metric("double([/statistics/uncompressed_data_size])")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)

        sync_reshard_table("//tmp/t", [[], [10]])
        sync_mount_table("//tmp/t", cell_id=cells[0])

        rows = [{"key": i, "value": str(i)} for i in range(3)]  # 3 rows
        rows.extend([{"key": i, "value": str(i)} for i in range(10, 11)])  # 1 row

        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        sleep(5)
        assert all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets"))

        set("//tmp/t/@tablet_balancer_config/enable_auto_tablet_move", True)

        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

    @authors("alexelexa")
    def test_parameterized_by_default(self):
        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        }
        set("//tmp/t/@tablet_balancer_config", config)

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)

        sync_reshard_table("//tmp/t", [[], [10]])
        sync_mount_table("//tmp/t", cell_id=cells[0])

        rows = [{"key": i, "value": str(i)} for i in range(3)]  # 3 rows
        rows.extend([{"key": i, "value": str(i)} for i in range(10, 11)])  # 1 row

        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        sleep(5)
        assert all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets"))

        set("//tmp/t/@tablet_balancer_config/enable_auto_tablet_move", True)

        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

    @authors("alexelexa")
    def test_config(self):
        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)

        parameterized_balancing_metric = "double([/statistics/uncompressed_data_size])"
        self._set_default_metric(parameterized_balancing_metric)
        self._set_group_config("party", {"parameterized": {"metric": parameterized_balancing_metric}, "type": "parameterized"})

        sync_reshard_table("//tmp/t", [[], [5]])
        sync_mount_table("//tmp/t", cell_id=cells[0])

        rows = [{"key": i, "value": str(i)} for i in [0, 5]]  # 3 rows
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        sleep(5)
        assert all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets"))

        set("//tmp/t/@tablet_balancer_config/enable_parameterized", False)
        set("//tmp/t/@tablet_balancer_config/group", "party")

        sleep(5)
        assert all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets"))

        remove("//tmp/t/@tablet_balancer_config/enable_parameterized")
        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

    @authors("alexelexa")
    @pytest.mark.parametrize(
        "parameterized_balancing_metric",
        [
            "double([/performance_counters/dynamic_row_write_count])",
            "double([/statistics/uncompressed_data_size])"
        ],
    )
    @pytest.mark.parametrize("in_memory_mode", ["none", "uncompressed"])
    def test_move_distribution(self, parameterized_balancing_metric, in_memory_mode):
        cells = sync_create_cells(2)

        self._create_sorted_table(
            "//tmp/t",
            in_memory_mode=in_memory_mode,
            optimize_for="lookup")
        self._set_default_metric(parameterized_balancing_metric)

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)

        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": True,
        }
        set("//tmp/t/@tablet_balancer_config", config)

        sync_reshard_table("//tmp/t", [[], [10], [20], [30]])
        sync_mount_table("//tmp/t", cell_id=cells[0])

        rows = [{"key": i, "value": str(i)} for i in range(3)]  # 3 rows
        rows.extend([{"key": i, "value": str(i)} for i in range(10, 11)])  # 1 row
        rows.extend([{"key": i, "value": str(i)} for i in range(20, 22)])  # 2 rows
        rows.extend([{"key": i, "value": str(i)} for i in range(30, 32)])  # 2 rows

        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        set("//tmp/t/@tablet_balancer_config/enable_parameterized", True)

        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

        wait(lambda: all(get("#{0}/@state".format(action)) in ("completed", "failed")
             for action in ls("//sys/tablet_actions")))

        tablets = get("//tmp/t/@tablets")
        assert tablets[0]["cell_id"] == tablets[1]["cell_id"]
        assert tablets[2]["cell_id"] == tablets[3]["cell_id"]

    @authors("alexelexa")
    @pytest.mark.parametrize("trigger_by", ["node", "cell"])
    def test_move_trigger(self, trigger_by):
        parameterized_balancing_metric = "double([/statistics/uncompressed_data_size])"

        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        self._set_default_metric(parameterized_balancing_metric)

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)

        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": True,
        }
        set("//tmp/t/@tablet_balancer_config", config)

        self._apply_dynamic_config_patch({
            f"parameterized_{trigger_by}_deviation_threshold": 0.3
        })

        other_trigger = "node" if trigger_by == "cell" else "cell"
        self._apply_dynamic_config_patch({
            f"parameterized_{other_trigger}_deviation_threshold": 0.
        })

        sync_reshard_table("//tmp/t", [[]] + [[i] for i in range(1, 20)])

        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=8, cell_id=cells[0])  # 9 out of 20
        sync_mount_table("//tmp/t", first_tablet_index=9, last_tablet_index=19, cell_id=cells[1])  # 11 out of 20

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(20)])  # 20 rows, one row per tablet
        sync_flush_table("//tmp/t")

        assert (sum(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")) == 9)
        assert (sum(t["cell_id"] == cells[1] for t in get("//tmp/t/@tablets")) == 11)

        set("//tmp/t/@tablet_balancer_config/enable_parameterized", True)

        sleep(5)

        assert (sum(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")) == 9)
        assert (sum(t["cell_id"] == cells[1] for t in get("//tmp/t/@tablets")) == 11)

        self._apply_dynamic_config_patch({
            f"parameterized_{trigger_by}_deviation_threshold": 0.
        })

        wait(lambda: sum(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")) == 10)

    @authors("alexelexa")
    @pytest.mark.parametrize(
        "parameterized_balancing_metric",
        [
            "double([/performance_counters/dynamic_row_write_count])",
            "double([/statistics/uncompressed_data_size])"
        ],
    )
    def test_split(self, parameterized_balancing_metric):
        sync_create_cells(2)

        self._apply_dynamic_config_patch({
            "pick_reshard_pivot_keys": True,
        })

        self._create_sorted_table("//tmp/t")
        self._set_default_metric(parameterized_balancing_metric)
        self._enable_parameterized_reshard("default")

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)

        config = {
            "enable_auto_reshard": True,
            "enable_auto_tablet_move": False,
            "desired_tablet_count": 2,
            "enable_parameterized": True
        }
        set("//tmp/t/@tablet_balancer_config", config)

        sync_mount_table("//tmp/t")

        sleep(5)
        assert get("//tmp/t/@tablet_count") == 1

        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        self._wait_full_iteration()

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(400)])
        sync_flush_table("//tmp/t")

        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", True)
        self._wait_full_iteration()

        wait(lambda: get("//tmp/t/@tablet_count") == 2)
        remove("//sys/tablet_balancer/config/pick_reshard_pivot_keys")

    @authors("alexelexa")
    @pytest.mark.parametrize(
        "parameterized_balancing_metric",
        [
            "double([/performance_counters/dynamic_row_write_count])",
            "double([/statistics/uncompressed_data_size])"
        ],
    )
    def test_merge(self, parameterized_balancing_metric):
        sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        self._set_default_metric(parameterized_balancing_metric)
        self._enable_parameterized_reshard("default")

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)

        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
            "desired_tablet_count": 2,
            "enable_parameterized": True
        }
        set("//tmp/t/@tablet_balancer_config", config)

        sync_reshard_table("//tmp/t", [[]] + [[i * 100] for i in range(1, 3)])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i * 100, "value": str(i)} for i in range(3)])
        insert_rows("//tmp/t", [{"key": 201, "value": "201"}])
        sync_flush_table("//tmp/t")

        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", True)

        wait(lambda: get("//tmp/t/@tablet_count") == 2)
        assert [[], [200]] == get("//tmp/t/@pivot_keys")

    @authors("h0tmi")
    @pytest.mark.parametrize(
        "config_source",
        [
            "group",
            "global",
            "flag"
        ],
    )
    def test_uniform_tables_distribution(self, config_source):
        cells = sync_create_cells(2)

        parameterized_balancing_metric = "double([/statistics/uncompressed_data_size])"

        tables = ["//tmp/t1", "//tmp/t2"]

        for table in tables:
            self._create_sorted_table(table)

        self._set_default_metric(parameterized_balancing_metric)

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)

        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": True,
        }

        for table in tables:
            set(table + "/@tablet_balancer_config", config)

        factors = {
            "cell_factor" : 1,
            "node_factor" : 1,
            "table_cell_factor" : 1,
            "table_node_factor" : 1,
        }

        if config_source == "group":
            set(
                "//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/default/parameterized/factors",
                factors
            )
        elif config_source == "global":
            set(
                "//sys/tablet_balancer/config/parameterized_factors",
                factors
            )
        else:
            set(
                "//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/default/parameterized/per_table_uniform",
                True
            )

        for table in tables:
            sync_reshard_table(table, [[], [10]])
        for table in tables:
            sync_mount_table(table, cell_id=cells[0])

        rows = [{"key": i, "value": str(i)} for i in range(2)]  # 2 rows
        rows.extend([{"key": i, "value": str(i)} for i in range(9, 10)])  # 1 row
        rows.extend([{"key": i, "value": str(i)} for i in range(11, 12)])  # 1 row

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)

        for table in tables:
            insert_rows(table, rows)
        for table in tables:
            sync_flush_table(table)

        for table in tables:
            wait(lambda: not all(t["cell_id"] == cells[0] for t in get(table + "/@tablets")))

        wait(lambda: all(get("#{0}/@state".format(action)) in ("completed", "failed")
             for action in ls("//sys/tablet_actions")))

        for table in tables:
            tablets = get(table + "/@tablets")
            assert tablets[0]["cell_id"] != tablets[1]["cell_id"]

##################################################################


@pytest.mark.enabled_multidaemon
class TestStandaloneTabletBalancerMulticell(TestStandaloneTabletBalancer):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestStandaloneTabletBalancerSlowMulticell(TestStandaloneTabletBalancerSlow):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestParameterizedBalancingMulticell(TestParameterizedBalancing):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2
