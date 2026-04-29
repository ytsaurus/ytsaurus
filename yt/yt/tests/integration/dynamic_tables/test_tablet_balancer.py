from yt_dynamic_tables_base import DynamicTablesBase
from .test_tablet_actions import TabletActionsBase, TabletBalancerBase
from .test_dynamic_tables_profiling import TestStatisticsReporterBase

from yt_commands import (
    authors, set, get, ls, exists, update, wait, sync_mount_table, sync_reshard_table,
    insert_rows, sync_create_cells, sync_flush_table, remove, get_driver,
    sync_compact_table, wait_for_tablet_state, create_tablet_cell_bundle,
    sync_unmount_table, print_debug, select_rows, WaitFailed, remount_table,
    create, create_table_replica, sync_enable_table_replica, update_nodes_dynamic_config)

from yt.common import update_inplace

import yt.yson as yson

import pytest

from time import sleep
import builtins

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

    def _disable_table_balancing(self, path, driver=None):
        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        }
        set(f"{path}/@tablet_balancer_config", config, driver=driver)

    def _set_default_metric(self, metric):
        set(
            "//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups",
            {
                "default": {"parameterized": {"metric": metric}}
            }
        )

    def _enable_parameterized_reshard(self, group):
        set(
            f"//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/{group}/parameterized/enable_reshard",
            True
        )

    def _set_group_config(self, group, config):
        set(
            f"//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/{group}",
            config
        )

    def _wait_full_iteration(self):
        first_iteration_start_time = get(f"{self.root_path}/orchid/tablet_balancer/last_iteration_start_time")
        wait(lambda: first_iteration_start_time < get(f"{self.root_path}/orchid/tablet_balancer/last_iteration_start_time"))

    def _get_state_freshness_time(self):
        if self.bundle_state_freshness_time is None:
            self.bundle_state_freshness_time = get(self.config_path + "/bundle_state_provider/state_freshness_time") / 1000
        return self.bundle_state_freshness_time

    def _set_allowed_replica_clusters(self, clusters):
        self._apply_dynamic_config_patch({
            "allowed_replica_clusters": clusters,
        })

    def _wait_until_config_change_applied(self):
        sleep(self._get_state_freshness_time())

    @classmethod
    def modify_tablet_balancer_config(cls, config, multidaemon_config):
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 100,
                "parameterized_timeout_on_start": 0,
            },
            "election_manager": {
                "transaction_ping_period": 100,
                "leader_cache_update_period": 100,
                "lock_acquisition_period": 100,
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
        cls.bundle_state_freshness_time = None

    @classmethod
    def _apply_dynamic_config_patch(cls, patch, driver=None):
        config = get(cls.config_path, driver=driver)
        update_inplace(config, patch)
        set(cls.config_path, config, driver=driver)
        effective_config = None

        instances = ls(cls.root_path + "/instances", driver=driver)

        def config_updated_on_all_instances():
            for instance in instances:
                effective_config = get(
                    "{}/instances/{}/orchid/dynamic_config_manager/effective_config".format(cls.root_path, instance), driver=driver)
                if update(effective_config, config) != effective_config:
                    return False
            return True

        try:
            wait(config_updated_on_all_instances)
        except WaitFailed:
            print_debug("Effective config:", effective_config)
            print_debug("Expected config:", update(effective_config, config))
            raise

    def setup_method(self, method):
        super(TestStandaloneTabletBalancerBase, self).setup_method(method)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)


@authors("alexelexa")
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

    def test_builtin_tablet_balancer_disabled(self):
        assert not get("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer")

    def test_standalone_tablet_balancer_on(self):
        assert self._get_enable_tablet_balancer()
        assert get("//sys/tablet_balancer/config/enable_everywhere")

    def test_non_existent_group_config(self):
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_balancer_config/group", "non-existent")
        sleep(1)
        assert get("//tmp/t/@tablet_balancer_config/group") == "non-existent"

    def test_fetch_cell_only_from_secondary_in_multicell(self):
        self._apply_dynamic_config_patch({
            "fetch_tablet_cells_from_secondary_masters": True
        })
        self._test_simple_reshard()

    def test_pick_pivot_keys_merge(self):
        self._apply_dynamic_config_patch({
            "pick_reshard_pivot_keys": True,
        })

        self._test_simple_reshard()

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

    @authors("navasardianna")
    def test_errors_in_bundle_orchid(self):
        self._configure_bundle("default")

        set(
            "//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups",
            "string instead of map. Bazinga!"
        )

        def _get_orchid(suffix):
            return get(f"{self.root_path}/orchid/tablet_balancer{suffix}")

        def _has_error_at_root():
            errors = _get_orchid("/retryable_bundle_errors")
            return len(errors) > 0 and "Bundle has unparsable tablet balancer config" in str(errors)

        def _has_error_in_bundle_orchid():
            errors = _get_orchid("/bundles/default/retryable_errors")
            return len(errors) > 0 and "Bundle has unparsable tablet balancer config" in str(errors)

        def _has_bundle_config_error():
            config_error = _get_orchid("/bundles/default/config_error")
            return config_error and "Bundle has unparsable tablet balancer config" in str(config_error)

        wait(lambda: _has_error_at_root())
        wait(lambda: _has_error_in_bundle_orchid())
        wait(lambda: _has_bundle_config_error())

        self._apply_dynamic_config_patch({
            "bundle_errors_ttl": 100,
        })

        remove("//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups")

        wait(lambda: len(_get_orchid("/retryable_bundle_errors")) == 0)
        wait(lambda: len(_get_orchid("/bundles/default/retryable_errors")) == 0)

    @authors("navasardianna")
    def test_config_in_bundle_orchid(self):
        self._configure_bundle("default")

        set(
            "//sys/tablet_cell_bundles/default/@tablet_balancer_config/min_tablet_size",
            1
        )

        def _check_config():
            config = get(f"{self.root_path}/orchid/tablet_balancer/bundles/default/config")
            return config and config["min_tablet_size"] == 1

        wait(lambda: _check_config())

    @authors("navasardianna")
    def test_table_config_orchid(self):
        self._configure_bundle("default")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        effective_config_path = \
            "//sys/tablet_balancer/orchid/tablet_balancer/bundles/default/tables/\\/\\/tmp\\/t/effective_config"

        def _has_expected_table():
            if not exists(effective_config_path):
                return False

            config = get(effective_config_path)
            return len(config) > 0 and config["group_name"] == "legacy"

        wait(lambda: _has_expected_table())

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

    @authors("ifsmirnov")
    def test_smooth_movement(self):
        self._configure_bundle("default")
        cell_ids = sync_create_cells(2)

        existing_action_ids = builtins.set(ls("//sys/tablet_actions"))

        def _get_singular_new_action_id():
            nonlocal existing_action_ids

            new_action_ids = builtins.set(ls("//sys/tablet_actions"))
            diff = new_action_ids - existing_action_ids
            if len(diff) != 1:
                print_debug(diff)
            assert len(diff) == 1
            existing_action_ids = new_action_ids

            return diff.pop()

        self._create_sorted_table(
            "//tmp/t",
            in_memory_mode="compressed",
            pivot_keys=[[], [1]],
            tablet_balancer_config={
                "enable_auto_reshard": False,
                "enable_auto_tablet_move": True,
            })
        sync_mount_table("//tmp/t", cell_id=cell_ids[0])
        insert_rows("//tmp/t", [{"key": 0, "value": "a"}, {"key": 1, "value": "b"}])

        def _run_and_get_action():
            sync_unmount_table("//tmp/t")
            self._wait_full_iteration()
            sync_mount_table("//tmp/t", cell_id=cell_ids[0])

            wait(lambda: len(builtins.set(t["cell_id"] for t in get("//tmp/t/@tablets"))) == 2)

            action_id = _get_singular_new_action_id()

            action = None

            def _check():
                nonlocal action
                # Error is requested only to be displayed in test logs.
                action = get(f"#{action_id}/@", attributes=["kind", "state", "error"])
                return action["state"] == "completed"

            wait(_check)
            return action

        assert _run_and_get_action()["kind"] == "move"

        set("//tmp/t/@tablet_balancer_config/enable_smooth_movement", True)
        assert _run_and_get_action()["kind"] == "smooth_move"

        remove("//tmp/t/@tablet_balancer_config/enable_smooth_movement")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_smooth_movement", True)
        assert _run_and_get_action()["kind"] == "smooth_move"

        set("//tmp/t/@tablet_balancer_config/enable_smooth_movement", False)
        assert _run_and_get_action()["kind"] == "move"

        set("//tmp/t/@tablet_balancer_config/enable_smooth_movement", True)
        set("//sys/tablet_balancer/config/enable_smooth_movement", False)
        assert _run_and_get_action()["kind"] == "move"

    @authors("atalmenev")
    @pytest.mark.parametrize("cell_count", [1, 2])
    def test_inplace_reshard(self, cell_count):
        cell_ids = sync_create_cells(cell_count)

        existing_action_ids = builtins.set(ls("//sys/tablet_actions"))

        self._apply_dynamic_config_patch({
            "pick_reshard_pivot_keys": True,
        })

        def _run_and_get_actions(desired_tablet_count, expected_action_count=1):
            nonlocal existing_action_ids

            config = get("//tmp/t/@tablet_balancer_config")
            config.update({
                "enable_auto_reshard": True,
                "desired_tablet_count": desired_tablet_count,
            })
            set("//tmp/t/@tablet_balancer_config", config)

            new_action_ids = None

            def _has_enough_actions():
                nonlocal new_action_ids
                all_action_ids = builtins.set(ls("//sys/tablet_actions"))
                diff = all_action_ids - existing_action_ids
                if len(diff) >= expected_action_count:
                    new_action_ids = diff
                    return True
                self._wait_full_iteration()
                return False

            wait(_has_enough_actions)
            existing_action_ids = builtins.set(ls("//sys/tablet_actions"))
            set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)

            def _check_action(action_id):
                action = get(f"#{action_id}/@", attributes=["kind", "state", "error", "inplace_reshard"])
                return action["state"] in ("completed", "failed"), action

            actions = []
            for action_id in new_action_ids:
                wait(lambda: _check_action(action_id)[0])
                _, action = _check_action(action_id)
                actions.append(action)

            return actions

        self._create_sorted_table(
            "//tmp/t",
            tablet_balancer_config={
                "enable_auto_reshard": False,
                "enable_auto_tablet_move": False,
            },
            compression_codec="none")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": key, "value": "A"} for key in range(100)])
        sync_flush_table("//tmp/t")

        if cell_count == 1:
            # Feature config: split and merge without inplace.
            assert not _run_and_get_actions(desired_tablet_count=2)[0]["inplace_reshard"]
            assert get("//tmp/t/@tablet_count") == 2

            set("//sys/tablet_balancer/config/enable_inplace_split", True)
            set("//sys/tablet_balancer/config/enable_inplace_merge", True)

            # Merge with inplace enabled (same cell).
            assert _run_and_get_actions(desired_tablet_count=1)[0]["inplace_reshard"]
            assert get("//tmp/t/@tablet_count") == 1

            # Split with inplace enabled.
            assert _run_and_get_actions(desired_tablet_count=2)[0]["inplace_reshard"]
            assert get("//tmp/t/@tablet_count") == 2

            # Feature config: disable inplace.
            set("//sys/tablet_balancer/config/enable_inplace_split", False)
            set("//sys/tablet_balancer/config/enable_inplace_merge", False)

            assert not _run_and_get_actions(desired_tablet_count=1)[0]["inplace_reshard"]
            assert get("//tmp/t/@tablet_count") == 1

            assert not _run_and_get_actions(desired_tablet_count=2)[0]["inplace_reshard"]
            assert get("//tmp/t/@tablet_count") == 2

            return

        # Cross-cell merge with smooth movement.
        set("//sys/tablet_balancer/config/enable_inplace_split", True)
        set("//sys/tablet_balancer/config/enable_inplace_merge", True)

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [50]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0, cell_id=cell_ids[0])
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1, cell_id=cell_ids[1])

        tablet_cell_ids = [tablet["cell_id"] for tablet in get("//tmp/t/@tablets")]
        assert tablet_cell_ids[0] != tablet_cell_ids[1]

        config = get("//tmp/t/@tablet_balancer_config")
        config.update({
            "enable_smooth_movement": True,
        })
        set("//tmp/t/@tablet_balancer_config", config)

        actions = _run_and_get_actions(desired_tablet_count=1, expected_action_count=2)

        actions_by_kind = {action["kind"]: action for action in actions}
        assert "smooth_move" in actions_by_kind
        assert "reshard" in actions_by_kind
        assert actions_by_kind["reshard"]["inplace_reshard"]
        assert get("//tmp/t/@tablet_count") == 1

    def test_many_bundles(self):
        bundles = ["default", "another", "third", "fourth"]

        create_tablet_cell_bundle("another")
        create_tablet_cell_bundle("third")
        create_tablet_cell_bundle("fourth")

        for i, bundle in enumerate(bundles):
            self._configure_bundle(bundle)
            sync_create_cells(1, tablet_cell_bundle=bundle)
            self._create_sorted_table(f"//tmp/t{i}", tablet_cell_bundle=bundle)

        for index in range(len(bundles)):
            sync_reshard_table(f"//tmp/t{index}", [[], [1]])
            sync_mount_table(f"//tmp/t{index}")

        for index in range(len(bundles)):
            wait(lambda: get(f"//tmp/t{index}/@tablet_count") == 1)


@authors("alexelexa")
@pytest.mark.enabled_multidaemon
class TestStandaloneTabletBalancerSlow(TestStandaloneTabletBalancerBase, TabletActionsBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def modify_tablet_balancer_config(cls, config, multidaemon_config):
        super(TestStandaloneTabletBalancerSlow, cls).modify_tablet_balancer_config(config, multidaemon_config)
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 5000,
            },
        })

    def test_action_hard_limit(self):
        self._set_enable_tablet_balancer(False)
        self._apply_dynamic_config_patch({
            "max_actions_per_group": 1
        })

        self._configure_bundle("default")
        sync_create_cells(2)

        self._create_sorted_table("//tmp/t", optimize_for="lookup")

        set("//tmp/t/@max_partition_data_size", 320)
        set("//tmp/t/@desired_partition_data_size", 256)
        set("//tmp/t/@min_partition_data_size", 240)
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@chunk_writer", {"block_size": 64})

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


@authors("alexelexa")
@pytest.mark.enabled_multidaemon
class TestParameterizedBalancing(TestStandaloneTabletBalancerBase, DynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def modify_tablet_balancer_config(cls, config, multidaemon_config):
        super(TestParameterizedBalancing, cls).modify_tablet_balancer_config(config, multidaemon_config)
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 5000,
                # Prevent cross-test interference when waiting for perf counter recalculation
                # on a bundle with the same (default) name.
                "parameterized_timeout": 5000,
            },
        })

    def test_auto_move(self):
        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        }
        set("//tmp/t/@tablet_balancer_config", config)
        self._wait_until_config_change_applied()

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

    def test_parameterized_by_default(self):
        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        }
        set("//tmp/t/@tablet_balancer_config", config)
        self._wait_until_config_change_applied()

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)

        sync_reshard_table("//tmp/t", [[], [10]])
        sync_mount_table("//tmp/t", cell_id=cells[0])

        rows = [{"key": i, "value": str(i)} for i in range(3)]  # 3 rows
        rows.extend([{"key": i, "value": str(i)} for i in range(10, 11)])  # 1 row

        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        # The first update of EMA counter may not change rates due to lack of information about previous timestamps.
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        sleep(5)
        assert all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets"))

        set("//tmp/t/@tablet_balancer_config/enable_auto_tablet_move", True)

        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

    def test_config(self):
        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        self._wait_until_config_change_applied()

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

    @pytest.mark.parametrize(
        "parameterized_balancing_metric",
        [
            "double([/performance_counters/dynamic_row_write_count])",
            "double([/statistics/uncompressed_data_size])",
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

    @pytest.mark.parametrize("trigger_by", ["node", "cell"])
    def test_move_trigger(self, trigger_by):
        parameterized_balancing_metric = "double([/statistics/uncompressed_data_size])"

        cells = sync_create_cells(2)

        self._create_sorted_table("//tmp/t")
        self._set_default_metric(parameterized_balancing_metric)

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
        self._wait_until_config_change_applied()

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(400)])
        sync_flush_table("//tmp/t")

        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", True)
        self._wait_until_config_change_applied()

        wait(lambda: get("//tmp/t/@tablet_count") == 2)
        remove("//sys/tablet_balancer/config/pick_reshard_pivot_keys")

    @pytest.mark.parametrize(
        "parameterized_balancing_metric",
        [
            "double([/performance_counters/dynamic_row_write_count])",
            "double([/statistics/uncompressed_data_size])"
        ],
    )
    def test_merge(self, parameterized_balancing_metric):
        sync_create_cells(2)

        self._create_sorted_table("//tmp/t", optimize_for="lookup")
        self._set_default_metric(parameterized_balancing_metric)
        self._enable_parameterized_reshard("default")

        config = {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
            "desired_tablet_count": 2,
            "enable_parameterized": True
        }
        set("//tmp/t/@tablet_balancer_config", config)

        sync_reshard_table("//tmp/t", [[]] + [[i * 100] for i in range(1, 3)])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i * 100, "value": "value" * 100 + str(i)} for i in range(3)])
        insert_rows("//tmp/t", [{"key": 201, "value": "value" * 100 + "201"}])
        sync_flush_table("//tmp/t")

        for i in range(3):
            wait(lambda: get(f"//tmp/t/@tablets/{i}/performance_counters/dynamic_row_write_count") > 0)
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", True)

        wait(lambda: get("//tmp/t/@tablet_count") == 2)
        assert [[], [200]] == get("//tmp/t/@pivot_keys")

    @authors("h0tmi", "alexelexa")
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

    def test_aliases(self):
        cells = sync_create_cells(2)

        self._set_default_metric("write_1h")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_verbose_logging", True)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)

        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        sync_reshard_table("//tmp/t", [[], [10], [20], [30]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=1, cell_id=cells[0])
        sync_mount_table("//tmp/t", first_tablet_index=2, last_tablet_index=3, cell_id=cells[1])

        insert_rows("//tmp/t", [{"key": i * 10 + 1, "value": str(i)} for i in range(2)])

        def _check():
            tablets = get("//tmp/t/@tablets")
            return tablets[0]["cell_id"] != tablets[1]["cell_id"]

        wait(lambda: _check())

    @authors("dave11ar")
    @pytest.mark.parametrize(
        "parameterized_balancing_metric, desired_tablet_metric",
        [
            ("double([/performance_counters/dynamic_row_write_count])", 1),
            ("double([/statistics/uncompressed_data_size])", 273)
        ],
    )
    def test_reactive_balancing(self, parameterized_balancing_metric, desired_tablet_metric):
        sync_create_cells(2)

        # Disable balancing via schedule.
        self._set_default_schedule_formula("0")

        chunk_count = 4
        chunk_size = 273

        set(
            "//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups",
            {
                "reactive": {
                    "parameterized": {
                        "metric": parameterized_balancing_metric,
                        "enable_reshard": True,
                    },
                    "type": "parameterized",
                },
            },
        )

        table = "//tmp/t"
        self._create_sorted_table(
            table,
            tablet_balancer_config={
                "enable_auto_reshard": True,
                "group": "reactive",
                "desired_tablet_metric": desired_tablet_metric,
            },
            mount_config={
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
                "enable_compaction_and_partitioning": False,
            }
        )

        sync_mount_table(table)

        def _add_chunk(i):
            insert_rows(table, [{"key": i, "value": "v"}])
            sync_flush_table(table)

        for i in range(chunk_count):
            _add_chunk(i)
            assert chunk_size * (i + 1) == get(f"{table}/@tablets/0/statistics/uncompressed_data_size")

        assert get(f"{table}/@tablet_count") == 1
        assert len(get(f"{table}/@chunk_ids")) == chunk_count

        set(f"{table}/@mount_config/overload_reactive_balancing", {
            "metric": parameterized_balancing_metric,
            "limit": desired_tablet_metric + 0.1,
        })
        remount_table(table)

        update_nodes_dynamic_config({
            "tablet_node" : {
                "overload_reporter" : {
                    "enable" : True,
                    "periodic_options": {
                        "period": 1,
                        "splay": 0,
                        "jitter": 0,
                    },
                },
            },
        })

        wait(lambda: get(f"{table}/@tablet_count") == 4)


##################################################################


@pytest.mark.enabled_multidaemon
class TestStandaloneTabletBalancerMulticell(TestStandaloneTabletBalancer):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


@pytest.mark.enabled_multidaemon
class TestStandaloneTabletBalancerSlowMulticell(TestStandaloneTabletBalancerSlow):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


@pytest.mark.enabled_multidaemon
class TestParameterizedBalancingMulticell(TestParameterizedBalancing):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


##################################################################

class TestReplicaBalancing(TestStandaloneTabletBalancerBase, TestStatisticsReporterBase, DynamicTablesBase):
    NUM_REMOTE_CLUSTERS = 1
    NUM_MASTERS_REMOTE_0 = 1

    REMOTE_CLUSTER_NAME = "remote_0"

    STATISTICS_TABLE_PATH = "//sys/tablet_balancer/performance_counters"

    @classmethod
    def modify_tablet_balancer_config(cls, config, multidaemon_config):
        super(TestReplicaBalancing, cls).modify_tablet_balancer_config(config, multidaemon_config)
        update_inplace(config, {
            "tablet_balancer": {
                "period" : 1000,
                "parameterized_timeout": 1000,
            },
        })

    @classmethod
    def setup_class(cls):
        super(TestReplicaBalancing, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)

    def teardown_method(self, method):
        for driver in (self.remote_driver, None):
            remove(self.STATISTICS_TABLE_PATH, driver=driver, force=True)
        super(TestReplicaBalancing, self).teardown_method(method)

    @authors("alexelexa")
    def test_balancing_one_table_by_another(self):
        self._set_default_metric("double([/statistics/uncompressed_data_size])")
        self._set_allowed_replica_clusters(self.get_cluster_names())
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/default/parameterized/replica_clusters", self.get_cluster_names())

        cells = []
        tables = []
        for driver in (self.remote_driver, None):
            self._create_sorted_table(
                "//tmp/t",
                tablet_balancer_config={
                    "enable_auto_reshard": False,
                    "enable_auto_tablet_move": False,
                },
                driver=driver)
            tables.append((get("//tmp/t/@id", driver=driver), driver))

        sync_reshard_table("//tmp/t", [[], [100], [200], [300]], driver=None)
        sync_reshard_table("//tmp/t", [[], [10], [100], [200], [300]], driver=self.remote_driver)

        for driver in (self.remote_driver, None):
            cells = sync_create_cells(2, driver=driver)
            sync_mount_table("//tmp/t", cell_id=cells[0], driver=driver)
            self._setup_statistics_reporter(driver=driver, bundle="system")
            self._apply_dynamic_config_patch({
                "use_statistics_reporter": True,
            }, driver=driver)

        value = "a" * 100
        rows = [{"key": i, "value": value} for i in range(30)]  # 30 rows
        rows.extend([{"key": i, "value": value} for i in range(100, 110)])  # 10 rows
        rows.extend([{"key": i, "value": value} for i in range(200, 220)])  # 20 rows
        rows.extend([{"key": i, "value": value} for i in range(300, 320)])  # 20 rows

        insert_rows("//tmp/t", rows, driver=self.remote_driver)
        sync_flush_table("//tmp/t", driver=self.remote_driver)

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)
        set("//tmp/t/@tablet_balancer_config/enable_auto_tablet_move", True)

        for table_id, driver in tables:
            def select():
                return select_rows(f"* from [{self.STATISTICS_TABLE_PATH}] where table_id = \"{table_id}\"", driver=driver)
            print_debug(select())
            wait(lambda: len(select()) > 0)
            print_debug(select())

        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

        wait(lambda: all(get("#{0}/@state".format(action)) in ("completed", "failed")
             for action in ls("//sys/tablet_actions")))

        tablets = get("//tmp/t/@tablets")
        assert tablets[0]["cell_id"] == tablets[1]["cell_id"]
        assert tablets[2]["cell_id"] == tablets[3]["cell_id"]

    @authors("alexelexa")
    def test_replica_reshard(self):
        self._set_default_metric("double([/statistics/uncompressed_data_size])")
        self._set_allowed_replica_clusters(self.get_cluster_names())
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/default/parameterized/replica_clusters", self.get_cluster_names())
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)

        self._apply_dynamic_config_patch({
            "action_manager": {"max_tablet_count_per_action": 3}
        })

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}]

        create("replicated_table", "//tmp/replicated", attributes={"schema": schema, "dynamic": True}, driver=self.remote_driver)
        self._disable_table_balancing("//tmp/replicated", driver=self.remote_driver)
        self._wait_until_config_change_applied()

        modes = ["async", "sync"]
        replicas = []
        for i, mode in enumerate(modes):
            replicas.append(create_table_replica(
                "//tmp/replicated",
                self.get_cluster_name(i),
                "//tmp/t",
                attributes={"mode": mode},
                driver=self.remote_driver,
            ))

        tables = []
        for replica_id, driver in zip(replicas, (None, self.remote_driver)):
            self._create_sorted_table(
                "//tmp/t",
                schema=schema,
                upstream_replica_id=replica_id,
                tablet_balancer_config={
                    "enable_auto_reshard": False,
                    "enable_auto_tablet_move": False,
                },
                driver=driver)
            tables.append((get("//tmp/t/@id", driver=driver), driver))

        major_pivot_keys = [[], [100], [200], [300]]
        sync_reshard_table("//tmp/t", [[], [10], [100], [200], [300]])
        sync_reshard_table("//tmp/t", major_pivot_keys, driver=self.remote_driver)

        for driver in (self.remote_driver, None):
            sync_create_cells(1, driver=driver)
            sync_mount_table("//tmp/t", driver=driver)
            self._setup_statistics_reporter(driver=driver, bundle="system")
            self._apply_dynamic_config_patch({
                "use_statistics_reporter": True,
            }, driver=driver)

        for replica in replicas:
            sync_enable_table_replica(replica, driver=self.remote_driver)

        sync_mount_table("//tmp/replicated", driver=self.remote_driver)

        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", True)

        for table_id, driver in tables:
            def select():
                return select_rows(f"* from [{self.STATISTICS_TABLE_PATH}] where table_id = \"{table_id}\"", driver=driver)
            wait(lambda: len(select()) > 0)

        def _check_replica_modes():
            for replica, mode in zip(replicas, modes):
                assert get(f"#{replica}/@mode", driver=self.remote_driver) == mode

        _check_replica_modes()
        wait(lambda: major_pivot_keys == get("//tmp/t/@pivot_keys"))

        major_pivot_keys_list = [
            [[], [10], [100], [200], [300]],
            [[], [200]],
            [[], [10], [100], [200], [300]],
            [[]],
        ]

        for major_pivot_keys in major_pivot_keys_list:
            assert get("//tmp/t/@pivot_keys") != major_pivot_keys
            sync_unmount_table("//tmp/t", driver=self.remote_driver)
            sync_reshard_table("//tmp/t", major_pivot_keys, driver=self.remote_driver)
            sync_mount_table("//tmp/t", driver=self.remote_driver)

            _check_replica_modes()

            wait(lambda: major_pivot_keys == get("//tmp/t/@pivot_keys"))

        minor_pivot_keys = [[], [1], [2], [3], [6]]
        major_pivot_keys = [[], [4]]
        expected_pivot_keys_after_first_action = [[], [3], [4]]

        sync_unmount_table("//tmp/t", driver=self.remote_driver)
        sync_unmount_table("//tmp/t")

        sync_reshard_table("//tmp/t", minor_pivot_keys)
        sync_reshard_table("//tmp/t", major_pivot_keys, driver=self.remote_driver)

        sync_mount_table("//tmp/t")
        sync_mount_table("//tmp/t", driver=self.remote_driver)

        wait(lambda: expected_pivot_keys_after_first_action == get("//tmp/t/@pivot_keys"))
        wait(lambda: major_pivot_keys == get("//tmp/t/@pivot_keys"))


##################################################################


class TestMultiClusterTabletBalancer(TestStandaloneTabletBalancerBase, TestStatisticsReporterBase, DynamicTablesBase):
    NUM_REMOTE_CLUSTERS = 1
    NUM_MASTERS_REMOTE_0 = 1

    REMOTE_CLUSTER_NAME = "remote_0"

    STATISTICS_TABLE_PATH = "//sys/tablet_balancer/performance_counters"

    @classmethod
    def modify_tablet_balancer_config(cls, config, multidaemon_config):
        super(TestMultiClusterTabletBalancer, cls).modify_tablet_balancer_config(config, multidaemon_config)
        update_inplace(config, {
            "tablet_balancer": {
                "parameterized_timeout": 1000,
            },
        })

    @classmethod
    def setup_class(cls):
        super(TestMultiClusterTabletBalancer, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)

    def teardown_method(self, method):
        for driver in (self.remote_driver, None):
            remove(self.STATISTICS_TABLE_PATH, driver=driver, force=True)
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "", driver=self.remote_driver)
        super(TestMultiClusterTabletBalancer, self).teardown_method(method)

    @authors("alexelexa")
    def test_failed_replica_bundle(self):
        sync_create_cells(1, driver=self.remote_driver)
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "iguana", driver=self.remote_driver)
        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.remote_driver) == "failed")

        self._apply_dynamic_config_patch({
            "cluster_state_provider": {"clusters_for_bundle_health_check": [self.REMOTE_CLUSTER_NAME]}
        })

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")

        self._wait_full_iteration()
        self._wait_full_iteration()
        self._wait_full_iteration()
        assert get("//tmp/t/@tablet_count") == 2

        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "", driver=self.remote_driver)
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

    @authors("alexelexa")
    def test_too_many_unhealthy_bundles(self):
        self._apply_dynamic_config_patch({
            "max_unhealthy_bundles_on_replica_cluster": 3,
            "cluster_state_provider": {
                "clusters_for_bundle_health_check": [self.REMOTE_CLUSTER_NAME],
            }
        })

        for bundle in ("one", "two", "three"):
            create_tablet_cell_bundle(bundle, driver=self.remote_driver)
            sync_create_cells(1, tablet_cell_bundle=bundle, driver=self.remote_driver)
            set(f"//sys/tablet_cell_bundles/{bundle}/@node_tag_filter", "iguana", driver=self.remote_driver)
            wait(lambda: get(f"//sys/tablet_cell_bundles/{bundle}/@health", driver=self.remote_driver) == "failed")

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")

        sleep(5)
        assert get("//tmp/t/@tablet_count") == 2

        set("//sys/tablet_cell_bundles/one/@node_tag_filter", "", driver=self.remote_driver)
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

        set("//sys/tablet_cell_bundles/two/@node_tag_filter", "", driver=self.remote_driver)
        set("//sys/tablet_cell_bundles/three/@node_tag_filter", "", driver=self.remote_driver)

    @authors("alexelexa")
    def test_banned_replica_clusters(self):
        sync_create_cells(1, driver=self.remote_driver)
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "iguana", driver=self.remote_driver)
        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.remote_driver) == "failed")

        self._apply_dynamic_config_patch({
            "cluster_state_provider": {"clusters_for_bundle_health_check": [self.REMOTE_CLUSTER_NAME]}
        })

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")

        self._wait_full_iteration()
        self._wait_full_iteration()
        self._wait_full_iteration()
        assert get("//tmp/t/@tablet_count") == 2

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [self.REMOTE_CLUSTER_NAME])
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "", driver=self.remote_driver)

    @authors("alexelexa")
    def test_banned_replica_clusters_for_replica_balancing(self):
        self._set_default_metric("double([/statistics/uncompressed_data_size])")

        self._apply_dynamic_config_patch({
            "allowed_replica_clusters": self.get_cluster_names(),
            "cluster_state_provider": {"meta_cluster_for_banned_replicas": self.get_cluster_name(0)}
        })

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/groups/default/parameterized/replica_clusters", self.get_cluster_names())
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_parameterized_by_default", True)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}]

        create("replicated_table", "//tmp/replicated", attributes={"schema": schema, "dynamic": True})
        self._disable_table_balancing("//tmp/replicated")
        self._wait_until_config_change_applied()

        replica_id = create_table_replica(
            "//tmp/replicated",
            self.get_cluster_name(0),
            "//tmp/t",
            attributes={"mode": "async"}
        )

        self._create_sorted_table(
            "//tmp/t",
            schema=schema,
            upstream_replica_id=replica_id,
            tablet_balancer_config={
                "enable_auto_reshard": False,
                "enable_auto_tablet_move": False,
            })

        # A fake replica without upstream_replica_id should fail replica balancing iteration.
        self._create_sorted_table(
            "//tmp/t",
            schema=schema,
            tablet_balancer_config={
                "enable_auto_reshard": False,
                "enable_auto_tablet_move": False,
            },
            driver=self.remote_driver)

        sync_reshard_table("//tmp/t", [[], [10]])
        sync_reshard_table("//tmp/t", [[]], driver=self.remote_driver)

        for driver in (self.remote_driver, None):
            sync_create_cells(1, driver=driver)
            sync_mount_table("//tmp/t", driver=driver)
            self._setup_statistics_reporter(driver=driver, bundle="system")
            self._apply_dynamic_config_patch({
                "use_statistics_reporter": True,
            }, driver=driver)

        sync_enable_table_replica(replica_id)

        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", True)

        sync_mount_table("//tmp/replicated")

        self._wait_full_iteration()
        self._wait_full_iteration()
        assert len(get("//tmp/t/@pivot_keys")) == 2

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [self.REMOTE_CLUSTER_NAME])

        wait(lambda: len(get("//tmp/t/@pivot_keys")) == 1)

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [])

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [10]])
        sync_mount_table("//tmp/t")

        self._apply_dynamic_config_patch({
            "cluster_state_provider": {"meta_cluster_for_banned_replicas": self.REMOTE_CLUSTER_NAME}
        })

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [self.REMOTE_CLUSTER_NAME])

        # Banned replica cluster on a different cluster than the meta cluster should do nothing.
        self._wait_full_iteration()
        assert len(get("//tmp/t/@pivot_keys")) == 2

        set(
            "//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters",
            [self.REMOTE_CLUSTER_NAME],
            driver=self.remote_driver,
        )

        wait(lambda: len(get("//tmp/t/@pivot_keys")) == 1)
