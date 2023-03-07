import pytest
import __builtin__

from test_dynamic_tables import DynamicTablesBase

from yt_env_setup import wait, parametrize_external, Restarter, NODES_SERVICE
from yt_commands import *

from flaky import flaky

from time import sleep
from datetime import datetime, timedelta

##################################################################

class TabletActionsBase(DynamicTablesBase):
    ENABLE_TABLET_BALANCER = True

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 2000,
            "peer_revocation_timeout" : 3000,
            "tablet_balancer": {
                "config_check_period": 100,
                "balance_period": 100,
            },
            "tablet_action_manager": {
                "tablet_actions_cleanup_period": 100,
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100
        },
        "tablet_node": {
            "security_manager": {
                "resource_limits_cache": {
                    "expire_after_access_time": 0,
                },
            },
        },
        "master_cache_service": {
            "capacity": 0
        },
    }

    def _configure_bundle(self, bundle):
        set("//sys/tablet_cell_bundles/{0}/@tablet_balancer_config".format(bundle), {
            "cell_balance_factor": 0.0,
            "min_tablet_size": 128,
            "max_tablet_size": 512,
            "desired_tablet_size": 256,
            "min_in_memory_tablet_size": 0,
            "max_in_memory_tablet_size": 512,
            "desired_in_memory_tablet_size": 256,
        })

    def _get_tablets(self, path):
        tablets = get(path + "/@tablets")
        while True:
            result = []
            for tablet in tablets:
                result.append(get("#{0}/@".format(tablet["tablet_id"])))

            retry = False
            for state in ["state", "expected_state"]:
                actual = {}
                for tablet in result:
                    actual[tablet[state]] = actual.get(tablet[state], 0) + 1
                expected = get(path + "/@tablet_count_by_" + state)
                expected = {k: v for k, v in expected.items() if v != 0}
                if expected != actual:
                    retry = True

            if not retry:
                return result

    def _tablets_distribution(self, table, cells=None):
        tablet_count = {}
        for tablet in get("{}/@tablets".format(table)):
            cell_id = tablet["cell_id"]
            tablet_count[cell_id] = tablet_count.get(cell_id, 0) + 1
        if cells is None:
            return sorted(tablet_count.values())
        else:
            return [tablet_count.get(cell_id, 0) for cell_id in cells]

    def _validate_state(self, tablets, state=None, expected_state=None):
        if state is not None:
            assert state == [tablet["state"] if s is not None else None for tablet, s in zip(tablets, state)]
        if expected_state is not None:
            assert expected_state == [
                tablet["expected_state"] if s is not None else None
                for tablet, s in zip(tablets, expected_state)]

    def _validate_tablets(self, path, state=None, expected_state=None):
        self._validate_state(self._get_tablets(path), state=state, expected_state=expected_state)

################################################################################

class TestTabletActions(TabletActionsBase):
    @authors("savrus")
    def test_create_action_permissions(self):
        create_user("u")
        create_tablet_cell_bundle("b")
        cells = sync_create_cells(2, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t", cell_id=cells[0])
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _create_action():
            create("tablet_action", "", attributes={
                "kind": "move",
                "tablet_ids": [tablet_id],
                "cell_ids": [cells[1]]},
                authenticated_user="u")

        with pytest.raises(YtError): _create_action()
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", ["use"]))
        _create_action()

    @authors("savrus")
    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_move(self, skip_freezing, freeze):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        e = "frozen" if freeze else "mounted"
        self._validate_tablets("//tmp/t", state=[e], expected_state=[e])
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "move",
            "skip_freezing": skip_freezing,
            "keep_finished": True,
            "tablet_ids": [tablet_id],
            "cell_ids": [cells[1]]})
        wait(lambda: len(ls("//sys/tablet_actions")) > 0)
        assert action == ls("//sys/tablet_actions")[0]
        wait(lambda: get("#{0}/@state".format(action)) == "completed")
        tablets = self._get_tablets("//tmp/t")
        assert tablets[0]["cell_id"] == cells[1]
        self._validate_state(tablets, state=[e], expected_state=[e])

    @authors("savrus")
    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_reshard(self, skip_freezing, freeze):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        e = "frozen" if freeze else "mounted"
        self._validate_tablets("//tmp/t", state=[e], expected_state=[e])

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "reshard",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "tablet_ids": [tablet_id],
            "pivot_keys": [[], [1]]})
        wait(lambda: len(ls("//sys/tablet_actions")) > 0)
        assert action == ls("//sys/tablet_actions")[0]
        wait(lambda: get("#{0}/@state".format(action)) == "completed")
        self._validate_tablets("//tmp/t", state=[e, e], expected_state=[e, e])

    # TODO(ifsmirnov): YT-10550
    @authors("ifsmirnov")
    @flaky(max_runs=5)
    def test_action_autoremove(self):
        cells = sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=cells[0])
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "reshard",
            "expiration_time": (datetime.utcnow() + timedelta(seconds=5)).isoformat(),
            "tablet_ids": [tablet_id],
            "pivot_keys": [[], [1]]})
        wait(lambda: len(ls("//sys/tablet_actions")) > 0)
        assert action == ls("//sys/tablet_actions")[0]
        wait(lambda: get("#{0}/@state".format(action)) == "completed")
        assert get("#{0}/@tablet_ids".format(action)) == []
        wait(lambda: not exists("#{0}".format(action)))

    @authors("ifsmirnov", "ilpauzner")
    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_failed_after_table_removed(self, skip_freezing, freeze):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        with Restarter(self.Env, NODES_SERVICE):
            action = create("tablet_action", "", attributes={
                "kind": "move",
                "keep_finished": True,
                "skip_freezing": skip_freezing,
                "tablet_ids": [tablet_id],
                "cell_ids": [cells[1]]})
            remove("//tmp/t")
            wait(lambda: get("#{0}/@state".format(action)) == "failed")
        assert get("#{0}/@error".format(action))

    @authors("ifsmirnov")
    @pytest.mark.parametrize("touch", ["mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_failed_after_tablet_touched(self, skip_freezing, freeze, touch):
        touches = {
            "mount": [mount_table, "mounted"],
            "unmount": [unmount_table, "unmounted"],
            "freeze": [freeze_table, "frozen"],
            "unfreeze": [unfreeze_table, "mounted"]
        }
        touch_callback = touches[touch][0]
        expected_touch_state = touches[touch][1]
        expected_action_state = "failed"
        expected_state = "frozen" if freeze else "mounted"

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        tablet1 = get("//tmp/t/@tablets/0/tablet_id")
        tablet2 = get("//tmp/t/@tablets/1/tablet_id")
        with Restarter(self.Env, NODES_SERVICE):
            action = create("tablet_action", "", attributes={
                "kind": "move",
                "keep_finished": True,
                "skip_freezing": skip_freezing,
                "tablet_ids": [tablet1, tablet2],
                "cell_ids": [cells[1], cells[1]]})
            try:
                touch_callback("//tmp/t", first_tablet_index=0, last_tablet_index=0)
            except Exception as e:
                expected_touch_state = expected_state
                expected_action_state = "completed"
            self._validate_tablets("//tmp/t", expected_state=[None, expected_state])

        wait(lambda: get("#{0}/@state".format(action)) == expected_action_state)
        if expected_action_state == "failed":
            assert get("#{0}/@error".format(action))
        wait(lambda: get("//tmp/t/@tablets/1/state") == expected_state)
        wait(lambda: get("//tmp/t/@tablets/0/state") == expected_touch_state)
        self._validate_tablets("//tmp/t", expected_state=[expected_touch_state, expected_state])

    @authors("ifsmirnov", "ilpauzner")
    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_failed_after_cell_destroyed(self, skip_freezing, freeze):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        expected_state = "frozen" if freeze else "mounted"
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        with Restarter(self.Env, NODES_SERVICE):
            action = create("tablet_action", "", attributes={
                "kind": "move",
                "keep_finished": True,
                "skip_freezing": skip_freezing,
                "tablet_ids": [tablet_id],
                "cell_ids": [cells[1]]})
            sync_remove_tablet_cells([cells[1]])
        self._validate_tablets("//tmp/t", expected_state=[expected_state])

        wait(lambda: get("#{0}/@state".format(action)) == "failed")
        assert get("#{0}/@error".format(action))
        wait_for_tablet_state("//tmp/t", expected_state)
        self._validate_tablets("//tmp/t", expected_state=[expected_state])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_tablet_static_memory(self, skip_freezing, freeze):
        self._configure_bundle("default")
        create_account("test_account")
        set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 1000)
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "A"*128} for i in xrange(1)])
        sync_unmount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def move(dst):
            action = create("tablet_action", "", attributes={
                "kind": "move",
                "skip_freezing": skip_freezing,
                "tablet_ids": [tablet_id],
                "cell_ids": [dst]})
            wait(lambda: get("#{0}/@cell_id".format(tablet_id)) == dst)
            expected = "frozen" if freeze else "mounted"
            wait(lambda: get("#{0}/@state".format(tablet_id)) == expected)

        set("//tmp/t/@in_memory_mode", "compressed")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") >= get("//tmp/t/@compressed_data_size"))

        size = get("//sys/accounts/test_account/@resource_usage/tablet_static_memory")

        move(cells[1])
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") == size)

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "none")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        move(cells[1])
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") == 0)

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "compressed")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        move(cells[1])
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") == size)

    @authors("ifsmirnov")
    def test_tablet_cell_decomission(self):
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=cells[0])
        sync_remove_tablet_cells([cells[0]])

        wait(lambda: get("//tmp/t/@tablets/0/cell_id") == cells[1])
        sync_remove_tablet_cells([cells[1]])

        wait_for_tablet_state("//tmp/t", "unmounted")

        actions = get("//sys/tablet_actions")
        assert len(actions) == 1
        action = get("//sys/tablet_actions/{0}/@".format(actions.keys()[0]))
        assert action["state"] == "orphaned"

        cells = sync_create_cells(1)
        wait_for_tablet_state("//tmp/t", "mounted")
        assert get("//tmp/t/@tablets/0/cell_id") == cells[0]
        assert len(get("//sys/tablet_actions")) == 0

    @authors("ifsmirnov")
    def test_removing_bundle_removes_actions(self):
        create_tablet_cell_bundle("b")
        cell_id, = sync_create_cells(1, "b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")
        action_id = create("tablet_action", "", attributes={
            "kind": "move",
            "tablet_ids": [get("//tmp/t/@tablets/0/tablet_id")],
            "cell_ids": [cell_id],
            "expiration_time": "2099-01-01"})
        wait(lambda: get("#{}/@state".format(action_id)) in ("completed", "failed"))
        assert get("#{}/@state".format(action_id)) == "completed"
        remove("//tmp/t")
        sync_remove_tablet_cells([cell_id])
        assert exists("//sys/tablet_actions/{}".format(action_id))
        remove_tablet_cell_bundle("b")
        assert exists("//sys/tablet_actions/{}".format(action_id))
        remove("#{}".format(action_id))
        assert not exists("//sys/tablet_actions/{}".format(action_id))

##################################################################

class TestTabletBalancer(TabletActionsBase):
    @authors("savrus")
    @pytest.mark.parametrize("freeze", [False, True])
    def test_cells_balance(self, freeze):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t", pivot_keys=[[], [1]])
        set("//tmp/t/@in_memory_mode", "uncompressed")
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0, cell_id=cells[0])
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1, cell_id=cells[1])
        insert_rows("//tmp/t", [{"key": i, "value": "A"*128} for i in xrange(2)])
        sync_flush_table("//tmp/t")
        if freeze:
            sync_freeze_table("//tmp/t")

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", True, recursive=True)
        sleep(1)
        e = "frozen" if freeze else "mounted"
        wait_for_tablet_state("//tmp/t", e)
        tablets = self._get_tablets("//tmp/t")
        self._validate_state(tablets, state=[e, e], expected_state=[e, e])
        cell0 = tablets[0]["cell_id"]
        cell1 = tablets[1]["cell_id"]
        assert cell0 != cell1

    @authors("savrus")
    def test_cells_balance_in_bundle(self):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        create_tablet_cell_bundle("b")
        self._configure_bundle("default")
        self._configure_bundle("b")
        cells = sync_create_cells(2)
        cells_b = sync_create_cells(4, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t1", pivot_keys=[[], [1], [2], [3]])
        self._create_sorted_table("//tmp/t2", pivot_keys=[[], [1], [2], [3]], tablet_cell_bundle="b")
        pairs = [("//tmp/t1", cells), ("//tmp/t2", cells_b)]
        for pair in pairs:
            table = pair[0]
            set(table + "/@in_memory_mode", "uncompressed")
            sync_mount_table(table, cell_id=pair[1][0])
            insert_rows(table, [{"key": i, "value": "A"*128} for i in xrange(4)])
            sync_flush_table(table)

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", True, recursive=True)
        for pair in pairs:
            table = pair[0]
            wait_for_tablet_state(table, "mounted")
            dcells = [tablet["cell_id"] for tablet in get(table + "/@tablets")]
            count = [cells.count(cell) for cell in pair[1]]
            assert all(c == count[0] for c in count)

    @authors("ifsmirnov")
    @parametrize_external
    def test_ext_memory_cells_balance(self, external):
        self._configure_bundle("default")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", False)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", False)
        cells = sync_create_cells(5)

        def create_sorted_table(name):
            if external:
                self._create_sorted_table(name, external_cell_tag=1)
            else:
                self._create_sorted_table(name, external=False)

        def reshard(table, tablet_count):
            reshard_table(table, [[]] + list([i] for i in range(1, tablet_count)))

        create_sorted_table("//tmp/t1")
        reshard("//tmp/t1", 13)
        sync_mount_table("//tmp/t1", cell_id=cells[0])

        for i in range(7):
            create_sorted_table("//tmp/t2.{}".format(i))
            sync_mount_table("//tmp/t2.{}".format(i), cell_id=cells[1])

        assert self._tablets_distribution("//tmp/t1", cells) == [13, 0, 0, 0, 0]

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", True)
        wait(lambda: self._tablets_distribution("//tmp/t1") == [2, 2, 3, 3, 3])

        for i in range(3, 15):
            name = "//tmp/t{}".format(i)
            create_sorted_table(name)
            reshard(name, 3)
            sync_mount_table(name, cell_id=cells[2])

        wait(lambda: all(
            max(self._tablets_distribution("//tmp/t{}".format(i), cells)) == 1
            for i
            in range(3, 15)
        ))

        # Add new cell and wait till slack tablets distribute evenly between cells
        cells += sync_create_cells(1)
        def wait_func():
            cell_fullness = [get("//sys/tablet_cells/{}/@tablet_count".format(c)) for c in cells]
            return max(cell_fullness) - min(cell_fullness) <= 1
        wait(wait_func)
        assert self._tablets_distribution("//tmp/t1") == [2, 2, 2, 2, 2, 3]

    @authors("ifsmirnov")
    @pytest.mark.parametrize("cell_count", [2, 3])
    @pytest.mark.parametrize("tablet_count", [6, 9, 10])
    def test_balancer_new_cell_added(self, cell_count, tablet_count):
        self._configure_bundle("default")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", False)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", True)
        cells = sync_create_cells(cell_count)

        self._create_sorted_table("//tmp/t")
        reshard_table("//tmp/t", [[]] + [[i] for i in range(1, tablet_count)])
        sync_mount_table("//tmp/t", cell_id=cells[0])

        def check_tablet_count():
            tablet_counts = [get("//sys/tablet_cells/{}/@tablet_count".format(i)) for i in cells]
            return tablet_count / cell_count <= min(tablet_counts) and max(tablet_counts) <= (tablet_count - 1) / cell_count + 1

        wait(lambda: check_tablet_count())

        new_cell = sync_create_cells(1)[0]
        cells += [new_cell]
        cell_count += 1
        wait(lambda: check_tablet_count())

    @authors("ifsmirnov")
    def test_balancer_in_memory_types(self):
        self._configure_bundle("default")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", False)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", True)
        cells = sync_create_cells(2)

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False)

        self._create_sorted_table("//tmp/in")
        set("//tmp/in/@in_memory_mode", "uncompressed")
        self._create_sorted_table("//tmp/ext")

        for table in "//tmp/in", "//tmp/ext":
            reshard_table(table, [[], [1], [2], [3]])
            sync_mount_table(table, cell_id=cells[0])
            insert_rows(table, [dict(key=0,value="a"*510)])
            insert_rows(table, [dict(key=1,value="a"*100)])
            insert_rows(table, [dict(key=2,value="a"*100)])
            insert_rows(table, [dict(key=3,value="a"*100)])
            sync_flush_table(table)

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", True)

        def wait_func():
            expected = {
                "//tmp/in": [1, 3],
                "//tmp/ext": [2, 2]}
            for table in "//tmp/in", "//tmp/ext":
                cell_cnt = dict((cell, 0) for cell in cells)
                for tablet in get("{}/@tablets".format(table)):
                    cell_cnt[tablet["cell_id"]] += 1
                distribution = sorted(cell_cnt.values())
                if expected[table] != distribution:
                    return False
            return True

        wait(wait_func)

    @authors("ifsmirnov")
    def test_ordered_tables_balance(self):
        self._configure_bundle("default")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", True)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_in_memory_cell_balancer", False)
        cells = sync_create_cells(2)

        # not in-memory
        self._create_ordered_table("//tmp/t1", tablet_count=4)
        sync_mount_table("//tmp/t1", cell_id=cells[0])

        wait(lambda: self._tablets_distribution("//tmp/t1") == [2, 2])

        # in-memory
        self._create_ordered_table("//tmp/t2", tablet_count=4)
        set("//tmp/t2/@in_memory_mode", "uncompressed")
        sync_mount_table("//tmp/t2", cell_id=cells[0])

        for i in range(3):
            insert_rows("//tmp/t2", [{"key": x, "value": "a" * 512, "$tablet_index": i} for x in range(10)])
        insert_rows("//tmp/t2", [{"key": x, "value": "a" * 2048, "$tablet_index": 3} for x in range(10)])
        sync_flush_table("//tmp/t2")

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_in_memory_cell_balancer", True)
        wait(lambda: self._tablets_distribution("//tmp/t2") == [1, 3])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("is_sorted", [True, False])
    def test_replicated_tables_balance(self, is_sorted):
        self._configure_bundle("default")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", True)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_in_memory_cell_balancer", True)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", False)
        cells = sync_create_cells(2)

        schema = [dict(name="key", type="int64"), dict(name="value", type="string")]
        if is_sorted:
            schema[0]["sort_order"] = "ascending"

        create("replicated_table", "//tmp/t", attributes=dict(dynamic=True, schema=schema))
        replica_id = create_table_replica("//tmp/t", self.get_cluster_name(0), "//tmp/r")
        create("table", "//tmp/r", attributes=dict(
            dynamic=True, schema=schema, upstream_replica_id=replica_id))

        if is_sorted:
            sync_reshard_table("//tmp/t", [[], [1], [2], [3]])
            sync_reshard_table("//tmp/r", [[], [2], [4], [6]])
        else:
            sync_reshard_table("//tmp/t", 4)
            sync_reshard_table("//tmp/r", 4)

        sync_mount_table("//tmp/t", cell_id=cells[0])
        sync_mount_table("//tmp/r", cell_id=cells[1])

        wait(lambda: self._tablets_distribution("//tmp/t") == [2, 2])
        wait(lambda: self._tablets_distribution("//tmp/r") == [2, 2])

    @authors("ifsmirnov")
    def test_tablet_balancer_with_active_action(self):
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["custom"])

        create_tablet_cell_bundle("broken")
        self._configure_bundle("default")
        set("//sys/tablet_cell_bundles/broken/@node_tag_filter", "custom")
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "!custom")

        cells_on_broken = sync_create_cells(1, tablet_cell_bundle="broken")
        cells_on_default = sync_create_cells(2, tablet_cell_bundle="default")

        self._create_sorted_table("//tmp/t1", tablet_cell_bundle="broken")
        self._create_sorted_table("//tmp/t2", tablet_cell_bundle="default")

        sync_mount_table("//tmp/t1", cell_id=cells_on_broken[0])
        self._decommission_all_peers(cells_on_broken[0])
        wait(lambda: get("#{}/@health".format(cells_on_broken[0])) == "failed")

        action = create("tablet_action", "", attributes={
            "kind": "move",
            "keep_finished": True,
            "tablet_ids": [get("//tmp/t1/@tablets/0/tablet_id")],
            "cell_ids": [cells_on_broken[0]]})

        def _check():
            assert get("#{}/@state".format(action)) == "freezing"
            self._validate_tablets("//tmp/t1", state=["freezing"], expected_state=["mounted"])

        _check()

        # test tablet balancing

        sync_reshard_table("//tmp/t2", [[], [1]])
        assert get("//tmp/t2/@tablet_count") == 2
        sync_mount_table("//tmp/t2")
        wait(lambda: get("//tmp/t2/@tablet_count") == 1)
        wait_for_tablet_state("//tmp/t2", "mounted")

        _check()

        # test cell balancing

        sync_unmount_table("//tmp/t2")
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False)
        set("//tmp/t2/@in_memory_mode", "uncompressed")
        sync_reshard_table("//tmp/t2", [[], [1]])

        sync_mount_table("//tmp/t2", cell_id=cells_on_default[0])
        insert_rows("//tmp/t2", [{"key": i, "value": "A"*128} for i in xrange(2)])
        sync_flush_table("//tmp/t2")

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", True)
        def wait_func():
            cells = [tablet["cell_id"] for tablet in list(get("//tmp/t2/@tablets"))]
            assert len(cells) == 2
            return cells[0] != cells[1]
        wait(wait_func)

        _check()

    @authors("ifsmirnov", "shakurov")
    @pytest.mark.parametrize("enable", [False, True])
    def test_tablet_balancer_schedule(self, enable):
        assert get("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer")
        set("//sys/@config/tablet_manager/tablet_balancer/tablet_balancer_schedule", "1" if enable else "0")
        sleep(1)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        if enable:
            wait(lambda: get("//tmp/t/@tablet_count") == 1)
        else:
            sleep(1)
            assert get("//tmp/t/@tablet_count") == 2

    @authors("ifsmirnov")
    def test_tablet_balancer_schedule_formulas(self):
        self._configure_bundle("default")
        sync_create_cells(1)

        self._create_sorted_table("//tmp/t")

        def check_balancer_is_active(should_be_active):
            sync_reshard_table("//tmp/t", [[], [1]])
            sync_mount_table("//tmp/t")
            if should_be_active:
                wait(lambda: get("//tmp/t/@tablet_count") == 1)
                wait_for_tablet_state("//tmp/t", "mounted")
            else:
                sleep(1)
                wait(lambda: get("//tmp/t/@tablet_count") == 2)
            sync_unmount_table("//tmp/t")

        global_config = "//sys/@config/tablet_manager/tablet_balancer/tablet_balancer_schedule"
        local_config = "//sys/tablet_cell_bundles/default/@tablet_balancer_config/tablet_balancer_schedule"

        check_balancer_is_active(True)
        with pytest.raises(YtError):
            set(global_config, "")
        with pytest.raises(YtError):
            set(global_config, "wrong_variable")
        check_balancer_is_active(True)

        with pytest.raises(YtError):
            set(local_config, "wrong_variable")

        set(local_config, "")
        check_balancer_is_active(True)

        set(local_config, "0")
        check_balancer_is_active(False)

        set(local_config, "")
        set(global_config, "0")
        sleep(1)
        check_balancer_is_active(False)

        set(global_config, "1")
        check_balancer_is_active(True)

        set(global_config, "1/0")
        sleep(1)
        check_balancer_is_active(False)

    @authors("savrus")
    def test_tablet_merge(self):
        self._configure_bundle("default")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

    @authors("savrus", "ifsmirnov")
    def test_tablet_split(self):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@max_partition_data_size", 320)
        set("//tmp/t/@desired_partition_data_size", 256)
        set("//tmp/t/@min_partition_data_size", 240)
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@chunk_writer", {"block_size": 64})

        # Create two chunks excelled from eden
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "A"*256} for i in xrange(2)])
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")
        chunks = get("//tmp/t/@chunk_ids")
        assert len(chunks) == 2
        for chunk in chunks:
            assert not get("#{0}/@eden".format(chunk))

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[]])
        sync_mount_table("//tmp/t")

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", True, recursive=True)
        wait(lambda: get("//tmp/t/@tablet_count") == 2)
        assert len(get("//tmp/t/@chunk_ids")) > 1

        wait_for_tablet_state("//tmp/t", "mounted")
        set("//tmp/t/@tablet_balancer_config/min_tablet_size", 512)
        set("//tmp/t/@tablet_balancer_config/max_tablet_size", 2048)
        set("//tmp/t/@tablet_balancer_config/desired_tablet_size", 1024)
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

        wait_for_tablet_state("//tmp/t", "mounted")
        remove("//tmp/t/@tablet_balancer_config/min_tablet_size")
        remove("//tmp/t/@tablet_balancer_config/max_tablet_size")
        remove("//tmp/t/@tablet_balancer_config/desired_tablet_size")
        wait(lambda: get("//tmp/t/@tablet_count") == 2)

        wait_for_tablet_state("//tmp/t", "mounted")
        set("//tmp/t/@tablet_balancer_config/desired_tablet_count", 1)
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

    @authors("savrus")
    def test_tablet_balancer_disabled(self):
        self._configure_bundle("default")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        sleep(1)
        assert get("//tmp/t/@tablet_count") == 2
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", False)
        remove("//tmp/t/@tablet_balancer_config/enable_auto_reshard")
        sleep(1)
        assert get("//tmp/t/@tablet_count") == 2
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", True)
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

    @authors("ifsmirnov")
    def test_tablet_balancer_table_config(self):
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t", in_memory_mode="uncompressed")
        sync_reshard_table("//tmp/t", [[],[1]])
        set("//tmp/t/@tablet_balancer_config", {
            "enable_auto_reshard": False,
            "enable_auto_tablet_move": False,
        })
        sync_mount_table("//tmp/t", cell_id=cells[0])
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(2)])
        sync_flush_table("//tmp/t")

        sleep(1)
        assert get("//tmp/t/@tablet_count") == 2
        assert all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets"))

        set("//tmp/t/@tablet_balancer_config/enable_auto_tablet_move", True)
        wait(lambda: not all(t["cell_id"] == cells[0] for t in get("//tmp/t/@tablets")))

        assert get("//tmp/t/@enable_tablet_balancer") == False
        remove("//tmp/t/@enable_tablet_balancer")
        assert get("//tmp/t/@tablet_balancer_config") == {
            "enable_auto_tablet_move": True,
            "enable_auto_reshard": True,
        }
        wait(lambda: get("//tmp/t/@tablet_count") == 1)

    @authors("ifsmirnov")
    def test_tablet_balancer_table_config_compats(self):
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t", in_memory_mode="uncompressed")

        set("//tmp/t/@tablet_balancer_config", {})
        set("//tmp/t/@min_tablet_size", 1)
        set("//tmp/t/@desired_tablet_size", 2)
        set("//tmp/t/@max_tablet_size", 3)
        set("//tmp/t/@desired_tablet_count", 4)
        assert get("//tmp/t/@tablet_balancer_config") == {
            "enable_auto_tablet_move": True,
            "enable_auto_reshard": True,
            "min_tablet_size": 1,
            "desired_tablet_size": 2,
            "max_tablet_size": 3,
            "desired_tablet_count": 4,
        }
        assert get("//tmp/t/@min_tablet_size") == 1
        assert get("//tmp/t/@desired_tablet_size") == 2
        assert get("//tmp/t/@max_tablet_size") == 3
        assert get("//tmp/t/@desired_tablet_count") == 4

        with pytest.raises(YtError): set("//tmp/t/@min_tablet_size", 5)
        with pytest.raises(YtError): set("//tmp/t/@tablet_balancer_config/min_tablet_size", 5)

        remove("//tmp/t/@min_tablet_size")
        remove("//tmp/t/@desired_tablet_size")
        remove("//tmp/t/@max_tablet_size")
        remove("//tmp/t/@desired_tablet_count")
        assert get("//tmp/t/@tablet_balancer_config") == {
            "enable_auto_tablet_move": True,
            "enable_auto_reshard": True,
        }

        assert not exists("//tmp/t/@enable_tablet_balancer")
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        assert get("//tmp/t/@enable_tablet_balancer") == False

    @authors("ifsmirnov")
    def test_sync_reshard(self):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False)
        cells = sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        sync_reshard_table_automatic("//tmp/t")
        assert get("//tmp/t/@tablet_count") == 1
        get("//sys/tablet_actions")
        tablet_actions = get("//sys/tablet_actions", attributes=["state"])
        assert len(tablet_actions) == 1
        assert all(v.attributes["state"] == "completed" for v in tablet_actions.values())

    @authors("ifsmirnov")
    def test_sync_move_all_tables(self):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False)
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t", in_memory_mode="uncompressed")

        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t", cell_id=cells[0])
        insert_rows("//tmp/t", [{"key": 0, "value": "a"}, {"key": 1, "value": "b"}])
        sync_flush_table("//tmp/t")

        sync_balance_tablet_cells("default")
        tablet_actions = get("//sys/tablet_actions", attributes=["state"])
        assert len(tablet_actions) == 1
        assert all(v.attributes["state"] == "completed" for v in tablet_actions.values())
        assert len(__builtin__.set(t["cell_id"] for t in get("//tmp/t/@tablets"))) == 2

    @authors("ifsmirnov")
    def test_sync_move_one_table(self):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False)
        cells = sync_create_cells(4)
        if is_multicell:
            self._create_sorted_table("//tmp/t1", external_cell_tag=1, in_memory_mode="uncompressed")
            self._create_sorted_table("//tmp/t2", external_cell_tag=2, in_memory_mode="uncompressed")
        else:
            self._create_sorted_table("//tmp/t1", in_memory_mode="uncompressed")
            self._create_sorted_table("//tmp/t2", in_memory_mode="uncompressed")

        tables = ["//tmp/t1", "//tmp/t2"]
        for idx, table in enumerate(tables):
            sync_reshard_table(table, [[], [1]])
            sync_mount_table(table, cell_id=cells[idx])
            insert_rows(table, [{"key": 0, "value": "a"}, {"key": 1, "value": "b"}])
            sync_flush_table(table)

        sync_balance_tablet_cells("default", ["//tmp/t1"])
        tablet_actions = get("//sys/tablet_actions", attributes=["state"])
        assert len(tablet_actions) == 1
        assert all(v.attributes["state"] == "completed" for v in tablet_actions.values())
        assert len(__builtin__.set(t["cell_id"] for t in get("//tmp/t1/@tablets"))) == 2
        assert len(__builtin__.set(t["cell_id"] for t in get("//tmp/t2/@tablets"))) == 1

    @authors("ifsmirnov")
    def test_sync_tablet_balancer_acl(self):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False)
        create_user("u")
        create_tablet_cell_bundle("b")
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", ["read", "write"]))
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("deny", "u", ["use"]))
        sync_create_cells(1, "b")

        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")
        with pytest.raises(YtError):
            sync_balance_tablet_cells("b", authenticated_user="u")
        with pytest.raises(YtError):
            sync_balance_tablet_cells("b", ["//tmp/t"], authenticated_user="u")
        with pytest.raises(YtError):
            sync_reshard_table_automatic("//tmp/t", authenticated_user="u")

        # Remove `deny` ACE.
        remove("//sys/tablet_cell_bundles/b/@acl/-1")
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", ["use"]))

        sync_balance_tablet_cells("b", authenticated_user="u")
        sync_balance_tablet_cells("b", ["//tmp/t"], authenticated_user="u")
        sync_reshard_table_automatic("//tmp/t", authenticated_user="u")

    @authors("ifsmirnov")
    def test_sync_tablet_balancer_wrong_type(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", dynamic=False)

        with pytest.raises(YtError):
            sync_reshard_table_automatic("//tmp/t")
        with pytest.raises(YtError):
            sync_reshard_table_automatic("/")
        with pytest.raises(YtError):
            sync_balance_tablet_cells("nonexisting_bundle")
        with pytest.raises(YtError):
            sync_balance_tablet_cells("default", ["//tmp/t"])

    @authors("ifsmirnov")
    def test_sync_move_table_wrong_bundle(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", in_memory_mode="uncompressed")
        sync_mount_table("//tmp/t")
        sync_balance_tablet_cells("b")
        with pytest.raises(YtError):
            sync_balance_tablet_cells("b", ["//tmp/t"])

##################################################################

class TestTabletActionsMulticell(TestTabletActions):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestTabletActionsRpcProxy(TestTabletActions):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

##################################################################

class TestTabletBalancerMulticell(TestTabletBalancer):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestTabletBalancerRpcProxy(TestTabletBalancer):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
