import pytest

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *
from yt.yson import YsonEntity, YsonList

from time import sleep

from yt.environment.helpers import assert_items_equal

##################################################################

class TestDynamicTablesBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 0

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 1000,
            "peer_revocation_timeout" : 3000
        }
    }

    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 2
    }

    def _create_simple_table(self, path, atomicity="full", optimize_for="lookup", tablet_cell_bundle="default"):
        create("table", path,
            attributes={
                "dynamic": True,
                "atomicity": atomicity,
                "optimize_for": optimize_for,
                "tablet_cell_bundle": tablet_cell_bundle,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
            })


    def _get_tablet_leader_address(self, tablet_id):
        cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
        peers = get("//sys/tablet_cells/" + cell_id + "/@peers")
        leader_peer = list(x for x in peers if x["state"] == "leading")[0]
        return leader_peer["address"]

    def _get_recursive(self, path, result=None):
        if result is None or result.attributes.get("opaque", False):
            result = get(path, attributes=["opaque"])
        if isinstance(result, dict):
            for key, value in result.iteritems():
                result[key] = self._get_recursive(path + "/" + key, value)
        if isinstance(result, list):
            for index, value in enumerate(result):
                result[index] = self._get_recursive(path + "/" + str(index), value)
        return result

    def _find_tablet_orchid(self, address, tablet_id):
        path = "//sys/nodes/" + address + "/orchid/tablet_cells"
        cells = ls(path)
        for cell_id in cells:
            if get(path + "/" + cell_id + "/state") == "leading":
                tablets = ls(path + "/" + cell_id + "/tablets")
                if tablet_id in tablets:
                    try:
                        return self._get_recursive(path + "/" + cell_id + "/tablets/" + tablet_id)
                    except:
                        return None
        return None

    def _get_pivot_keys(self, path):
        tablets = get(path + "/@tablets")
        return [tablet["pivot_key"] for tablet in tablets]

    def _ban_all_peers(self, cell_id):
        address_list = []
        peers = get("#" + cell_id + "/@peers")
        for x in peers:
            addr = x["address"]
            address_list.append(addr)
            self.set_node_banned(addr, True)
        clear_metadata_caches()
        return address_list

    def _unban_peers(self, address_list):
        for addr in address_list:
            self.set_node_banned(addr, False)

##################################################################

class TestDynamicTables(TestDynamicTablesBase):
    def test_force_unmount_on_remove(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)
        assert self._find_tablet_orchid(address, tablet_id) is not None

        remove("//tmp/t")
        sleep(1)
        assert self._find_tablet_orchid(address, tablet_id) is None

    def test_metadata_cache_invalidation(self):
        def sync_mount_table_and_preserve_cache(path, **kwargs):
            kwargs["path"] = path
            execute_command("mount_table", kwargs)
            wait(lambda: all(x["state"] == "mounted" for x in get(path + "/@tablets")))

        def sync_unmount_table_and_preserve_cache(path, **kwargs):
            kwargs["path"] = path
            execute_command("unmount_table", kwargs)
            wait(lambda: all(x["state"] == "unmounted" for x in get(path + "/@tablets")))

        def reshard_and_preserve_cache(path, pivots):
            sync_unmount_table_and_preserve_cache(path)
            reshard_table(path, pivots)
            sync_mount_table_and_preserve_cache(path)

        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t1")
        self.sync_mount_table("//tmp/t1")

        rows = [{"key": i, "value": str(i)} for i in xrange(3)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t1", rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        sync_unmount_table_and_preserve_cache("//tmp/t1")
        with pytest.raises(YtError): lookup_rows("//tmp/t1", keys)
        clear_metadata_caches()
        sync_mount_table_and_preserve_cache("//tmp/t1")

        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        sync_unmount_table_and_preserve_cache("//tmp/t1")
        with pytest.raises(YtError): select_rows("* from [//tmp/t1]")
        clear_metadata_caches()
        sync_mount_table_and_preserve_cache("//tmp/t1")

        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        reshard_and_preserve_cache("//tmp/t1", [[], [1]])
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        reshard_and_preserve_cache("//tmp/t1", [[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

    def test_no_copy_mounted(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t1")
        self.sync_mount_table("//tmp/t1")

        with pytest.raises(YtError): copy("//tmp/t1", "//tmp/t2")

    def test_no_move_mounted(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t1")
        self.sync_mount_table("//tmp/t1")

        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t2")

    def test_move_unmounted(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t1")
        self.sync_mount_table("//tmp/t1")
        self.sync_unmount_table("//tmp/t1")

        table_id1 = get("//tmp/t1/@id")
        tablet_id1 = get("//tmp/t1/@tablets/0/tablet_id")
        assert get("#" + tablet_id1 + "/@table_id") == table_id1

        move("//tmp/t1", "//tmp/t2")

        mount_table("//tmp/t2")
        sleep(1)
        assert get("//tmp/t2/@tablets/0/state") == "mounted"

        table_id2 = get("//tmp/t2/@id")
        tablet_id2 = get("//tmp/t2/@tablets/0/tablet_id")
        assert get("#" + tablet_id2 + "/@table_id") == table_id2
        assert get("//tmp/t2/@tablets/0/tablet_id") == tablet_id2

    def test_swap(self):
        self.test_move_unmounted()

        self._create_simple_table("//tmp/t3")
        self.sync_mount_table("//tmp/t3")
        self.sync_unmount_table("//tmp/t3")

        reshard_table("//tmp/t3", [[], [100], [200], [300], [400]])
        self.sync_mount_table("//tmp/t3")
        self.sync_unmount_table("//tmp/t3")

        move("//tmp/t3", "//tmp/t1")

        assert self._get_pivot_keys("//tmp/t1") == [[], [100], [200], [300], [400]]

    def test_move_multiple_rollback(self):
        self.sync_create_cells(1)

        set("//tmp/x", {})
        self._create_simple_table("//tmp/x/a")
        self._create_simple_table("//tmp/x/b")
        self.sync_mount_table("//tmp/x/a")
        self.sync_unmount_table("//tmp/x/a")
        self.sync_mount_table("//tmp/x/b")

        def get_tablet_ids(path):
            return list(x["tablet_id"] for x in get(path + "/@tablets"))

        # NB: children are moved in lexicographic order
        # //tmp/x/a is fine to move
        # //tmp/x/b is not
        tablet_ids_a = get_tablet_ids("//tmp/x/a")
        tablet_ids_b = get_tablet_ids("//tmp/x/b")

        with pytest.raises(YtError): move("//tmp/x", "//tmp/y")

        assert get("//tmp/x/a/@dynamic")
        assert get("//tmp/x/b/@dynamic")
        assert_items_equal(get_tablet_ids("//tmp/x/a"), tablet_ids_a)
        assert_items_equal(get_tablet_ids("//tmp/x/b"), tablet_ids_b)

    def test_move_in_tx_commit(self):
        self._create_simple_table("//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert len(get("//tmp/t1/@tablets")) == 1
        assert len(get("//tmp/t2/@tablets", tx=tx)) == 1
        commit_transaction(tx)
        assert len(get("//tmp/t2/@tablets")) == 1

    def test_move_in_tx_abort(self):
        self._create_simple_table("//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert len(get("//tmp/t1/@tablets")) == 1
        assert len(get("//tmp/t2/@tablets", tx=tx)) == 1
        abort_transaction(tx)
        assert len(get("//tmp/t1/@tablets")) == 1


    def test_tablet_assignment(self):
        self.sync_create_cells(3)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", [[]] + [[i] for i in xrange(11)])
        assert get("//tmp/t/@tablet_count") == 12

        self.sync_mount_table("//tmp/t")

        cells = ls("//sys/tablet_cells", attributes=["tablet_count"])
        assert len(cells) == 3
        for cell in cells:
            assert cell.attributes["tablet_count"] == 4

    def test_follower_start(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        for i in xrange(0, 10):
            rows = [{"key": i, "value": "test"}]
            keys = [{"key": i}]
            insert_rows("//tmp/t", rows)
            assert lookup_rows("//tmp/t", keys) == rows

    def test_follower_catchup(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        cell_id = ls("//sys/tablet_cells")[0]
        peers = get("#" + cell_id + "/@peers")
        follower_address = list(x["address"] for x in peers if x["state"] == "following")[0]
        self.set_node_banned(follower_address, True)

        for i in xrange(0, 100):
            rows = [{"key": i, "value": "test"}]
            keys = [{"key": i}]
            insert_rows("//tmp/t", rows)
            assert lookup_rows("//tmp/t", keys) == rows

        assert get("#" + cell_id + "/@health") == "good"

    def test_run_reassign_leader(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        cell_id = ls("//sys/tablet_cells")[0]
        peers = get("#" + cell_id + "/@peers")
        leader_address = list(x["address"] for x in peers if x["state"] == "leading")[0]
        follower_address = list(x["address"] for x in peers if x["state"] == "following")[0]

        self.set_node_banned(leader_address, True)
        sleep(3.0)
        clear_metadata_caches()

        assert get("#" + cell_id + "/@health") == "good"
        peers = get("#" + cell_id + "/@peers")
        leaders = list(x["address"] for x in peers if x["state"] == "leading")
        assert len(leaders) == 1
        assert leaders[0] == follower_address

        assert lookup_rows("//tmp/t", keys) == rows

    def test_run_reassign_all_peers(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        cell_id = ls("//sys/tablet_cells")[0]
        self._ban_all_peers(cell_id)
        sleep(3.0)

        assert get("#" + cell_id + "/@health") == "good"
        assert lookup_rows("//tmp/t", keys) == rows

    def test_tablet_cell_create_permission(self):
        create_user("u")
        with pytest.raises(YtError): create_tablet_cell(authenticated_user="u")
        set("//sys/schemas/tablet_cell/@acl/end", make_ace("allow", "u", "create"))
        id = create_tablet_cell(authenticated_user="u")
        assert exists("//sys/tablet_cells/{0}/changelogs".format(id))
        assert exists("//sys/tablet_cells/{0}/snapshots".format(id))

    def test_tablet_cell_bundle_create_permission(self):
        create_user("u")
        with pytest.raises(YtError): create_tablet_cell_bundle("b", authenticated_user="u")
        set("//sys/schemas/tablet_cell_bundle/@acl/end", make_ace("allow", "u", "create"))
        create_tablet_cell_bundle("b", authenticated_user="u")

    def test_validate_dynamic_attr(self):
        create("table", "//tmp/t")
        assert not get("//tmp/t/@dynamic")
        with pytest.raises(YtError): mount_table("//tmp/t")
        with pytest.raises(YtError): unmount_table("//tmp/t")
        with pytest.raises(YtError): remount_table("//tmp/t")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[]])

    def test_dynamic_table_schema_validation(self):
        with pytest.raises(YtError): create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [{"data": "string"}]
            })

    def test_mount_permission_denied(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        create_user("u")
        with pytest.raises(YtError): mount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): unmount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): remount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[]], authenticated_user="u")

    def test_mount_permission_allowed(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        create_user("u")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "mount"))
        self.sync_mount_table("//tmp/t", authenticated_user="u")
        self.sync_unmount_table("//tmp/t", authenticated_user="u")
        remount_table("//tmp/t", authenticated_user="u")
        reshard_table("//tmp/t", [[]], authenticated_user="u")

    def test_default_cell_bundle(self):
        assert ls("//sys/tablet_cell_bundles") == ["default"]
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        assert get("//tmp/t/@tablet_cell_bundle") == "default"
        cells = get("//sys/tablet_cells", attributes=["tablet_cell_bundle"])
        assert len(cells) == 1
        assert list(cells.itervalues())[0].attributes["tablet_cell_bundle"] == "default"

    def test_cell_bundle_name_validation(self):
        with pytest.raises(YtError): create_tablet_cell_bundle("")

    def test_cell_bundle_name_create_uniqueness_validation(self):
        create_tablet_cell_bundle("b")
        with pytest.raises(YtError): create_tablet_cell_bundle("b")

    def test_cell_bundle_rename(self):
        create_tablet_cell_bundle("b")
        set("//sys/tablet_cell_bundles/b/@name", "b1")
        assert get("//sys/tablet_cell_bundles/b1/@name") == "b1"

    def test_cell_bundle_rename_uniqueness_validation(self):
        create_tablet_cell_bundle("b1")
        create_tablet_cell_bundle("b2")
        with pytest.raises(YtError): set("//sys/tablet_cell_bundles/b1/@name", "b2")

    def test_table_with_custom_cell_bundle(self):
        create_tablet_cell_bundle("b")
        assert get("//sys/tablet_cell_bundles/@ref_counter") == 1
        create("table", "//tmp/t", attributes={"tablet_cell_bundle": "b"})
        assert get("//tmp/t/@tablet_cell_bundle") == "b"
        assert get("//sys/tablet_cell_bundles/b/@ref_counter") == 2
        remove("//tmp/t")
        gc_collect()
        assert get("//sys/tablet_cell_bundles/b/@ref_counter") == 1
        
    def test_table_with_custom_cell_bundle_name_validation(self):
        with pytest.raises(YtError): create("table", "//tmp/t", attributes={"tablet_cell_bundle": "b"})

    def test_cell_bundle_use_permission(self):
        create_tablet_cell_bundle("b")
        create_user("u")
        with pytest.raises(YtError): create("table", "//tmp/t", attributes={"tablet_cell_bundle": "b"}, authenticated_user="u")
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", "use"))
        create("table", "//tmp/t", attributes={"tablet_cell_bundle": "b"}, authenticated_user="u")

    def test_cell_bundle_with_custom_peer_count(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count": 2}})
        get("//sys/tablet_cell_bundles/b/@options")
        assert get("//sys/tablet_cell_bundles/b/@options/peer_count") == 2
        cell_id = create_tablet_cell(attributes={"tablet_cell_bundle": "b"})
        assert cell_id in get("//sys/tablet_cell_bundles/b/@tablet_cell_ids")
        assert get("//sys/tablet_cells/" + cell_id + "/@tablet_cell_bundle") == "b"
        assert len(get("//sys/tablet_cells/" + cell_id + "/@peers")) == 2

    def test_distributed_commit(self):
        cell_count = 5
        self.sync_create_cells(cell_count)
        cell_ids = ls("//sys/tablet_cells")
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", [[]] + [[i * 100] for i in xrange(cell_count - 1)])
        self.sync_mount_table("//tmp/t")
        for i in xrange(len(cell_ids)):
            mount_table("//tmp/t", first_tablet_index = i, last_tablet_index=i, cell_id = cell_ids[i])
        wait(lambda: all(x["state"] == "mounted" for x in get("//tmp/t/@tablets")))
        rows = [{"key": i * 100 - j, "value": "payload" + str(i)} 
                for i in xrange(cell_count)
                for j in xrange(10)]
        insert_rows("//tmp/t", rows)
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, rows)

    def test_tablet_ops_require_exclusive_lock(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", mode="exclusive", tx=tx)
        with pytest.raises(YtError): mount_table("//tmp/t")
        with pytest.raises(YtError): unmount_table("//tmp/t")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[], [1]])
        with pytest.raises(YtError): freeze_table("//tmp/t")
        with pytest.raises(YtError): unfreeze_table("//tmp/t")

    def test_cell_bundle_node_tag_filter(self):
        node = list(get("//sys/nodes"))[0]
        with pytest.raises(YtError):
            set("//sys/nodes/{0}/@user_tags".format(node), ["custom!"])
        set("//sys/nodes/{0}/@user_tags".format(node), ["custom"])
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "!custom")

        create_tablet_cell_bundle("custom", attributes={"node_tag_filter": "custom"})
        default_cell = self.sync_create_cells(1)[0]
        custom_cell = self.sync_create_cells(1, tablet_cell_bundle="custom")[0]

        for peer in get("#{0}/@peers".format(custom_cell)):
            assert peer["address"] == node

        for peer in get("#{0}/@peers".format(default_cell)):
            assert peer["address"] != node

    @pytest.mark.parametrize("freeze", [False, True])
    @pytest.mark.parametrize("second_command", ["freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["unmount", "freeze", "unfreeze"])
    def test_state_transition_conflict(self, freeze, first_command, second_command):
        callbacks = {
            "unmount": lambda x: unmount_table(x),
            "freeze": lambda x: freeze_table(x),
            "unfreeze": lambda x: unfreeze_table(x)
        }

        M = "mounted"
        F = "frozen"
        E = "error"

        expect = None
        if freeze:
            expect = {
                "unmount":  {"freeze": E, "unfreeze": E},
                "freeze":   {"freeze": F, "unfreeze": M},
                "unfreeze": {"freeze": E, "unfreeze": M},
            }
        else:
            expect = {
                "unmount":  {"freeze": E, "unfreeze": E},
                "freeze":   {"freeze": F, "unfreeze": E},
                "unfreeze": {"freeze": F, "unfreeze": M},
            }

        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t", freeze=freeze)
        cell = get("//tmp/t/@tablets/0/cell_id")
        banned_peers = self._ban_all_peers(cell)
        callbacks[first_command]("//tmp/t")
        expected = expect[first_command][second_command]
        if expected == E:
            with pytest.raises(YtError):
                callbacks[second_command]("//tmp/t")
        else:
            callbacks[second_command]("//tmp/t")
            self._unban_peers(banned_peers)
            self._wait_for_tablets("//tmp/t", expected)

##################################################################

class TestTabletActions(TestDynamicTablesBase):
    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 1000,
            "peer_revocation_timeout" : 3000,
            "tablet_balancer": {
                "enable_in_memory_balancer": True,
                "enable_tablet_size_balancer": True,
                "balance_period": 100,
                "enabled_check_period": 100,
                "min_tablet_size": 128,
                "max_tablet_size": 512,
                "desired_tablet_size": 512,
            }
        }
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100
        },
        "tablet_node": {
            "tablet_manager": {
                "error_backoff_time": 100
            }
        }
    }

    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_move(self, skip_freezing, freeze):
        set("//sys/@disable_tablet_balancer", True)
        cells = self.sync_create_cells(2)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "move",
            "skip_freezing": skip_freezing,
            "keep_finished": True,
            "tablet_ids": [tablet_id],
            "cell_ids": [cells[1]]})
        wait(lambda: get("#{0}/@state".format(action)) == "completed")
        assert get("//tmp/t/@tablets/0/cell_id") == cells[1]
        expected_state = "frozen" if freeze else "mounted"
        assert get("//tmp/t/@tablets/0/state") == expected_state
        remove("#" + action)
        sleep(1.0)

        action = create("tablet_action", "", attributes={
            "kind": "move",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "freeze": not freeze,
            "tablet_ids": [tablet_id],
            "cell_ids": [cells[0]]})
        wait(lambda: get("#{0}/@state".format(action)) == "completed")
        assert get("//tmp/t/@tablets/0/cell_id") == cells[0]
        expected_state = "frozen" if not freeze else "mounted"
        assert get("//tmp/t/@tablets/0/state") == expected_state

    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_reshard(self, skip_freezing, freeze):
        set("//sys/@disable_tablet_balancer", True)
        cells = self.sync_create_cells(2)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "reshard",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "tablet_ids": [tablet_id],
            "pivot_keys": [[], [1]]})
        wait(lambda: get("#{0}/@state".format(action)) == "completed")
        tablets = list(get("//tmp/t/@tablets"))
        assert len(tablets) == 2
        expected_state = "frozen" if freeze else "mounted"
        assert tablets[0]["state"] == expected_state
        assert tablets[1]["state"] == expected_state
        remove("#" + action)
        sleep(1.0)

        action = create("tablet_action", "", attributes={
            "kind": "reshard",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "freeze": not freeze,
            "tablet_ids": [tablets[0]["tablet_id"], tablets[1]["tablet_id"]],
            "tablet_count": 1})
        wait(lambda: get("#{0}/@state".format(action)) == "completed")
        tablets = list(get("//tmp/t/@tablets"))
        assert len(tablets) == 1
        expected_state = "frozen" if not freeze else "mounted"
        assert tablets[0]["state"] == expected_state


    @pytest.mark.parametrize("freeze", [False, True])
    def test_tablet_cell_balance(self, freeze):
        set("//sys/@disable_tablet_balancer", True)
        cells = self.sync_create_cells(2)
        self._create_simple_table("//tmp/t1")
        self._create_simple_table("//tmp/t2")
        set("//tmp/t1/@in_memory_mode", "compressed")
        set("//tmp/t2/@in_memory_mode", "compressed")
        self.sync_mount_table("//tmp/t1", cell_id=cells[0])
        self.sync_mount_table("//tmp/t2", cell_id=cells[0])
        insert_rows("//tmp/t1", [{"key": 0, "value": "0"}])
        insert_rows("//tmp/t2", [{"key": 1, "value": "1"}])
        self.sync_flush_table("//tmp/t1")
        self.sync_flush_table("//tmp/t2")
        if freeze:
            self.sync_freeze_table("//tmp/t1")
            self.sync_freeze_table("//tmp/t2")

        remove("//sys/@disable_tablet_balancer")
        sleep(1)
        expected_state = "frozen" if freeze else "mounted"
        self._wait_for_tablets("//tmp/t1", expected_state)
        self._wait_for_tablets("//tmp/t2", expected_state)
        cell0 = get("//tmp/t1/@tablets/0/cell_id")
        cell1 = get("//tmp/t2/@tablets/0/cell_id")
        assert cell0 != cell1

    def test_tablet_merge(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", [[], [1]])
        self.sync_mount_table("//tmp/t")
        sleep(1)
        self._wait_for_tablets("//tmp/t", "mounted")
        assert get("//tmp/t/@tablet_count") == 1

    def test_tablet_split(self):
        set("//sys/@disable_tablet_balancer", True)
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        # Create two chunks excelled from eden
        reshard_table("//tmp/t", [[], [1]])
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "A"*256} for i in xrange(2)])
        self.sync_flush_table("//tmp/t")
        self.sync_compact_table("//tmp/t")
        chunks = get("//tmp/t/@chunk_ids")
        assert len(chunks) == 2
        for chunk in chunks:
            assert not get("#{0}/@eden".format(chunk))

        self.sync_unmount_table("//tmp/t")
        reshard_table("//tmp/t", [[]])
        self.sync_mount_table("//tmp/t")

        remove("//sys/@disable_tablet_balancer")
        sleep(1)
        self._wait_for_tablets("//tmp/t", "mounted")
        assert len(get("//tmp/t/@chunk_ids")) > 1
        assert get("//tmp/t/@tablet_count") == 2

    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_failed_after_table_removed(self, skip_freezing, freeze):
        set("//sys/@disable_tablet_balancer", True)
        cells = self.sync_create_cells(2)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        self._ban_all_peers(cells[0])
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "move",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "tablet_ids": [tablet_id],
            "cell_ids": [cells[1]]})
        remove("//tmp/t")
        wait(lambda: get("#{0}/@state".format(action)) == "failed")
        assert get("#{0}/@error".format(action))

    @pytest.mark.parametrize("touch", ["mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_failed_after_tablet_touched(self, skip_freezing, freeze, touch):
        touch_callbacks = {
            "mount": mount_table,
            "unmount": unmount_table,
            "freeze": freeze_table,
            "unfreeze": unfreeze_table
        }
        callback = touch_callbacks[touch]

        set("//sys/@disable_tablet_balancer", True)
        cells = self.sync_create_cells(2)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", [[], [1]])
        self.sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        banned_peers = self._ban_all_peers(cells[0])
        tablet1 = get("//tmp/t/@tablets/0/tablet_id")
        tablet2 = get("//tmp/t/@tablets/1/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "move",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "tablet_ids": [tablet1, tablet2],
            "cell_ids": [cells[1], cells[1]]})
        try:
            callback("//tmp/t", first_tablet_index=0, last_tablet_index=0)
        except Exception as e:
            pass
        self._unban_peers(banned_peers)
        wait(lambda: get("#{0}/@state".format(action)) == "failed")
        assert get("#{0}/@error".format(action))
        expected_state = "frozen" if freeze else "mounted"
        wait(lambda: get("//tmp/t/@tablets/1/state") == expected_state)
        # FIXME(savrus) Enable after YT-6770
        #wait(lambda: get("//tmp/t/@tablets/0/state") == "unmounted")

    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_failed_after_cell_destroyed(self, skip_freezing, freeze):
        set("//sys/@disable_tablet_balancer", True)
        cells = self.sync_create_cells(2)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action = create("tablet_action", "", attributes={
            "kind": "move",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "tablet_ids": [tablet_id],
            "cell_ids": [cells[1]]})
        remove("#" + cells[1])
        wait(lambda: get("#{0}/@state".format(action)) == "failed")
        assert get("#{0}/@error".format(action))
        expected_state = "frozen" if freeze else "mounted"
        self._wait_for_tablets("//tmp/t", expected_state)

##################################################################

class TestDynamicTablesMulticell(TestDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2

    def test_cannot_make_external_table_dynamic(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): alter_table("//tmp/t", dynamic=True)
