import pytest

from yt_env_setup import YTEnvSetup, wait, skip_if_rpc_driver_backend, parametrize_external
from yt_commands import *

from yt.environment.helpers import assert_items_equal

from flaky import flaky

from time import sleep

from collections import Counter

import itertools

import __builtin__

##################################################################

class TestDynamicTablesBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 0
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 2
    }

    def _create_sorted_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

    def _create_ordered_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

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
        def do():
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
        for attempt in xrange(5):
            data = do()
            if data is not None:
                return data
        return None

    def _get_pivot_keys(self, path):
        tablets = get(path + "/@tablets")
        return [tablet["pivot_key"] for tablet in tablets]

    def _decommission_all_peers(self, cell_id):
        addresses = []
        peers = get("#" + cell_id + "/@peers")
        for x in peers:
            addr = x["address"]
            addresses.append(addr)
            set_node_decommissioned(addr, True)
        clear_metadata_caches()
        return addresses

    def _set_nodes_decommission(self, addresses, decomission):
        for addr in addresses:
            set_node_decommissioned(addr, decomission)

    def _get_profiling(self, table, filter=None, filter_table=False):
        tablets = get(table + "/@tablets")
        assert len(tablets) == 1
        tablet = tablets[0]
        address = get("#%s/@peers/0/address" % tablet["cell_id"])
        filter_value = (filter, table if filter_table else tablet[filter]) if filter else None

        class Profiling:
            def __init__(self):
                self._shifts = {}

            def _get_counter_impl(self, counter_name):
                try:
                    counters = get("//sys/nodes/%s/orchid/profiling/tablet_node/%s" % (address, counter_name))
                    if filter_value:
                        filter, value = filter_value
                        for counter in counters[::-1]:
                            tags = counter["tags"]
                            if filter in tags and tags[filter] == value:
                                return counter["value"]
                    else:
                        return counters[-1]["value"]
                except YtResponseError as error:
                    if not error.is_resolve_error():
                        raise
                return 0

            def get_counter(self, counter_name):
                # Get difference since last query since typically we are interested in couter rate.
                # (Same table name is shared between tests and there is no way to reset couters.)
                result = self._get_counter_impl(counter_name)
                if counter_name not in self._shifts:
                    self._shifts[counter_name] = result
                return result - self._shifts[counter_name]

        return Profiling()

    def _get_table_profiling(self, table):
        return self._get_profiling(table, "table_path", filter_table=True)


##################################################################

class TestDynamicTablesSingleCell(TestDynamicTablesBase):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "cpu_per_tablet_slot": 1.0,
            },
        },
    }

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 1000,
            "peer_revocation_timeout" : 3000,
        }
    }

    def test_force_unmount_on_remove(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)
        assert self._find_tablet_orchid(address, tablet_id) is not None

        remove("//tmp/t")
        sleep(1)
        assert self._find_tablet_orchid(address, tablet_id) is None

    def test_no_copy_mounted(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1")
        sync_mount_table("//tmp/t1")

        with pytest.raises(YtError): copy("//tmp/t1", "//tmp/t2")

    @pytest.mark.parametrize("freeze", [False, True])
    def test_no_move_mounted(self, freeze):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1")
        sync_mount_table("//tmp/t1", freeze=freeze)

        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t2")

    def test_move_unmounted(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1")
        sync_mount_table("//tmp/t1")
        sync_unmount_table("//tmp/t1")

        table_id1 = get("//tmp/t1/@id")
        tablet_id1 = get("//tmp/t1/@tablets/0/tablet_id")
        assert get("#" + tablet_id1 + "/@table_id") == table_id1

        move("//tmp/t1", "//tmp/t2")

        sync_mount_table("//tmp/t2")

        table_id2 = get("//tmp/t2/@id")
        tablet_id2 = get("//tmp/t2/@tablets/0/tablet_id")
        assert get("#" + tablet_id2 + "/@table_id") == table_id2
        assert get("//tmp/t2/@tablets/0/tablet_id") == tablet_id2

    def test_swap(self):
        self.test_move_unmounted()

        self._create_sorted_table("//tmp/t3")
        sync_mount_table("//tmp/t3")
        sync_unmount_table("//tmp/t3")

        sync_reshard_table("//tmp/t3", [[], [100], [200], [300], [400]])

        sync_mount_table("//tmp/t3")
        sync_unmount_table("//tmp/t3")

        move("//tmp/t3", "//tmp/t1")

        assert self._get_pivot_keys("//tmp/t1") == [[], [100], [200], [300], [400]]

    def test_move_multiple_rollback(self):
        sync_create_cells(1)

        set("//tmp/x", {})
        self._create_sorted_table("//tmp/x/a")
        self._create_sorted_table("//tmp/x/b")
        sync_mount_table("//tmp/x/a")
        sync_unmount_table("//tmp/x/a")
        sync_mount_table("//tmp/x/b")

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
        self._create_sorted_table("//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert len(get("//tmp/t1/@tablets")) == 1
        assert len(get("//tmp/t2/@tablets", tx=tx)) == 1
        commit_transaction(tx)
        assert len(get("//tmp/t2/@tablets")) == 1

    def test_move_in_tx_abort(self):
        self._create_sorted_table("//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert len(get("//tmp/t1/@tablets")) == 1
        assert len(get("//tmp/t2/@tablets", tx=tx)) == 1
        abort_transaction(tx)
        assert len(get("//tmp/t1/@tablets")) == 1


    def test_tablet_assignment(self):
        sync_create_cells(3)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[]] + [[i] for i in xrange(11)])
        assert get("//tmp/t/@tablet_count") == 12

        sync_mount_table("//tmp/t")

        cells = ls("//sys/tablet_cells", attributes=["tablet_count"])
        assert len(cells) == 3
        for cell in cells:
            assert cell.attributes["tablet_count"] == 4

    def test_follower_start(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        for i in xrange(0, 10):
            rows = [{"key": i, "value": "test"}]
            keys = [{"key": i}]
            insert_rows("//tmp/t", rows)
            assert lookup_rows("//tmp/t", keys) == rows

    def test_follower_catchup(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        cell_id = ls("//sys/tablet_cells")[0]
        peers = get("#" + cell_id + "/@peers")
        follower_address = list(x["address"] for x in peers if x["state"] == "following")[0]

        set_node_decommissioned(follower_address, True)
        sleep(3.0)
        clear_metadata_caches()

        assert get("#" + cell_id + "/@health") == "good"
        for i in xrange(0, 100):
            rows = [{"key": i, "value": "test"}]
            keys = [{"key": i}]
            insert_rows("//tmp/t", rows)
            assert lookup_rows("//tmp/t", keys) == rows

    def test_run_reassign_leader(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        cell_id = ls("//sys/tablet_cells")[0]
        peers = get("#" + cell_id + "/@peers")
        leader_address = list(x["address"] for x in peers if x["state"] == "leading")[0]
        follower_address = list(x["address"] for x in peers if x["state"] == "following")[0]

        set_node_decommissioned(leader_address, True)
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
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        cell_id = ls("//sys/tablet_cells")[0]
        self._decommission_all_peers(cell_id)
        sleep(3.0)

        assert get("#" + cell_id + "/@health") == "good"
        assert lookup_rows("//tmp/t", keys) == rows

    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory_flush(self, mode):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        set("//tmp/t/@in_memory_mode", mode)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_flush_table("//tmp/t")

    def test_tablet_cell_create_permission(self):
        create_user("u")
        with pytest.raises(YtError): create_tablet_cell(authenticated_user="u")
        set("//sys/schemas/tablet_cell/@acl/end", make_ace("allow", "u", "create"))
        id = create_tablet_cell(authenticated_user="u")
        assert exists("//sys/tablet_cells/{0}/changelogs".format(id))
        assert exists("//sys/tablet_cells/{0}/snapshots".format(id))

    def test_tablet_cell_journal_acl(self):
        create_user("u")
        acl = [make_ace("allow", "u", "read")]
        create_tablet_cell_bundle("b", attributes={
            "options": {"snapshot_acl" : acl, "changelog_acl": acl}})
        cell_id = sync_create_cells(1, tablet_cell_bundle="b")[0]
        assert get("//sys/tablet_cells/{0}/changelogs/@inherit_acl".format(cell_id)) == False
        assert get("//sys/tablet_cells/{0}/snapshots/@inherit_acl".format(cell_id)) == False
        assert get("//sys/tablet_cells/{0}/changelogs/@effective_acl".format(cell_id)) == acl
        assert get("//sys/tablet_cells/{0}/snapshots/@effective_acl".format(cell_id)) == acl

    @pytest.mark.parametrize("domain", ["snapshot_acl", "changelog_acl"])
    def test_create_tablet_cell_with_broken_acl(self, domain):
        create_user("u")
        acl = [make_ace("allow", "unknown_user", "read")]
        create_tablet_cell_bundle("b", attributes={"options": {domain: acl}})

        with pytest.raises(YtError):
            sync_create_cells(1, tablet_cell_bundle="b")
        assert len(ls("//sys/tablet_cells")) == 0

        set("//sys/tablet_cell_bundles/b/@options/{}".format(domain), [make_ace("allow", "u", "read")])
        sync_create_cells(1, tablet_cell_bundle="b")
        assert len(ls("//sys/tablet_cells")) == 1

    def test_tablet_cell_bundle_create_permission(self):
        create_user("u")
        with pytest.raises(YtError): create_tablet_cell_bundle("b", authenticated_user="u")
        set("//sys/schemas/tablet_cell_bundle/@acl/end", make_ace("allow", "u", "create"))
        create_tablet_cell_bundle("b", authenticated_user="u")

    def test_set_tablet_cell_bundle_failure(self):
        sync_create_cells(1)
        create_user("u")
        create_tablet_cell_bundle("b")
        self._create_sorted_table("//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@tablet_cell_bundle", "b", authenticated_user="u")

        sync_mount_table("//tmp/t")
        self.Env.kill_nodes()
        unmount_table("//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@tablet_cell_bundle", "b")
        self.Env.start_nodes()

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
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        create_user("u")
        with pytest.raises(YtError): mount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): unmount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): remount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[]], authenticated_user="u")

    def test_mount_permission_allowed(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        create_user("u")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "mount"))
        sync_mount_table("//tmp/t", authenticated_user="u")
        sync_unmount_table("//tmp/t", authenticated_user="u")
        remount_table("//tmp/t", authenticated_user="u")
        sync_reshard_table("//tmp/t", [[]], authenticated_user="u")

    def test_mount_permission_allowed_by_ancestor(self):
        sync_create_cells(1)
        create("map_node", "//tmp/d")
        self._create_sorted_table("//tmp/d/t")
        create_user("u")
        set("//tmp/d/@acl/end", make_ace("allow", "u", "mount"))
        sync_mount_table("//tmp/d/t", authenticated_user="u")
        sync_unmount_table("//tmp/d/t", authenticated_user="u")
        remount_table("//tmp/d/t", authenticated_user="u")
        sync_reshard_table("//tmp/d/t", [[]], authenticated_user="u")

    def test_default_cell_bundle(self):
        assert ls("//sys/tablet_cell_bundles") == ["default"]
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
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
        sync_create_cells(cell_count)
        cell_ids = ls("//sys/tablet_cells")
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[]] + [[i * 100] for i in xrange(cell_count - 1)])
        for i in xrange(len(cell_ids)):
            mount_table("//tmp/t", first_tablet_index=i, last_tablet_index=i, cell_id=cell_ids[i])
        wait_for_tablet_state("//tmp/t", "mounted")
        rows = [{"key": i * 100 - j, "value": "payload" + str(i)}
                for i in xrange(cell_count)
                for j in xrange(10)]
        insert_rows("//tmp/t", rows)
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, rows)

    def test_tablet_ops_require_exclusive_lock(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        tx = start_transaction()
        lock("//tmp/t", mode="exclusive", tx=tx)
        with pytest.raises(YtError): mount_table("//tmp/t")
        with pytest.raises(YtError): unmount_table("//tmp/t")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[], [1]])
        with pytest.raises(YtError): freeze_table("//tmp/t")
        with pytest.raises(YtError): unfreeze_table("//tmp/t")

    def test_no_storage_change_for_mounted(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@vital", False)
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", 2)
        with pytest.raises(YtError): set("//tmp/t/@media", {"default": {"replication_factor": 2}})

    def test_cell_bundle_node_tag_filter(self):
        node = list(get("//sys/nodes"))[0]
        with pytest.raises(YtError):
            set("//sys/nodes/{0}/@user_tags".format(node), ["custom!"])
        set("//sys/nodes/{0}/@user_tags".format(node), ["custom"])
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "!custom")

        create_tablet_cell_bundle("custom", attributes={"node_tag_filter": "custom"})
        default_cell = sync_create_cells(1)[0]
        custom_cell = sync_create_cells(1, tablet_cell_bundle="custom")[0]

        for peer in get("#{0}/@peers".format(custom_cell)):
            assert peer["address"] == node

        for peer in get("#{0}/@peers".format(default_cell)):
            assert peer["address"] != node

    @pytest.mark.parametrize("enable_tablet_cell_balancer", [True, False])
    def test_cell_bundle_distribution(self, enable_tablet_cell_balancer):
        set("//sys/@config/tablet_manager/tablet_cell_balancer/enable_tablet_cell_balancer", enable_tablet_cell_balancer)
        create_tablet_cell_bundle("custom")
        nodes = ls("//sys/nodes")
        node_count = len(nodes)
        bundles = ["default", "custom"]

        cell_ids = {}
        for _ in xrange(node_count):
            for bundle in bundles:
                cell_id = create_tablet_cell(attributes={"tablet_cell_bundle": bundle})
                cell_ids[cell_id] = bundle
        wait_for_cells(cell_ids.keys())

        def _check(nodes, floor, ceil):
            def predicate():
                for node in nodes:
                    slots = get("//sys/nodes/{0}/@tablet_slots".format(node))
                    count = Counter([cell_ids[slot["cell_id"]] for slot in slots if slot["state"] != "none"])
                    for bundle in bundles:
                        if not floor <= count[bundle] <= ceil:
                            return False
                return True
            wait(predicate)
            wait_for_cells(cell_ids.keys())

        _check(nodes, 1, 1)

        nodes = list(get("//sys/nodes").keys())

        set("//sys/nodes/{0}/@disable_tablet_cells".format(nodes[0]), True)
        _check(nodes[:1], 0, 0)
        _check(nodes[1:], 1, 2)

        if not enable_tablet_cell_balancer:
            return

        set("//sys/nodes/{0}/@disable_tablet_cells".format(nodes[0]), False)
        _check(nodes, 1, 1)

        for node in nodes[:len(nodes)/2]:
            set("//sys/nodes/{0}/@disable_tablet_cells".format(node), True)
        _check(nodes[len(nodes)/2:], 2, 2)

        for node in nodes[:len(nodes)/2]:
            set("//sys/nodes/{0}/@disable_tablet_cells".format(node), False)
        _check(nodes, 1, 1)

    def test_cell_bundle_options(self):
        set("//sys/schemas/tablet_cell_bundle/@options", {
            "changelog_read_quorum": 3,
            "changelog_write_quorum": 3,
            "changelog_replication_factor": 5})
        create_tablet_cell_bundle("custom", attributes={"options": {
            "changelog_account": "tmp",
            "snapshot_account": "tmp"}})
        options = get("//sys/tablet_cell_bundles/custom/@options")
        assert options["changelog_read_quorum"] == 3
        assert options["changelog_write_quorum"] == 3
        assert options["changelog_replication_factor"] == 5
        assert options["snapshot_account"] == "tmp"
        assert options["changelog_account"] == "tmp"

        remove("//sys/schemas/tablet_cell_bundle/@options")
        with pytest.raises(YtError):
            set("//sys/tablet_cell_bundles/default/@options", {})
        with pytest.raises(YtError):
            create_tablet_cell_bundle("invalid", initialize_options=False)
        with pytest.raises(YtError):
            create_tablet_cell_bundle(
                "invalid",
                initialize_options=False,
                attributes={"options": {}})

    def test_tablet_count_by_state(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")

        def _verify(unmounted, frozen, mounted):
            count_by_state = get("//tmp/t/@tablet_count_by_state")
            assert count_by_state["unmounted"] == unmounted
            assert count_by_state["frozen"] == frozen
            assert count_by_state["mounted"] == mounted
            for state, count in count_by_state.items():
                if state not in ["unmounted", "mounted", "frozen"]:
                    assert count == 0

        _verify(1, 0, 0)
        sync_reshard_table("//tmp/t", [[], [0], [1]])
        _verify(3, 0, 0)
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1, freeze=True)
        _verify(2, 1, 0)
        sync_mount_table("//tmp/t", first_tablet_index=2, last_tablet_index=2)
        _verify(1, 1, 1)
        sync_unmount_table("//tmp/t")
        _verify(3, 0, 0)

    def test_tablet_table_path_attribute(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        assert get("#" + tablet_id + "/@table_path") == "//tmp/t"

    def test_tablet_error_attributes(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        # Decommission all unused nodes to make flush fail due to
        # high replication factor.
        cell = get("//tmp/t/@tablets/0/cell_id")
        nodes_to_save = __builtin__.set()
        for peer in get("#" + cell + "/@peers"):
            nodes_to_save.add(peer["address"])

        for node in ls("//sys/nodes"):
            if node not in nodes_to_save:
                set_node_decommissioned(node, True)

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@replication_factor", 10)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        unmount_table("//tmp/t")

        wait(lambda: bool(get("//tmp/t/@tablet_errors")))

        tablet = get("//tmp/t/@tablets/0/tablet_id")
        errors = get("//tmp/t/@tablet_errors")

        assert len(errors) == 1
        assert errors[0]["attributes"]["background_activity"] == "flush"
        assert errors[0]["attributes"]["tablet_id"] == tablet
        assert get("#" + tablet + "/@errors")[0]["attributes"]["background_activity"] == "flush"
        assert get("#" + tablet + "/@state") == "unmounting"
        assert get("//tmp/t/@tablets/0/error_count") == 1
        assert get("//tmp/t/@tablet_error_count") == 1

        for node in ls("//sys/nodes"):
            set_node_decommissioned(node, False)

    def test_tablet_error_count(self):
        LARGE_STRING = "a" * 15 * 1024 * 1024
        MAX_UNVERSIONED_ROW_WEIGHT = 512 * 1024 * 1024

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        # Create several versions such that their total weight exceeds
        # MAX_UNVERSIONED_ROW_WEIGHT. No error happens in between because rows
        # are flushed chunk by chunk.
        row = [{"key": 0, "value": LARGE_STRING}]
        for i in range(MAX_UNVERSIONED_ROW_WEIGHT / len(LARGE_STRING) + 2):
            insert_rows("//tmp/t", row)
        sync_freeze_table("//tmp/t")

        chunk_count = get("//tmp/t/@chunk_count")
        set("//tmp/t/@min_compaction_store_count", chunk_count)
        set("//tmp/t/@max_compaction_store_count", chunk_count)
        set("//tmp/t/@compaction_data_size_base", get("//tmp/t/@compressed_data_size") - 100)

        sync_unfreeze_table("//tmp/t")
        set("//tmp/t/@forced_compaction_revision", get("//tmp/t/@revision"))
        set("//tmp/t/@forced_compaction_revision", get("//tmp/t/@revision"))
        remount_table("//tmp/t")

        # Compaction fails with "Versioned row data weight is too large".
        #  wait(lambda: bool(get("//tmp/t/@tablet_errors")))

        # Temporary debug output by ifsmirnov
        def wait_func():
            get("//tmp/t/@tablets")
            get("//tmp/t/@chunk_ids")
            get("//tmp/t/@tablet_statistics")
            return bool(get("//tmp/t/@tablet_errors"))
        wait(wait_func)

        assert len(get("//tmp/t/@tablet_errors")) == 1
        assert get("//tmp/t/@tablet_error_count") == 1

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")

        # After reshard all errors should be gone.
        assert len(get("//tmp/t/@tablet_errors")) == 0
        assert get("//tmp/t/@tablet_error_count") == 0

    def test_disallowed_dynamic_table_alter(self):
        sorted_schema = make_schema([
                {"name": "key", "type": "string", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ], unique_keys=True, strict=True)
        ordered_schema = make_schema([
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
            ], strict=True)

        create("table", "//tmp/t1", attributes={"schema": ordered_schema, "dynamic": True})
        create("table", "//tmp/t2", attributes={"schema": sorted_schema, "dynamic": True})
        with pytest.raises(YtError):
            alter_table("//tmp/t1", schema=sorted_schema)
        with pytest.raises(YtError):
            alter_table("//tmp/t2", schema=ordered_schema)

    def test_disable_tablet_cells(self):
        cell = sync_create_cells(1)[0]
        peer = get("#{0}/@peers/0/address".format(cell))
        set("//sys/nodes/{0}/@disable_tablet_cells".format(peer), True)
        def check():
            peers = get("#{0}/@peers".format(cell))
            if len(peers) == 0:
                return False
            if "address" not in peers[0]:
                return False
            if peers[0]["address"] == peer:
                return False
            return True
        wait(check)

    def test_tablet_slot_charges_cpu_resource_limit(self):
        get_cpu = lambda x: get("//sys/nodes/{0}/orchid/job_controller/resource_limits/cpu".format(x))

        node = ls("//sys/nodes")[0]
        empty_node_cpu = get_cpu(node)

        create_tablet_cell_bundle("b")
        cell = sync_create_cells(1, tablet_cell_bundle="b")[0]
        peer = get("#{0}/@peers/0/address".format(cell))

        assigned_node_cpu = get_cpu(peer)
        assert int(empty_node_cpu - assigned_node_cpu) == 1

        def _get_orchid(path):
            return get("//sys/nodes/{0}/orchid/tablet_cells/{1}{2}".format(peer, cell, path))

        assert _get_orchid("/dynamic_config_version") == 0

        set("//sys/tablet_cell_bundles/b/@dynamic_options/cpu_per_tablet_slot", 0.0)
        wait(lambda: _get_orchid("/dynamic_config_version") == 1)
        assert _get_orchid("/dynamic_options/cpu_per_tablet_slot") == 0.0

        assigned_node_cpu = get_cpu(peer)
        assert int(empty_node_cpu - assigned_node_cpu) == 0

    @skip_if_rpc_driver_backend
    def test_bundle_node_list(self):
        create_tablet_cell_bundle("b", attributes={"node_tag_filter": "b"})

        node = ls("//sys/nodes")[0]
        set("//sys/nodes/{0}/@user_tags".format(node), ["b"])
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        set("//sys/nodes/{0}/@banned".format(node), True)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == []
        set("//sys/nodes/{0}/@banned".format(node), False)
        wait(lambda: get("//sys/nodes/{0}/@state".format(node)) == "online")
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        set("//sys/nodes/{0}/@decommissioned".format(node), True)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == []
        set("//sys/nodes/{0}/@decommissioned".format(node), False)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        set("//sys/nodes/{0}/@disable_tablet_cells".format(node), True)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == []
        set("//sys/nodes/{0}/@disable_tablet_cells".format(node), False)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        build_snapshot(cell_id=None)
        self.Env.kill_master_cell()
        self.Env.start_master_cell()

        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

    def test_update_only_key_columns(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1}], update=True)

        assert len(select_rows("* from [//tmp/t]")) == 0

        insert_rows("//tmp/t", [{"key": 1, "value": "x"}])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1}], update=True)

        assert len(select_rows("* from [//tmp/t]")) == 1

    @skip_if_rpc_driver_backend
    @pytest.mark.parametrize("is_sorted", [True, False])
    def test_column_selector_dynamic_tables(self, is_sorted):
        sync_create_cells(1)

        key_schema = {"name": "key", "type": "int64"}
        value_schema = {"name": "value", "type": "int64"}
        if is_sorted:
            key_schema["sort_order"] = "ascending"

        schema = make_schema(
            [key_schema, value_schema],
            strict=True,
            unique_keys=True if is_sorted else False)
        create("table", "//tmp/t", attributes={"schema": schema, "external": False})

        write_table("//tmp/t", [{"key": 0, "value": 1}])

        def check_reads(is_dynamic_sorted):
            assert read_table("//tmp/t{key}") == [{"key": 0}]
            assert read_table("//tmp/t{value}") == [{"value": 1}]
            assert read_table("//tmp/t{key,value}") == [{"key": 0, "value": 1}]
            assert read_table("//tmp/t") == [{"key": 0, "value": 1}]
            if is_dynamic_sorted:
                with pytest.raises(YtError):
                    read_table("//tmp/t{zzzzz}")
            else:
                assert read_table("//tmp/t{zzzzz}") == [{}]

        write_table("//tmp/t", [{"key": 0, "value": 1}])
        check_reads(False)
        alter_table("//tmp/t", dynamic=True, schema=schema)
        check_reads(is_sorted)

        if is_sorted:
            sync_mount_table("//tmp/t")
            sync_compact_table("//tmp/t")
            check_reads(True)

##################################################################

class TestDynamicTablesPermissions(TestDynamicTablesBase):
    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "security_manager": {
                "table_permission_cache": {
                    "expire_after_access_time": 0,
                },
            },
        },
        "master_cache_service": {
            "capacity": 0
        },
    }

    def test_safe_mode(self):
        sync_create_cells(1)
        create_user("u")
        self._create_ordered_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}], authenticated_user="u")
        set("//sys/@config/enable_safe_mode", True)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}], authenticated_user="u")
        with pytest.raises(YtError):
            trim_rows("//tmp/t", 0, 1, authenticated_user="u")
        assert select_rows("key, value from [//tmp/t]", authenticated_user="u") == [{"key": 0, "value": "0"}]
        set("//sys/@config/enable_safe_mode", False)
        trim_rows("//tmp/t", 0, 1, authenticated_user="u")
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}], authenticated_user="u")
        assert select_rows("key, value from [//tmp/t]", authenticated_user="u") == [{"key": 1, "value": "1"}]

##################################################################

class TestDynamicTablesResourceLimits(TestDynamicTablesBase):
    DELTA_NODE_CONFIG = {
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

    def _verify_resource_usage(self, account, resource, expected):
        sleep(0.5)
        assert get("//sys/accounts/{0}/@resource_usage/{1}".format(account, resource)) == expected
        assert get("//sys/accounts/{0}/@committed_resource_usage/{1}".format(account, resource)) == expected

    def _multicell_set(self, path, value):
        set(path, value)
        for i in xrange(self.Env.secondary_master_cell_count):
            driver = get_driver(i + 1)
            wait(lambda: get(path, driver=driver) == value)

    def _multicell_wait(self, predicate):
        for i in xrange(self.Env.secondary_master_cell_count):
            driver = get_driver(i + 1)
            wait(predicate(driver))

    @pytest.mark.parametrize("resource", ["chunk_count", "disk_space_per_medium/default"])
    def test_resource_limits(self, resource):
        create_account("test_account")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")
        sync_mount_table("//tmp/t")

        set("//sys/accounts/test_account/@resource_limits/" + resource, 0)
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_flush_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        set("//sys/accounts/test_account/@resource_limits/" + resource, 10000)
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        set("//sys/accounts/test_account/@resource_limits/" + resource, 0)
        sync_unmount_table("//tmp/t")

    def test_tablet_count_limit_create(self):
        create_account("test_account")
        sync_create_cells(1)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 0)
        with pytest.raises(YtError):
            self._create_sorted_table("//tmp/t", account="test_account")
        with pytest.raises(YtError):
            self._create_ordered_table("//tmp/t", account="test_account")

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        with pytest.raises(YtError):
            self._create_ordered_table("//tmp/t", account="test_account", tablet_count=2)
        with pytest.raises(YtError):
            self._create_sorted_table("//tmp/t", account="test_account", pivot_keys=[[], [1]])

        assert get("//sys/accounts/test_account/@ref_counter") == 1

        set("//sys/accounts/test_account/@resource_limits/tablet_count", 4)
        self._create_ordered_table("//tmp/t1", account="test_account", tablet_count=2)
        self._verify_resource_usage("test_account", "tablet_count", 2)
        self._create_sorted_table("//tmp/t2", account="test_account", pivot_keys=[[], [1]])
        self._verify_resource_usage("test_account", "tablet_count", 4)

    def test_tablet_count_limit_reshard(self):
        create_account("test_account")
        sync_create_cells(1)
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        self._create_sorted_table("//tmp/t1", account="test_account")
        self._create_ordered_table("//tmp/t2", account="test_account")

        # Wait for resource usage since tabels can be placed to different cells.
        self._multicell_wait(lambda x: lambda: get("//sys/accounts/test_account/@resource_usage/tablet_count", driver=x) == 2)

        with pytest.raises(YtError):
            reshard_table("//tmp/t1", [[], [1]])
        with pytest.raises(YtError):
            reshard_table("//tmp/t2", 2)

        set("//sys/accounts/test_account/@resource_limits/tablet_count", 4)
        sync_reshard_table("//tmp/t1", [[], [1]])
        sync_reshard_table("//tmp/t2", 2)
        self._verify_resource_usage("test_account", "tablet_count", 4)

    def test_tablet_count_limit_copy(self):
        create_account("test_account")
        set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_count") == 1)

        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/t_copy", preserve_account=True)

        set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        copy("//tmp/t", "//tmp/t_copy", preserve_account=True)
        self._verify_resource_usage("test_account", "tablet_count", 2)

    def test_tablet_count_copy_across_accounts(self):
        create_account("test_account1")
        create_account("test_account2")
        set("//sys/accounts/test_account1/@resource_limits/tablet_count", 10)
        set("//sys/accounts/test_account2/@resource_limits/tablet_count", 0)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account1")

        self._verify_resource_usage("test_account1", "tablet_count", 1)

        create("map_node", "//tmp/dir", attributes={"account": "test_account2"})

        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/dir/t_copy", preserve_account=False)

        self._verify_resource_usage("test_account2", "tablet_count", 0)

        set("//sys/accounts/test_account2/@resource_limits/tablet_count", 1)
        copy("//tmp/t", "//tmp/dir/t_copy", preserve_account=False)

        self._verify_resource_usage("test_account1", "tablet_count", 1)
        self._verify_resource_usage("test_account2", "tablet_count", 1)

    def test_tablet_count_remove(self):
        create_account("test_account")
        set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")
        self._verify_resource_usage("test_account", "tablet_count", 1)
        remove("//tmp/t")
        sleep(1)
        self._verify_resource_usage("test_account", "tablet_count", 0)

    def test_tablet_count_set_account(self):
        create_account("test_account")
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t", tablet_count=2)

        # Not implemented: YT-7050
        #set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        #with pytest.raises(YtError):
        #    set("//tmp/t/@account", "test_account")

        set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        set("//tmp/t/@account", "test_account")
        self._verify_resource_usage("test_account", "tablet_count", 2)

    def test_tablet_count_alter_table(self):
        create_account("test_account")
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t")
        set("//tmp/t/@account", "test_account")

        self._verify_resource_usage("test_account", "tablet_count", 1)
        alter_table("//tmp/t", dynamic=False)
        self._verify_resource_usage("test_account", "tablet_count", 0)

        set("//sys/accounts/test_account/@resource_limits/tablet_count", 0)
        with pytest.raises(YtError):
            alter_table("//tmp/t", dynamic=True)

        set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        alter_table("//tmp/t", dynamic=True)
        self._verify_resource_usage("test_account", "tablet_count", 1)

    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory_accounting(self, mode):
        create_account("test_account")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_unmount_table("//tmp/t")

        set("//tmp/t/@in_memory_mode", mode)
        with pytest.raises(YtError):
            mount_table("//tmp/t")

        def _verify():
            sleep(0.5)
            data_size = get("//tmp/t/@{0}_data_size".format(mode))
            resource_usage = get("//sys/accounts/test_account/@resource_usage")
            committed_resource_usage = get("//sys/accounts/test_account/@committed_resource_usage")
            assert resource_usage["tablet_static_memory"] == data_size
            assert resource_usage == committed_resource_usage
            assert get("//tmp/t/@resource_usage/tablet_count") == 1
            assert get("//tmp/t/@resource_usage/tablet_static_memory") == data_size
            assert get("//tmp/@recursive_resource_usage/tablet_count") == 1
            assert get("//tmp/@recursive_resource_usage/tablet_static_memory") == data_size

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 1000)
        sync_mount_table("//tmp/t")
        _verify()

        sync_compact_table("//tmp/t")
        _verify();

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 0)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 1000)
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        sync_compact_table("//tmp/t")
        _verify();

        sync_unmount_table("//tmp/t")
        self._verify_resource_usage("test_account", "tablet_static_memory", 0)

    def test_remount_in_memory_accounting(self):
        create_account("test_account")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 2048)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "A" * 1024}])
        sync_flush_table("//tmp/t")

        def _test(mode):
            data_size = get("//tmp/t/@{0}_data_size".format(mode))
            sync_unmount_table("//tmp/t")
            set("//tmp/t/@in_memory_mode", mode)
            sync_mount_table("//tmp/t")
            def _check():
                resource_usage = get("//sys/accounts/test_account/@resource_usage")
                committed_resource_usage = get("//sys/accounts/test_account/@committed_resource_usage")
                return resource_usage["tablet_static_memory"] == data_size and \
                    resource_usage == committed_resource_usage
            wait(_check)

        _test("compressed")
        _test("uncompressed")

    def test_insert_during_tablet_static_memory_limit_violation(self):
        create_account("test_account")
        set("//sys/accounts/test_account/@resource_limits/tablet_count", 10)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1", account="test_account", in_memory_mode="compressed")
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"key": 0, "value": "0"}])
        sync_flush_table("//tmp/t1")
        assert get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") > 0
        assert get("//sys/accounts/test_account/@violated_resource_limits/tablet_static_memory")
        with pytest.raises(YtError):
            insert_rows("//tmp/t1", [{"key": 1, "value": "1"}])

        self._create_sorted_table("//tmp/t2", account="test_account")
        sync_mount_table("//tmp/t2")
        insert_rows("//tmp/t2", [{"key": 2, "value": "2"}])

##################################################################

class TestDynamicTableStateTransitions(TestDynamicTablesBase):
    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 1000,
            "peer_revocation_timeout" : 600000,
        }
    }

    def _get_expected_state(self, initial, first_command, second_command):
        M = "mounted"
        F = "frozen"
        E = "error"
        U = "unmounted"

        expected = {
            "mounted":
                {
                    "mount":        {"mount": M, "frozen_mount": E, "unmount": U, "freeze": F, "unfreeze": M},
                    # frozen_mount
                    "unmount":      {"mount": E, "frozen_mount": E, "unmount": U, "freeze": E, "unfreeze": E},
                    "freeze":       {"mount": E, "frozen_mount": F, "unmount": U, "freeze": F, "unfreeze": E},
                    "unfreeze":     {"mount": M, "frozen_mount": E, "unmount": U, "freeze": F, "unfreeze": M},
                },
            "frozen":
                {
                    # mount
                    "frozen_mount": {"mount": E, "frozen_mount": F, "unmount": U, "freeze": F, "unfreeze": M},
                    "unmount":      {"mount": E, "frozen_mount": E, "unmount": U, "freeze": E, "unfreeze": E},
                    "freeze":       {"mount": E, "frozen_mount": F, "unmount": U, "freeze": F, "unfreeze": M},
                    "unfreeze":     {"mount": M, "frozen_mount": E, "unmount": E, "freeze": E, "unfreeze": M},
                },
            "unmounted":
                {
                    "mount":        {"mount": M, "frozen_mount": E, "unmount": E, "freeze": E, "unfreeze": E},
                    "frozen_mount": {"mount": E, "frozen_mount": F, "unmount": E, "freeze": F, "unfreeze": E},
                    "unmount":      {"mount": M, "frozen_mount": F, "unmount": U, "freeze": E, "unfreeze": E},
                    # freeze
                    # unfreeze
                }
            }
        return expected[initial][first_command][second_command]

    def _get_callback(self, command):
        callbacks = {
            "mount": lambda x: mount_table(x),
            "frozen_mount": lambda x: mount_table(x, freeze=True),
            "unmount": lambda x: unmount_table(x),
            "freeze": lambda x: freeze_table(x),
            "unfreeze": lambda x: unfreeze_table(x)
        }
        return callbacks[command]

    @pytest.mark.parametrize(["initial", "command"], [
        ["mounted", "frozen_mount"],
        ["frozen", "mount"],
        ["unmounted", "freeze"],
        ["unmounted", "unfreeze"]])
    def test_initial_incompatible(self, initial, command):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")

        if initial == "mounted":
            sync_mount_table("//tmp/t")
        elif initial == "frozen":
            sync_mount_table("//tmp/t", freeze=True)

        with pytest.raises(YtError):
            self._get_callback(command)("//tmp/t")

    def _do_test_transition(self, initial, first_command, second_command):
        expected = self._get_expected_state(initial, first_command, second_command)
        if expected == "error":
            self.Env.kill_nodes()
            self._get_callback(first_command)("//tmp/t")
            with pytest.raises(YtError):
                self._get_callback(second_command)("//tmp/t")
            self.Env.start_nodes()
        else:
            self._get_callback(first_command)("//tmp/t")
            self._get_callback(second_command)("//tmp/t")
            wait_for_tablet_state("//tmp/t", expected)
        wait(lambda: get("//tmp/t/@tablet_state") != "transient")

    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["mount", "unmount", "freeze", "unfreeze"])
    def test_state_transition_conflict_mounted(self, first_command, second_command):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")
        cell = get("//tmp/t/@tablets/0/cell_id")
        sync_create_cells(1)
        self._do_test_transition("mounted", first_command, second_command)

    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["frozen_mount", "unmount", "freeze", "unfreeze"])
    def test_state_transition_conflict_frozen(self, first_command, second_command):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", freeze=True)
        cell = get("//tmp/t/@tablets/0/cell_id")
        sync_create_cells(1)
        self._do_test_transition("frozen", first_command, second_command)

    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["mount", "frozen_mount", "unmount"])
    def test_state_transition_conflict_unmounted(self, first_command, second_command):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        self._do_test_transition("unmounted", first_command, second_command)

    @pytest.mark.parametrize("inverse", [False, True])
    def test_freeze_expectations(self, inverse):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", pivot_keys=[[], [1]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        callbacks = [
            lambda: freeze_table("//tmp/t", first_tablet_index=0, last_tablet_index=0),
            lambda: mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1, freeze=True)
        ]

        for callback in reversed(callbacks) if inverse else callbacks:
            callback()

        wait_for_tablet_state("//tmp/t", "frozen")
        wait(lambda: get("//tmp/t/@tablet_state") != "transient")
        assert get("//tmp/t/@expected_tablet_state") == "frozen"

##################################################################

class TestTabletActions(TestDynamicTablesBase):
    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 1000,
            "peer_revocation_timeout" : 3000,
            "tablet_balancer": {
                "enable_tablet_balancer": True,
                "config_check_period": 100,
                "balance_period": 100,
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
        result = []
        for tablet in tablets:
            result.append(get("#{0}/@".format(tablet["tablet_id"])))

        for state in ["state", "expected_state"]:
            actual = {}
            for tablet in result:
                actual[tablet[state]] = actual.get(tablet[state], 0) + 1
            expected = get(path + "/@tablet_count_by_" + state)
            expected = {k: v for k, v in expected.items() if v != 0}
            assert_items_equal(expected, actual)

        return result

    def _validate_state(self, tablets, state=None, expected_state=None):
        if state is not None:
            assert state == [tablet["state"] if s is not None else None for tablet, s in zip(tablets, state)]
        if expected_state is not None:
            assert expected_state == [tablet["expected_state"] if s is not None else None for tablet, s in zip(tablets, expected_state)]

    def _validate_tablets(self, path, state=None, expected_state=None):
        self._validate_state(self._get_tablets(path), state=state, expected_state=expected_state)

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

    def test_ext_memory_cells_balance(self):
        # TODO(ifsmirnov): parametrize external_cell_tag for multicell
        self._configure_bundle("default")
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", False)
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", False)
        cells = sync_create_cells(5)

        def reshard(table, tablet_count):
            reshard_table(table, [[]] + list([i] for i in range(1, tablet_count)))

        def tablets_distribution(table):
            cnt = dict((i, 0) for i in range(len(cells)))
            tablets = get("{}/@tablets".format(table))
            for cell_id in [tablet["cell_id"] for tablet in tablets]:
                cnt[cells.index(cell_id)] += 1
            return list(cnt.values())

        self._create_sorted_table("//tmp/t1", external=False)
        reshard("//tmp/t1", 13)
        sync_mount_table("//tmp/t1", cell_id=cells[0])

        for i in range(7):
            self._create_sorted_table("//tmp/t2.{}".format(i), external=False)
            sync_mount_table("//tmp/t2.{}".format(i), cell_id=cells[1])

        assert tablets_distribution("//tmp/t1") == [13, 0, 0, 0, 0]

        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_cell_balancer", True)
        wait(lambda: sorted(tablets_distribution("//tmp/t1")) == [2, 2, 3, 3, 3])

        for i in range(3, 15):
            name = "//tmp/t{}".format(i)
            self._create_sorted_table(name, external=False)
            reshard(name, 3)
            sync_mount_table(name, cell_id=cells[2])

        wait(lambda: all(
            max(tablets_distribution("//tmp/t{}".format(i))) == 1
            for i
            in range(3, 15)
        ))

        # Add new cell and wait till slack tablets distribute evenly between cells
        cells += sync_create_cells(1)
        def wait_func():
            cell_fullness = [get("//sys/tablet_cells/{}/@tablet_count".format(c)) for c in cells]
            return max(cell_fullness) - min(cell_fullness) <= 1
        wait(wait_func)
        assert sorted(tablets_distribution("//tmp/t1")) == [2, 2, 2, 2, 2, 3]

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

    def test_tablet_balancer_with_active_action(self):
        node = ls("//sys/nodes")[0]
        set("//sys/nodes/{0}/@user_tags".format(node), ["custom"])

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
        sync_flush_table("//tmp/t2");

        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", True)
        def wait_func():
            cells = [tablet["cell_id"] for tablet in list(get("//tmp/t2/@tablets"))]
            assert len(cells) == 2
            return cells[0] != cells[1]
        wait(wait_func)

        _check()

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
                assert get("//tmp/t/@tablet_count") == 2
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

    def test_tablet_merge(self):
        self._configure_bundle("default")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        sleep(1)
        wait_for_tablet_state("//tmp/t", "mounted")
        assert get("//tmp/t/@tablet_count") == 1

    def test_tablet_split(self):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        sync_create_cells(2)
        self._create_sorted_table("//tmp/t")

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
        sleep(1)
        wait_for_tablet_state("//tmp/t", "mounted")
        assert len(get("//tmp/t/@chunk_ids")) > 1
        assert get("//tmp/t/@tablet_count") == 2

        set("//tmp/t/@min_tablet_size", 512)
        set("//tmp/t/@max_tablet_size", 2048)
        set("//tmp/t/@desired_tablet_size", 1024)
        sleep(1)
        wait_for_tablet_state("//tmp/t", "mounted")
        assert get("//tmp/t/@tablet_count") == 1

        remove("//tmp/t/@min_tablet_size")
        remove("//tmp/t/@max_tablet_size")
        remove("//tmp/t/@desired_tablet_size")
        sleep(1)
        wait_for_tablet_state("//tmp/t", "mounted")
        assert get("//tmp/t/@tablet_count") == 2

        set("//tmp/t/@desired_tablet_count", 1)
        sleep(1)
        wait_for_tablet_state("//tmp/t", "mounted")
        assert get("//tmp/t/@tablet_count") == 1

    def test_tablet_balancer_disabled(self):
        self._configure_bundle("default")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@enable_tablet_balancer", False)
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        sleep(1)
        assert get("//tmp/t/@tablet_count") == 2
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", False)
        remove("//tmp/t/@enable_tablet_balancer")
        sleep(1)
        assert get("//tmp/t/@tablet_count") == 2
        set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/enable_tablet_size_balancer", True)
        sleep(1)
        wait_for_tablet_state("//tmp/t", "mounted")
        assert get("//tmp/t/@tablet_count") == 1

    @pytest.mark.parametrize("skip_freezing", [False, True])
    @pytest.mark.parametrize("freeze", [False, True])
    def test_action_failed_after_table_removed(self, skip_freezing, freeze):
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False, recursive=True)
        self._configure_bundle("default")
        cells = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        self.Env.kill_nodes()
        action = create("tablet_action", "", attributes={
            "kind": "move",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "tablet_ids": [tablet_id],
            "cell_ids": [cells[1]]})
        remove("//tmp/t")
        wait(lambda: get("#{0}/@state".format(action)) == "failed")
        self.Env.start_nodes()
        assert get("#{0}/@error".format(action))

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
        self.Env.kill_nodes()
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
        self.Env.start_nodes()

        wait(lambda: get("#{0}/@state".format(action)) == expected_action_state)
        if expected_action_state == "failed":
            assert get("#{0}/@error".format(action))
        wait(lambda: get("//tmp/t/@tablets/1/state") == expected_state)
        wait(lambda: get("//tmp/t/@tablets/0/state") == expected_touch_state)
        self._validate_tablets("//tmp/t", expected_state=[expected_touch_state, expected_state])

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
        self.Env.kill_nodes()
        action = create("tablet_action", "", attributes={
            "kind": "move",
            "keep_finished": True,
            "skip_freezing": skip_freezing,
            "tablet_ids": [tablet_id],
            "cell_ids": [cells[1]]})
        sync_remove_tablet_cells([cells[1]])
        self.Env.start_nodes()
        self._validate_tablets("//tmp/t", expected_state=[expected_state])

        wait(lambda: get("#{0}/@state".format(action)) == "failed")
        assert get("#{0}/@error".format(action))
        wait_for_tablet_state("//tmp/t", expected_state)
        self._validate_tablets("//tmp/t", expected_state=[expected_state])

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
            multicell_sleep()

        set("//tmp/t/@in_memory_mode", "compressed")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        multicell_sleep()
        size = get("//sys/accounts/test_account/@resource_usage/tablet_static_memory")
        assert size >= get("//tmp/t/@compressed_data_size")

        move(cells[1])
        assert get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") == size

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "none")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        move(cells[1])
        assert get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") == 0

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "compressed")
        sync_mount_table("//tmp/t", cell_id=cells[0], freeze=freeze)
        move(cells[1])
        assert get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") == size

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

##################################################################

class TestDynamicTablesMulticell(TestDynamicTablesSingleCell):
    NUM_SECONDARY_MASTER_CELLS = 2

    def test_external_dynamic(self):
        cells = sync_create_cells(1)
        self._create_sorted_table("//tmp/t", external_cell_tag=1)
        assert get("//tmp/t/@external") == True
        cell_tag = get("//tmp/t/@external_cell_tag")
        table_id = get("//tmp/t/@id")

        driver = get_driver(1)
        assert get("#{0}/@dynamic".format(table_id), driver=driver)
        assert get("#{0}/@dynamic".format(table_id))

        sync_mount_table("//tmp/t")

        assert get("//sys/tablet_cells/{0}/@tablet_count".format(cells[0]), driver=driver) == 1
        assert get("//sys/tablet_cells/{0}/@tablet_count".format(cells[0])) == 1

        tablet = get("//tmp/t/@tablets/0")
        assert get("//sys/tablet_cells/{0}/@tablet_ids".format(cells[0]), driver=driver) == [tablet["tablet_id"]] 
        assert get("//sys/tablet_cells/{0}/@tablet_ids".format(cells[0])) == [tablet["tablet_id"]] 

        multicell_sleep()

        multicell_statistics = get("//sys/tablet_cells/{0}/@multicell_statistics".format(cells[0]))
        statistics = get("//sys/tablet_cells/{0}/@total_statistics".format(cells[0]))

        assert multicell_statistics[str(cell_tag)]["tablet_count"] == 1
        assert statistics["tablet_count"] == 1

        rows = [{"key": 0, "value": "0"}]
        keys = [{"key": r["key"]} for r in rows]
        insert_rows("//tmp/t", rows)
        assert lookup_rows("//tmp/t", keys) == rows

        sync_freeze_table("//tmp/t")

        multicell_sleep()

        primary_data_size = get("//tmp/t/@uncompressed_data_size")
        secondary_data_size = get("#" + table_id + "/@uncompressed_data_size", driver=driver)
        assert primary_data_size == secondary_data_size

        sync_compact_table("//tmp/t")
        sync_unmount_table("//tmp/t")

        multicell_sleep()

        primary_data_size = get("//tmp/t/@uncompressed_data_size")
        secondary_data_size = get("#" + table_id + "/@uncompressed_data_size", driver=driver)
        assert primary_data_size == secondary_data_size

class TestTabletActionsMulticell(TestTabletActions):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestDynamicTablesResourceLimitsMulticell(TestDynamicTablesResourceLimits):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestDynamicTableStateTransitionsMulticell(TestDynamicTableStateTransitions):
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################

class TestDynamicTablesRpcProxy(TestDynamicTablesSingleCell):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_PROXY = True

class TestDynamicTablesResourceLimitsRpcProxy(TestDynamicTablesResourceLimits):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

class TestDynamicTableStateTransitionsRpcProxy(TestDynamicTableStateTransitions):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

class TestTabletActionsRpcProxy(TestTabletActions):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
