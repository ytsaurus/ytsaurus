import pytest

from yt_env_setup import YTEnvSetup, wait, parametrize_external,\
    Restarter, NODES_SERVICE, MASTERS_SERVICE
from yt_commands import *
from yt_helpers import *

from yt.environment.helpers import assert_items_equal

from flaky import flaky

from time import sleep

from collections import Counter

import __builtin__

##################################################################

class WriteAceRemoved:
    def __init__(self, path):
        self._path = path

    def __enter__(self):
        acl = get(self._path + "/@acl")
        self._aces = [ace for ace in acl if "write" in ace["permissions"]]
        acl = [ace for ace in acl if "write" not in ace["permissions"]]
        set(self._path + "/@acl", acl)

    def __exit__(self, exc_type, exc_val, exc_tb):
        acl = get(self._path + "/@acl")
        set(self._path + "/@acl", acl + self._aces)
        return False

##################################################################

class DynamicTablesBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 0
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 2000,
            "peer_revocation_timeout" : 3000,
        },
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
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
            path = "//sys/cluster_nodes/" + address + "/orchid/tablet_cells"
            cells = ls(path)
            for cell_id in cells:
                if get(path + "/" + cell_id + "/state") in ("leading", "following"):
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
        return addresses

    def _get_table_profiling(self, table):
        tablets = get(table + "/@tablets")
        assert len(tablets) == 1
        tablet = tablets[0]
        address = get("#%s/@peers/0/address" % tablet["cell_id"])

        class Profiling:
            def get_counter(self, counter_name):
                try:
                    counters = get("//sys/cluster_nodes/%s/orchid/profiling/tablet_node/%s" % (address, counter_name))
                    for counter in counters[::-1]:
                        tags = counter["tags"]
                        if tags.get("table_path", None) == table:
                            return counter["value"]
                except YtResponseError as error:
                    if not error.is_resolve_error():
                        raise
                return 0

        return Profiling()

##################################################################

class DynamicTablesSingleCellBase(DynamicTablesBase):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "cpu_per_tablet_slot": 1.0,
            },
        },
    }


    @authors("babenko")
    def test_barrier_timestamp(self):
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t")
        sync_mount_table("//tmp/t")
        ts = generate_timestamp()
        wait(lambda: get_tablet_infos("//tmp/t", [0])["tablets"][0]["barrier_timestamp"] >= ts)

    @authors("babenko")
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

    def _wait_cell_good(self, cell_id, decommissioned_addresses=[]):
        def check():
            peers = get("#{0}/@peers".format(cell_id))
            expected_config_version = get("#{0}/@config_version".format(cell_id))

            for peer in peers:
                address = peer.get("address", None)
                if address is None or address in decommissioned_addresses:
                    return False

                try:
                    actual_config_version = get("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}/config_version".format(address, cell_id))
                    if actual_config_version != expected_config_version:
                        return False
                    state = get("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}/state".format(address, cell_id))
                    if state != "leading" and state != "following":
                        return False
                except:
                    return False

            if get("#{0}/@health".format(cell_id)) != "good":
                return False

            return True

        wait(check)

    def _check_cell_stable(self, cell_id):
        addresses = [peer["address"] for peer in get("#" + cell_id + "/@peers")]
        metrics = [Metric.at_node(address, "hydra/restart_count", with_tags={"cell_id": cell_id}) for address in addresses]
        sleep(10.0)
        for metric in metrics:
            assert metric.update().get(verbose=True) == 0

    @authors("kiselyovp")
    @pytest.mark.parametrize("decommission_through_extra_peers", [False, True])
    def test_follower_catchup(self, decommission_through_extra_peers):
        set("//sys/@config/tablet_manager/decommission_through_extra_peers", decommission_through_extra_peers)
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        cell_id = ls("//sys/tablet_cells")[0]
        peers = get("#" + cell_id + "/@peers")
        follower_address = list(x["address"] for x in peers if x["state"] == "following")[0]

        set_node_decommissioned(follower_address, True)
        self._wait_cell_good(cell_id, [follower_address])

        for i in xrange(0, 100):
            rows = [{"key": i, "value": "test"}]
            keys = [{"key": i}]
            insert_rows("//tmp/t", rows)
            assert lookup_rows("//tmp/t", keys) == rows

    @authors("kiselyovp")
    @pytest.mark.parametrize("decommission_through_extra_peers", [False, True])
    def test_run_reassign_leader(self, decommission_through_extra_peers):
        set("//sys/@config/tablet_manager/decommission_through_extra_peers", decommission_through_extra_peers)
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
        self._wait_cell_good(cell_id, [leader_address])

        assert get("#" + cell_id + "/@health") == "good"
        peers = get("#" + cell_id + "/@peers")
        leaders = list(x["address"] for x in peers if x["state"] == "leading")
        assert len(leaders) == 1
        assert leaders[0] == follower_address

        assert lookup_rows("//tmp/t", keys) == rows

    @authors("kiselyovp")
    @pytest.mark.parametrize("decommission_through_extra_peers", [False, True])
    def test_run_reassign_all_peers(self, decommission_through_extra_peers):
        set("//sys/@config/tablet_manager/decommission_through_extra_peers", decommission_through_extra_peers)
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        cell_id = ls("//sys/tablet_cells")[0]

        addresses = self._decommission_all_peers(cell_id)
        self._wait_cell_good(cell_id, addresses)

        assert lookup_rows("//tmp/t", keys) == rows

    @authors("babenko")
    @pytest.mark.parametrize("peer_count", [1, 2])
    def test_recover_after_prerequisite_failure(self, peer_count):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : peer_count}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        cell_id = get("//tmp/t/@tablets/0/cell_id")
        tx_id = get("#{}/@prerequisite_transaction_id".format(cell_id))
        old_config_version = get("#{}/@config_version".format(cell_id))

        abort_transaction(tx_id)

        def check_config_version():
            new_config_version = get("#{}/@config_version".format(cell_id))
            return new_config_version > old_config_version
        wait(check_config_version)

        self._wait_cell_good(cell_id, [])

        def check_insert():
            try:
                insert_rows("//tmp/t", [{"key": 2, "value": "2"}])
                return True
            except:
                return False
        wait(check_insert)

        assert select_rows("* from [//tmp/t]") == [{"key": 1, "value": "1"}, {"key": 2, "value": "2"}]

    @authors("gritukan")
    def test_decommission_through_extra_peers(self):
        set("//sys/@config/tablet_manager/decommission_through_extra_peers", True)
        set("//sys/@config/tablet_manager/decommissioned_leader_reassignment_timeout", 7000)
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 1}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        first_rows = [{"key": i, "value": str(i + 5)} for i in range(5)]
        first_keys = [{"key": i} for i in range(5)]
        insert_rows("//tmp/t", first_rows)

        def get_peers():
            return get("#" + cell_id + "/@peers")

        cell_id = ls("//sys/tablet_cells")[0]
        first_peer_address = get_peers()[0]["address"]

        set_node_decommissioned(first_peer_address, True)
        wait(lambda: len(get_peers()) == 2 and get_peers()[1]["state"] == "following")
        second_peer_address = get_peers()[1]["address"]
        wait(lambda: len(get_peers()) == 1)
        assert get_peers()[0]["address"] == second_peer_address
        wait(lambda: get_peers()[0]["state"] == "leading")
        self._wait_cell_good(cell_id, [first_peer_address])

        assert lookup_rows("//tmp/t", first_keys) == first_rows

    @authors("gritukan")
    def test_decommission_interrupted(self):
        set("//sys/@config/tablet_manager/decommission_through_extra_peers", True)
        set("//sys/@config/tablet_manager/decommissioned_leader_reassignment_timeout", 7000)
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 1}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        first_rows = [{"key": i, "value": str(i + 5)} for i in range(5)]
        first_keys = [{"key": i} for i in range(5)]
        insert_rows("//tmp/t", first_rows)

        def get_peers():
            return get("#" + cell_id + "/@peers")

        cell_id = ls("//sys/tablet_cells")[0]
        first_peer_address = get_peers()[0]["address"]

        set_node_decommissioned(first_peer_address, True)
        wait(lambda: len(get_peers()) == 2 and get_peers()[1]["state"] == "following")

        set_node_decommissioned(first_peer_address, False)
        wait(lambda: len(get_peers()) == 1)
        assert get_peers()[0]["address"] == first_peer_address

        self._wait_cell_good(cell_id, ["non_existent_address"])

        assert lookup_rows("//tmp/t", first_keys) == first_rows

    @authors("gritukan")
    def test_dynamic_peer_count(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 1}})
        sync_create_cells(1, tablet_cell_bundle="b")
        cell_id = ls("//sys/tablet_cells")[0]

        def get_peers():
            return get("#" + cell_id + "/@peers")

        assert len(get_peers()) == 1
        first_peer_address = get_peers()[0]["address"]

        self._wait_cell_good(cell_id)

        set("//sys/tablet_cells/{}/@peer_count".format(cell_id), 2)

        with pytest.raises(YtError):
            set("//sys/tablet_cells/{}/@peer_count".format(cell_id), 1)

        self._wait_cell_good(cell_id)

        assert len(get_peers()) == 2
        second_peer_address = get_peers()[1]["address"]
        assert first_peer_address != second_peer_address

        self._check_cell_stable(cell_id)

        set_node_decommissioned(first_peer_address, True)
        self._wait_cell_good(cell_id, [first_peer_address])

        assert len(get_peers()) == 2
        assert get_peers()[1]["address"] == second_peer_address

        remove("//sys/tablet_cells/{}/@peer_count".format(cell_id))

        self._wait_cell_good(cell_id)

        assert len(get_peers()) == 1
        assert get_peers()[0]["address"] == second_peer_address

        self._check_cell_stable(cell_id)

    @authors("savrus")
    def test_tablet_cell_health_statistics(self):
        cell_id = sync_create_cells(1)[0]
        wait(lambda: get("#{0}/@health".format(cell_id)) == "good")

    @authors("kiselyovp")
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

    @authors("kiselyovp")
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

    @authors("kiselyovp")
    def test_get_table_pivot_keys(self):
        create("file", "//tmp/f")
        with pytest.raises(YtError): get_table_pivot_keys("//tmp/f")

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        with pytest.raises(YtError): get_table_pivot_keys("//tmp/t")
        remove("//tmp/t")

        sync_create_cells(1)
        self._create_ordered_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key" : 1, "value" : "abacaba"}])
        with pytest.raises(YtError): get_table_pivot_keys("//tmp/t")
        remove("//tmp/t")

        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [100], [200], [300]])
        assert get_table_pivot_keys("//tmp/t") == [{}, {"key": 100}, {"key": 200}, {"key": 300}]
        remove("//tmp/t")

        create_dynamic_table("//tmp/t", schema=[
            {"name": "key1", "type_v3": optional_type("int64"), "sort_order": "ascending"},
            {"name": "key2", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ])
        sync_reshard_table("//tmp/t", [[], [100, "lol"], [200], [300, "abacaba"]])
        assert get_table_pivot_keys("//tmp/t") == [
            {},
            {"key1": 100, "key2": "lol"},
            {"key1": 200},
            {"key1": 300, "key2": "abacaba"}]

##################################################################

class TestDynamicTablesSingleCell(DynamicTablesSingleCellBase):
    @authors("babenko")
    def test_force_unmount_on_remove(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        assert self._find_tablet_orchid(address, tablet_id) is not None

        remove("//tmp/t")
        wait(lambda: self._find_tablet_orchid(address, tablet_id) is None)

    @authors("babenko")
    def test_no_copy_mounted(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1")
        sync_mount_table("//tmp/t1")

        with pytest.raises(YtError): copy("//tmp/t1", "//tmp/t2")

    @authors("savrus", "babenko")
    @pytest.mark.parametrize("freeze", [False, True])
    def test_no_move_mounted(self, freeze):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1")
        sync_mount_table("//tmp/t1", freeze=freeze)

        with pytest.raises(YtError): move("//tmp/t1", "//tmp/t2")

    @authors("babenko")
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

    @authors("babenko")
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

    @authors("babenko")
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

    @authors("babenko")
    def test_move_in_tx_commit(self):
        self._create_sorted_table("//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert len(get("//tmp/t1/@tablets")) == 1
        assert len(get("//tmp/t2/@tablets", tx=tx)) == 1
        commit_transaction(tx)
        assert len(get("//tmp/t2/@tablets")) == 1

    @authors("babenko")
    def test_move_in_tx_abort(self):
        self._create_sorted_table("//tmp/t1")
        tx = start_transaction()
        move("//tmp/t1", "//tmp/t2", tx=tx)
        assert len(get("//tmp/t1/@tablets")) == 1
        assert len(get("//tmp/t2/@tablets", tx=tx)) == 1
        abort_transaction(tx)
        assert len(get("//tmp/t1/@tablets")) == 1


    @authors("babenko")
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

    @authors("lukyan")
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory_flush(self, mode):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        set("//tmp/t/@in_memory_mode", mode)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_flush_table("//tmp/t")
        wait(lambda: lookup_rows(
            "//tmp/t",
            [{"key": 0}],
            read_from="follower",
            timestamp=AsyncLastCommittedTimestamp) == [{"key": 0, "value": "0"}])

    @authors("babenko")
    def test_tablet_cell_create_permission(self):
        create_user("u")
        with pytest.raises(YtError): create_tablet_cell(authenticated_user="u")
        set("//sys/schemas/tablet_cell/@acl/end", make_ace("allow", "u", "create"))
        id = create_tablet_cell(authenticated_user="u")
        assert exists("//sys/tablet_cells/{0}/changelogs".format(id))
        assert exists("//sys/tablet_cells/{0}/snapshots".format(id))

    @authors("savrus")
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

    @authors("ifsmirnov")
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

    @authors("babenko")
    def test_tablet_cell_bundle_create_permission(self):
        create_user("u")
        with pytest.raises(YtError): create_tablet_cell_bundle("b", authenticated_user="u")
        set("//sys/schemas/tablet_cell_bundle/@acl/end", make_ace("allow", "u", "create"))
        create_tablet_cell_bundle("b", authenticated_user="u")

    @authors("savrus")
    def test_tablet_cell_acl_change(self):
        create_user("u")
        acl = [make_ace("allow", "unknown_user", "read")]
        create_tablet_cell_bundle("b")
        cell_id = sync_create_cells(1, tablet_cell_bundle="b")[0]

        with pytest.raises(YtError):
            get("//sys/tablet_cells/{}/changelogs".format(cell_id), authenticated_user="u")

        set("//sys/tablet_cell_bundles/b/@options/changelog_acl", [make_ace("allow", "u", "read")])
        get("//sys/tablet_cells/{}/changelogs".format(cell_id), authenticated_user="u")
        wait_for_cells([cell_id])

    @authors("savrus")
    def test_tablet_cell_multiplexing_change(self):
        create_tablet_cell_bundle("b")
        cell_id = sync_create_cells(1, tablet_cell_bundle="b")[0]
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        set("//sys/tablet_cell_bundles/b/@options/enable_changelog_multiplexing", False)
        sleep(0.5)
        wait_for_cells([cell_id])
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "value": "1"}]

        insert_rows("//tmp/t", [{"key": 1, "value": "2"}])
        set("//sys/tablet_cell_bundles/b/@options/enable_changelog_multiplexing", True)
        sleep(0.5)
        wait_for_cells([cell_id])
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "value": "2"}]

    @authors("babenko")
    def test_validate_dynamic_attr(self):
        create("table", "//tmp/t")
        assert not get("//tmp/t/@dynamic")
        with pytest.raises(YtError): mount_table("//tmp/t")
        with pytest.raises(YtError): unmount_table("//tmp/t")
        with pytest.raises(YtError): remount_table("//tmp/t")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[]])

    @authors("babenko")
    def test_dynamic_table_schema_validation(self):
        with pytest.raises(YtError): create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [{"data": "string"}]
            })

    @authors("savrus")
    def test_mount_map_node_failure(self):
        sync_create_cells(1)
        with pytest.raises(YtError): mount_table("//tmp")
        with pytest.raises(YtError): unmount_table("//tmp")
        with pytest.raises(YtError): freeze_table("//tmp")
        with pytest.raises(YtError): unfreeze_table("//tmp")
        with pytest.raises(YtError): reshard_table("//tmp", [[]])

    @authors("babenko")
    def test_mount_permission_denied(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        create_user("u")
        with pytest.raises(YtError): mount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): unmount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): remount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[]], authenticated_user="u")

    @authors("babenko", "levysotsky")
    def test_mount_permission_allowed(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        create_user("u")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "mount"))
        sync_mount_table("//tmp/t", authenticated_user="u")
        sync_unmount_table("//tmp/t", authenticated_user="u")
        remount_table("//tmp/t", authenticated_user="u")
        sync_reshard_table("//tmp/t", [[]], authenticated_user="u")

    @authors("lexolordan")
    def test_force_unmount_allowed_and_denied(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        create_user('u')
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", "use"))
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b", authenticated_user="u")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "mount"))
        sync_mount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): sync_unmount_table("//tmp/t", force=True, authenticated_user="u")
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", "administer"))
        sync_unmount_table("//tmp/t", force=True, authenticated_user="u")

    @authors("lexolordan")
    def test_cell_bundle_use_permission(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        create_user('u')
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", "use"))
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b", authenticated_user="u")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "mount"))
        sync_mount_table("//tmp/t", authenticated_user="u")

        set("//sys/tablet_cell_bundles/b/@acl", [make_ace("deny", "u", "use")])
        with pytest.raises(YtError): sync_unmount_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): sync_freeze_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): sync_unfreeze_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError): remount_table("//tmp/t", authenticated_user="u")

        set("//sys/tablet_cell_bundles/b/@acl", [make_ace("allow", "u", "use")])
        sync_freeze_table("//tmp/t", authenticated_user="u")
        sync_unfreeze_table("//tmp/t", authenticated_user="u")
        remount_table("//tmp/t", authenticated_user="u")
        sync_unmount_table("//tmp/t", authenticated_user="u")

    @authors("savrus")
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

    @authors("babenko")
    def test_default_cell_bundle(self):
        assert ls("//sys/tablet_cell_bundles") == ["default"]
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        assert get("//tmp/t/@tablet_cell_bundle") == "default"
        cells = get("//sys/tablet_cells", attributes=["tablet_cell_bundle"])
        assert len(cells) == 1
        assert list(cells.itervalues())[0].attributes["tablet_cell_bundle"] == "default"

    @authors("babenko")
    def test_cell_bundle_name_validation(self):
        with pytest.raises(YtError): create_tablet_cell_bundle("")

    @authors("babenko")
    def test_cell_bundle_name_create_uniqueness_validation(self):
        create_tablet_cell_bundle("b")
        with pytest.raises(YtError): create_tablet_cell_bundle("b")

    @authors("babenko")
    def test_cell_bundle_rename(self):
        create_tablet_cell_bundle("b")
        set("//sys/tablet_cell_bundles/b/@name", "b1")
        assert get("//sys/tablet_cell_bundles/b1/@name") == "b1"

    @authors("babenko")
    def test_cell_bundle_rename_uniqueness_validation(self):
        create_tablet_cell_bundle("b1")
        create_tablet_cell_bundle("b2")
        with pytest.raises(YtError): set("//sys/tablet_cell_bundles/b1/@name", "b2")

    @authors("babenko")
    def test_table_with_custom_cell_bundle(self):
        create_tablet_cell_bundle("b")
        create("table", "//tmp/t", attributes={"tablet_cell_bundle": "b"})
        assert get("//tmp/t/@tablet_cell_bundle") == "b"
        remove("//sys/tablet_cell_bundles/b")
        assert get("//sys/tablet_cell_bundles/b/@life_stage") in ["removal_started", "removal_pre_committed"]
        remove("//tmp/t")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b"))

    @authors("babenko")
    def test_table_with_custom_cell_bundle_name_validation(self):
        with pytest.raises(YtError): create("table", "//tmp/t", attributes={"tablet_cell_bundle": "b"})

    @authors("babenko")
    def test_cell_bundle_requires_use_permission_on_mount(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        create_user("u")
        # create does not require use
        create("table", "//tmp/t",
            attributes={
                "tablet_cell_bundle": "b",
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}
                ]
            },
            authenticated_user="u")
        # copy also does not require use
        copy("//tmp/t", "//tmp/t2", authenticated_user="u")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "mount"))
        # mount requires use
        with pytest.raises(YtError): mount_table("//tmp/t", authenticated_user="u")
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", "use"))
        mount_table("//tmp/t", authenticated_user="u")

    @authors("savrus", "babenko")
    def test_cell_bundle_attr_change_requires_use_not_write(self):
        create_tablet_cell_bundle("b")
        create_user("u")
        set("//sys/tablet_cell_bundles/b/@acl/end", make_ace("allow", "u", "use"))
        with WriteAceRemoved("//sys/schemas/tablet_cell_bundle"):
            set("//sys/tablet_cell_bundles/b/@tablet_balancer_config/enable_cell_balancer", False, authenticated_user="u")
            with pytest.raises(YtError):
                set("//sys/tablet_cell_bundles/b/@node_tag_filter", "b", authenticated_user="u")

    @authors("babenko")
    def test_cell_bundle_with_custom_peer_count(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count": 2}})
        get("//sys/tablet_cell_bundles/b/@options")
        assert get("//sys/tablet_cell_bundles/b/@options/peer_count") == 2
        cell_id = create_tablet_cell(attributes={"tablet_cell_bundle": "b"})
        assert cell_id in get("//sys/tablet_cell_bundles/b/@tablet_cell_ids")
        assert get("//sys/tablet_cells/" + cell_id + "/@tablet_cell_bundle") == "b"
        assert len(get("//sys/tablet_cells/" + cell_id + "/@peers")) == 2

    @authors("babenko")
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

    @authors("babenko")
    def test_no_storage_change_for_mounted(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@vital", False)
        with pytest.raises(YtError): set("//tmp/t/@replication_factor", 2)
        with pytest.raises(YtError): set("//tmp/t/@media", {"default": {"replication_factor": 2}})

    @authors("savrus")
    def test_cell_bundle_node_tag_filter(self):
        node = list(get("//sys/cluster_nodes"))[0]
        with pytest.raises(YtError):
            set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["custom!"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["custom"])
        set("//sys/tablet_cell_bundles/default/@node_tag_filter", "!custom")

        create_tablet_cell_bundle("custom", attributes={"node_tag_filter": "custom"})
        default_cell = sync_create_cells(1)[0]
        custom_cell = sync_create_cells(1, tablet_cell_bundle="custom")[0]

        for peer in get("#{0}/@peers".format(custom_cell)):
            assert peer["address"] == node

        for peer in get("#{0}/@peers".format(default_cell)):
            assert peer["address"] != node

    def _test_cell_bundle_distribution(self, enable_tablet_cell_balancer, test_decommission=False):
        set("//sys/@config/tablet_manager/tablet_cell_balancer/rebalance_wait_time", 500)
        set("//sys/@config/tablet_manager/tablet_cell_balancer/enable_tablet_cell_balancer", enable_tablet_cell_balancer)
        if test_decommission:
            set("//sys/@config/tablet_manager/decommission_through_extra_peers", True)

        create_tablet_cell_bundle("custom")
        nodes = ls("//sys/cluster_nodes")
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
                    slots = get("//sys/cluster_nodes/{0}/@tablet_slots".format(node))
                    count = Counter([cell_ids[slot["cell_id"]] for slot in slots if slot["state"] != "none"])
                    for bundle in bundles:
                        if not floor <= count[bundle] <= ceil:
                            return False
                return True
            wait(predicate)
            wait_for_cells(cell_ids.keys())

        if test_decommission:
            for idx, node in enumerate(nodes):
                set_node_decommissioned(node, True)
                _check([node], 0, 0)
                _check(nodes[:idx], 1, 2)
                _check(nodes[idx+1:], 1, 2)
                set_node_decommissioned(node, False)
                _check(nodes, 1, 1)

        _check(nodes, 1, 1)

        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(nodes[0]), True)
        _check(nodes[:1], 0, 0)
        _check(nodes[1:], 1, 2)

        if not enable_tablet_cell_balancer:
            return

        set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(nodes[0]), False)
        _check(nodes, 1, 1)

        for node in nodes[:len(nodes)/2]:
            set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), True)
        _check(nodes[len(nodes)/2:], 2, 2)

        for node in nodes[:len(nodes)/2]:
            set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), False)
        _check(nodes, 1, 1)

    @authors("savrus")
    def test_cell_bundle_distribution_new(self):
        self._test_cell_bundle_distribution(True)

    @authors("savrus")
    @flaky(max_runs=5)
    def test_cell_bundle_distribution_old(self):
        self._test_cell_bundle_distribution(False)

    @authors("gritukan")
    def test_tablet_cell_balancer_works_after_decommission(self):
        self._test_cell_bundle_distribution(True, True)

    @authors("savrus")
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

    @authors("savrus")
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

    @authors("iskhakovt")
    def test_tablet_table_path_attribute(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        assert get("#" + tablet_id + "/@table_path") == "//tmp/t"

    @authors("iskhakovt")
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

        for node in ls("//sys/cluster_nodes"):
            if node not in nodes_to_save:
                set_node_decommissioned(node, True)

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@replication_factor", 10)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        unmount_table("//tmp/t")

        wait(lambda: bool(get("//tmp/t/@tablet_error_count")))

        tablet = get("//tmp/t/@tablets/0/tablet_id")

        address = get_tablet_leader_address(tablet)
        orchid = self._find_tablet_orchid(address, tablet)
        errors = orchid["errors"]

        assert len(errors) == 1
        assert errors[0]["attributes"]["background_activity"] == "flush"
        assert errors[0]["attributes"]["tablet_id"] == tablet
        assert get("#" + tablet + "/@state") == "unmounting"
        assert get("//tmp/t/@tablets/0/error_count") == 1
        assert get("//tmp/t/@tablet_error_count") == 1

        for node in ls("//sys/cluster_nodes"):
            set_node_decommissioned(node, False)

    @authors("ifsmirnov")
    def test_tablet_error_count(self):
        LARGE_STRING = "a" * 15 * 1024 * 1024
        MAX_UNVERSIONED_ROW_WEIGHT = 512 * 1024 * 1024

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
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
        set("//tmp/t/@forced_compaction_revision", 1)
        set("//tmp/t/@enable_compaction_and_partitioning", True)
        remount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        def _get_errors():
            orchid = self._find_tablet_orchid(address, tablet_id)
            return orchid["errors"]

        # Compaction fails with "Versioned row data weight is too large".
        # Temporary debug output by ifsmirnov
        def wait_func():
            get("//tmp/t/@tablets")
            get("//tmp/t/@chunk_ids")
            get("//tmp/t/@tablet_statistics")
            return bool(_get_errors())
        wait(wait_func)

        assert len(_get_errors()) == 1
        wait(lambda: get("//tmp/t/@tablet_error_count") == 1)

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")

        # After reshard all errors should be gone.
        assert get("//tmp/t/@tablet_error_count") == 0

    @authors("savrus", "babenko")
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

    def _disable_tablet_cells_on_peer(self, cell):
        peer = get("#{0}/@peers/0/address".format(cell))
        set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(peer), True)
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

    @authors("savrus")
    def test_disable_tablet_cells(self):
        cell = sync_create_cells(1)[0]
        self._disable_tablet_cells_on_peer(cell)

    @authors("savrus", "gritukan")
    def test_tablet_slot_charges_cpu_resource_limit(self):
        get_cpu = lambda x: get("//sys/cluster_nodes/{0}/orchid/job_controller/resource_limits/cpu".format(x))

        create_tablet_cell_bundle("b")
        cell = sync_create_cells(1, tablet_cell_bundle="b")[0]
        peer = get("#{0}/@peers/0/address".format(cell))

        node = list(__builtin__.set(ls("//sys/cluster_nodes")) - __builtin__.set([peer]))[0]

        def get_cpu_delta():
            empty_node_cpu = get_cpu(node)
            assigned_node_cpu = get_cpu(peer)
            return empty_node_cpu - assigned_node_cpu

        wait(lambda: int(get_cpu_delta()) == 1)

        def _get_orchid(path):
            return get("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}{2}".format(peer, cell, path))

        assert _get_orchid("/dynamic_config_version") == 0

        set("//sys/tablet_cell_bundles/b/@dynamic_options/cpu_per_tablet_slot", 0.0)
        wait(lambda: _get_orchid("/dynamic_config_version") == 1)
        assert _get_orchid("/dynamic_options/cpu_per_tablet_slot") == 0.0

        wait(lambda: int(get_cpu_delta()) == 0)

    @authors("savrus", "babenko")
    def test_bundle_node_list(self):
        create_tablet_cell_bundle("b", attributes={"node_tag_filter": "b"})

        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["b"])
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        set("//sys/cluster_nodes/{0}/@banned".format(node), True)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == []
        set("//sys/cluster_nodes/{0}/@banned".format(node), False)
        wait(lambda: get("//sys/cluster_nodes/{0}/@state".format(node)) == "online")
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        set("//sys/cluster_nodes/{0}/@decommissioned".format(node), True)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == []
        set("//sys/cluster_nodes/{0}/@decommissioned".format(node), False)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), True)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == []
        set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), False)
        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

        build_snapshot(cell_id=None)
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get("//sys/tablet_cell_bundles/b/@nodes") == [node]

    @authors("ifsmirnov")
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
            assert read_table("//tmp/t{zzzzz}") == [{}]

        write_table("//tmp/t", [{"key": 0, "value": 1}])
        check_reads(False)
        alter_table("//tmp/t", dynamic=True, schema=schema)
        check_reads(is_sorted)

        if is_sorted:
            sync_mount_table("//tmp/t")
            sync_compact_table("//tmp/t")
            check_reads(True)

    @authors("ifsmirnov")
    @parametrize_external
    def test_mount_with_target_cell_ids(self, external):
        cells = sync_create_cells(4)

        set("//sys/@config/tablet_manager/tablet_cell_decommissioner/enable_tablet_cell_decommission", False)
        remove("#{0}".format(cells[3]))
        assert get("#{0}/@tablet_cell_life_stage".format(cells[3])) != "running"

        if external:
            self._create_sorted_table("//tmp/t", external_cell_tag=1)
        else:
            self._create_sorted_table("//tmp/t", external=False)

        sync_reshard_table("//tmp/t", [[], [1], [2]])

        # At most one of `cell_id` and `target_cell_ids` must be set.
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t", cell_id=cells[0], target_cell_ids=cells[:3])

        # `target_cell_ids` must not contain invalid cell ids.
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t", target_cell_ids=[cells[0], cells[1], "1-2-3-4"])

        # `target_cell_ids` must be of corresponding size.
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t", target_cell_ids=cells[:2])
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=1, target_cell_ids=cells[:3])

        # Target cells may not be decommissioned.
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0, target_cell_ids=[cells[3]])
        assert exists("#{0}".format(cells[3]))

        target_cell_ids = [cells[0], cells[0], cells[1]]
        sync_mount_table("//tmp/t", target_cell_ids=target_cell_ids)
        assert target_cell_ids == [tablet["cell_id"] for tablet in get("//tmp/t/@tablets")]

        # Cells are not changed for mounted tablets.
        sync_mount_table("//tmp/t", target_cell_ids=[cells[0], cells[2], cells[2]])
        assert target_cell_ids == [tablet["cell_id"] for tablet in get("//tmp/t/@tablets")]

        sync_unmount_table("//tmp/t", first_tablet_index=0, last_tablet_index=1)
        sync_mount_table("//tmp/t", target_cell_ids=[cells[2], cells[1], cells[0]])
        assert [cells[2], cells[1], cells[1]] == [tablet["cell_id"] for tablet in get("//tmp/t/@tablets")]

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=2, target_cell_ids=[cells[1], cells[2]])
        assert [None, cells[1], cells[2]] == [tablet.get("cell_id") for tablet in get("//tmp/t/@tablets")]

    @authors("aozeritsky")
    def test_modification_access_time(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "test"}]
        time_before = get("//tmp/t/@modification_time")
        insert_rows("//tmp/t", rows)
        wait(lambda: get("//tmp/t/@modification_time") != time_before)
        time_after = get("//tmp/t/@modification_time")
        assert time_after > time_before

        time_before = get("//tmp/t/@access_time")
        keys = [{"key": r["key"]} for r in rows]
        assert lookup_rows("//tmp/t", keys) == rows
        wait(lambda: get("//tmp/t/@access_time") != time_before)
        time_after = get("//tmp/t/@access_time")
        assert time_after > time_before

        time_before = time_after
        select_rows("* from [//tmp/t]")
        wait(lambda: get("//tmp/t/@access_time") != time_before)
        time_after = get("//tmp/t/@access_time")
        assert time_after > time_before

    @authors("savrus")
    def test_remove_tablet_cell(self):
        cells = sync_create_cells(1)
        remove("#" + cells[0])
        wait(lambda: not exists("//sys/tablet_cells/{0}".format(cells[0])))

    @authors("savrus")
    def test_tablet_cell_decommission(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 1}})
        cell = sync_create_cells(1, tablet_cell_bundle="b")[0]
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in xrange(2)]
        rows2 = [{"key": i, "value": str(i + 1)} for i in xrange(2)]
        keys = [{"key": i} for i in xrange(2)]

        insert_rows("//tmp/t", rows1)

        set("//sys/@config/tablet_manager/tablet_cell_decommissioner/enable_tablet_cell_removal", False)
        set("//sys/tablet_cell_bundles/b/@dynamic_options/suppress_tablet_cell_decommission", True)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        version = get("//sys/tablet_cell_bundles/b/@dynamic_config_version")
        wait(lambda: get("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}/dynamic_config_version".format(address, cell)) == version)

        remove("#{0}".format(cell))
        wait(lambda: get("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}/life_stage".format(address, cell)) == "decommissioning_on_node")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", rows2)

        self._create_sorted_table("//tmp/t2", tablet_cell_bundle="b")
        with pytest.raises(YtError):
            mount_table("//tmp/t2")

        assert get("#{0}/@tablet_cell_life_stage".format(cell)) == "decommissioning_on_node"
        set("//sys/tablet_cell_bundles/b/@dynamic_options/suppress_tablet_cell_decommission", False)
        wait(lambda: get("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}/life_stage".format(address, cell)) == "decommissioned")

        set("//sys/@config/tablet_manager/tablet_cell_decommissioner/enable_tablet_cell_removal", True)
        wait(lambda: not exists("#{0}".format(cell)))

    @authors("savrus")
    def test_force_remove_tablet_cell(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 1}})
        set("//sys/tablet_cell_bundles/b/@dynamic_options/suppress_tablet_cell_decommission", True)
        cell = sync_create_cells(1, tablet_cell_bundle="b")[0]

        remove("#{0}".format(cell), force=True)
        wait(lambda: not exists("#" + cell))

    @authors("savrus")
    def test_force_remove_tablet_cell_after_decommission(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 1}})
        set("//sys/tablet_cell_bundles/b/@dynamic_options/suppress_tablet_cell_decommission", True)
        cell = sync_create_cells(1, tablet_cell_bundle="b")[0]

        remove("#{0}".format(cell))
        wait(lambda: get("#{0}/@tablet_cell_life_stage".format(cell)) == "decommissioning_on_node")

        remove("#{0}".format(cell), force=True)
        wait(lambda: not exists("#" + cell))

    @authors("savrus")
    def test_cumulative_statistics(self):
        cell = sync_create_cells(1)[0]
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", rows)
        insert_rows("//tmp/t", rows)

        self._disable_tablet_cells_on_peer(cell)

        def check():
            changelog = "//sys/tablet_cells/{0}/changelogs/000000001".format(cell)
            if not exists(changelog):
                return False
            chunk_list = get(changelog + "/@chunk_list_id")
            statistics = get("#{0}/@statistics".format(chunk_list))
            if not statistics["sealed"]:
                return False
            cumulative_statistics = get("#{0}/@cumulative_statistics".format(chunk_list))
            assert cumulative_statistics[-1]["row_count"] == statistics["row_count"]
            assert cumulative_statistics[-1]["chunk_count"] == statistics["chunk_count"]
            assert cumulative_statistics[-1]["data_size"] == statistics["uncompressed_data_size"]
            return True

        wait(check)

    @authors("ifsmirnov")
    def test_chunk_view_attributes(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "a"} for i in range(5)])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [2]])

        sync_mount_table("//tmp/t")
        chunk_views = get("//sys/chunk_views", attributes=["chunk_id", "lower_limit", "upper_limit"])
        for value in chunk_views.itervalues():
            attrs = value.attributes
            if attrs["lower_limit"] == {} and attrs["upper_limit"] == {"key": [2]}:
                break
        else:
            assert False

        table_chunks = get("//tmp/t/@chunk_ids")
        assert len(table_chunks) == 2
        assert table_chunks[0] == table_chunks[1]
        assert len(chunk_views) == 2
        assert all(attr.attributes["chunk_id"] == table_chunks[0] for attr in chunk_views.values())
        chunk_tree = get("#{}/@tree".format(get("//tmp/t/@chunk_list_id")))
        assert chunk_tree.attributes["rank"] == 2
        assert len(chunk_tree) == 2
        for tablet in chunk_tree:
            for store in tablet:
                if store.attributes["type"] == "chunk_view":
                    assert store.attributes["id"] in chunk_views
                    assert store.attributes["type"] == "chunk_view"
                    assert len(store) == 1
                    assert store[0] == table_chunks[0]
                else:
                    assert store.attributes["type"] == "dynamic_store"

    @authors("savrus", "ifsmirnov")
    def test_select_rows_access_tracking(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1")
        self._create_sorted_table("//tmp/t2")
        sync_mount_table("//tmp/t1")
        sync_mount_table("//tmp/t2")

        t1_access_time = get("//tmp/t1/@access_time")
        t2_access_time = get("//tmp/t2/@access_time")

        select_rows("* from [//tmp/t1]", suppress_access_tracking=True)
        select_rows("* from [//tmp/t2]")

        # Wait for node heartbeat to arrive.
        wait(lambda: get("//tmp/t2/@access_time") != t2_access_time)
        assert get("//tmp/t1/@access_time") == t1_access_time

    @authors("ifsmirnov")
    def test_changelog_id_attribute(self):
        cell_id = sync_create_cells(1)[0]

        def _get_latest_file(dir_name):
            files = ls("//sys/tablet_cells/{}/{}".format(cell_id, dir_name))
            return int(max(files)) if files else -1

        def _get_attr(attr):
            return get("#{}/@{}".format(cell_id, attr))

        wait(lambda: _get_attr("health") == "good")
        wait(lambda: _get_latest_file("snapshots") == _get_attr("max_snapshot_id"))
        wait(lambda: _get_latest_file("changelogs") == _get_attr("max_changelog_id"))

        def _try_build_snapshot():
            try:
                build_snapshot(cell_id=cell_id)
                return True
            except:
                return False

        wait(_try_build_snapshot)
        wait(lambda: _get_latest_file("snapshots") == _get_attr("max_snapshot_id"))
        wait(lambda: _get_latest_file("changelogs") == _get_attr("max_changelog_id"))

##################################################################

class TestDynamicTablesSafeMode(DynamicTablesBase):
    USE_PERMISSION_CACHE = False

    DELTA_NODE_CONFIG = {
        "master_cache_service": {
            "capacity": 0
        }
    }

    @authors("savrus")
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

class TestDynamicTablesResourceLimits(DynamicTablesBase):
    USE_PERMISSION_CACHE = False

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "security_manager": {
                "resource_limits_cache": {
                    "expire_after_access_time": 0,
                },
            },
        }
    }

    def _verify_resource_usage(self, account, resource, expected):
        def resource_usage_matches(driver):
            return lambda: (get("//sys/accounts/{0}/@resource_usage/{1}".format(account, resource), driver=driver) == expected and
                    get("//sys/accounts/{0}/@committed_resource_usage/{1}".format(account, resource), driver=driver) == expected)
        self._multicell_wait(resource_usage_matches)

    def _multicell_set(self, path, value):
        set(path, value)
        for i in xrange(self.Env.secondary_master_cell_count):
            driver = get_driver(i + 1)
            wait(lambda: exists(path, driver=driver) and get(path, driver=driver) == value)

    def _multicell_wait(self, predicate):
        for i in xrange(self.Env.secondary_master_cell_count):
            driver = get_driver(i + 1)
            wait(predicate(driver))

    @authors("savrus")
    @pytest.mark.parametrize("resource", ["chunk_count", "disk_space_per_medium/default"])
    def test_resource_limits(self, resource):
        create_account("test_account")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")
        sync_mount_table("//tmp/t")

        set("//sys/accounts/test_account/@resource_limits/" + resource, 0)
        def _wait_func(driver):
            limits = get("//sys/accounts/test_account/@resource_limits", driver=driver)
            if resource == "chunk_count":
                return limits["chunk_count"] == 0
            else:
                return limits["disk_space"] == 0
        self._multicell_wait(lambda driver: lambda: _wait_func(driver))

        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_flush_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        self._multicell_set("//sys/accounts/test_account/@resource_limits/" + resource, 10000)
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        set("//sys/accounts/test_account/@resource_limits/" + resource, 0)
        sync_unmount_table("//tmp/t")

    @authors("savrus")
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

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 4)
        self._create_ordered_table("//tmp/t1", account="test_account", tablet_count=2)
        self._verify_resource_usage("test_account", "tablet_count", 2)
        self._create_sorted_table("//tmp/t2", account="test_account", pivot_keys=[[], [1]])
        self._verify_resource_usage("test_account", "tablet_count", 4)

    @authors("lexolordan")
    def test_mount_mounted_table(self):
        create_account("test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        sync_create_cells(1)

        def _create_table(table_name):
            self._create_sorted_table(table_name, account="test_account", external_cell_tag=1)

            sync_mount_table(table_name)
            insert_rows(table_name, [{"key": 0, "value": "0"}])
            sync_unmount_table(table_name)

        _create_table("//tmp/t0")
        _create_table("//tmp/t1")

        data_size = get("//tmp/t0/@uncompressed_data_size")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", data_size)

        set("//tmp/t0/@in_memory_mode", "uncompressed")
        sync_mount_table("//tmp/t0")

        self._verify_resource_usage("test_account", "tablet_static_memory", data_size)

        set("//tmp/t1/@in_memory_mode", "uncompressed")
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t1")

        remount_table("//tmp/t0")
        sync_unmount_table("//tmp/t0")
        self._verify_resource_usage("test_account", "tablet_static_memory", 0)

    @authors("savrus")
    def test_tablet_count_limit_reshard(self):
        create_account("test_account")
        sync_create_cells(1)
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        self._create_sorted_table("//tmp/t1", account="test_account")
        self._create_ordered_table("//tmp/t2", account="test_account")

        # Wait for resource usage since tabels can be placed to different cells.
        self._multicell_wait(lambda driver: lambda: get("//sys/accounts/test_account/@resource_usage/tablet_count", driver=driver) == 2)

        with pytest.raises(YtError):
            reshard_table("//tmp/t1", [[], [1]])
        with pytest.raises(YtError):
            reshard_table("//tmp/t2", 2)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 4)
        sync_reshard_table("//tmp/t1", [[], [1]])
        sync_reshard_table("//tmp/t2", 2)
        self._verify_resource_usage("test_account", "tablet_count", 4)

    @authors("savrus")
    def test_tablet_count_limit_copy(self):
        create_account("test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")
        wait(lambda: get("//tmp/t/@resource_usage/tablet_count") == 1)

        # Wait for usage propagation to primary.
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_count") == 1)
        # Wait for usage propagation from primary.
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_count", driver=get_driver(get("//tmp/t/@native_cell_tag"))) == 1)

        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/t_copy", preserve_account=True)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        copy("//tmp/t", "//tmp/t_copy", preserve_account=True)
        self._verify_resource_usage("test_account", "tablet_count", 2)

    @authors("shakurov")
    def test_tablet_count_copy_across_accounts(self):
        create_account("test_account1")
        create_account("test_account2")
        self._multicell_set("//sys/accounts/test_account1/@resource_limits/tablet_count", 10)
        self._multicell_set("//sys/accounts/test_account2/@resource_limits/tablet_count", 0)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account1")

        self._verify_resource_usage("test_account1", "tablet_count", 1)
        wait(lambda: get("//tmp/t/@resource_usage/tablet_count") == 1)

        create("map_node", "//tmp/dir", attributes={"account": "test_account2"})

        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/dir/t_copy", preserve_account=False)

        self._verify_resource_usage("test_account2", "tablet_count", 0)

        self._multicell_set("//sys/accounts/test_account2/@resource_limits/tablet_count", 1)
        copy("//tmp/t", "//tmp/dir/t_copy", preserve_account=False)

        self._verify_resource_usage("test_account1", "tablet_count", 1)
        self._verify_resource_usage("test_account2", "tablet_count", 1)

    @authors("savrus")
    def test_tablet_count_remove(self):
        create_account("test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")
        self._verify_resource_usage("test_account", "tablet_count", 1)
        remove("//tmp/t")
        self._verify_resource_usage("test_account", "tablet_count", 0)

    @authors("savrus")
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

    @authors("savrus")
    def test_tablet_count_alter_table(self):
        create_account("test_account")
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t")
        set("//tmp/t/@account", "test_account")

        self._verify_resource_usage("test_account", "tablet_count", 1)
        alter_table("//tmp/t", dynamic=False)
        self._verify_resource_usage("test_account", "tablet_count", 0)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 0)
        with pytest.raises(YtError):
            alter_table("//tmp/t", dynamic=True)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        alter_table("//tmp/t", dynamic=True)
        self._verify_resource_usage("test_account", "tablet_count", 1)

    @authors("savrus")
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
            data_size = get("//tmp/t/@{0}_data_size".format(mode))
            resource_usage = get("//sys/accounts/test_account/@resource_usage")
            committed_resource_usage = get("//sys/accounts/test_account/@committed_resource_usage")
            return (resource_usage["tablet_static_memory"] == data_size and
                    resource_usage == committed_resource_usage and
                    get("//tmp/t/@resource_usage/tablet_count") == 1 and
                    get("//tmp/t/@resource_usage/tablet_static_memory") == data_size and
                    get("//tmp/@recursive_resource_usage/tablet_count") == 1 and
                    get("//tmp/@recursive_resource_usage/tablet_static_memory") == data_size)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 1000)
        sync_mount_table("//tmp/t")
        wait(_verify)

        sync_compact_table("//tmp/t")
        wait(_verify)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 0)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 1000)
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        sync_compact_table("//tmp/t")
        wait(_verify)

        sync_unmount_table("//tmp/t")
        self._verify_resource_usage("test_account", "tablet_static_memory", 0)

    @authors("savrus")
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

    @authors("savrus")
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

    @authors("ifsmirnov")
    def test_snapshot_account_resource_limits_violation(self):
        create_account("test_account")
        create_tablet_cell_bundle("custom", attributes={"options": {
            "snapshot_account": "test_account"}})

        sync_create_cells(1, tablet_cell_bundle="custom")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="custom")
        create("table", "//tmp/junk", attributes={"account": "test_account"})
        write_table("//tmp/junk", [{"key": "value"}])
        set("//sys/accounts/test_account/@resource_limits/disk_space_per_medium/default", 0)
        self._multicell_wait(lambda driver: lambda: get("//sys/accounts/test_account/@resource_limits/disk_space", driver=driver) == 0)

        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1}])

        set("//sys/accounts/test_account/@resource_limits/disk_space_per_medium/default", 10**9)

        def _wait_func():
            try:
                insert_rows("//tmp/t", [{"key": 1}])
                return True
            except:
                return False
        wait(_wait_func)

    @authors("savrus", "ifsmirnov")
    def test_chunk_view_accounting(self):
        create_account("test_account")
        create_account("other_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 10)
        self._multicell_set("//sys/accounts/other_account/@resource_limits/tablet_count", 10)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [2]])
        assert any(
            "//tmp/t" in value.attributes["owning_nodes"]
            for value in get("//sys/chunk_views", attributes=["owning_nodes"]).values())

        def _verify(account, disk_space):
            def wait_func():
                usage = get("//sys/accounts/{}/@resource_usage".format(account))
                committed_usage = get("//sys/accounts/{}/@committed_resource_usage".format(account))
                return usage == committed_usage and usage["disk_space"] == disk_space
            wait(wait_func)


        disk_space = get("//sys/accounts/test_account/@resource_usage/disk_space")
        _verify("test_account", disk_space)

        set("//tmp/t/@account", "other_account")
        _verify("test_account", 0)
        _verify("other_account", disk_space)

##################################################################

class TestDynamicTablesMulticell(TestDynamicTablesSingleCell):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("savrus")
    def test_external_dynamic(self):
        cells = sync_create_cells(1)
        self._create_sorted_table("//tmp/t", external=True, external_cell_tag=2)
        assert get("//tmp/t/@external")
        cell_tag = get("//tmp/t/@external_cell_tag")
        table_id = get("//tmp/t/@id")

        driver = get_driver(2)
        assert get("#{0}/@dynamic".format(table_id), driver=driver)
        assert get("#{0}/@dynamic".format(table_id))

        sync_mount_table("//tmp/t")

        wait(lambda: get("//sys/tablet_cells/{0}/@tablet_count".format(cells[0]), driver=driver) == 1)
        wait(lambda: get("//sys/tablet_cells/{0}/@tablet_count".format(cells[0])) == 1)

        tablet = get("//tmp/t/@tablets/0")
        assert get("//sys/tablet_cells/{0}/@tablet_ids".format(cells[0]), driver=driver) == [tablet["tablet_id"]]
        assert get("//sys/tablet_cells/{0}/@tablet_ids".format(cells[0])) == [tablet["tablet_id"]]

        wait(lambda:  get("//sys/tablet_cells/{0}/@multicell_statistics".format(cells[0]))[str(cell_tag)]["tablet_count"] ==  1)
        wait(lambda: get("//sys/tablet_cells/{0}/@total_statistics".format(cells[0]))["tablet_count"] == 1)

        rows = [{"key": 0, "value": "0"}]
        keys = [{"key": r["key"]} for r in rows]
        insert_rows("//tmp/t", rows)
        assert lookup_rows("//tmp/t", keys) == rows

        sync_freeze_table("//tmp/t")

        wait(lambda: get("//tmp/t/@uncompressed_data_size") == get("#{}/@uncompressed_data_size".format(table_id), driver=driver))

        sync_compact_table("//tmp/t")
        sync_unmount_table("//tmp/t")

        wait(lambda: get("//tmp/t/@uncompressed_data_size") == get("#{}/@uncompressed_data_size".format(table_id), driver=driver))

    @authors("savrus")
    def test_peer_change_on_prerequisite_transaction_abort(self):
        cells = sync_create_cells(1)
        driver = get_driver(1)

        def prepare():
            cells.extend(sync_create_cells(10))
            sync_remove_tablet_cells(cells[:10])
            for l in xrange(10):
                cells.pop(0)
            cell = cells[0]
            node = get("#{0}/@peers/0/address".format(cell))
            assert get("#{0}/@peers/0/address".format(cell), driver=driver) == node

            tx = get("#{0}/@prerequisite_transaction_id".format(cell))
            abort_transaction(tx)
            wait(lambda: exists("#{0}/@prerequisite_transaction_id".format(cell)))
            wait(lambda: get("#{0}/@peers/0/state".format(cell)) == "leading")
            return get("#{0}/@peers/0/address".format(cell)) != node

        wait(prepare)
        cell = cells[0]
        node = get("#{0}/@peers/0/address".format(cell))
        assert get("#{0}/@peers/0/address".format(cell), driver=driver) == node

    @authors("savrus")
    @pytest.mark.parametrize("freeze", [False, True])
    def test_mount_orphaned(self, freeze):
        self._create_sorted_table("//tmp/t")
        cells = sync_create_cells(1)

        requests = []
        requests.append(make_batch_request("remove", path="#" + cells[0]))
        requests.append(make_batch_request("mount_table", path="//tmp/t", cell_id=cells[0], freeze=freeze))
        rsps = execute_batch(requests)
        assert len(rsps[1]["output"]) == 0

        expected_state = "frozen" if freeze  else "mounted"
        assert get("//tmp/t/@expected_tablet_state") == expected_state
        assert get("//tmp/t/@tablets/0/state") == "unmounted"

        actions = get("//sys/tablet_actions")
        assert len(actions) == 1
        assert get("#{0}/@state".format(list(actions)[0])) == "orphaned"

        sync_create_cells(1)
        wait_for_tablet_state("//tmp/t", expected_state)
        assert get("//tmp/t/@tablets/0/state") == expected_state

class TestDynamicTablesPortal(TestDynamicTablesMulticell):
    ENABLE_TMP_PORTAL = True

class TestDynamicTablesResourceLimitsMulticell(TestDynamicTablesResourceLimits):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestDynamicTablesResourceLimitsPortal(TestDynamicTablesResourceLimitsMulticell):
    ENABLE_TMP_PORTAL = True

##################################################################

class TestDynamicTablesRpcProxy(TestDynamicTablesSingleCell):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

class TestDynamicTablesWithCompressionRpcProxy(DynamicTablesSingleCellBase):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

    DELTA_DRIVER_CONFIG = {
        "request_codec": "lz4",
        "response_codec": "quick_lz",
    }

class TestDynamicTablesWithModernCompressionRpcProxy(DynamicTablesSingleCellBase):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

    DELTA_DRIVER_CONFIG = {
        "request_codec": "lz4",
        "response_codec": "quick_lz",
        "enable_legacy_rpc_codecs": False
    }

class TestDynamicTablesWithAbandoningLeaderLeaseDuringRecovery(DynamicTablesSingleCellBase):
    def setup_method(self, method):
        super(DynamicTablesSingleCellBase, self).setup_method(method)
        set("//sys/@config/tablet_manager/abandon_leader_lease_during_recovery", True)
