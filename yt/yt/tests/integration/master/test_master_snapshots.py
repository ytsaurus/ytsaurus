from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_helpers import profiler_factory

# COMPAT(babenko): drop this once the local 'create_account' is no longer needed.
import yt_commands

from yt_commands import (
    authors, wait, create, ls, get, set, copy, remove,
    exists, concatenate,
    remove_account, create_proxy_role, create_account_resource_usage_lease, start_transaction, abort_transaction, commit_transaction, lock, insert_rows,
    lookup_rows, alter_table, write_table, read_table, wait_for_cells,
    sync_create_cells, sync_mount_table, sync_freeze_table, sync_reshard_table, get_singular_chunk_id,
    get_account_disk_space, create_dynamic_table, build_snapshot,
    build_master_snapshots, clear_metadata_caches, create_pool_tree, create_pool, move, create_domestic_medium,
    create_chaos_cell_bundle, sync_create_chaos_cell, generate_chaos_cell_id, select_rows)
from yt_helpers import master_exit_read_only_sync

from yt.common import YtError
from yt_type_helpers import make_schema, normalize_schema

from yt.environment.helpers import assert_items_equal

import pytest

from copy import deepcopy
import builtins
import time

##################################################################


# COMPAT(babenko): drop this once all involved versions (including those in compat tests) are >= 22.4.
def create_account(name, parent_name=None, empty=False, **kwargs):
    yt_commands.create_account(name, parent_name, empty, **kwargs)
    if kwargs.get("sync", True):
        wait(
            lambda: exists("//sys/accounts/{0}".format(name))
            and get("//sys/accounts/{0}/@life_stage".format(name)) == "creation_committed"
        )


def check_simple_node():
    set("//tmp/a", 42)

    yield

    assert get("//tmp/a") == 42


def check_schema():
    def get_schema(strict):
        return make_schema(
            [{"name": "value", "type": "string", "required": True}],
            unique_keys=False,
            strict=strict,
        )

    create("table", "//tmp/table1", attributes={"schema": get_schema(True)})
    create("table", "//tmp/table2", attributes={"schema": get_schema(True)})
    create("table", "//tmp/table3", attributes={"schema": get_schema(False)})

    yield

    assert normalize_schema(get("//tmp/table1/@schema")) == get_schema(True)
    assert normalize_schema(get("//tmp/table2/@schema")) == get_schema(True)
    assert normalize_schema(get("//tmp/table3/@schema")) == get_schema(False)


def check_forked_schema():
    schema1 = make_schema(
        [{"name": "foo", "type": "string", "required": True}],
        unique_keys=False,
        strict=True,
    )
    schema2 = make_schema(
        [
            {"name": "foo", "type": "string", "required": True},
            {"name": "bar", "type": "string", "required": True},
        ],
        unique_keys=False,
        strict=True,
    )

    create("table", "//tmp/forked_schema_table", attributes={"schema": schema1})
    tx = start_transaction(timeout=120000)
    lock("//tmp/forked_schema_table", mode="snapshot", tx=tx)

    alter_table("//tmp/forked_schema_table", schema=schema2)

    yield

    assert normalize_schema(get("//tmp/forked_schema_table/@schema")) == schema2
    assert normalize_schema(get("//tmp/forked_schema_table/@schema", tx=tx)) == schema1


def check_removed_account():
    create_account("a1")
    create_account("a2")

    for i in range(0, 5):
        table = "//tmp/a1_table{0}".format(i)
        create("table", table, attributes={"account": "a1"})
        write_table(table, {"a": "b"})
        copy(table, "//tmp/a2_table{0}".format(i), attributes={"account": "a2"})

    for i in range(0, 5):
        chunk_id = get_singular_chunk_id("//tmp/a2_table{0}".format(i))
        wait(lambda: len(get("#{0}/@requisition".format(chunk_id))) == 2)

    for i in range(0, 5):
        remove("//tmp/a1_table" + str(i))

    remove_account("a1", sync=False)

    yield

    for i in range(0, 5):
        chunk_id = get_singular_chunk_id("//tmp/a2_table{0}".format(i))
        wait(lambda: len(get("#{0}/@requisition".format(chunk_id))) == 1)


def check_hierarchical_accounts():
    create_account("b1")
    create_account("b2")
    create_account("b11", "b1")
    create_account("b21", "b2")

    create("table", "//tmp/b11_table", attributes={"account": "b11"})
    write_table("//tmp/b11_table", {"a": "b"})
    create("table", "//tmp/b21_table", attributes={"account": "b21"})
    write_table("//tmp/b21_table", {"a": "b", "c": "d"})
    remove_account("b2", sync=False)

    # XXX(kiselyovp) this might be flaky
    wait(lambda: get("//sys/accounts/b11/@resource_usage/disk_space_per_medium/default") > 0)
    wait(lambda: get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default") > 0)
    b11_disk_usage = get("//sys/accounts/b11/@resource_usage/disk_space_per_medium/default")
    b21_disk_usage = get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default")

    yield

    accounts = ls("//sys/accounts")

    for account in accounts:
        assert not account.startswith("#")

    topmost_accounts = ls("//sys/account_tree")
    for account in [
        "sys",
        "tmp",
        "intermediate",
        "chunk_wise_accounting_migration",
        "b1",
        "b2",
    ]:
        assert account in accounts
        assert account in topmost_accounts
    for account in ["b11", "b21", "root"]:
        assert account in accounts
        assert account not in topmost_accounts

    # Check invariants in master will catch /sys/account_tree ref counter mismatch on load.

    assert ls("//sys/account_tree/b1") == ["b11"]
    assert ls("//sys/account_tree/b2") == ["b21"]
    assert ls("//sys/account_tree/b1/b11") == []
    assert ls("//sys/account_tree/b2/b21") == []

    assert get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default") == b21_disk_usage
    assert get("//sys/accounts/b2/@recursive_resource_usage/disk_space_per_medium/default") == b21_disk_usage

    set("//tmp/b21_table/@account", "b11")
    wait(lambda: not exists("//sys/account_tree/b2"), iter=120, sleep_backoff=0.5)
    assert not exists("//sys/accounts/b2")
    assert exists("//sys/accounts/b11")

    assert get("//sys/accounts/b11/@resource_usage/disk_space_per_medium/default") == b11_disk_usage + b21_disk_usage


def check_master_memory():
    resource_limits = {
        "disk_space_per_medium": {"default": 100000},
        "chunk_count": 100,
        "node_count": 100,
        "tablet_count": 100,
        "tablet_static_memory": 100000,
        "master_memory":
        {
            "total": 100000,
            "chunk_host": 100000,
        }
    }
    create_account("a", attributes={"resource_limits": resource_limits})

    create("map_node", "//tmp/dir1", attributes={"account": "a", "attr_for_dir": "BooYa"})
    create("table", "//tmp/dir1/t", attributes={"account": "a", "attr_for_tab": "Boopers"})
    write_table("//tmp/dir1/t", {"a": 1})
    write_table(
        "<chunk_sort_columns=[{name=a;sort_order=descending};{name=b;sort_order=descending};{name=c;sort_order=descending}];append=true>//tmp/dir1/t",
        {"a": 42, "b": 123, "c": 88005553535})

    copy("//tmp/dir1", "//tmp/dir2", preserve_account=True)

    sync_create_cells(1)
    schema = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]
    create_dynamic_table("//tmp/d1", schema=schema, account="a")
    create_dynamic_table("//tmp/d2", schema=schema, account="a")

    sync_reshard_table("//tmp/d1", [[], [1], [2]])
    rows = [{"key": 0, "value": "0"}]
    sync_mount_table("//tmp/d1")
    insert_rows("//tmp/d1", rows)
    sync_freeze_table("//tmp/d1")

    time.sleep(3)

    master_memory_usage = get("//sys/accounts/a/@resource_usage/master_memory")

    yield

    wait(lambda: get("//sys/accounts/a/@resource_usage/master_memory") == master_memory_usage)


def check_dynamic_tables():
    sync_create_cells(1)
    create_dynamic_table(
        "//tmp/table_dynamic",
        schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ],
    )
    rows = [{"key": 0, "value": "0"}]
    keys = [{"key": 0}]
    sync_mount_table("//tmp/table_dynamic")
    insert_rows("//tmp/table_dynamic", rows)
    sync_freeze_table("//tmp/table_dynamic")

    yield

    clear_metadata_caches()
    wait_for_cells()
    assert lookup_rows("//tmp/table_dynamic", keys) == rows


def check_chaos_replicated_table():
    if not exists("//sys/chaos_cell_bundles/chaos_bundle"):
        create_chaos_cell_bundle(name="chaos_bundle", peer_cluster_names=["primary"])
    cell_id = generate_chaos_cell_id()
    sync_create_chaos_cell("chaos_bundle", cell_id, ["primary"])
    set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

    create("chaos_replicated_table", "//tmp/crt", attributes={
        "chaos_cell_bundle": "chaos_bundle",
        "schema": [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ],
    })

    yield

    clear_metadata_caches()
    wait_for_cells()
    assert len(get("//tmp/crt/@schema")) == 2


def check_security_tags():
    for i in range(10):
        create(
            "table",
            "//tmp/table_with_tags" + str(i),
            attributes={"security_tags": ["atag" + str(i), "btag" + str(i)]},
        )

    yield

    for i in range(10):
        assert_items_equal(
            get("//tmp/table_with_tags" + str(i) + "/@security_tags"),
            ["atag" + str(i), "btag" + str(i)],
        )


def check_transactions():
    tx1 = start_transaction(timeout=120000)
    tx2 = start_transaction(tx=tx1, timeout=120000)

    create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 12})
    create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 13})
    create("table", "//tmp/p1/t", tx=tx1)  # replicate tx1 to cell 12
    create("table", "//tmp/p2/t", tx=tx2)  # replicate tx2 to cell 13
    assert get("#{}/@replicated_to_cell_tags".format(tx1)) == [12, 13]
    assert get("#{}/@replicated_to_cell_tags".format(tx2)) == [13]

    yield

    assert get("#{}/@replicated_to_cell_tags".format(tx1)) == [12, 13]
    assert get("#{}/@replicated_to_cell_tags".format(tx2)) == [13]


def check_duplicate_attributes():
    attrs = []
    for i in range(10):
        attrs.append(str(i) * 2000)

    for i in range(10):
        create("map_node", "//tmp/m{}".format(i))
        for j in range(len(attrs)):
            set("//tmp/m{}/@a{}".format(i, j), attrs[j])

    yield

    for i in range(10):
        for j in range(len(attrs)):
            assert get("//tmp/m{}/@a{}".format(i, j)) == attrs[j]


def check_proxy_roles():
    create("http_proxy_role_map", "//sys/http_proxy_roles")
    create("rpc_proxy_role_map", "//sys/rpc_proxy_roles")
    create_proxy_role("h", "http")
    create_proxy_role("r", "rpc")

    yield

    assert get("//sys/http_proxy_roles/h/@proxy_kind") == "http"
    assert get("//sys/rpc_proxy_roles/r/@proxy_kind") == "rpc"


def check_attribute_tombstone_yt_14682():
    create("table", "//tmp/table_with_attr")
    set("//tmp/table_with_attr/@attr", "value")

    tx = start_transaction(timeout=60000)
    remove("//tmp/table_with_attr/@attr", tx=tx)

    yield

    assert get("//tmp/table_with_attr/@attr") == "value"
    assert not exists("//tmp/table_with_attr/@attr", tx=tx)


def check_error_attribute():
    cell_id = sync_create_cells(1)[0]
    get("#{}/@peers/0/address".format(cell_id))
    tx_id = get("#{}/@prerequisite_transaction_id".format(cell_id))
    commit_transaction(tx_id)
    wait(lambda: exists("#{}/@peers/0/last_revocation_reason".format(cell_id)))

    yield

    assert exists("#{}/@peers/0/last_revocation_reason".format(cell_id))


def check_account_subtree_size_recalculation():
    set("//sys/@config/security_manager/max_account_subtree_size", 3)
    create_account("b", empty=True)
    create_account("c", empty=True)
    create_account("ca", "c", empty=True)
    create_account("d", empty=True)
    create_account("da", "d", empty=True)
    create_account("db", "d", empty=True)

    yield

    set("//sys/@config/security_manager/max_account_subtree_size", 3)
    with pytest.raises(YtError):
        move("//sys/account_tree/b", "//sys/account_tree/d/dc")
    move("//sys/account_tree/d/db", "//sys/account_tree/c/cb")
    move("//sys/account_tree/b", "//sys/account_tree/d/db")
    with pytest.raises(YtError):
        move("//sys/account_tree/c/ca", "//sys/account_tree/d/dc")
    set("//sys/@config/security_manager/max_account_subtree_size", 4)
    move("//sys/account_tree/c/ca", "//sys/account_tree/d/dc")
    with pytest.raises(YtError):
        move("//sys/account_tree/c", "//sys/account_tree/d/da/daa")
    set("//sys/@config/security_manager/max_account_subtree_size", 6)
    move("//sys/account_tree/c", "//sys/account_tree/d/da/daa")


def check_scheduler_pool_subtree_size_recalculation():
    set("//sys/@config/scheduler_pool_manager/max_scheduler_pool_subtree_size", 3)
    create_pool_tree("tree1", wait_for_orchid=False)
    create_pool_tree("tree2", wait_for_orchid=False)
    create_pool_tree("tree3", wait_for_orchid=False)
    create_pool("a", pool_tree="tree1", wait_for_orchid=False)
    create_pool("aa", pool_tree="tree1", parent_name="a", wait_for_orchid=False)
    create_pool("aaa", pool_tree="tree1", parent_name="aa", wait_for_orchid=False)
    create_pool("b", pool_tree="tree1", wait_for_orchid=False)
    create_pool("ba", pool_tree="tree1", parent_name="b", wait_for_orchid=False)
    create_pool("c", pool_tree="tree1", wait_for_orchid=False)
    create_pool("a", pool_tree="tree2", wait_for_orchid=False)
    create_pool("aa", pool_tree="tree2", parent_name="a", wait_for_orchid=False)

    yield

    set("//sys/@config/scheduler_pool_manager/max_scheduler_pool_subtree_size", 3)
    with pytest.raises(YtError):
        create_pool("aab", pool_tree="tree1", parent_name="aa", wait_for_orchid=False)
    create_pool("bb", pool_tree="tree1", parent_name="b", wait_for_orchid=False)
    with pytest.raises(YtError):
        create_pool("bba", pool_tree="tree1", parent_name="bb", wait_for_orchid=False)
    create_pool("ca", pool_tree="tree1", parent_name="c", wait_for_orchid=False)
    create_pool("cb", pool_tree="tree1", parent_name="c", wait_for_orchid=False)
    with pytest.raises(YtError):
        create_pool("cd", pool_tree="tree1", parent_name="c", wait_for_orchid=False)
    create_pool("aaa", pool_tree="tree2", parent_name="aa", wait_for_orchid=False)
    with pytest.raises(YtError):
        create_pool("aaaa", pool_tree="tree2", parent_name="aaa", wait_for_orchid=False)
    create_pool("a", pool_tree="tree3", wait_for_orchid=False)


def check_account_resource_usage_lease():
    create_account("a42")
    tx = start_transaction(timeout=60000)
    lease_id = create_account_resource_usage_lease(account="a42", transaction_id=tx)
    assert exists("#" + lease_id)

    set("#{}/@resource_usage".format(lease_id), {"disk_space_per_medium": {"default": 1000}})
    assert get_account_disk_space("a42") == 1000

    yield

    assert exists("#" + lease_id)
    assert get_account_disk_space("a42") == 1000
    abort_transaction(tx)

    assert not exists("#" + lease_id)
    assert get_account_disk_space("a42") == 0


def check_exported_chunks():
    create("table", "//tmp/t1", attributes={"external_cell_tag": 12})
    write_table("//tmp/t1", {"a": "b"})
    create("table", "//tmp/t2", attributes={"external_cell_tag": 13})
    write_table("//tmp/t2", {"c": "d"})

    create("table", "//tmp/t")
    concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/t")

    yield

    assert read_table("//tmp/t") == [{"a": "b"}, {"c": "d"}]


def check_chunk_locations():
    node_to_location_uuids = {}

    nodes = ls("//sys/cluster_nodes", attributes=["chunk_locations"])
    for node in nodes:
        node_address = str(node)
        location_uuids = node.attributes["chunk_locations"].keys()
        node_to_location_uuids[node_address] = location_uuids

    create_domestic_medium("nvme_override")
    overridden_node_address = str(nodes[0])
    overridden_location_uuids = node_to_location_uuids[overridden_node_address]
    for location_uuid in overridden_location_uuids:
        set("//sys/chunk_locations/{}/@medium_override".format(location_uuid), "nvme_override")

    def check_everything():
        for node_address, location_uuids in node_to_location_uuids.items():
            assert exists("//sys/cluster_nodes/{}".format(node_address))
            found_location_uuids = get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys()
            assert_items_equal(location_uuids, found_location_uuids)

        assert exists("//sys/media/nvme_override")

        for location_uuid in overridden_location_uuids:
            assert get("//sys/chunk_locations/{}/@medium_override".format(location_uuid)) == "nvme_override"

    check_everything()

    yield

    check_everything()


def check_tablet_balancer_config():
    set("//sys/tablet_cell_bundles/default/@tablet_balancer_config/animal", "panda")
    assert get("//sys/tablet_cell_bundles/default/@tablet_balancer_config/animal") == "panda"

    create_dynamic_table(
        "//tmp/tablet_balancer_test_table",
        schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ],
    )

    set("//tmp/tablet_balancer_test_table/@tablet_balancer_config/animal", "zebra")
    assert get("//tmp/tablet_balancer_test_table/@tablet_balancer_config/animal") == "zebra"

    yield

    assert get("//sys/tablet_cell_bundles/default/@tablet_balancer_config/animal") == "panda"
    assert get("//tmp/tablet_balancer_test_table/@tablet_balancer_config/animal") == "zebra"


def check_performance_counters_refactoring():
    path = "//tmp/performance_counters_test_table"
    create_dynamic_table(
        path,
        schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ],
    )
    sync_mount_table(path)

    insert_rows(path, [{"key": 0, "value": "hello"}])

    perf_counter = path + "/@tablets/0/performance_counters"
    wait(lambda: get(perf_counter + "/dynamic_row_write_data_weight_count") > 0)

    select_rows(f"* from [{path}]")
    wait(lambda: get(perf_counter + "/dynamic_row_read_data_weight_count") > 0)
    assert get(perf_counter + "/dynamic_row_read_count") == 1

    yield

    assert get(perf_counter + "/dynamic_row_read_count") == 1

    select_rows(f"* from [{path}]")
    wait(lambda: get(perf_counter + "/dynamic_row_read_count") > 1)


def check_queue_agent_objects():
    revisions_path = "//sys/@queue_agent_object_revisions"

    if not exists("//sys/chaos_cell_bundles/chaos_bundle"):
        create_chaos_cell_bundle(name="chaos_bundle", peer_cluster_names=["primary"])
    cell_id = generate_chaos_cell_id()
    sync_create_chaos_cell("chaos_bundle", cell_id, ["primary"])
    set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

    queue_schema = [
        {"name": "$timestamp", "type": "uint64"},
        {"name": "$cumulative_data_weight", "type": "int64"},
        {"name": "data", "type": "string"},
    ]

    consumer_schema = [
        {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
        {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
        {"name": "partition_index", "type": "uint64", "sort_order": "ascending", "required": True},
        {"name": "offset", "type": "uint64", "required": True},
        {"name": "meta", "type": "any", "required": False},
    ]

    create("table", "//tmp/q", attributes={"dynamic": True, "schema": queue_schema})
    create("replicated_table", "//tmp/rep_q", attributes={"dynamic": True, "schema": queue_schema})
    create("chaos_replicated_table",
           "//tmp/chaos_rep_q",
           attributes={"chaos_cell_bundle": "chaos_bundle",
                       "schema": queue_schema})

    create("table",
           "//tmp/c",
           attributes={"dynamic": True,
                       "schema": consumer_schema,
                       "treat_as_queue_consumer": True})
    create("replicated_table",
           "//tmp/rep_c",
           attributes={"dynamic": True,
                       "schema": consumer_schema,
                       "treat_as_queue_consumer": True})
    create("chaos_replicated_table",
           "//tmp/chaos_rep_c",
           attributes={"chaos_cell_bundle": "chaos_bundle",
                       "schema": consumer_schema,
                       "treat_as_queue_consumer": True})
    queue_agent_revision = get(revisions_path)
    assert builtins.set(queue_agent_revision["queues"].keys()) == {"//tmp/q", "//tmp/rep_q", "//tmp/chaos_rep_q"}
    assert builtins.set(queue_agent_revision["consumers"].keys()) == {"//tmp/c", "//tmp/rep_c", "//tmp/chaos_rep_c"}
    yield

    queue_agent_revision = get(revisions_path)
    assert builtins.set(queue_agent_revision["queues"].keys()) == {"//tmp/q", "//tmp/rep_q", "//tmp/chaos_rep_q"}
    assert builtins.set(queue_agent_revision["consumers"].keys()) == {"//tmp/c", "//tmp/rep_c", "//tmp/chaos_rep_c"}


def get_monitoring(monitoring_prefix, master):
    return get(
        "{}/{}/orchid/monitoring/hydra".format(monitoring_prefix, master),
        default=None,
        suppress_transaction_coordinator_sync=True,
        suppress_upstream_sync=True)


def is_leader_in_read_only(monitoring_prefix, master_list):
    for master in master_list:
        monitoring = get_monitoring(monitoring_prefix, master)
        if monitoring is not None and monitoring["state"] == "leading" and monitoring["read_only"]:
            return True
    return False


def all_peers_in_read_only(monitoring_prefix, master_list):
    for master in master_list:
        monitoring = get_monitoring(monitoring_prefix, master)
        if monitoring is None or not monitoring["read_only"]:
            return False
    return True


def no_peers_in_read_only(monitoring_prefix, master_list):
    for master in master_list:
        monitoring = get_monitoring(monitoring_prefix, master)
        if monitoring is None or monitoring["read_only"]:
            return False
    return True


MASTER_SNAPSHOT_CHECKER_LIST = [
    check_simple_node,
    check_schema,
    check_forked_schema,
    check_dynamic_tables,
    check_chaos_replicated_table,
    check_security_tags,
    check_master_memory,
    check_hierarchical_accounts,
    check_tablet_balancer_config,
    check_duplicate_attributes,
    check_proxy_roles,
    check_attribute_tombstone_yt_14682,
    check_error_attribute,
    check_account_subtree_size_recalculation,
    check_scheduler_pool_subtree_size_recalculation,
    check_exported_chunks,
    check_chunk_locations,
    check_performance_counters_refactoring,
    check_account_resource_usage_lease,
    check_queue_agent_objects,
    check_removed_account,  # keep this item last as it's sensitive to timings
]

MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST = deepcopy(MASTER_SNAPSHOT_CHECKER_LIST)

# Master memory is a volatile currency, so we do not run compat tests for it.
MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST.remove(check_master_memory)

# Chunk locations API has been changed, no way compat tests could work.
MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST.remove(check_chunk_locations)

# Test for fix of chaos replicated consumers for update from 23.1 to version 23.2, do not run it in compat from 23.2 to trunk.
MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST.remove(check_queue_agent_objects)


class TestMasterSnapshots(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_MASTER_CACHES = 1
    NUM_CHAOS_NODES = 1
    USE_DYNAMIC_TABLES = True

    def _build_master_snapshots(self):
        build_master_snapshots()

    @authors("ermolovd")
    def test(self):
        if self.is_multicell():
            MASTER_SNAPSHOT_CHECKER_LIST.append(check_transactions)

        checker_state_list = [iter(c()) for c in MASTER_SNAPSHOT_CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        self._build_master_snapshots()

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)

    @authors("gritukan")
    def test_master_snapshots_free_space_profiling(self):
        master_address = ls("//sys/primary_masters")[0]

        def check_sensor(path):
            sensors = profiler_factory().at_primary_master(master_address).list()
            return path in sensors
        wait(lambda: check_sensor(b"yt/snapshots/free_space"))
        wait(lambda: check_sensor(b"yt/snapshots/available_space"))
        wait(lambda: check_sensor(b"yt/changelogs/free_space"))
        wait(lambda: check_sensor(b"yt/changelogs/available_space"))

##################################################################


class TestMasterSnapshotsMulticell(TestMasterSnapshots):
    NUM_SECONDARY_MASTER_CELLS = 3


##################################################################


class TestMasterChangelogsMulticell(TestMasterSnapshotsMulticell):
    def _build_master_snapshots(self):
        pass

##################################################################


class TestMastersSnapshotsShardedTx(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 4
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["transaction_coordinator"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["chunk_host"]},
    }

    @authors("aleksandra-zh")
    def test_reads_in_read_only(self):
        tx = start_transaction(coordinator_master_cell_tag=11)
        create("map_node", "//tmp/m", tx=tx)

        build_snapshot(cell_id=self.Env.configs["master"][0]["primary_master"]["cell_id"], set_read_only=True)

        primary = ls("//sys/primary_masters",
                     suppress_transaction_coordinator_sync=True,
                     suppress_upstream_sync=True)
        wait(lambda: is_leader_in_read_only("//sys/primary_masters", primary))

        abort_transaction(tx)

        for secondary_master in self.Env.configs["master"][0]["secondary_masters"]:
            build_snapshot(cell_id=secondary_master["cell_id"], set_read_only=True)

        secondary_masters = get("//sys/secondary_masters",
                                suppress_transaction_coordinator_sync=True,
                                suppress_upstream_sync=True)
        for cell_tag in secondary_masters:
            addresses = list(secondary_masters[cell_tag].keys())
            wait(lambda: is_leader_in_read_only("//sys/secondary_masters/{}".format(cell_tag), addresses))

        # Must not hang on this.
        get("//sys/primary_masters/{}/orchid/monitoring/hydra".format(primary[0]),
            suppress_transaction_coordinator_sync=True,
            suppress_upstream_sync=True)

    @authors("aleksandra-zh")
    def test_all_peers_in_read_only(self):
        build_master_snapshots(set_read_only=True)

        primary = ls(
            "//sys/primary_masters",
            suppress_transaction_coordinator_sync=True,
            suppress_upstream_sync=True)
        wait(lambda: all_peers_in_read_only("//sys/primary_masters", primary))

        secondary_masters = get(
            "//sys/secondary_masters",
            suppress_transaction_coordinator_sync=True,
            suppress_upstream_sync=True)
        for cell_tag in secondary_masters:
            addresses = list(secondary_masters[cell_tag].keys())
            wait(lambda: all_peers_in_read_only("//sys/secondary_masters/{}".format(cell_tag), addresses))

        # Must not hang on this.
        build_master_snapshots(set_read_only=True)


class TestMastersPersistentReadOnly(YTEnvSetup):
    @authors("danilalexeev")
    def test_read_only_after_recovery(self):
        build_master_snapshots(set_read_only=True)

        primary = ls("//sys/primary_masters",
                     suppress_transaction_coordinator_sync=True,
                     suppress_upstream_sync=True)
        wait(lambda: is_leader_in_read_only("//sys/primary_masters", primary))

        secondary_masters = get(
            "//sys/secondary_masters",
            suppress_transaction_coordinator_sync=True,
            suppress_upstream_sync=True)
        for cell_tag in secondary_masters:
            addresses = list(secondary_masters[cell_tag].keys())
            wait(lambda: is_leader_in_read_only("//sys/secondary_masters/{}".format(cell_tag), addresses))

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        wait(lambda: is_leader_in_read_only("//sys/primary_masters", primary))

        with pytest.raises(YtError):
            create("table", "//tmp/t1")

        master_exit_read_only_sync()

        create("table", "//tmp/t2")

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        wait(lambda: no_peers_in_read_only("//sys/primary_masters", primary))

        for cell_tag in secondary_masters:
            addresses = list(secondary_masters[cell_tag].keys())
            wait(lambda: no_peers_in_read_only("//sys/secondary_masters/{}".format(cell_tag), addresses))


class TestMastersSnapshotsShardedTxCTxS(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 4
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["transaction_coordinator"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["chunk_host"]},
    }

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            }
        }
    }


class TestMastersPersistentReadOnlyMulticell(TestMastersPersistentReadOnly):
    NUM_SECONDARY_MASTER_CELLS = 2
