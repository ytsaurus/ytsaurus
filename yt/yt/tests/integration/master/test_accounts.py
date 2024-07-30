from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, get_active_primary_master_follower_address, is_active_primary_master_follower,
    is_active_primary_master_leader, switch_leader, wait, create, ls, get, set, copy, move, remove, exists,
    create_account, remove_account, transfer_account_resources, create_user, create_domestic_medium,
    write_file, write_table, merge, sync_create_cells, sync_mount_table,  make_ace, ping_transaction,
    sync_unmount_table, sync_reshard_table, get_singular_chunk_id, lock, insert_rows, alter_table,
    multicell_sleep, set_account_disk_space_limit, get_account_disk_space, get_account_committed_disk_space,
    create_dynamic_table, get_chunk_owner_disk_space, cluster_resources_equal, master_memory_sleep,
    assert_true_for_all_cells, wait_true_for_all_cells, gc_collect, get_driver, raises_yt_error,
    get_active_primary_master_leader_address, start_transaction, abort_transaction, commit_transaction,
    get_currently_active_pirmary_master_follower_addresses)

from yt.yson import to_yson_type, YsonEntity
from yt.common import YtError

from yt_helpers import profiler_factory

from flaky import flaky

import pytest
from time import sleep
from operator import itemgetter
from copy import deepcopy
import builtins

##################################################################


def multiply_recursive(dict_or_value, multiplier):
    if not isinstance(dict_or_value, dict):
        return dict_or_value * multiplier
    result = {}
    for key in dict_or_value:
        result[key] = multiply_recursive(dict_or_value[key], multiplier)
    return result


def add_recursive(lhs, rhs):
    assert isinstance(lhs, dict) == isinstance(rhs, dict)
    if not isinstance(lhs, dict):
        return lhs + rhs
    result = {}
    for key in lhs:
        if key in rhs:
            result[key] = add_recursive(lhs[key], rhs[key])
        else:
            result[key] = lhs[key]
    for key in rhs:
        if key not in lhs:
            result[key] = rhs[key]
    return result


def subtract_recursive(lhs, rhs):
    return add_recursive(lhs, multiply_recursive(rhs, -1))


##################################################################


class AccountsTestSuiteBase(YTEnvSetup):
    NUM_TEST_PARTITIONS = 3

    NUM_MASTERS = 1
    NUM_NODES = 3

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    _root_account_name = "root"
    _non_root_builtin_accounts = [
        "sys",
        "tmp",
        "intermediate",
        "chunk_wise_accounting_migration",
        "sequoia",
    ]
    _builtin_accounts = [_root_account_name] + _non_root_builtin_accounts

    def _build_resource_limits(
        self,
        node_count=0,
        chunk_count=0,
        tablet_count=0,
        tablet_static_memory=0,
        disk_space=0,
        master_memory=0,
        chunk_host_master_memory=0,
        master_memory_per_cell={},
        include_disk_space=False,
    ):
        limits = {
            "disk_space_per_medium": {"default": disk_space},
            "chunk_count": chunk_count,
            "node_count": node_count,
            "tablet_count": tablet_count,
            "tablet_static_memory": tablet_static_memory,
            "master_memory": {
                "total": master_memory,
                "chunk_host": chunk_host_master_memory,
                "per_cell": master_memory_per_cell,
            }
        }
        if include_disk_space:
            limits["disk_space"] = disk_space
        return limits

    def _get_disk_space_for_medium(self, disk_space_map, medium_name="default"):
        return disk_space_map.get(medium_name, 0)

    def _get_account_node_count(self, account):
        return get("//sys/accounts/{0}/@resource_usage/node_count".format(account))

    def _get_account_chunk_count(self, account):
        return get("//sys/accounts/{0}/@resource_usage/chunk_count".format(account))

    def _get_account_node_count_limit(self, account):
        return get("//sys/accounts/{0}/@resource_limits/node_count".format(account))

    def _set_account_node_count_limit(self, account, value, **kwargs):
        set("//sys/accounts/{0}/@resource_limits/node_count".format(account), value, **kwargs)

    def _set_account_master_memory(self, account, value, **kwargs):
        set("//sys/accounts/{0}/@resource_limits/master_memory/total".format(account), value, **kwargs)

    def _get_account_chunk_count_limit(self, account):
        return get("//sys/accounts/{0}/@resource_limits/chunk_count".format(account))

    def _set_account_chunk_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/chunk_count".format(account), value)

    def _set_account_zero_limits(self, account):
        set("//sys/accounts/{0}/@resource_limits".format(account), {})

    def _multiply_account_limits(self, account, multiplier):
        old_limits = get("//sys/accounts/{0}/@resource_limits".format(account))
        new_limits = multiply_recursive(old_limits, multiplier)
        set("//sys/accounts/{0}/@resource_limits".format(account), new_limits)

    def _is_account_disk_space_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/disk_space".format(account))

    def _is_account_node_count_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/node_count".format(account))

    def _is_account_chunk_count_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/chunk_count".format(account))

    def _get_tx_disk_space(self, tx, account):
        disk_space = get("#{0}/@resource_usage/{1}/disk_space_per_medium".format(tx, account))
        return self._get_disk_space_for_medium(disk_space, "default")

    def _get_tx_chunk_count(self, tx, account):
        return get("#{0}/@resource_usage/{1}/chunk_count".format(tx, account))

    def _wait_for_tmp_account_usage(self):
        gc_collect()
        multicell_sleep()
        node_count = get("//sys/accounts/tmp/@committed_resource_usage/node_count")
        expected_usage = self._build_resource_limits(node_count=node_count)
        expected_usage["disk_space"] = 0
        wait(
            lambda: cluster_resources_equal(get("//sys/accounts/tmp/@committed_resource_usage"), expected_usage)
            and cluster_resources_equal(get("//sys/accounts/tmp/@resource_usage"), expected_usage)
        )

    # A context manager used for waiting until a chunk owner node which has been created, modified or moved
    # to another account gets accounted for in resource usage of account(s). Might not handle cases of
    # non-monotonous resource usage changes correctly (e.g. overwriting a chunk with a chunk of the same size).
    class WaitForAccountUsage:
        def __init__(self, node, new_account=None, tx="0-0-0-0", append=False):
            self._node = node
            self._new_account = new_account
            self._tx = tx
            self._append = append

        def _is_branched_in_tx(self, node, tx):
            if not exists(node, tx=tx):
                return False
            if tx == "0-0-0-0":
                return True

            id = get("{0}/@id".format(node))
            return id in [
                node_id
                for cell_tag, node_ids in get("#{0}/@branched_node_ids".format(tx)).items()
                for node_id in node_ids
            ]

        def _count_branched_versions(self, node, tx):
            result = 0
            while True:
                if self._is_branched_in_tx(node, tx):
                    result += 1
                if tx == "0-0-0-0":
                    break
                tx = get("#{0}/@parent_id".format(tx))
            return result

        def _get_account_resource_usage(self, account):
            if self._tx == "0-0-0-0":
                return get("//sys/accounts/{0}/@committed_resource_usage".format(account))
            else:
                return get("//sys/accounts/{0}/@resource_usage".format(account))

        def _get_cell_name(self, cell_tag):
            return get("//sys/@config/multicell_manager/cell_descriptors/{}/name".format(cell_tag), default=str(cell_tag))

        def _is_chunk_host(self, cell_tag):
            path = "//sys/@config/multicell_manager/cell_descriptors/{}/roles".format(cell_tag)
            return "chunk_host" in get(path) if exists(path) else True

        def _add_node_resource_usage_to_account(self, account_resource_usage, node_resource_usage, node, subtract=False):
            sign = -1 if subtract else 1
            account_master_memory = account_resource_usage["master_memory"]
            account_resource_usage.pop("master_memory")
            node_master_memory = node_resource_usage.get("master_memory", 0)
            node_resource_usage.pop("master_memory", None)
            if subtract:
                account_resource_usage = subtract_recursive(account_resource_usage, node_resource_usage)
            else:
                account_resource_usage = add_recursive(account_resource_usage, node_resource_usage)
            node_resource_usage["master_memory"] = node_master_memory
            account_master_memory["total"] += node_master_memory * sign
            cell_tags = {
                get("{}/@native_cell_tag".format(node), default=None),
                get("{}/@external_cell_tag".format(node), default=None)
            }
            for cell_tag in cell_tags:
                if cell_tag is None:
                    continue
                account_master_memory["per_cell"][self._get_cell_name(cell_tag)] += node_master_memory * sign
                if (self._is_chunk_host(cell_tag)):
                    account_master_memory["chunk_host"] += node_master_memory * sign
            account_resource_usage["master_memory"] = account_master_memory
            return account_resource_usage

        def __enter__(self):
            if exists(self._node, tx=self._tx):
                self._old_account = get("{0}/@account".format(self._node), tx=self._tx)
                if self._new_account is None:
                    self._new_account = self._old_account
            else:
                self._old_account = self._new_account
                assert self._old_account is not None

            self._expected_old_account_usage = self._get_account_resource_usage(self._old_account)
            if self._new_account != self._old_account:
                self._expected_new_account_usage = self._get_account_resource_usage(self._new_account)

            self._expected_old_account_usage["node_count"] -= self._count_branched_versions(self._node, self._tx)
            was_branched = self._is_branched_in_tx(self._node, self._tx)
            if was_branched or self._append:
                old_resource_usage = get("{0}/@resource_usage".format(self._node), tx=self._tx)
                old_resource_usage["node_count"] = 0
                self._expected_old_account_usage = self._add_node_resource_usage_to_account(
                    self._expected_old_account_usage,
                    old_resource_usage,
                    self._node,
                    True
                )

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_tb is not None:
                return

            if exists(self._node, tx=self._tx):
                new_account = get("{0}/@account".format(self._node), tx=self._tx)
            else:
                new_account = self._old_account
            assert new_account == self._new_account

            is_branched = self._is_branched_in_tx(self._node, tx=self._tx)
            if is_branched or self._append:
                new_resource_usage = get("{0}/@resource_usage".format(self._node), tx=self._tx)
                new_resource_usage["node_count"] = 0
            else:
                new_resource_usage = {}

            branched_versions = self._count_branched_versions(self._node, self._tx)
            if self._new_account == self._old_account:
                self._expected_old_account_usage["node_count"] += branched_versions
            else:
                self._expected_new_account_usage["node_count"] += branched_versions

            if self._new_account == self._old_account:
                self._expected_old_account_usage = self._add_node_resource_usage_to_account(
                    self._expected_old_account_usage,
                    new_resource_usage,
                    self._node)
                wait(
                    lambda: cluster_resources_equal(
                        self._get_account_resource_usage(self._old_account),
                        self._expected_old_account_usage,
                    ),
                    iter=20,
                )
            else:
                self._expected_new_account_usage = self._add_node_resource_usage_to_account(
                    self._expected_new_account_usage,
                    new_resource_usage,
                    self._node)
                wait(
                    lambda: cluster_resources_equal(
                        self._get_account_resource_usage(self._old_account),
                        self._expected_old_account_usage,
                    )
                    and cluster_resources_equal(
                        self._get_account_resource_usage(self._new_account),
                        self._expected_new_account_usage,
                    ),
                    iter=20,
                )

    def setup_method(self, method):
        super(AccountsTestSuiteBase, self).setup_method(method)
        set(
            "//sys/@config/security_manager/enable_master_memory_usage_account_overcommit_validation",
            False,
        )

    def teardown_method(self, method):
        for cell_index in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            driver = get_driver(cell_index)
            accounts = ls("//sys/accounts", driver=driver)
            for account in accounts:
                if account.startswith("#"):
                    assert not exists("//sys/accounts/{0}".format(account), driver=driver)
        super(AccountsTestSuiteBase, self).teardown_method(method)


##################################################################


class TestAccounts(AccountsTestSuiteBase):
    @authors("babenko", "ignat")
    def test_init(self):
        assert sorted(ls("//sys/accounts")) == sorted(self._builtin_accounts)
        assert get("//@account") == "sys"
        assert get("//sys/@account") == "sys"
        assert get("//tmp/@account") == "tmp"
        assert get("//sys/account_tree/@ref_counter") == get("//sys/accounts/@count")

    @authors("ignat")
    def test_account_create1(self):
        create_account("max")
        assert sorted(ls("//sys/accounts")) == sorted(self._builtin_accounts + ["max"])
        assert get_account_disk_space("max") == 0
        assert self._get_account_node_count("max") == 0
        assert self._get_account_chunk_count("max") == 0

    @authors("babenko", "ignat")
    def test_account_create2(self):
        with pytest.raises(YtError):
            create_account("sys")
        with pytest.raises(YtError):
            create_account("tmp")

    @authors("babenko", "ignat")
    def test_account_remove_builtin(self):
        with pytest.raises(YtError):
            remove_account("sys")
        with pytest.raises(YtError):
            remove_account("tmp")

    @authors("babenko", "ignat")
    def test_account_create3(self):
        create_account("max")
        with pytest.raises(YtError):
            create_account("max")

    @authors("babenko", "ignat")
    def test_empty_name_fail(self):
        with pytest.raises(YtError):
            create_account("")

    @authors("kiselyovp")
    def test_odd_names(self):
        for name in [
            "max/",
            "tmp/nested",
            "na\\me",
            "na@me",
            "na&me",
            "na*me",
            "na#me",
            "na[me",
            "na]me",
            "na$me",
            "a" * 101,
        ]:
            with pytest.raises(YtError):
                create_account(name)
        create_account("1337-th15-15_F1N3")
        assert exists("//sys/accounts/1337-th15-15_F1N3")

    @authors("babenko", "ignat")
    def test_account_attr1(self):
        set("//tmp/a", {})
        assert get("//tmp/a/@account") == "tmp"

    @authors("babenko")
    def test_account_attr2(self):
        # should not crash
        get("//sys/accounts/tmp/@")

    @authors("ignat")
    def test_account_attr3(self):
        set("//tmp/a", {"x": 1, "y": 2})
        assert get("//tmp/a/@account") == "tmp"
        assert get("//tmp/a/x/@account") == "tmp"
        assert get("//tmp/a/y/@account") == "tmp"
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@account") == "tmp"
        assert get("//tmp/b/x/@account") == "tmp"
        assert get("//tmp/b/y/@account") == "tmp"

    @authors("ignat", "shakurov", "kiselyovp")
    def test_account_attr4(self):
        create_account("max")
        assert self._get_account_node_count("max") == 0
        assert self._get_account_chunk_count("max") == 0
        assert get_account_disk_space("max") == 0

        self._wait_for_tmp_account_usage()
        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")
            write_table("//tmp/t", {"a": "b"})

        table_disk_space = get_chunk_owner_disk_space("//tmp/t")
        tmp_node_count = self._get_account_node_count("tmp")
        tmp_chunk_count = self._get_account_chunk_count("tmp")
        tmp_disk_space = get_account_disk_space("tmp")

        with self.WaitForAccountUsage("//tmp/t", new_account="max"):
            set("//tmp/t/@account", "max")

        assert self._get_account_node_count("tmp") == tmp_node_count - 1
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count - 1
        assert get_account_disk_space("tmp") == tmp_disk_space - table_disk_space

        assert self._get_account_node_count("max") == 1
        assert self._get_account_chunk_count("max") == 1
        assert get_account_disk_space("max") == table_disk_space

    @authors("babenko", "ignat")
    def test_account_attr5(self):
        create_account("max")
        set("//tmp/a", {})
        tx = start_transaction()
        with pytest.raises(YtError):
            set("//tmp/a/@account", "max", tx=tx)

    @authors("babenko", "ignat")
    def test_remove_immediately(self):
        create_account("max")
        remove_account("max")

    @authors("babenko", "ignat", "kiselyovp")
    def test_remove_delayed(self):
        create_account("max")
        set("//tmp/a", {})
        set("//tmp/a/@account", "max")
        remove_account("max", sync=False)

        assert get("//sys/accounts/max/@life_stage") == "removal_started"
        with pytest.raises(YtError):
            create("map_node", "//tmp/b", attributes={"account": "max"})
        create("map_node", "//tmp/b")
        with pytest.raises(YtError):
            set("//tmp/b/@account", "max")
        remove_account("max", sync=False)
        assert get("//sys/accounts/max/@life_stage") == "removal_started"

        remove("//tmp/a")
        wait(lambda: not exists("//sys/accounts/max"))

    @authors("babenko")
    def test_file1(self):
        wait(lambda: get_account_disk_space("tmp") == 0)

        create("file", "//tmp/f1")
        write_file("//tmp/f1", b"some_data")

        wait(lambda: get_account_disk_space("tmp") > 0)
        space = get_account_disk_space("tmp")
        assert space > 0

        create("file", "//tmp/f2")
        write_file("//tmp/f2", b"some_data")

        wait(lambda: get_account_disk_space("tmp") == 2 * space)

        remove("//tmp/f1")

        wait(lambda: get_account_disk_space("tmp") == space)

        remove("//tmp/f2")

        wait(lambda: get_account_disk_space("tmp") == 0)

    @authors("babenko", "kiselyovp")
    def test_file2(self):
        self._wait_for_tmp_account_usage()

        with self.WaitForAccountUsage("//tmp/f", new_account="tmp"):
            create("file", "//tmp/f")
            write_file("//tmp/f", b"some_data")

        space = get_account_disk_space("tmp")

        create_account("max")
        with self.WaitForAccountUsage("//tmp/f", new_account="max"):
            set("//tmp/f/@account", "max")

        assert get_account_disk_space("tmp") == 0
        assert get_account_disk_space("max") == space

        with self.WaitForAccountUsage("//tmp/f"):
            remove("//tmp/f")
            gc_collect()

        assert get_account_disk_space("max") == 0

    @authors("babenko", "ignat", "shakurov", "kiselyovp")
    def test_file3(self):
        create_account("max")

        assert get_account_disk_space("max") == 0

        create("file", "//tmp/f", attributes={"account": "max"})
        write_file("//tmp/f", b"some_data")

        wait(lambda: get_account_disk_space("max") > 0)

        remove("//tmp/f")

        gc_collect()
        wait(lambda: get_account_disk_space("max") == 0)

    @authors("babenko", "kiselyovp")
    def test_file4(self):
        create_account("max")

        with self.WaitForAccountUsage("//tmp/f", new_account="max"):
            create("file", "//tmp/f", attributes={"account": "max"})
            write_file("//tmp/f", b"some_data")

        space = get_account_disk_space("max")
        assert space > 0

        rf = get("//tmp/f/@replication_factor")
        set("//tmp/f/@replication_factor", rf * 2)

        wait(lambda: get_account_disk_space("max") == space * 2)

    @authors("shakurov", "kiselyovp")
    def test_table1(self):
        self._wait_for_tmp_account_usage()
        node_count = self._get_account_node_count("tmp")

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")
            write_table("//tmp/t", {"a": "b"})

        assert get_account_disk_space("tmp") > 0
        assert self._get_account_node_count("tmp") == node_count + 1
        assert self._get_account_chunk_count("tmp") == 1

    @authors("babenko", "kiselyovp")
    def test_table2(self):
        self._wait_for_tmp_account_usage()

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")

        tx = start_transaction(timeout=60000)
        for i in range(0, 5):
            with self.WaitForAccountUsage("//tmp/t", tx=tx, append=True):
                write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx)

            ping_transaction(tx)

            account_space = get_account_disk_space("tmp")
            tx_space = self._get_tx_disk_space(tx, "tmp")

            assert get_account_committed_disk_space("tmp") == 0
            assert account_space > 0
            assert account_space == tx_space
            assert get_chunk_owner_disk_space("//tmp/t") == 0
            assert get_chunk_owner_disk_space("//tmp/t", tx=tx) == tx_space
            last_space = tx_space

        commit_transaction(tx)

        assert get_chunk_owner_disk_space("//tmp/t") == last_space
        wait(lambda: get_account_committed_disk_space("tmp") == last_space)

    @authors("babenko", "kiselyovp")
    def test_table3(self):
        self._wait_for_tmp_account_usage()

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")
            write_table("//tmp/t", {"a": "b"})

        space1 = get_account_disk_space("tmp")
        assert space1 > 0

        tx = start_transaction(timeout=60000)
        with self.WaitForAccountUsage("//tmp/t", tx=tx):
            write_table("//tmp/t", {"xxxx": "yyyy"}, tx=tx)

        space2 = self._get_tx_disk_space(tx, "tmp")
        assert space1 != space2
        assert get_account_disk_space("tmp") == space1 + space2
        assert get_chunk_owner_disk_space("//tmp/t") == space1
        assert get_chunk_owner_disk_space("//tmp/t", tx=tx) == space2

        commit_transaction(tx)

        assert get_chunk_owner_disk_space("//tmp/t") == space2
        wait(lambda: get_account_disk_space("tmp") == space2)

    @authors("babenko", "kiselyovp")
    def test_table4(self):
        wait(lambda: get_account_disk_space("tmp") == 0)

        tx = start_transaction(timeout=60000)
        create("table", "//tmp/t", tx=tx)
        write_table("//tmp/t", {"a": "b"}, tx=tx)

        wait(lambda: get_account_disk_space("tmp") > 0)

        abort_transaction(tx)

        wait(lambda: get_account_disk_space("tmp") == 0)

    @authors("ignat", "kiselyovp")
    def test_table5(self):
        self._wait_for_tmp_account_usage()
        tmp_node_count = self._get_account_node_count("tmp")
        tmp_chunk_count = self._get_account_chunk_count("tmp")

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")
            write_table("//tmp/t", {"a": "b"})

        assert self._get_account_node_count("tmp") == tmp_node_count + 1
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count + 1
        space = get_account_disk_space("tmp")
        assert space > 0

        create_account("max")

        with self.WaitForAccountUsage("//tmp/t", new_account="max"):
            set("//tmp/t/@account", "max")

        assert self._get_account_node_count("tmp") == tmp_node_count
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count
        assert self._get_account_node_count("max") == 1
        assert self._get_account_chunk_count("max") == 1
        assert get_account_disk_space("tmp") == 0
        assert get_account_disk_space("max") == space

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            set("//tmp/t/@account", "tmp")

        assert self._get_account_node_count("tmp") == tmp_node_count + 1
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count + 1
        assert self._get_account_node_count("max") == 0
        assert self._get_account_chunk_count("max") == 0
        assert get_account_disk_space("tmp") == space
        assert get_account_disk_space("max") == 0

    @authors("sandello", "kiselyovp")
    def test_table6(self):
        self._wait_for_tmp_account_usage()

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")

        tx = start_transaction(timeout=60000)
        with self.WaitForAccountUsage("//tmp/t", tx=tx):
            write_table("//tmp/t", {"a": "b"}, tx=tx)

        space = get_chunk_owner_disk_space("//tmp/t", tx=tx)
        assert space > 0
        assert get_account_disk_space("tmp") == space

        tx2 = start_transaction(tx=tx, timeout=60000)

        assert get_chunk_owner_disk_space("//tmp/t", tx=tx2) == space

        with self.WaitForAccountUsage("//tmp/t", tx=tx2, append=True):
            write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx2)

        assert get_chunk_owner_disk_space("//tmp/t", tx=tx2) == space * 2
        assert get_account_disk_space("tmp") == space * 2

        commit_transaction(tx2)

        assert get_chunk_owner_disk_space("//tmp/t", tx=tx) == space * 2
        wait(lambda: get_account_disk_space("tmp") == space * 2)

        commit_transaction(tx)

        assert get_chunk_owner_disk_space("//tmp/t") == space * 2
        wait(lambda: get_account_disk_space("tmp") == space * 2)

    @authors("ignat")
    def test_node_count_limits1(self):
        create_account("max")
        assert not self._is_account_node_count_limit_violated("max")
        self._set_account_node_count_limit("max", 1000)
        self._set_account_node_count_limit("max", 2000)
        self._set_account_node_count_limit("max", 0)
        assert not self._is_account_node_count_limit_violated("max")
        with pytest.raises(YtError):
            self._set_account_node_count_limit("max", -1)

    @authors("babenko", "ignat", "kiselyovp")
    def test_node_count_limits2(self):
        create_account("max")
        assert self._get_account_node_count("max") == 0

        create("table", "//tmp/t")
        set("//tmp/t/@account", "max")

        wait(lambda: self._get_account_node_count("max") == 1)

    @authors("babenko", "kiselyovp")
    def test_node_count_limits3(self):
        create_account("max")
        create("table", "//tmp/t")

        self._set_account_node_count_limit("max", 0)

        with pytest.raises(YtError):
            set("//tmp/t/@account", "max")

    @authors("babenko")
    def test_node_count_limits4(self):
        create_account("max")
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        self._set_account_node_count_limit("max", 0)
        with pytest.raises(YtError):
            set("//tmp/t/@account", "max")

    @authors("shakurov")
    def test_node_count_limits5(self):
        create_account("max")
        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "max")
        create("table", "//tmp/a/t1")
        write_table("//tmp/a/t1", {"a": "b"})

        multicell_sleep()

        node_count = self._get_account_node_count("max")
        self._set_account_node_count_limit("max", node_count)

        multicell_sleep()

        # Shouldn't work 'cause node count usage is checked synchronously.
        with pytest.raises(YtError):
            copy("//tmp/a/t1", "//tmp/a/t2")

    @authors("kiselyovp")
    def test_node_count_limits6(self):
        create_account("max", attributes={"resource_limits": {"node_count": 1}})
        create("map_node", "//tmp/node", attributes={"account": "max"})
        with pytest.raises(YtError):
            create("map_node", "//tmp/fail", attributes={"account": "max"})
        remove("//tmp/node")
        wait(lambda: self._get_account_node_count("max") == 0)
        create("map_node", "//tmp/noderino", attributes={"account": "max"})

    @authors("ignat")
    def test_chunk_count_limits1(self):
        create_account("max")
        assert not self._is_account_chunk_count_limit_violated("max")
        self._set_account_chunk_count_limit("max", 1000)
        self._set_account_chunk_count_limit("max", 2000)
        self._set_account_chunk_count_limit("max", 0)
        assert not self._is_account_chunk_count_limit_violated("max")
        with pytest.raises(YtError):
            wait(lambda: self._set_account_chunk_count_limit("max", -1))

    @authors("babenko", "ignat", "kiselyovp")
    def test_chunk_count_limits2(self):
        create_account("max")
        assert self._get_account_chunk_count("max") == 0

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        set("//tmp/t/@account", "max")

        wait(lambda: self._get_account_chunk_count("max") == 1)

    @authors("shakurov", "kiselyovp")
    def test_chunk_count_limits3(self):
        create_account("max")
        create("map_node", "//tmp/a")
        with self.WaitForAccountUsage("//tmp/a", new_account="max"):
            set("//tmp/a/@account", "max")
        with self.WaitForAccountUsage("//tmp/a/t1", new_account="max"):
            create("table", "//tmp/a/t1")
            write_table("//tmp/a/t1", {"a": "b"})

        self._set_account_chunk_count_limit("max", 1)

        copy("//tmp/a/t1", "//tmp/a/t2")

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        assert self._get_account_chunk_count("max") == 1

        copy("//tmp/t", "//tmp/a/t3", pessimistic_quota_check=False)

        # After a requisition update, max's chunk count usage should've increased.
        wait(lambda: self._get_account_chunk_count("max") == 2)
        create("table", "//tmp/a/t4")
        with pytest.raises(YtError):
            write_table("//tmp/a/t4", {"a": "b"})

    @authors("ignat")
    def test_disk_space_limits1(self):
        create_account("max")
        assert not self._is_account_disk_space_limit_violated("max")
        set_account_disk_space_limit("max", 1000)
        set_account_disk_space_limit("max", 2000)
        set_account_disk_space_limit("max", 0)
        assert not self._is_account_disk_space_limit_violated("max")
        with pytest.raises(YtError):
            wait(lambda: set_account_disk_space_limit("max", -1))

    @authors("ignat", "kiselyovp")
    def test_disk_space_limits2(self):
        create_account("max")
        set_account_disk_space_limit("max", 1000000)

        with self.WaitForAccountUsage("//tmp/t", new_account="max"):
            create("table", "//tmp/t")
            set("//tmp/t/@account", "max")
            write_table("//tmp/t", {"a": "b"})

        assert get_account_disk_space("max") > 0
        assert not self._is_account_disk_space_limit_violated("max")

        set_account_disk_space_limit("max", 0)

        assert self._is_account_disk_space_limit_violated("max")
        with pytest.raises(YtError):
            write_table("//tmp/t", {"a": "b"})
        # Wait for upload tx to abort
        wait(lambda: get("//tmp/t/@locks") == [])

        set_account_disk_space_limit("max", get_account_disk_space("max") + 1)
        assert not self._is_account_disk_space_limit_violated("max")

        with self.WaitForAccountUsage("//tmp/t", append=True):
            write_table("<append=true>//tmp/t", {"a": "b"})

        assert self._is_account_disk_space_limit_violated("max")

    @authors("ignat", "kiselyovp")
    def test_disk_space_limits3(self):
        create_account("max")
        set_account_disk_space_limit("max", 1000000)

        with self.WaitForAccountUsage("//tmp/f1", new_account="max"):
            create("file", "//tmp/f1", attributes={"account": "max"})
            write_file("//tmp/f1", b"some_data")

        assert get_account_disk_space("max") > 0
        assert not self._is_account_disk_space_limit_violated("max")

        set_account_disk_space_limit("max", 0)
        assert self._is_account_disk_space_limit_violated("max")

        create("file", "//tmp/f2", attributes={"account": "max"})
        with pytest.raises(YtError):
            write_file("//tmp/f2", b"some_data")

        set_account_disk_space_limit("max", get_account_disk_space("max") + 1)
        assert not self._is_account_disk_space_limit_violated("max")

        create("file", "//tmp/f3", attributes={"account": "max"})
        write_file("//tmp/f3", b"some_data")

        wait(lambda: self._is_account_disk_space_limit_violated("max"))

    @authors("shakurov", "kiselyovp")
    def test_disk_space_limits4(self):
        create("map_node", "//tmp/a")
        create("file", "//tmp/a/f1")
        write_file("//tmp/a/f1", b"some_data")
        create("file", "//tmp/a/f2")
        write_file("//tmp/a/f2", b"some_data")

        disk_space = get_chunk_owner_disk_space("//tmp/a/f1")
        disk_space_2 = get_chunk_owner_disk_space("//tmp/a/f2")
        assert disk_space == disk_space_2

        create_account("max")
        create("map_node", "//tmp/b")
        set("//tmp/b/@account", "max")

        set_account_disk_space_limit("max", disk_space * 2)
        copy("//tmp/a", "//tmp/b/a")

        wait(lambda: get_account_disk_space("max") == disk_space * 2)
        assert exists("//tmp/b/a")

        multicell_sleep()

        def write_multiple_chunks_to_file():
            for i in range(20):
                write_file("//tmp/b/a/f3", "some_data {0}".format(i).encode("utf-8"))

        create("file", "//tmp/b/a/f3")
        # Writing new data should fail...
        with pytest.raises(YtError):
            wait(write_multiple_chunks_to_file)

        # Wait for upload tx to abort
        wait(lambda: get("//tmp/b/a/f3/@locks") == [])
        remove("//tmp/b/a/f3")
        # ...but copying existing data should be ok...
        copy("//tmp/b/a/f2", "//tmp/b/a/f3")

        # ...and shouldn't increase disk space usage.
        wait(lambda: get_account_disk_space("max") == disk_space * 2)

        remove("//tmp/b/a")

        wait(lambda: get_account_disk_space("max") == 0 and self._get_account_node_count("max") == 1)

        assert not exists("//tmp/b/a")

    @authors("shakurov", "kiselyovp")
    def test_disk_space_limits5(self):
        create_account("max")
        with self.WaitForAccountUsage("//tmp/a", new_account="max"):
            create("map_node", "//tmp/a")
            set("//tmp/a/@account", "max")
        with self.WaitForAccountUsage("//tmp/a/t1", new_account="max"):
            create("table", "//tmp/a/t1")
            write_table("//tmp/a/t1", {"a": "b"})

        disk_space = get_chunk_owner_disk_space("//tmp/a/t1")
        set_account_disk_space_limit("max", disk_space)

        copy("//tmp/a/t1", "//tmp/a/t2")

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        assert get_account_disk_space("max") == disk_space

        copy("//tmp/t", "//tmp/a/t3", pessimistic_quota_check=False)

        # After a requisition update, max's disk space usage should've increased.
        wait(lambda: get_account_disk_space("max") == 2 * disk_space)
        create("table", "//tmp/a/t4")
        with pytest.raises(YtError):
            write_table("//tmp/a/t4", {"a": "b"})

    @authors("babenko", "kiselyovp")
    def test_committed_usage(self):
        self._wait_for_tmp_account_usage()
        assert get_account_committed_disk_space("tmp") == 0

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")
            write_table("//tmp/t", {"a": "b"})

        space = get_chunk_owner_disk_space("//tmp/t")
        assert space > 0
        assert get_account_committed_disk_space("tmp") == space

        tx = start_transaction(timeout=60000)
        with self.WaitForAccountUsage("//tmp/t", append=True, tx=tx):
            write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx)

        assert get_account_committed_disk_space("tmp") == space

        commit_transaction(tx)

        wait(lambda: get_account_committed_disk_space("tmp") == space * 2)

    @authors("babenko", "kiselyovp")
    def test_nested_tx_uncommitted_usage(self):
        self._wait_for_tmp_account_usage()

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")
            write_table("<append=true>//tmp/t", {"a": "b"})
            write_table("<append=true>//tmp/t", {"a": "b"})

        assert self._get_account_chunk_count("tmp") == 2

        tx1 = start_transaction(timeout=60000)
        tx2 = start_transaction(tx=tx1, timeout=60000)

        with self.WaitForAccountUsage("//tmp/t", append=True, tx=tx2):
            write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx2)

        assert self._get_account_chunk_count("tmp") == 3
        assert get("//tmp/t/@update_mode") == "none"
        assert get("//tmp/t/@update_mode", tx=tx1) == "none"
        assert get("//tmp/t/@update_mode", tx=tx2) == "append"

        assert self._get_tx_chunk_count(tx1, "tmp") == 0
        assert self._get_tx_chunk_count(tx2, "tmp") == 1

        commit_transaction(tx2)

        wait(lambda: self._get_tx_chunk_count(tx1, "tmp") == 1)
        assert get("//tmp/t/@update_mode") == "none"
        assert get("//tmp/t/@update_mode", tx=tx1) == "append"
        assert self._get_account_chunk_count("tmp") == 3

        commit_transaction(tx1)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 3)
        assert get("//tmp/t/@update_mode") == "none"
        assert self._get_account_chunk_count("tmp") == 3

    @authors("babenko", "ignat", "kiselyovp")
    def test_copy(self):
        create_account("a1")
        create_account("a2")

        with self.WaitForAccountUsage("//tmp/x1", new_account="a1"):
            create("map_node", "//tmp/x1", attributes={"account": "a1"})
            assert get("//tmp/x1/@account") == "a1"

        with self.WaitForAccountUsage("//tmp/x2", new_account="a2"):
            create("map_node", "//tmp/x2", attributes={"account": "a2"})
            assert get("//tmp/x2/@account") == "a2"

        with self.WaitForAccountUsage("//tmp/x1/t", new_account="a1"):
            create("table", "//tmp/x1/t")
            assert get("//tmp/x1/t/@account") == "a1"
            write_table("//tmp/x1/t", {"a": "b"})

        space = get_account_disk_space("a1")
        assert space > 0
        assert space == get_account_committed_disk_space("a1")

        with self.WaitForAccountUsage("//tmp/x2/t", new_account="a2"):
            copy("//tmp/x1/t", "//tmp/x2/t")
            assert get("//tmp/x2/t/@account") == "a2"

        assert space == get_account_disk_space("a2")
        assert space == get_account_committed_disk_space("a2")

    @authors("shakurov", "kiselyovp")
    def test_chunk_wise_accounting1(self):
        create_domestic_medium("hdd2")
        create_domestic_medium("hdd3")
        create_account("a")

        gc_collect()
        tmp_node_count = self._get_account_node_count("tmp")
        node_count = self._get_account_node_count("a")

        # 1) Just basic accounting.

        create("table", "//tmp/t1")
        set("//tmp/t1/@media/default/replication_factor", 1)

        write_table("//tmp/t1", {"a": "b"})

        chunk_size = get_chunk_owner_disk_space("//tmp/t1")

        media = get("//tmp/t1/@media")
        media["default"]["replication_factor"] = 3
        media["hdd2"] = {"replication_factor": 4, "data_parts_only": True}
        set("//tmp/t1/@media", media)

        tmp_resource_usage = {
            "node_count": tmp_node_count + 1,
            "chunk_count": 1,
            "disk_space_per_medium": {
                "default": 3 * chunk_size,
                "hdd2": 4 * chunk_size,
            },
        }
        wait(lambda: self._check_resource_usage("tmp", tmp_resource_usage))

        # 2) Chunks shared among accounts should be charged to both, but with different factors.

        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "a")

        with pytest.raises(YtError):
            copy("//tmp/t1", "//tmp/a/t1")
        set_account_disk_space_limit("a", 100000, "hdd2")
        copy("//tmp/t1", "//tmp/a/t1")

        resource_usage = {
            "node_count": node_count + 2,
            "chunk_count": 1,
            "disk_space_per_medium": {
                "default": 3 * chunk_size,
                "hdd2": 4 * chunk_size,
            },
        }
        wait(
            lambda: self._check_resource_usage("tmp", tmp_resource_usage)
            and self._check_resource_usage("a", resource_usage)
        )

        del media["default"]
        media["hdd2"]["replication_factor"] = 2
        media["hdd3"] = {"replication_factor": 5, "data_parts_only": False}
        set("//tmp/a/t1/@primary_medium", "hdd3")
        set("//tmp/a/t1/@media", media)
        resource_usage["disk_space_per_medium"]["default"] = 0
        resource_usage["disk_space_per_medium"]["hdd2"] = 2 * chunk_size
        resource_usage["disk_space_per_medium"]["hdd3"] = 5 * chunk_size

        wait(
            lambda: self._check_resource_usage("tmp", tmp_resource_usage)
            and self._check_resource_usage("a", resource_usage)
        )

        # 3) Copying chunks you already own isn't charged - unless the copy requires higher replication factor.

        copy("//tmp/a/t1", "//tmp/a/t2")
        resource_usage["node_count"] += 1

        wait(
            lambda: self._check_resource_usage("tmp", tmp_resource_usage)
            and self._check_resource_usage("a", resource_usage)
        )

        assert get("//tmp/a/t1/@media") == get("//tmp/a/t2/@media")

        # Add a new medium,..
        media["default"] = {"replication_factor": 2, "data_parts_only": False}
        # ...increase RF on another medium (this should make a difference),..
        media["hdd2"]["replication_factor"] = 3
        # ...and decrease on yet another one (this shouldn't make a difference).
        media["hdd3"]["replication_factor"] = 4
        set("//tmp/a/t2/@media", media)
        resource_usage["disk_space_per_medium"]["default"] = 2 * chunk_size
        resource_usage["disk_space_per_medium"]["hdd2"] = 3 * chunk_size

        wait(
            lambda: self._check_resource_usage("tmp", tmp_resource_usage)
            and self._check_resource_usage("a", resource_usage)
        )

        # 4) Basic transaction accounting - committing.

        tx = start_transaction(timeout=60000)
        create("table", "//tmp/a/t3")
        committed_resource_usage = deepcopy(resource_usage)
        committed_resource_usage["node_count"] += 1
        resource_usage["node_count"] += 2
        write_table("//tmp/a/t3", {"a": "b"}, tx=tx)
        resource_usage["chunk_count"] += 1
        resource_usage["disk_space_per_medium"]["default"] += 3 * chunk_size

        def check_tx_resource_usage(tx):
            tx_resource_usage = get("//sys/transactions/{0}/@resource_usage".format(tx))
            return (
                tx_resource_usage["a"]["node_count"] == 1
                and tx_resource_usage["a"]["chunk_count"] == 1
                and tx_resource_usage["a"]["disk_space_per_medium"].get("default", 0) == 3 * chunk_size
            )

        wait(
            lambda: check_tx_resource_usage(tx)
            and self._check_resource_usage("a", resource_usage)
            and self._check_committed_resource_usage("a", committed_resource_usage)
        )

        commit_transaction(tx)
        resource_usage["node_count"] -= 1

        wait(
            lambda: self._check_resource_usage("a", resource_usage)
            and self._check_committed_resource_usage("a", resource_usage)
        )

        # 5) Basic accounting with some additional data.

        set(
            "//tmp/a/t3/@media",
            {
                "default": {"replication_factor": 2, "data_parts_only": False},
                "hdd2": {"replication_factor": 3, "data_parts_only": True},
            },
        )
        resource_usage["disk_space_per_medium"]["default"] -= chunk_size
        resource_usage["disk_space_per_medium"]["hdd2"] += 3 * chunk_size

        wait(lambda: self._check_resource_usage("a", resource_usage))

        # 6) Transaction accounting - aborting.

        tx = start_transaction(timeout=60000)
        create("table", "//tmp/a/t4")
        committed_resource_usage = deepcopy(resource_usage)
        committed_resource_usage["node_count"] += 1
        resource_usage["node_count"] += 2
        write_table("//tmp/a/t4", {"a": "b"}, tx=tx)
        resource_usage["chunk_count"] += 1
        resource_usage["disk_space_per_medium"]["default"] += 3 * chunk_size

        wait(
            lambda: check_tx_resource_usage(tx)
            and self._check_resource_usage("a", resource_usage)
            and self._check_committed_resource_usage("a", committed_resource_usage)
        )

        abort_transaction(tx)

        wait(
            lambda: self._check_resource_usage("a", committed_resource_usage)
            and self._check_committed_resource_usage("a", committed_resource_usage)
        )
        resource_usage = deepcopy(committed_resource_usage)

        # 7) Appending.

        write_table("<append=true>//tmp/a/t3", {"a": "b"})
        resource_usage["chunk_count"] += 1
        resource_usage["disk_space_per_medium"]["default"] += 2 * chunk_size
        resource_usage["disk_space_per_medium"]["hdd2"] += 3 * chunk_size

        wait(lambda: self._check_resource_usage("a", resource_usage))

    @authors("shakurov", "kiselyovp")
    def test_chunk_wise_accounting2(self):
        create_domestic_medium("hdd4")
        create_domestic_medium("hdd5")
        create_account("a")

        gc_collect()
        tmp_node_count = self._get_account_node_count("tmp")
        node_count = self._get_account_node_count("a")

        codec = "reed_solomon_6_3"
        codec_data_ratio = 6.0 / 9.0

        # 1) Basic erasure-aware accounting.

        create("table", "//tmp/t1")
        set("//tmp/t1/@erasure_codec", codec)
        write_table("//tmp/t1", {"a": "b"})

        chunk_size = get_chunk_owner_disk_space("//tmp/t1")

        media = get("//tmp/t1/@media")
        media["default"]["replication_factor"] = 3
        media["hdd4"] = {"replication_factor": 1, "data_parts_only": True}
        set("//tmp/t1/@media", media)

        tmp_resource_usage = {
            "node_count": tmp_node_count + 1,
            "chunk_count": 1,
            "disk_space_per_medium": {
                "default": chunk_size,
                "hdd4": int(codec_data_ratio * chunk_size),
            },
        }
        wait(lambda: self._check_resource_usage("tmp", tmp_resource_usage))

        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "a")

        # 1) Sharing chunks.

        with pytest.raises(YtError):
            copy("//tmp/t1", "//tmp/a/t1")
        set_account_disk_space_limit("a", 100000, "hdd4")
        copy("//tmp/t1", "//tmp/a/t1")

        resource_usage = {
            "node_count": node_count + 2,
            "chunk_count": 1,
            "disk_space_per_medium": {
                "default": chunk_size,
                "hdd4": int(codec_data_ratio * chunk_size),
            },
        }
        wait(
            lambda: self._check_resource_usage("tmp", tmp_resource_usage)
            and self._check_resource_usage("a", resource_usage)
        )

        # 2) Sharing chunks within single account.

        copy("//tmp/a/t1", "//tmp/a/t2")
        resource_usage["node_count"] += 1

        wait(
            lambda: self._check_resource_usage("tmp", tmp_resource_usage)
            and self._check_resource_usage("a", resource_usage)
        )

        media["hdd5"] = {"replication_factor": 5, "data_parts_only": False}
        set("//tmp/a/t2/@media", media)
        resource_usage["disk_space_per_medium"]["hdd5"] = chunk_size

        wait(
            lambda: self._check_resource_usage("tmp", tmp_resource_usage)
            and self._check_resource_usage("a", resource_usage)
        )

    def _check_resource_usage(self, account, resource_usage):
        return self._check_resource_usage_impl(account, resource_usage, False)

    def _check_committed_resource_usage(self, account, resource_usage):
        return self._check_resource_usage_impl(account, resource_usage, True)

    def _check_resource_usage_impl(self, account, resource_usage, committed):
        actual_resource_usage = get(
            "//sys/accounts/{0}/@{1}resource_usage".format(account, "committed_" if committed else "")
        )
        if (
            actual_resource_usage["node_count"] != resource_usage["node_count"]
            or actual_resource_usage["chunk_count"] != resource_usage["chunk_count"]
        ):
            return False
        for medium, disk_space in resource_usage["disk_space_per_medium"].items():
            if actual_resource_usage["disk_space_per_medium"].get(medium, 0) != disk_space:
                return False
        return True

    @authors("babenko")
    def test_move_preserve_account_success(self):
        # setup
        create_account("a")
        set_account_disk_space_limit("a", 100000)
        create("map_node", "//tmp/x")
        set("//tmp/x/@account", "a")
        create("table", "//tmp/x/t")
        write_table("//tmp/x/t", {"a": "b"})

        # make "a" overcommitted
        self._set_account_zero_limits("a")

        # move must succeed
        move("//tmp/x", "//tmp/y", preserve_account=True)

    @authors("babenko")
    def test_move_dont_preserve_account_success(self):
        # setup
        create_account("a")
        set_account_disk_space_limit("a", 100000)
        create("map_node", "//tmp/x")
        set("//tmp/x/@account", "a")
        create("table", "//tmp/x/t")
        write_table("//tmp/x/t", {"a": "b"})
        create("map_node", "//tmp/for_y")
        set("//tmp/for_y/@account", "a")

        # make "a" overcommitted
        self._set_account_zero_limits("a")

        # move must succeed
        move("//tmp/x", "//tmp/for_y/y", preserve_account=False)

    @authors("babenko")
    def test_move_dont_preserve_account_fail(self):
        # setup
        create("map_node", "//tmp/x")
        create_account("a")
        create("map_node", "//tmp/for_y")
        set("//tmp/for_y/@account", "a")

        # make "a" overcommitted
        self._set_account_zero_limits("a")

        # move must fail
        with pytest.raises(YtError):
            move("//tmp/x", "//tmp/for_y/y", preserve_account=False)

    @authors("babenko")
    def test_copy_preserve_account_fail(self):
        # setup
        create_account("a")
        create("map_node", "//tmp/x")
        set("//tmp/x/@account", "a")

        # make "a" overcommitted
        self._set_account_zero_limits("a")

        # copy must fail
        with pytest.raises(YtError):
            copy("//tmp/x", "//tmp/y", preserve_account=True)

    @authors("babenko")
    def test_copy_dont_preserve_account_fail(self):
        # setup
        create_account("a")
        create("map_node", "//tmp/x")
        create("map_node", "//tmp/for_y")
        set("//tmp/x/@account", "a")
        set("//tmp/for_y/@account", "a")

        # make "a" overcommitted
        self._set_account_zero_limits("a")

        # copy must fail
        with pytest.raises(YtError):
            copy("//tmp/x", "//tmp/for_y/y", preserve_account=False)

    @authors("babenko", "ignat")
    def test_rename_success(self):
        create_account("a1")
        set("//sys/accounts/a1/@name", "a2")
        assert get("//sys/accounts/a2/@name") == "a2"

    @authors("babenko", "ignat")
    def test_rename_fail(self):
        create_account("a1")
        create_account("a2")
        with pytest.raises(YtError):
            set("//sys/accounts/a1/@name", "a2")

    @authors("babenko", "kiselyovp")
    def test_set_account_fail_yt_6207(self):
        self._wait_for_tmp_account_usage()

        create_account("a")
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        assert get("//tmp/t/@account") == "tmp"

        wait_true_for_all_cells(
            self.Env,
            lambda driver: get("//sys/accounts/tmp/@resource_usage/disk_space", driver=driver) > 0,
        )
        assert_true_for_all_cells(
            self.Env,
            lambda driver: get("//sys/accounts/a/@resource_usage/disk_space", driver=driver) == 0,
        )

        create_user("u")
        with pytest.raises(YtError):
            set("//tmp/t/@account", "a", authenticated_user="u")

        assert_true_for_all_cells(
            self.Env,
            lambda driver: get("//sys/accounts/tmp/@resource_usage/disk_space", driver=driver) > 0,
        )
        assert_true_for_all_cells(
            self.Env,
            lambda driver: get("//sys/accounts/a/@resource_usage/disk_space", driver=driver) == 0,
        )

    @authors("babenko", "kiselyovp")
    def test_change_account_with_snapshot_lock(self):
        self._wait_for_tmp_account_usage()

        tmp_nc = get("//sys/accounts/tmp/@resource_usage/node_count")
        create_account("a")
        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")

        assert get("//sys/accounts/a/@ref_counter") == 1
        assert get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc + 1
        assert get("//sys/accounts/a/@resource_usage/node_count") == 0

        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        assert get("//sys/accounts/a/@ref_counter") == 1
        wait(lambda: get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc + 2)
        assert get("//sys/accounts/a/@resource_usage/node_count") == 0

        set("//tmp/t/@account", "a")
        assert get("//sys/accounts/a/@ref_counter") == 4
        wait(
            lambda: get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc + 1
            and get("//sys/accounts/a/@resource_usage/node_count") == 1
        )

        abort_transaction(tx)
        wait(
            lambda: get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc
            and get("//sys/accounts/a/@resource_usage/node_count") == 1
        )
        assert get("//sys/accounts/a/@ref_counter") == 4

    def _get_master_memory_usage(self, account):
        master_memory = get("//sys/accounts/{}/@resource_usage/master_memory/total".format(account))
        assert master_memory >= 0
        return master_memory

    def _get_detailed_master_memory_usage(self, account, memory_type):
        master_memory = get("//sys/accounts/{}/@resource_usage/detailed_master_memory/{}".format(account, memory_type))
        assert master_memory >= 0
        return master_memory

    @authors("aleksandra-zh")
    def test_master_memory(self):
        create_account("a")

        create("table", "//tmp/t", attributes={"account": "a"})
        master_memory_sleep()

        wait(lambda: self._get_master_memory_usage("a") > 0)
        prev_usage = self._get_master_memory_usage("a")

        node_usage = self._get_detailed_master_memory_usage("a", "nodes")
        assert node_usage > 0

        set("//tmp/t/@a", "a")
        master_memory_sleep()

        wait(lambda: self._get_master_memory_usage("a") > prev_usage)
        wait(
            lambda: self._get_master_memory_usage("a") - prev_usage
            == self._get_detailed_master_memory_usage("a", "attributes"))
        prev_usage = self._get_master_memory_usage("a")

        remove("//tmp/t/@a")
        master_memory_sleep()

        wait(lambda: self._get_master_memory_usage("a") < prev_usage)

        remove("//tmp/t")
        wait(lambda: self._get_master_memory_usage("a") == 0)

    @authors("aleksandra-zh")
    def test_master_memory_copy(self):
        create_account("a")

        create("table", "//tmp/t", attributes={"account": "a"})
        set("//tmp/t/@a", "a")
        write_table("//tmp/t", {"a": "b"})

        master_memory_sleep()
        wait(lambda: self._get_master_memory_usage("tmp") > 0)
        prev_usage = self._get_master_memory_usage("tmp")

        wait(lambda: self._get_detailed_master_memory_usage("a", "chunks") > 0)
        chunks_usage = self._get_detailed_master_memory_usage("a", "chunks")

        copy("//tmp/t", "//tmp/t2")
        wait(lambda: self._get_master_memory_usage("tmp") > prev_usage)
        wait(lambda: self._get_detailed_master_memory_usage("tmp", "chunks") == chunks_usage)

    @authors("aleksandra-zh")
    def test_master_memory_chunks(self):
        create_account("a")
        create_account("b")

        create("table", "//tmp/t1", attributes={"account": "a"})
        write_table("//tmp/t1", {"a": "b"})

        copy("//tmp/t1", "//tmp/t2")
        set("//tmp/t2/@account", "b")

        wait(lambda: self._get_master_memory_usage("b") > 0)
        wait(lambda: self._get_detailed_master_memory_usage("b", "chunks") > 0)

    @authors("h0pless")
    def test_master_memory_chunk_heartbeat_race(self):
        create_account("juan")
        create("table", "//tmp/table")
        write_table("//tmp/table", {"a": "b"})
        remove("//tmp/table")
        wait(lambda: self._get_master_memory_usage("juan") == 0)

    @authors("aleksandra-zh")
    def test_master_memory_change_account(self):
        create_account("a")
        assert self._get_master_memory_usage("a") == 0

        create("table", "//tmp/t", attributes={"account": "a"})
        write_table("//tmp/t", {"a": "b"})

        wait(lambda: self._get_master_memory_usage("a") > 0)

        create_account("b")
        assert self._get_master_memory_usage("b") == 0
        set("//tmp/t/@account", "b")

        wait(lambda: self._get_master_memory_usage("b") > 0)
        wait(lambda: self._get_master_memory_usage("a") == 0)

    def _prepare_dynamic_table(self, path, account, sorted=True):
        sync_create_cells(1)
        schema = [{"name": "key", "type": "int64"}, {"name": "value", "type": "string"}]
        if sorted:
            schema[0]["sort_order"] = "ascending"

        create_dynamic_table(path, schema=schema, account=account)

        sync_mount_table(path)
        insert_rows(path, [{"key": 0, "value": "0"}])
        sync_unmount_table(path)

    @authors("aleksandra-zh")
    def test_master_memory_copy_dynamic_table(self):
        resource_limits = self._build_resource_limits(
            node_count=10,
            chunk_count=10,
            tablet_count=10,
            tablet_static_memory=10000,
            disk_space=100000,
            master_memory=10000,
        )
        create_account("a", attributes={"resource_limits": resource_limits})

        self._prepare_dynamic_table("//tmp/t1", "a")
        master_memory_sleep()

        wait(lambda: self._get_master_memory_usage("a") > 0)
        prev_usage = self._get_master_memory_usage("a")
        copy("//tmp/t1", "//tmp/t2", preserve_account=True)
        wait(lambda: self._get_master_memory_usage("a") > prev_usage)

        create_account("b", attributes={"resource_limits": resource_limits})
        set("//tmp/t2/@account", "b")

        wait(lambda: self._get_master_memory_usage("b") > 0)

    @authors("aleksandra-zh")
    def test_master_memory_dynamic_to_static(self):
        resource_limits = self._build_resource_limits(
            node_count=10,
            chunk_count=10,
            tablet_count=10,
            tablet_static_memory=10000,
            disk_space=100000,
            master_memory=10000,
        )
        create_account("a", attributes={"resource_limits": resource_limits})
        self._prepare_dynamic_table("//tmp/t", "a", sorted=False)

        wait(lambda: self._get_detailed_master_memory_usage("a", "tablets") > 0)
        alter_table("//tmp/t", dynamic=False)
        wait(lambda: self._get_detailed_master_memory_usage("a", "tablets") == 0)

    @authors("aleksandra-zh")
    def test_master_memory_dynamic_table_reshard(self):
        resource_limits = self._build_resource_limits(
            node_count=10,
            chunk_count=10,
            tablet_count=100,
            tablet_static_memory=10000,
            disk_space=100000,
            master_memory=10000,
        )
        create_account("a", attributes={"resource_limits": resource_limits})

        self._prepare_dynamic_table("//tmp/t", "a")
        sync_mount_table("//tmp/t")

        master_memory_sleep()
        wait(lambda: self._get_detailed_master_memory_usage("a", "tablets") > 0)
        prev_usage = self._get_detailed_master_memory_usage("a", "tablets")

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[]] + [[i] for i in range(11)])
        sync_mount_table("//tmp/t")

        master_memory_sleep()
        wait(lambda: self._get_detailed_master_memory_usage("a", "tablets") > prev_usage)
        prev_usage = self._get_detailed_master_memory_usage("a", "tablets")

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[]] + [[i] for i in range(2)])
        sync_mount_table("//tmp/t")

        wait(lambda: self._get_detailed_master_memory_usage("a", "tablets") < prev_usage)

    @authors("aleksandra-zh")
    def test_master_memory_pivot_keys(self):
        resource_limits = self._build_resource_limits(
            node_count=10,
            chunk_count=10,
            tablet_count=100,
            tablet_static_memory=10000,
            disk_space=100000,
            master_memory=100000,
        )
        create_account("a", attributes={"resource_limits": resource_limits})

        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "key", "type": "string", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            account="a",
        )

        sync_mount_table("//tmp/t")

        master_memory_sleep()

        wait(lambda: self._get_detailed_master_memory_usage("a", "tablets") > 0)
        tablets_usage = self._get_detailed_master_memory_usage("a", "tablets")

        wait(lambda: self._get_master_memory_usage("a") > 0)
        prev_usage = self._get_master_memory_usage("a")

        sync_unmount_table("//tmp/t")
        key_length = 10000
        sync_reshard_table("//tmp/t", [[]] + [["a" * key_length]])
        sync_mount_table("//tmp/t")

        wait(lambda: self._get_master_memory_usage("a") - prev_usage >= key_length - 100)
        wait(lambda: self._get_detailed_master_memory_usage("a", "tablets") - tablets_usage >= key_length - 100)

    @authors("aleksandra-zh")
    def test_master_memory_violate_limits(self):
        set("//sys/@config/security_manager/enable_master_memory_usage_validation", True)
        create_account("a")
        create("table", "//tmp/t", attributes={"account": "a"})
        set("//sys/accounts/a/@resource_limits/master_memory/total", 0)
        with pytest.raises(YtError):
            set("//tmp/t/@sdflkf", "sdlzkfj")
        set("//sys/accounts/a/@resource_limits/master_memory/total", 1000000)
        set("//sys/accounts/a/@resource_limits/master_memory/chunk_host", 1000000)
        set("//tmp/t/@sdflkf", "sdlzkfj")
        set("//sys/accounts/a/@resource_limits/master_memory/total", 0)
        remove("//tmp/t/@sdflkf")

    @authors("aleksandra-zh")
    def test_master_memory_all(self):
        create_account("a")
        assert self._get_master_memory_usage("a") == 0

        create(
            "map_node",
            "//tmp/dir1",
            attributes={"account": "a", "sdkjnfkdjs": "lsdkfj"},
        )
        wait(lambda: self._get_master_memory_usage("a") > 0)
        current_usage = self._get_master_memory_usage("a")

        create("map_node", "//tmp/dir1/dir2", attributes={"account": "a"})

        wait(lambda: self._get_master_memory_usage("a") > current_usage)
        current_usage = self._get_master_memory_usage("a")

        create("table", "//tmp/dir1/dir2/t", attributes={"account": "a", "aksdj": "sdkjf"})

        wait(lambda: self._get_master_memory_usage("a") > current_usage)
        current_usage = self._get_master_memory_usage("a")

        copy("//tmp/dir1/dir2", "//tmp/dir1/dir3", preserve_account=True)

        wait(lambda: self._get_master_memory_usage("a") > current_usage)
        current_usage = self._get_master_memory_usage("a")

        move("//tmp/dir1/dir2", "//tmp/dir1/dir4", preserve_account=True)

        copy("//tmp/dir1/dir3/t", "//tmp/dir1/dir4/t1", preserve_account=True)

        wait(lambda: self._get_master_memory_usage("a") > current_usage)
        current_usage = self._get_master_memory_usage("a")

        move("//tmp/dir1/dir3/t", "//tmp/dir1/dir3/t1", preserve_account=True)

        remove("//tmp/dir1")
        wait(lambda: self._get_master_memory_usage("a") == 0)

    @authors("danilalexeev")
    def test_transient_master_memory(self):
        limits = self._build_resource_limits(
            node_count=10,
            chunk_count=40,
            disk_space=9001,
            master_memory=1000,
        )

        def get_transient_master_memory_usage(account, memory_type):
            master_memory = get("//sys/accounts/{}/@transient_master_memory_usage/{}".format(account, memory_type))
            assert master_memory >= 0
            return master_memory

        create_account("child", attributes={"resource_limits": limits})
        create_account("parent", attributes={"resource_limits": limits})

        create("table", "//tmp/t", attributes={"account": "child"})
        write_table("//tmp/t", {"a": 0xbe, "b": 0xbebe, "c": 0xbebebe})
        wait(lambda: get_transient_master_memory_usage("child", "nodes") > 0)

        set('//sys/accounts/child/@parent_name', "parent")
        wait(lambda: get_transient_master_memory_usage("parent", "nodes")
             == get_transient_master_memory_usage("child", "nodes"))

        remove("//tmp/t")
        wait(lambda: get_transient_master_memory_usage("parent", "nodes") == 0)

    @authors("shakurov")
    def test_master_memory_schema_accounting(self):
        create_account("a")

        create("table", "//tmp/t1", attributes={"account": "a"})
        create("table", "//tmp/t2", attributes={
            "account": "tmp",
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]})

        t1_schema_id = get("//tmp/t1/@schema_id")
        t2_schema_id = get("//tmp/t2/@schema_id")
        assert builtins.set(get("#" + t2_schema_id + "/@referencing_accounts")) == {"tmp"}

        t1_schema_master_memory = get("#" + t1_schema_id + "/@memory_usage")
        t2_schema_master_memory = get("#" + t2_schema_id + "/@memory_usage")
        memory_usage_delta = t2_schema_master_memory - t1_schema_master_memory
        assert memory_usage_delta > 0

        old_memory_usage = t1_schema_master_memory
        if get("//tmp/t1/@external"):
            old_memory_usage += t1_schema_master_memory
        wait(lambda: get("//sys/accounts/a/@resource_usage/detailed_master_memory/schemas") == old_memory_usage)

        copy("//tmp/t2", "//tmp/t1", force=True)  # NB: overwriting.
        set("//tmp/t1/@account", "a")
        assert get("//tmp/t1/@schema_id") == t2_schema_id
        expected_memory_usage = old_memory_usage + memory_usage_delta
        if get("//tmp/t1/@external"):
            # Schema object is present on external cell also.
            expected_memory_usage += memory_usage_delta
        assert builtins.set(get("#" + t2_schema_id + "/@referencing_accounts")) == {"tmp", "a"}
        wait(lambda: get("//sys/accounts/a/@resource_usage/detailed_master_memory/schemas") == expected_memory_usage,
             sleep_backoff=2.0)

    @authors("h0pless")
    def test_master_memory_chunk_schema_accounting(self):
        create_account("parent")
        create_account("child", "parent")

        schema = [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "int64"},
            {"name": "c", "type": "int64"}
        ]

        cell_tag = 11 if self.is_multicell() else 10
        create("table", "//tmp/t1", attributes={
            "account": "child",
            "schema": schema,
            "external_cell_tag": cell_tag})
        master_memory_sleep()

        def get_schema_schema_usage(account):
            return get("//sys/accounts/{}/@resource_usage/detailed_master_memory/schemas".format(account))

        def get_schema_recursive_schema_usage(account):
            return get("//sys/accounts/{}/@recursive_resource_usage/detailed_master_memory/schemas".format(account))

        wait(lambda: get_schema_schema_usage("child") > 0)
        starting_schema_usage = get_schema_schema_usage("child")
        wait(lambda: get_schema_recursive_schema_usage("parent") == starting_schema_usage)

        write_table(
            "<chunk_sort_columns=[{name=a;sort_order=descending};{name=b;sort_order=descending};{name=c;sort_order=descending}];append=true>//tmp/t1",
            {"a": 42, "b": 123, "c": 88555})

        wait(lambda: get_schema_schema_usage("child") > starting_schema_usage)
        new_schema_usage = get_schema_schema_usage("child")
        wait(lambda: get_schema_recursive_schema_usage("parent") == new_schema_usage)

        create("table", "//tmp/t2", attributes={
            "account": "child",
            "schema": schema,
            "external_cell_tag": cell_tag})

        sleep(1)
        assert get_schema_schema_usage("child") == new_schema_usage
        assert get_schema_recursive_schema_usage("parent") == new_schema_usage

        remove("//tmp/t1")
        wait(lambda: get_schema_schema_usage("child") == starting_schema_usage)
        wait(lambda: get_schema_recursive_schema_usage("parent") == starting_schema_usage)

        create("table", "//tmp/t3", attributes={
            "account": "parent",
            "schema": schema,
            "external_cell_tag": cell_tag})

        wait(lambda: get_schema_schema_usage("parent") == starting_schema_usage)
        wait(lambda: get_schema_recursive_schema_usage("parent") == 2 * starting_schema_usage)
        assert get_schema_schema_usage("child") == starting_schema_usage

    @authors("babenko")
    def test_regular_disk_usage(self):
        create("table", "//tmp/t")
        set("//tmp/t/@replication_factor", 5)
        write_table("//tmp/t", {"a": "b"})
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        assert (
            get("//tmp/t/@resource_usage/disk_space_per_medium/default")
            == get("#{0}/@statistics/regular_disk_space".format(chunk_list_id)) * 5
        )

    @authors("babenko")
    def test_erasure_disk_usage(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        set("//tmp/t/@replication_factor", 5)
        write_table("//tmp/t", {"a": "b"})
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        assert get("//tmp/t/@resource_usage/disk_space_per_medium/default") == get(
            "#{0}/@statistics/erasure_disk_space".format(chunk_list_id)
        )

    @authors("babenko")
    def test_create_with_invalid_attrs_yt_7093(self):
        with pytest.raises(YtError):
            create_account("x", attributes={"resource_limits": 123})
        assert not exists("//sys/accounts/x")

        with pytest.raises(YtError):
            create_account(
                "y",
                attributes={"resource_limits": self._build_resource_limits(disk_space=-1)},
            )
        assert not exists("//sys/accounts/y")

    @authors("shakurov", "kiselyovp")
    def test_requisitions(self):
        create_domestic_medium("hdd6")
        create_account("a")

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")

        def check_chunk_requisition(chunk_id, expected):
            requisition = get("#" + chunk_id + "/@requisition")
            requisition = sorted(requisition, key=itemgetter("account", "medium"))
            return requisition == expected

        expected_requisition = [
            {
                "account": "tmp",
                "medium": "default",
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
                "committed": True,
            }
        ]
        wait(lambda: check_chunk_requisition(chunk_id, expected_requisition))

        # Link the chunk to another table...
        copy("//tmp/t", "//tmp/t2")
        set("//tmp/t2/@account", "a")

        # ...and modify the original table's properties in some way.
        tbl_media = get("//tmp/t/@media")
        tbl_media["hdd6"] = {"replication_factor": 7, "data_parts_only": True}
        tbl_media["default"] = {"replication_factor": 4, "data_parts_only": False}
        set("//tmp/t/@media", tbl_media)

        expected_requisition = [
            {
                "account": "a",
                "medium": "default",
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
                "committed": True,
            },
            {
                "account": "tmp",
                "medium": "default",
                "replication_policy": {
                    "replication_factor": 4,
                    "data_parts_only": False,
                },
                "committed": True,
            },
            {
                "account": "tmp",
                "medium": "hdd6",
                "replication_policy": {
                    "replication_factor": 7,
                    "data_parts_only": True,
                },
                "committed": True,
            },
        ]
        wait(lambda: check_chunk_requisition(chunk_id, expected_requisition))

    @authors("shakurov")
    def test_inherited_account_override_yt_8391(self):
        create_account("a1")
        create_account("a2")

        create_user("u1")
        create_user("u2")

        set("//sys/accounts/a1/@acl", [make_ace("allow", "u1", "use")])
        set("//sys/accounts/a2/@acl", [make_ace("allow", "u2", "use")])

        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/dir1",
                attributes={"account": "a1"},
                authenticated_user="u2",
            )

        create(
            "map_node",
            "//tmp/dir1",
            attributes={"account": "a1"},
            authenticated_user="u1",
        )

        with pytest.raises(YtError):
            create("map_node", "//tmp/dir1/dir2", authenticated_user="u2")

        create(
            "map_node",
            "//tmp/dir1/dir2",
            attributes={"account": "a2"},
            authenticated_user="u2",
        )

        assert get("//tmp/dir1/@account") == "a1"
        assert get("//tmp/dir1/dir2/@account") == "a2"

    @authors("shakurov")
    def test_recursive_create_with_explicit_account(self):
        create_account("a")
        create(
            "document",
            "//tmp/one/two/three",
            recursive=True,
            attributes={"account": "a"},
        )
        assert get("//tmp/@account") == "tmp"
        assert get("//tmp/one/two/three/@account") == "a"
        assert get("//tmp/one/two/@account") == "a"
        assert get("//tmp/one/@account") == "a"

    @authors("shakurov")
    def test_nested_tx_copy(self):
        gc_collect()

        create("table", "//tmp/t")

        multicell_sleep()
        node_count = get("//sys/accounts/tmp/@resource_usage/node_count")
        committed_node_count = get("//sys/accounts/tmp/@committed_resource_usage/node_count")

        tx1 = start_transaction()
        copy("//tmp/t", "//tmp/t1", tx=tx1)

        node_count += 3  # one for branched map node, one for cloned table, one for branched cloned table
        committed_node_count += 1  # one for cloned table
        multicell_sleep()
        assert get("//sys/accounts/tmp/@resource_usage/node_count") == node_count
        assert get("//sys/accounts/tmp/@committed_resource_usage/node_count") == committed_node_count

        commit_transaction(tx1)

        # Transaction changes disappear...
        node_count -= 3
        committed_node_count -= 1
        # but the newly committed node remains.
        node_count += 1
        committed_node_count += 1
        multicell_sleep()
        gc_collect()
        assert get("//sys/accounts/tmp/@resource_usage/node_count") == node_count
        assert get("//sys/accounts/tmp/@committed_resource_usage/node_count") == committed_node_count

    @authors("shakurov", "kiselyovp")
    def test_branched_nodes_not_checked_yt_8551(self):
        self._wait_for_tmp_account_usage()

        with self.WaitForAccountUsage("//tmp/t", new_account="tmp"):
            create("table", "//tmp/t")

        node_count = get("//sys/accounts/tmp/@resource_usage/node_count")
        committed_node_count = get("//sys/accounts/tmp/@committed_resource_usage/node_count")

        tx1 = start_transaction()
        copy("//tmp/t", "//tmp/t1", tx=tx1)

        node_count += 3  # one for branched map node, one for cloned table, one for branched cloned table
        committed_node_count += 1  # one for cloned table
        wait(lambda: get("//sys/accounts/tmp/@resource_usage/node_count") == node_count)
        wait(lambda: get("//sys/accounts/tmp/@committed_resource_usage/node_count") == committed_node_count)

        copy("//tmp/t", "//tmp/t2", tx=tx1)

        node_count += 2  # one for cloned table, one for branched cloned table
        committed_node_count += 1  # one for cloned table
        wait(lambda: get("//sys/accounts/tmp/@resource_usage/node_count") == node_count)
        wait(lambda: get("//sys/accounts/tmp/@committed_resource_usage/node_count") == committed_node_count)

        self._set_account_node_count_limit("tmp", committed_node_count)
        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/t3", tx=tx1)

        self._set_account_node_count_limit("tmp", committed_node_count + 1)
        copy("//tmp/t", "//tmp/t3", tx=tx1)

        node_count += 2
        committed_node_count += 1
        wait(lambda: get("//sys/accounts/tmp/@resource_usage/node_count") == node_count)
        wait(lambda: get("//sys/accounts/tmp/@committed_resource_usage/node_count") == committed_node_count)

        self._set_account_node_count_limit("tmp", node_count + 2)
        copy("//tmp/t", "//tmp/t4", tx=tx1)

        node_count += 2
        committed_node_count += 1
        wait(lambda: get("//sys/accounts/tmp/@resource_usage/node_count") == node_count)
        wait(lambda: get("//sys/accounts/tmp/@committed_resource_usage/node_count") == committed_node_count)

    @authors("shakurov")
    @flaky(max_runs=3)
    def test_totals(self):
        self._set_account_zero_limits("chunk_wise_accounting_migration")

        def add_resource_limits(*resources):
            result = {
                "disk_space_per_medium": {"default": 0},
                "disk_space": 0,
                "chunk_count": 0,
                "node_count": 0,
                "tablet_count": 0,
                "tablet_static_memory": 0,
                "master_memory":
                {
                    "total": 0,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            }
            for r in resources:
                result["disk_space_per_medium"]["default"] += r["disk_space_per_medium"].get("default", 0)
                result["disk_space"] += r["disk_space"]
                result["chunk_count"] += r["chunk_count"]
                result["node_count"] += r["node_count"]
                result["tablet_count"] += r["tablet_count"]
                result["tablet_static_memory"] += r["tablet_static_memory"]
                result["master_memory"]["total"] += r["master_memory"]["total"]
                result["master_memory"]["chunk_host"] += r["master_memory"]["chunk_host"]

            return result

        def add_resource_usage(*resources):
            result = {
                "disk_space_per_medium": {"default": 0},
                "disk_space": 0,
                "chunk_count": 0,
                "node_count": 0,
                "tablet_count": 0,
                "tablet_static_memory": 0,
                "master_memory":
                {
                    "total": 0,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            }
            for r in resources:
                result["disk_space_per_medium"]["default"] += r["disk_space_per_medium"].get("default", 0)
                result["disk_space"] += r["disk_space"]
                result["chunk_count"] += r["chunk_count"]
                result["node_count"] += r["node_count"]
                result["tablet_count"] += r["tablet_count"]
                result["tablet_static_memory"] += r["tablet_static_memory"]
                result["master_memory"]["total"] += r["master_memory"]["total"]
                result["master_memory"]["chunk_host"] += r["master_memory"]["chunk_host"]

            return result

        resource_limits = get("//sys/accounts/@total_resource_limits")

        create_account("a1")
        set(
            "//sys/accounts/a1/@resource_limits",
            {
                "disk_space_per_medium": {"default": 1000},
                "chunk_count": 1,
                "node_count": 1,
                "tablet_count": 0,
                "tablet_static_memory": 0,
                "master_memory":
                {
                    "total": 1000,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            },
        )
        create_account("a2")
        set(
            "//sys/accounts/a2/@resource_limits",
            {
                "disk_space_per_medium": {"default": 1000},
                "chunk_count": 1,
                "node_count": 1,
                "tablet_count": 0,
                "tablet_static_memory": 0,
                "master_memory":
                {
                    "total": 1000,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            },
        )

        total_resource_limits = add_resource_limits(
            resource_limits,
            {
                "disk_space_per_medium": {"default": 2000},
                "disk_space": 2000,
                "chunk_count": 2,
                "node_count": 2,
                "tablet_count": 0,
                "tablet_static_memory": 0,
                "master_memory":
                {
                    "total": 2000,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            },
        )

        # A cleanup from preceding tests may still be happening in background. Wait until totals have stabilized.
        resource_usage = get("//sys/accounts/@total_resource_usage")
        committed_resource_usage = get("//sys/accounts/@total_committed_resource_usage")
        stable_iteration_count = 0
        for i in range(0, 30):
            sleep(0.3)
            new_resource_usage = get("//sys/accounts/@total_resource_usage")
            new_committed_resource_usage = get("//sys/accounts/@total_committed_resource_usage")
            if cluster_resources_equal(resource_usage, new_resource_usage) and cluster_resources_equal(
                committed_resource_usage, new_committed_resource_usage
            ):
                stable_iteration_count += 1
                if stable_iteration_count == 10:
                    # Totals have been stable long enough, continue.
                    break
            else:
                resource_usage = new_resource_usage
                committed_resource_usage = new_committed_resource_usage
                stable_iteration_count = 0

        assert cluster_resources_equal(get("//sys/accounts/@total_resource_limits"), total_resource_limits)

        create("table", "//tmp/t1", attributes={"account": "a1"})
        create("table", "//tmp/t2", attributes={"account": "a2"})
        write_table("//tmp/t1", {"a": "b"})
        write_table("//tmp/t2", {"c": "d"})

        wait(lambda: get_account_disk_space("a1") > 0)
        wait(lambda: get_account_disk_space("a2") > 0)

        def totals_match():
            resource_usage1 = get("//sys/accounts/a1/@resource_usage")
            committed_resource_usage1 = get("//sys/accounts/a1/@committed_resource_usage")

            resource_usage2 = get("//sys/accounts/a2/@resource_usage")
            committed_resource_usage2 = get("//sys/accounts/a2/@committed_resource_usage")

            total_resource_usage = add_resource_usage(resource_usage, resource_usage1, resource_usage2)
            total_committed_resource_usage = add_resource_usage(
                committed_resource_usage,
                committed_resource_usage1,
                committed_resource_usage2,
            )

            return cluster_resources_equal(
                get("//sys/accounts/@total_resource_usage"), total_resource_usage
            ) and cluster_resources_equal(
                get("//sys/accounts/@total_committed_resource_usage"),
                total_committed_resource_usage,
            )

        wait(totals_match)

    @authors("kvk1920")
    def test_get_and_set_total_children_resource_limits(self):
        create_account("parent")
        create_account("child", "parent")
        limits = {
            "node_count": 615000,
            "chunk_count": 3521000,
            "tablet_count": 45500,
            "tablet_static_memory": 28680932609228,
            "disk_space_per_medium": {
                "default": 1801118414798848,
            },
            "disk_space": 1801118414798848,
            "master_memory": {
                "total": 966367641600,
                "chunk_host": 107374182400,
                "per_cell": {},
            },
        }
        for account in ("parent", "child"):
            set(f"//sys/accounts/{account}/@resource_limits", limits)
        set("//sys/accounts/parent/@resource_limits", get("//sys/accounts/parent/@total_children_resource_limits"))
        assert get("//sys/accounts/parent/@resource_limits") == limits


class TestAccountTree(AccountsTestSuiteBase):
    USE_DYNAMIC_TABLES = True

    def _create_account_acl(self, users):
        return [
            make_ace("allow", users, ["use", "modify_children"], "object_only"),
            make_ace("allow", users, ["write", "remove", "administer"], "descendants_only"),
        ]

    def _get_account_tree_master_memory_usage(self, account):
        master_memory = get("//sys/accounts/" + account + "/@recursive_resource_usage/master_memory/total")
        assert master_memory >= 0
        return master_memory

    def setup_method(self, method):
        super(TestAccountTree, self).setup_method(method)
        self._old_schema_acl = get("//sys/schemas/account/@acl")
        set(
            "//sys/schemas/account/@acl",
            [
                make_ace("allow", "everyone", "read"),
                make_ace("allow", "users", "create"),
            ],
        )

    def teardown_method(self, method):
        set("//sys/schemas/account/@acl", self._old_schema_acl)
        super(TestAccountTree, self).teardown_method(method)

    # XXX(kiselyovp) test for changing multiple attributes at the same time?
    # (name/parent_name/resource_limits, incorrect values are welcome)
    # XXX(kiselyovp) a test for "account migration"

    @authors("kiselyovp", "kvk1920")
    def test_root_account(self):
        assert exists("//sys/accounts/{0}".format(self._root_account_name))
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/test",
                attributes={"account": self._root_account_name},
            )
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@account", self._root_account_name)

        root_attributes = get("//sys/accounts/{0}/@".format(self._root_account_name))
        for attribute in [
            "resource_limits",
            "resource_usage",
            "committed_resource_usage",
            "multicell_statistics",
            "upper_resource_limits",
            "own_resource_limits",
            "violated_resource_limits",
        ]:
            assert attribute not in root_attributes
        for attribute in [
            "recursive_resource_usage",
            "recursive_committed_resource_usage",
            "recursive_violated_resource_limits",
        ]:
            assert attribute in root_attributes

        with pytest.raises(YtError):
            self._set_account_zero_limits(self._root_account_name)
        with pytest.raises(YtError):
            set("//sys/accounts/{0}/@parent_name".format(self._root_account_name), "sys")

        assert exists("//sys/accounts/@root_account_resource_limits")

        # TODO(kvk1920): Move to separate test.
        # Should not crash.
        get("//sys/accounts/@total_resource_limits")

    @authors("kiselyovp")
    def test_create1(self):
        with pytest.raises(YtError):
            create_account(self._root_account_name)
        create_account("max", empty=True)
        assert ls("//sys/account_tree/max") == []

        create_account("a1", "max", empty=True)
        create_account("a2", "max", empty=True)
        assert sorted(ls("//sys/account_tree")) == sorted(self._non_root_builtin_accounts + ["max"])
        assert sorted(ls("//sys/accounts")) == sorted(self._builtin_accounts + ["max", "a1", "a2"])
        assert sorted(ls("//sys/account_tree/max")) == ["a1", "a2"]
        assert ls("//sys/account_tree/max/a1") == []
        with pytest.raises(YtError):
            ls("//sys/account_tree/max/")
        with pytest.raises(YtError):
            ls("//sys/account_tree/max/3")

    @authors("kiselyovp")
    def test_create2(self):
        with pytest.raises(YtError):
            create("map_node", "//sys/accounts/tmp/node")
        with pytest.raises(YtError):
            create("file", "//sys/account_tree/file")
        with pytest.raises(YtError):
            create("account", "//tmp/account")

    @authors("kiselyovp")
    def test_create3(self):
        create_account("max", empty=True)
        create_account("nested", "max", empty=True)
        with pytest.raises(YtError):
            create_account("max", empty=True)
        with pytest.raises(YtError):
            create_account("max", "max", empty=True)
        with pytest.raises(YtError):
            create_account("nested", empty=True)
        with pytest.raises(YtError):
            create_account("nested", "max", empty=True)
        with pytest.raises(YtError):
            create_account("child", "fake", empty=True)

    @authors("kiselyovp")
    def test_create4(self):
        create_account("a0", empty=True)
        create_account("a1", empty=True)
        create_account("a2", "a0", empty=True)
        create_account("a3", "a0", empty=True)
        with pytest.raises(YtError):
            create_account("a0", "a1", empty=True)
        with pytest.raises(YtError):
            create_account("a1", "a1", empty=True)
        with pytest.raises(YtError):
            create_account("a2", "a1", empty=True)
        with pytest.raises(YtError):
            create_account("a3", empty=True)

    @authors("kiselyovp")
    def test_create5(self):
        parent_id = create_account("yt", empty=True)
        assert create_account("yt", empty=True, ignore_existing=True) == parent_id
        child_id = create_account("never_mind", "yt", empty=True, ignore_existing=True)
        with pytest.raises(YtError):
            create_account("never_mind", empty=True, ignore_existing=True)
        assert create_account("never_mind", "yt", empty=True, ignore_existing=True) == child_id

    @authors("kiselyovp")
    def test_depth_limit1(self):
        depth_limit = 10
        for i in range(1, depth_limit + 1):
            create_account(str(i), None if i == 1 else str(i - 1), empty=True)

        with pytest.raises(YtError):
            create_account(str(depth_limit + 1), str(depth_limit), empty=True)

    @authors("kiselyovp")
    def test_depth_limit2(self):
        depth_limit = 10
        left_depth = (depth_limit + 1) // 2
        right_depth = depth_limit // 2 + 1

        for i in range(1, left_depth + 1):
            create_account("L" + str(i), None if i == 1 else "L" + str(i - 1), empty=True)
        for i in range(1, right_depth + 1):
            create_account("R" + str(i), None if i == 1 else "R" + str(i - 1), empty=True)
        with pytest.raises(YtError):
            set("//sys/account_tree/R1/@parent_name", "L" + str(left_depth))
        set("//sys/account_tree/R1/@parent_name", "L" + str(left_depth - 1))
        with pytest.raises(YtError):
            create_account("2deep4u", "R" + str(right_depth), empty=True)

    @authors("kiselyovp")
    def test_get(self):
        create_account("max", empty=True)
        create_account("a1", "max", empty=True)
        create_account("a2", "max", empty=True)

        max_subtree = {"a1": {}, "a2": {}}
        root_subtree = {account: {} for account in self._non_root_builtin_accounts}
        root_subtree["max"] = max_subtree

        assert get("//sys/accounts") == {
            account: YsonEntity() for account in self._builtin_accounts + ["max", "a1", "a2"]
        }

        assert get("//sys/accounts/sys") == {}
        assert get("//sys/account_tree/sys") == {}
        assert get("//sys/accounts/a1") == {}
        assert get("//sys/account_tree/max/a1") == {}
        assert get("//sys/accounts/a2") == {}
        assert get("//sys/account_tree/max/a2") == {}
        assert get("//sys/accounts/max") == max_subtree
        assert get("//sys/account_tree/max") == max_subtree
        assert get("//sys/accounts/" + self._root_account_name) == root_subtree
        assert get("//sys/account_tree") == root_subtree

    @authors("kiselyovp")
    def test_get_with_attributes(self):
        create_account("max", empty=True)
        create_account("a1", "max", empty=True)
        create_account("a2", "max", empty=True)

        max_with_attributes = to_yson_type(
            {account: to_yson_type({}, {"name": account, "type": "account"}) for account in ["a1", "a2"]},
            {"name": "max", "type": "account"},
        )
        root_dict = {
            account: to_yson_type({}, {"name": account, "type": "account"})
            for account in self._non_root_builtin_accounts
        }
        root_dict["max"] = max_with_attributes
        root_with_attributes = to_yson_type(root_dict, {"name": self._root_account_name, "type": "account"})
        assert get("//sys/accounts/max", attributes=["name", "type"]) == max_with_attributes
        assert get("//sys/account_tree", attributes=["name", "type"]) == root_with_attributes

    @authors("kiselyovp")
    def test_list_with_attributes(self):
        create_account("max", empty=True)
        create_account("child", "max", empty=True)
        create_account("child2", "max", empty=True)
        create_account("grandchild", "child", empty=True)
        assert ls("//sys/account_tree/max", attributes=["name", "type"]) == [
            to_yson_type(account, {"name": account, "type": "account"}) for account in ["child", "child2"]
        ]

    @authors("kiselyovp")
    def test_set(self):
        with pytest.raises(YtError):
            set("//sys/accounts/" + self._root_account_name, {"key": "value"})
        assert exists("//sys/account_tree/tmp")
        with pytest.raises(YtError):
            set("//sys/accounts/tmp", {"key": "value"}, force=True)
        with pytest.raises(YtError):
            set("//sys/accounts/tmp", {"key1": {"key2": {}}}, recursive=True)

    @authors("kiselyovp")
    def test_remove1(self):
        create_account("max", empty=True)
        create_account("nested", "max", empty=True)
        remove_account("nested", sync=False)
        assert exists("//sys/account_tree/max")
        wait(lambda: not exists("//sys/account_tree/max/nested"))
        with pytest.raises(YtError):
            remove("//sys/account_tree/max/nested")
        remove("//sys/account_tree/max/nested", force=True)

    @authors("kiselyovp")
    def test_remove2(self):
        create_account("max", empty=True)
        create_account("max42", "max", empty=True)
        create_account("max69", "max", empty=True)
        with pytest.raises(YtError):
            remove_account("max", recursive=False)
        remove_account("max")
        assert not exists("//sys/account_tree/max/max42")
        assert not exists("//sys/account_tree/max/max69")

    @authors("kiselyovp")
    def test_remove3(self):
        create_account("max")
        create_account("max42", "max")
        create_account("max69", "max", empty=True)

        create("map_node", "//tmp/max42", attributes={"account": "max42"})
        remove_account("max", recursive=True, force=True, sync=False)
        wait(lambda: not exists("//sys/account_tree/max/max69"))
        assert exists("//sys/account_tree/max/max42")
        assert get("//sys/account_tree/max/@life_stage") == "removal_started"
        with pytest.raises(YtError):
            create_account("child", "max42", empty=True)
        with pytest.raises(YtError):
            create_account("child", "max", empty=True)
        create_account("child", empty=True)
        with pytest.raises(YtError):
            set("//sys/account_tree/child/@parent_name", "max")

        remove("//tmp/max42")
        wait(lambda: not exists("//sys/account_tree/max"))

    @authors("kiselyovp")
    def test_remove4(self):
        create_account("max")
        create_account("a1", "max", empty=True)
        create_account("a2", "max", empty=True)

        remove("//sys/accounts/a1/*")
        assert exists("//sys/accounts/a1")
        remove("//sys/account_tree/max/*")
        wait(lambda: not exists("//sys/account_tree/max/a1"))
        wait(lambda: not exists("//sys/account_tree/max/a2"))

        create_account("a1", "max")
        create_account("a2", "max", empty=True)

        create("table", "//tmp/t", attributes={"account": "a1"})
        remove("//sys/account_tree/max/*")
        wait(lambda: not exists("//sys/account_tree/max/a2"))
        assert exists("//sys/account_tree/max/a1")
        assert get("//sys/accounts/a1/@life_stage") == "removal_started"
        remove("//tmp/t")
        wait(lambda: not exists("//sys/account_tree/max/a1"))

        assert get("//sys/accounts/max/@life_stage") == "creation_committed"

    @authors("kiselyovp")
    def test_rename(self):
        create_account("max")
        create_account("42", "max", empty=True)
        create_account("69", "max")

        with pytest.raises(YtError):
            set("//sys/accounts/42/@name", "")
        with pytest.raises(YtError):
            set("//sys/accounts/42/@name", self._root_account_name)
        with pytest.raises(YtError):
            set("//sys/accounts/42/@name", "max/420")
        with pytest.raises(YtError):
            set("//sys/accounts/42/@name", "69")
        with pytest.raises(YtError):
            set("//sys/accounts/42/@name", "slash/42")
        with pytest.raises(YtError):
            set("//sys/accounts/42/@name", "a" * 101)
        set("//sys/accounts/42/@name", "42")
        assert exists("//sys/account_tree/max/42")
        old_id = get("//sys/accounts/42/@id")
        set("//sys/accounts/42/@name", "420")
        assert get("//sys/accounts/420/@id") == old_id
        assert not exists("//sys/account_tree/max/42")
        assert not exists("//sys/account_tree/420")
        assert exists("//sys/account_tree/max/420")

        create("map_node", "//tmp/max69", attributes={"account": "69"})
        assert get("//tmp/max69/@account") == "69"
        set("//sys/accounts/69/@name", "1337")
        assert not exists("//sys/account_tree/max/69")
        assert exists("//sys/account_tree/max/1337")
        assert get("//tmp/max69/@account") == "1337"

    @authors("kiselyovp")
    def test_move1(self):
        create_account("metrika")
        create("map_node", "//tmp/metrika")
        with pytest.raises(YtError):
            copy("//sys/account_tree/metrika", "//sys/account_tree/market")
        with pytest.raises(YtError):
            move("//tmp/metrika", "//sys/account_tree/metrika/node")
        with pytest.raises(YtError):
            move("//tmp/metrika", "//sys/account_tree/node")
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika", "//tmp/metrika/account")
        with pytest.raises(YtError):
            move("//sys/account_tree", "//tmp/metrika/account")
        with pytest.raises(YtError):
            move("//sys/account_tree", "//sys/account_tree/tmp/tree")

    @authors("kiselyovp")
    def test_move2(self):
        create_account("metrika")
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika", "//sys/account_tree/metrika")
        create_account("prod", "metrika")
        with pytest.raises(YtError):
            move(
                "//sys/account_tree/metrika/prod",
                "//sys/account_tree/metrika-prod",
                force=True,
            )
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika/prod", "//sys/account_tree/tmp", force=True)
        with pytest.raises(YtError):
            move(
                "//sys/account_tree/metrika/prod",
                "//sys/account_tree/metrika-prod",
                recursive=True,
            )
        with pytest.raises(YtError):
            move(
                "//sys/account_tree/metrika",
                "//sys/account_tree/metrika/prod/surprise",
                recursive=True,
            )
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika", "//sys/account_tree/metrika/surprise")
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika", "//sys/account_tree/metrika/prod/surprise")
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika/fake", "//sys/account_tree/fake")

        with pytest.raises(YtError):
            move("//sys/account_tree/metrika/prod", "//sys/account_tree/market/prod")
        create_account("market")
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika/prod", "//sys/account_tree/market")
        assert (
            copy(
                "//sys/account_tree/metrika/prod",
                "//sys/account_tree/market",
                ignore_existing=True,
            )
            == get("//sys/account_tree/market/@id")
        )

        with pytest.raises(YtError):
            move("//sys/account_tree/metrika/prod", "//sys/account_tree/market/market")
        with pytest.raises(YtError):
            move("//sys/account_tree/metrika/prod", "//sys/account_tree/market/tmp")
        with pytest.raises(YtError):
            move("//sys/account_tree/market", "//sys/account_tree/metrika/prod/sys")
        with pytest.raises(YtError):
            move("//sys/account_tree/market", "//sys/account_tree/metrika/sys")
        with pytest.raises(YtError):
            move("//sys/account_tree/market", "//sys/account_tree/tmp/prod")

    @authors("kiselyovp")
    def test_move3(self):
        create_account("metrika")
        create_account("metrika-dev", "metrika")
        create_account("metrika-prod", "metrika", empty=True)
        create_account("market")
        create_account("market-dev", "market")
        create_account("market-prod", "market", empty=True)

        with pytest.raises(YtError):
            move(
                "//sys/account_tree/metrika/metrika-dev",
                "//sys/account_tree/metrika/metrika-prod/0",
            )

        old_id = get("//sys/accounts/market-prod/@id")
        move(
            "//sys/account_tree/market/market-prod",
            "//sys/account_tree/metrika/market-prod",
        )
        assert get("//sys/accounts/market-prod/@id") == old_id
        assert self._get_account_node_count_limit("market-prod") == 0
        assert not exists("//sys/account_tree/market/market-prod")
        assert ls("//sys/account_tree/market") == ["market-dev"]

    @authors("kiselyovp")
    def test_move4(self):
        create_account("metrika")
        create_account("market")
        create_account("metrika-dev", "metrika")
        create("file", "//tmp/f")
        write_file("//tmp/f", b"abacaba")
        set("//tmp/f/@account", "metrika-dev")

        set("//sys/accounts/metrika-dev/@parent_name", "market")
        assert not exists("//sys/account_tree/metrika/metrika-dev")
        assert exists("//sys/account_tree/market/metrika-dev")
        assert get("//tmp/f/@account", "metrika-dev")

        set("//sys/accounts/metrika-dev/@parent_name", self._root_account_name)
        assert not exists("//sys/account_tree/market/metrika-dev")
        assert exists("//sys/account_tree/metrika-dev")
        assert get("//tmp/f/@account", "metrika-dev")

        move("//sys/account_tree/metrika-dev", "//sys/account_tree/metrika/dev")
        assert not exists("//sys/account_tree/metrika-dev")
        assert exists("//sys/account_tree/metrika/dev")
        assert get("//tmp/f/@account", "dev")

    @authors("kiselyovp")
    def test_move_removed_account(self):
        create_account("max")
        create_account("tesuto")
        create("file", "//tmp/file", attributes={"account": "tesuto"})
        remove_account("tesuto", sync=False)
        assert exists("//sys/account_tree/tesuto")

        with pytest.raises(YtError):
            move("//sys/account_tree/max", "//sys/account_tree/tesuto/max")
        set("//sys/accounts/tesuto/@parent_name", "max")
        assert exists("//sys/account_tree/max/tesuto")
        assert not exists("//sys/account_tree/tesuto")
        set("//sys/accounts/tesuto/@name", "test")
        assert get("//tmp/file/@account") == "test"
        assert exists("//sys/account_tree/max/test")

        remove_account("max", sync=False)
        remove("//tmp/file")
        wait(lambda: not exists("//sys/accounts/max"))

    @authors("kiselyovp")
    def test_move_removed_account_fail(self):
        create_account("yt")
        create("map_node", "//tmp/yt", attributes={"account": "yt"})
        remove_account("yt", sync=False)

        create_account("YaMR", empty=True)
        with pytest.raises(YtError):
            move("//sys/account_tree/yt", "//sys/account_tree/YaMR/2.0")

        for cell_index in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            driver = get_driver(cell_index)
            assert not exists("//sys/account_tree/YaMR/yt", driver=driver)
            assert not exists("//sys/account_tree/YaMR/2.0", driver=driver)
            assert exists("//sys/account_tree/yt", driver=driver)
        assert get("//tmp/yt/@account") == "yt"

    @authors("kiselyovp")
    def test_move_child_from_removed_account(self):
        create_account("YaMR")
        create_account("dev", "YaMR")
        create("map_node", "//tmp/dev", attributes={"account": "dev"})

        create_account("yt")
        remove_account("YaMR", sync=False)
        assert get("//sys/accounts/YaMR/@life_stage") == "removal_started"
        assert exists("//sys/accounts/dev")
        set("//sys/accounts/dev/@name", "development")
        assert get("//sys/account_tree/YaMR/@life_stage") == "removal_started"

        set("//sys/account_tree/YaMR/development/@parent_name", "yt")
        wait(lambda: not exists("//sys/accounts/YaMR"))

    @authors("shakurov")
    def test_nested_limits1(self):
        create_account("yt", empty=True)
        self._set_account_node_count_limit("yt", 4)
        create_account("max", "yt", empty=True)
        create_account("min", "yt", empty=True)
        self._set_account_node_count_limit("max", 3)
        self._set_account_node_count_limit("min", 1)

        create("map_node", "//tmp/dir1", attributes={"account": "min"})
        with pytest.raises(YtError):
            create("map_node", "//tmp/dir2", attributes={"account": "min"})
        create("map_node", "//tmp/dir2", attributes={"account": "max"})
        create("map_node", "//tmp/dir3", attributes={"account": "max"})
        create("map_node", "//tmp/dir4", attributes={"account": "max"})
        with pytest.raises(YtError):
            create("map_node", "//tmp/dir5", attributes={"account": "max"})
        with pytest.raises(YtError):
            create("map_node", "//tmp/dir5", attributes={"account": "yt"})

        with pytest.raises(YtError):
            self._set_account_node_count_limit("max", 4)
        set("//sys/account_tree/yt/@allow_children_limit_overcommit", True)
        self._set_account_node_count_limit("max", 4)
        with pytest.raises(YtError):
            self._set_account_node_count_limit("max", 5)

        with pytest.raises(YtError):
            create("map_node", "//tmp/dir5", attributes={"account": "max"})
        remove("//tmp/dir1")
        wait(lambda: get("//sys/accounts/min/@resource_usage/node_count") == 0)
        create("map_node", "//tmp/dir5", attributes={"account": "max"})

        self._set_account_node_count_limit("max", 3)

        assert self._is_account_node_count_limit_violated("max")
        assert not self._is_account_node_count_limit_violated("yt")

    @authors("shakurov")
    def test_nested_limits2(self):
        create_account("yt", empty=True)
        self._set_account_node_count_limit("yt", 4)
        with pytest.raises(YtError):
            create_account("max", "yt", attributes={"resource_limits": {"node_count": 5}})

        create("map_node", "//tmp/yt", attributes={"account": "yt"})

        create_account("min", "yt", attributes={"resource_limits": {"node_count": 1}})
        create("map_node", "//tmp/yt/d1", attributes={"account": "min"})

        create_account("max", "yt", attributes={"resource_limits": {"node_count": 3}})
        create("map_node", "//tmp/yt/d2", attributes={"account": "max"})
        create("map_node", "//tmp/yt/d3", attributes={"account": "max"})
        with pytest.raises(YtError):
            create("map_node", "//tmp/yt/d4", attributes={"account": "max"})

    @authors("shakurov")
    def test_nested_limits3(self):
        create_account("yt", empty=True)
        self._set_account_node_count_limit("yt", 4)

        create_account("min", "yt", attributes={"resource_limits": {"node_count": 1}})
        create("map_node", "//tmp/d1", attributes={"account": "min"})
        with pytest.raises(YtError):
            create("map_node", "//tmp/d2", attributes={"account": "min"})
        move("//sys/account_tree/yt/min", "//sys/account_tree/yt/minimal")
        with pytest.raises(YtError):
            create("map_node", "//tmp/d3", attributes={"account": "minimal"})
        set("//sys/accounts/minimal/@name", "min")
        with pytest.raises(YtError):
            create("map_node", "//tmp/d4", attributes={"account": "min"})

    @authors("shakurov")
    def test_nested_usage(self):
        create_account("yt", attributes={"resource_limits": {"node_count": 10}})
        create_account("yt-dev", "yt", attributes={"resource_limits": {"node_count": 5}})
        create_account("yt-dev-spof", "yt-dev", attributes={"resource_limits": {"node_count": 2}})
        create_account("yt-dev-dt", "yt-dev", empty=True)
        create_account("yt-dev-spof-1", "yt-dev-spof", empty=True)
        create("table", "//tmp/yt", attributes={"account": "yt-dev-spof"})

        wait(
            lambda: get("//sys/accounts/yt/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev-spof/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev-spof-1/@recursive_resource_usage/node_count") == 0
            and get("//sys/accounts/yt-dev-dt/@recursive_resource_usage/node_count") == 0
        )

        remove("//tmp/yt")

        wait(
            lambda: get("//sys/accounts/yt/@recursive_resource_usage/node_count") == 0
            and get("//sys/accounts/yt-dev/@recursive_resource_usage/node_count") == 0
            and get("//sys/accounts/yt-dev-spof/@recursive_resource_usage/node_count") == 0
            and get("//sys/accounts/yt-dev-spof-1/@recursive_resource_usage/node_count") == 0
            and get("//sys/accounts/yt-dev-dt/@recursive_resource_usage/node_count") == 0
        )

    @authors("aleksandra-zh")
    def test_account_tree_master_memory_chunks(self):
        create_account("a")
        create_account("b", "a")

        create("table", "//tmp/t1", attributes={"account": "b"})
        write_table("//tmp/t1", {"a": "b"})

        wait(lambda: self._get_account_tree_master_memory_usage("b") > 0)
        wait(lambda: self._get_account_tree_master_memory_usage("a") > 0)
        wait(lambda: self._get_account_tree_master_memory_usage("b") == self._get_account_tree_master_memory_usage("a"))

    @authors("aleksandra-zh")
    def test_account_tree_master_memory_nodes(self):
        create_account("a")
        create_account("b", "a")

        create("map_node", "//tmp/dir1", attributes={"account": "b"})
        create("table", "//tmp/dir1/t", attributes={"account": "b", "aksdj": "sdkjf"})

        wait(lambda: self._get_account_tree_master_memory_usage("b") > 0)
        wait(lambda: self._get_account_tree_master_memory_usage("a") > 0)
        wait(lambda: self._get_account_tree_master_memory_usage("b") == self._get_account_tree_master_memory_usage("a"))

    @authors("kiselyovp")
    def test_nested_usage2(self):
        create_account(
            "yt",
            attributes={
                "resource_limits": self._build_resource_limits(node_count=100, disk_space=10000, chunk_count=1000)
            },
        )
        self._multiply_account_limits("yt", 4)
        create_account(
            "yt-dev",
            "yt",
            attributes={
                "resource_limits": self._build_resource_limits(node_count=10, disk_space=1000, chunk_count=100)
            },
        )
        self._multiply_account_limits("yt-dev", 2)
        create_account(
            "yt-prod",
            "yt",
            attributes={
                "resource_limits": self._build_resource_limits(node_count=10, disk_space=1000, chunk_count=100)
            },
        )
        create_account(
            "yt-morda",
            "yt-dev",
            attributes={"resource_limits": self._build_resource_limits(node_count=5, disk_space=100, chunk_count=10)},
        )

        with self.WaitForAccountUsage("//tmp/yt", new_account="yt"):
            create("map_node", "//tmp/yt", attributes={"account": "yt"})
        with self.WaitForAccountUsage("//tmp/yt/yt-dev", new_account="yt-dev"):
            create("map_node", "//tmp/yt/yt-dev", attributes={"account": "yt-dev"})
        with self.WaitForAccountUsage("//tmp/yt/yt-prod", new_account="yt-prod"):
            create("map_node", "//tmp/yt/yt-prod", attributes={"account": "yt-prod"})
        with self.WaitForAccountUsage("//tmp/yt/yt-dev/yt-morda", new_account="yt-morda"):
            create(
                "map_node",
                "//tmp/yt/yt-dev/yt-morda",
                attributes={"account": "yt-morda"},
            )
        with self.WaitForAccountUsage("//tmp/yt/file", new_account="yt"):
            create("file", "//tmp/yt/file")
            write_file("//tmp/yt/file", b"abacaba")
        with self.WaitForAccountUsage("//tmp/yt/yt-prod/table", new_account="yt-prod"):
            create("table", "//tmp/yt/yt-prod/table")
            write_table("//tmp/yt/yt-prod/table", {"a": "b"})
        with self.WaitForAccountUsage("//tmp/yt/yt-dev/yt-morda/table", new_account="yt-morda"):
            create("table", "//tmp/yt/yt-dev/yt-morda/table")
        tx = start_transaction()
        with self.WaitForAccountUsage("//tmp/yt/yt-dev/yt-morda/table", tx=tx):
            write_table("//tmp/yt/yt-dev/yt-morda/table", {"a": "b", "c": "d"}, tx=tx)

        def check_recursive_usage(account, descendants):
            for attribute in ["resource_usage", "committed_resource_usage"]:
                usage = {}
                for descendant in descendants:
                    descendant_usage = get("//sys/accounts/{0}/@{1}".format(descendant, attribute))
                    usage = add_recursive(usage, descendant_usage)
                expected_usage = get("//sys/accounts/{0}/@recursive_{1}".format(account, attribute))
                if expected_usage != usage:
                    return False
            return True

        master_memory_sleep()
        check_recursive_usage("yt", ["yt", "yt-dev", "yt-prod", "yt-morda"])
        check_recursive_usage("yt-dev", ["yt-dev", "yt-morda"])
        check_recursive_usage("yt-prod", ["yt-prod"])
        check_recursive_usage("yt-morda", ["yt-morda"])

    @authors("shakurov")
    def test_nested_usage_account_removal(self):
        create_account("yt", attributes={"resource_limits": {"node_count": 10}})
        create_account("yt-dev", "yt", attributes={"resource_limits": {"node_count": 5}})
        create_account("yt-dev-spof", "yt-dev", attributes={"resource_limits": {"node_count": 2}})
        create_account("yt-dev-dt", "yt-dev", empty=True)
        create_account("yt-dev-spof-1", "yt-dev-spof", empty=True)
        create("table", "//tmp/yt", attributes={"account": "yt-dev-spof"})

        wait(
            lambda: get("//sys/accounts/yt/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev-spof/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev-spof-1/@recursive_resource_usage/node_count") == 0
            and get("//sys/accounts/yt-dev-dt/@recursive_resource_usage/node_count") == 0
        )

        remove_account("yt-dev-spof-1")
        remove_account("yt-dev-spof", sync=False)

        wait(
            lambda: get("//sys/accounts/yt/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev/@recursive_resource_usage/node_count") == 1
            and get("//sys/accounts/yt-dev-dt/@recursive_resource_usage/node_count") == 0
        )

    @authors("kiselyovp")
    def test_no_overdraft_after_move(self):
        create_account("metrika", attributes={"resource_limits": {"node_count": 3}})
        create_account("metrika-dev", "metrika", attributes={"resource_limits": {"node_count": 2}})

        create("map_node", "//tmp/metrika", attributes={"account": "metrika"})
        create("map_node", "//tmp/metrika/dev", attributes={"account": "metrika-dev"})
        create("map_node", "//tmp/metrika/dev/webvisor")
        assert not self._is_account_node_count_limit_violated("metrika")
        self._set_account_node_count_limit("metrika", 2)
        assert self._is_account_node_count_limit_violated("metrika")

        move("//sys/account_tree/metrika/metrika-dev", "//sys/account_tree/metrika-dev")
        wait(lambda: not self._is_account_node_count_limit_violated("metrika"))

        create("map_node", "//tmp/metrika/admin")
        wait(lambda: not self._is_account_node_count_limit_violated("metrika"))

    @authors("kiselyovp")
    def test_failed_move_from_overdrafted(self):
        create_account("metrika", attributes={"resource_limits": {"node_count": 2}})
        create_account("metrika-dev", "metrika", attributes={"resource_limits": {"node_count": 1}})

        create("map_node", "//tmp/metrika", attributes={"account": "metrika"})
        create("map_node", "//tmp/metrika-dev", attributes={"account": "metrika-dev"})
        self._set_account_node_count_limit("metrika", 1)

        create_account("empty", empty=True)
        with pytest.raises(YtError):
            set("//sys/accounts/metrika-dev/@parent_name", "empty")
        assert not exists("//sys/account_tree/empty/metrika-dev")
        assert exists("//sys/account_tree/metrika/metrika-dev")
        assert get("//sys/accounts/metrika-dev/@parent_name") == "metrika"

    @authors("kiselyovp")
    def test_read_acl(self):
        create_user("u")
        create_account("parent", empty=True)
        get("//sys/accounts/parent", authenticated_user="u")
        set("//sys/accounts/parent/@acl/end", make_ace("deny", "u", "read"))
        with pytest.raises(YtError):
            get("//sys/accounts/parent", authenticated_user="u")
        create_account("child", "parent", empty=True)
        with pytest.raises(YtError):
            get("//sys/accounts/child", authenticated_user="u")

    @authors("kiselyovp")
    def test_write_acl(self):
        create_user("u")
        create_account("parent", empty=True)
        with pytest.raises(YtError):
            create_account("child", "parent", empty=True, authenticated_user="u")
        with pytest.raises(YtError):
            set(
                "//sys/accounts/parent/@resource_limits/node_count",
                1,
                authenticated_user="u",
            )

        set("//sys/accounts/parent/@acl/end", make_ace("allow", "u", "write"))
        set(
            "//sys/accounts/parent/@resource_limits/node_count",
            1,
            authenticated_user="u",
        )
        create_account(
            "child",
            "parent",
            attributes={"resource_limits": {"node_count": 1}},
            authenticated_user="u",
        )

        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/u",
                attributes={"account": "parent"},
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/u",
                attributes={"account": "child"},
                authenticated_user="u",
            )

    @authors("kiselyovp")
    def test_modify_children_acl(self):
        create_user("u")
        create_account(
            "parent",
            empty=True,
            attributes={"acl": [make_ace("allow", "u", "modify_children")]},
        )
        with pytest.raises(YtError):
            set(
                "//sys/accounts/parent/@resource_limits/node_count",
                1,
                authenticated_user="u",
            )
        set("//sys/accounts/parent/@resource_limits/node_count", 1)

        create_account(
            "child",
            "parent",
            attributes={"resource_limits": {"node_count": 1}},
            authenticated_user="u",
        )

        with pytest.raises(YtError):
            set(
                "//sys/accounts/child/@resource_limits/node_count",
                0,
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/u",
                attributes={"account": "parent"},
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/u",
                attributes={"account": "child"},
                authenticated_user="u",
            )

    @authors("kiselyovp")
    def test_administer_acl(self):
        create_user("u1")
        create_user("u2")
        create_account(
            "parent",
            attributes={
                "acl": [make_ace("allow", "u1", "administer")],
                "resource_limits": {"node_count": 1000},
            },
        )
        with pytest.raises(YtError):
            set(
                "//sys/accounts/parent/@acl/end",
                make_ace("allow", "u2", "use"),
                authenticated_user="u2",
            )
        with pytest.raises(YtError):
            create_account("child", "parent", authenticated_user="u1")
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/u1",
                attributes={"account": "parent"},
                authenticated_user="u1",
            )

        set(
            "//sys/accounts/parent/@acl",
            self._create_account_acl("u1"),
            authenticated_user="u1",
        )
        create(
            "map_node",
            "//tmp/u1",
            attributes={"account": "parent"},
            authenticated_user="u1",
        )
        # administer permission for parent is needed to create a child with acl
        with pytest.raises(YtError):
            create_account(
                "child",
                "parent",
                empty=True,
                attributes={"acl": [make_ace("allow", "u2", "use")]},
                authenticated_user="u1",
            )
        create_account(
            "child",
            "parent",
            attributes={"resource_limits": {"node_count": 1}},
            authenticated_user="u1",
        )
        set("//sys/accounts/child/@acl/end", make_ace("allow", "u2", "use"))
        create(
            "map_node",
            "//tmp/u2",
            attributes={"account": "child"},
            authenticated_user="u2",
        )

    @authors("kiselyovp")
    def test_remove_acl(self):
        create_user("u")
        create_account("parent", attributes={"acl": [make_ace("allow", "u", "remove")]})
        create_account("child", "parent", empty=True)

        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/u",
                attributes={"account": "parent"},
                authenticated_user="u",
            )
        # write or modify_children permission for parent of the account being removed
        with pytest.raises(YtError):
            remove_account("parent", authenticated_user="u")
        with pytest.raises(YtError):
            remove_account("child", authenticated_user="u")
        set("//sys/accounts/parent/@acl/end", make_ace("allow", "u", "modify_children"))
        remove_account("child", authenticated_user="u")

        set("//sys/accounts/parent/@acl", [make_ace("allow", "u", ["write"])])
        create_account("child", "parent", empty=True)
        with pytest.raises(YtError):
            remove_account("child", authenticated_user="u")
        set("//sys/accounts/child/@acl/end", make_ace("allow", "u", ["remove"]))
        remove_account("child", authenticated_user="u")

    @authors("kiselyovp")
    def test_nested_acls1(self):
        create_user("user")
        with pytest.raises(YtError):
            create_account("account", "tmp", empty=True, authenticated_user="user")

    # XXX(kiselyovp) these tests are too large
    # XXX(kiselyovp) a decorator to inject authenticated_user into commands
    @authors("kiselyovp")
    def test_nested_acls2(self):
        create_user("babenko")
        create_user("max42")
        create_user("kiselyovp")

        with pytest.raises(YtError):
            create_account("logozhuj", empty=True, authenticated_user="babenko")
        create_account("yt", empty=True, attributes={"acl": self._create_account_acl("babenko")})
        with pytest.raises(YtError):
            set("//sys/accounts/yt/@name", "logozhuj", authenticated_user="babenko")
        with pytest.raises(YtError):
            remove_account("yt", authenticated_user="babenko")
        create_account("max42", "yt", empty=True, authenticated_user="babenko")
        set(
            "//sys/account_tree/yt/max42/@acl",
            self._create_account_acl("max42"),
            authenticated_user="babenko",
        )
        create_account("kiselyovp", "yt", empty=True, authenticated_user="babenko")
        set(
            "//sys/account_tree/yt/kiselyovp/@acl",
            self._create_account_acl("kiselyovp"),
            authenticated_user="babenko",
        )

        with pytest.raises(YtError):
            set(
                "//sys/account_tree/yt/max42/@parent_name",
                "kiselyovp",
                authenticated_user="kiselyovp",
            )
        with pytest.raises(YtError):
            remove_account("max42", authenticated_user="kiselyovp")
        with pytest.raises(YtError):
            remove_account("kiselyovp", authenticated_user="kiselyovp")
        with pytest.raises(YtError):
            remove_account("yt", authenticated_user="kiselyovp")
        assert sorted(ls("//sys/account_tree/yt", authenticated_user="kiselyovp")) == [
            "kiselyovp",
            "max42",
        ]
        with pytest.raises(YtError):
            create_account("surprise", "max42", empty=True, authenticated_user="kiselyovp")

        create_account("tesuto", "kiselyovp", empty=True, authenticated_user="kiselyovp")
        with pytest.raises(YtError):
            set(
                "//sys/account_tree/yt/kiselyovp/@acl/end",
                make_ace("allow", "max42", "use"),
                authenticated_user="kiselyovp",
            )
        create_account("empty", "tesuto", empty=True, authenticated_user="kiselyovp")
        set(
            "//sys/accounts/tesuto/@acl/end",
            make_ace("deny", "max42", "read"),
            authenticated_user="kiselyovp",
        )
        with pytest.raises(YtError):
            get("//sys/accounts/tesuto", authenticated_user="max42")
        with pytest.raises(YtError):
            get(
                "//sys/account_tree/yt/kiselyovp/tesuto/empty",
                authenticated_user="max42",
            )
        set("//sys/accounts/tesuto/@acl", [], authenticated_user="babenko")
        remove_account("tesuto", authenticated_user="kiselyovp")

        remove_account("max42", authenticated_user="babenko")
        remove_account("kiselyovp", authenticated_user="babenko")

    @authors("kiselyovp")
    def test_nested_acls3(self):
        create_user("babenko")
        create_user("kiselyovp")

        create_account("yt", empty=True, attributes={"acl": self._create_account_acl("babenko")})
        create_account("yt-dev", "yt", empty=True, authenticated_user="babenko")
        create_account(
            "yt-tests",
            "yt-dev",
            empty=True,
            attributes={"acl": [make_ace("deny", "kiselyovp", "read")]},
            authenticated_user="babenko",
        )
        with pytest.raises(YtError):
            get("//sys/account_tree/yt/yt-dev/yt-tests", authenticated_user="kiselyovp")

    @authors("kiselyovp")
    def test_nested_acls4(self):
        create_user("babenko")
        create_user("renadeen")
        create_user("andozer")

        create_account("yt", empty=True)
        self._set_account_node_count_limit("yt", 15)

        set("//sys/account_tree/yt/@acl", self._create_account_acl("babenko"))
        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt", 100500, authenticated_user="babenko")

        create_account(
            "yt-dev",
            "yt",
            authenticated_user="babenko",
            attributes={"resource_limits": {"node_count": 4}},
        )
        create_account(
            "yt-prod",
            "yt",
            authenticated_user="babenko",
            attributes={"resource_limits": {"node_count": 8}},
        )
        set(
            "//sys/account_tree/yt/yt-dev/@acl",
            self._create_account_acl("renadeen") + [make_ace("allow", "renadeen", "use")],
            authenticated_user="babenko",
        )
        set(
            "//sys/account_tree/yt/yt-prod/@acl",
            self._create_account_acl(["renadeen", "andozer"]) + [make_ace("allow", ["renadeen", "andozer"], "use")],
            authenticated_user="babenko",
        )

        create(
            "map_node",
            "//tmp/yt",
            attributes={"acl": [make_ace("allow", "everyone", ["read", "write", "remove"])]},
        )
        set("//tmp/yt/@account", "yt", authenticated_user="babenko")
        create(
            "map_node",
            "//tmp/yt/renadeen",
            attributes={"account": "yt-dev"},
            authenticated_user="renadeen",
        )
        create(
            "map_node",
            "//tmp/yt/andozer",
            attributes={"account": "yt-prod"},
            authenticated_user="andozer",
        )

        create("map_node", "//tmp/yt/renadeen/never_mind")
        create_account(
            "huj",
            "yt-dev",
            authenticated_user="renadeen",
            attributes={"resource_limits": {"node_count": 1}},
        )
        set(
            "//tmp/yt/renadeen/never_mind/@account",
            "huj",
            authenticated_user="renadeen",
        )

        with pytest.raises(YtError):
            set(
                "//tmp/yt/renadeen/never_mind/@account",
                "yt",
                authenticated_user="renadeen",
            )
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/yt/renadeen/work",
                attributes={"account": "huj"},
                authenticated_user="renadeen",
            )
        create_account(
            "kurwa",
            "yt-dev",
            empty=True,
            authenticated_user="renadeen",
            attributes={"resource_limits": {"node_count": 2}},
        )
        create(
            "map_node",
            "//tmp/yt/renadeen/work",
            attributes={"account": "kurwa"},
            authenticated_user="renadeen",
        )

        with pytest.raises(YtError):
            set("//sys/accounts/kurwa/@name", "work", authenticated_user="andozer")
        with pytest.raises(YtError):
            create_account("work", "yt-dev", empty=True, authenticated_user="andozer")
        with pytest.raises(YtError):
            remove_account("huj", authenticated_user="andozer")

        set("//sys/accounts/kurwa/@name", "work", authenticated_user="babenko")
        assert get("//tmp/yt/renadeen/work/@account") == "work"
        remove_account("huj", authenticated_user="babenko", sync=False)
        remove("//tmp/yt/renadeen/never_mind")
        wait(lambda: not exists("//sys/accounts/huj"))
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/yt/babenko",
                attributes={"account": "work"},
                authenticated_user="babenko",
            )
        set("//sys/account_tree/yt/yt-dev/@acl/end", make_ace("allow", "babenko", "use"))
        create(
            "map_node",
            "//tmp/yt/babenko",
            attributes={"account": "work"},
            authenticated_user="babenko",
        )

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt-prod", 10, authenticated_user="andozer")
        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt", 100500, authenticated_user="andozer")
        with pytest.raises(YtError):
            move(
                "//sys/account_tree/yt/yt-dev/work",
                "//sys/account_tree/yt/yt-prod/work",
                authenticated_user="andozer",
            )
        create_account(
            "interface",
            "yt-prod",
            authenticated_user="andozer",
            attributes={"resource_limits": {"node_count": 2}},
        )
        self._set_account_node_count_limit("interface", 1, authenticated_user="andozer")
        create(
            "map_node",
            "//tmp/yt/andozer/interface",
            attributes={"account": "interface"},
            authenticated_user="andozer",
        )
        set(
            "//sys/accounts/interface/@parent_name",
            "yt-dev",
            authenticated_user="renadeen",
        )
        remove("//tmp/yt/andozer/interface")
        with pytest.raises(YtError):
            create(
                "map_node",
                "//tmp/yt/andozer/interface",
                attributes={"account": "interface"},
                authenticated_user="andozer",
            )
        wait(lambda: self._get_account_node_count("interface") == 0)
        create(
            "map_node",
            "//tmp/yt/renadeen/morda",
            attributes={"account": "interface"},
            authenticated_user="renadeen",
        )
        with pytest.raises(YtError):
            move(
                "//sys/account_tree/yt/yt-dev/interface",
                "//sys/account_tree/yt/yt-prod/interface",
                authenticated_user="andozer",
            )
        move(
            "//sys/account_tree/yt/yt-dev/interface",
            "//sys/account_tree/yt/yt-prod/morda",
            authenticated_user="babenko",
        )

    @authors("kiselyovp")
    def test_parent_name_and_path(self):
        create_account("yt", empty=True)
        create_account("yt-dev", "yt", empty=True)
        create_account("yt-tesuto", "yt-dev", empty=True)

        with pytest.raises(YtError):
            get("//sys/accounts/{0}/@parent_name".format(self._root_account_name))
        assert get("//sys/accounts/{0}/@path".format(self._root_account_name)) == "//sys/account_tree"
        assert get("//sys/accounts/yt/@parent_name") == self._root_account_name
        assert get("//sys/accounts/yt/@path") == "//sys/account_tree/yt"
        assert get("//sys/accounts/yt-dev/@parent_name") == "yt"
        assert get("//sys/accounts/yt-dev/@path") == "//sys/account_tree/yt/yt-dev"
        assert get("//sys/accounts/yt-tesuto/@parent_name") == "yt-dev"
        assert get("//sys/accounts/yt-tesuto/@path") == "//sys/account_tree/yt/yt-dev/yt-tesuto"

        create_account("yt-prod", "yt", empty=True)
        move(
            "//sys/account_tree/yt/yt-dev/yt-tesuto",
            "//sys/account_tree/yt/yt-prod/yt-test",
        )
        assert get("//sys/accounts/yt-test/@parent_name") == "yt-prod"
        assert get("//sys/accounts/yt-test/@path") == "//sys/account_tree/yt/yt-prod/yt-test"

    @authors("akozhikhov")
    def test_error_upon_resource_limit_violation(self):
        create_domestic_medium("nvme1")

        create_account("yt", attributes={"resource_limits": {"disk_space_per_medium": {"default": 1}}})
        create_account("yt2", attributes={"resource_limits": {"disk_space_per_medium": {"default": 2, "nvme1": 1}}})

        with raises_yt_error("violated_resources {'disk_space_per_medium': {'default': 1, 'nvme1': 1}}"):
            set("//sys/account_tree/yt2/@parent_name", "yt")

        set("//sys/accounts/yt/@resource_limits/disk_space_per_medium", {"default": 2})
        with raises_yt_error("violated_resources {'disk_space_per_medium': {'nvme1': 1}}"):
            set("//sys/account_tree/yt2/@parent_name", "yt")

        set("//sys/accounts/yt/@resource_limits/disk_space_per_medium", {"default": 1, "nvme1": 1})
        with raises_yt_error("violated_resources {'disk_space_per_medium': {'default': 1}}"):
            set("//sys/account_tree/yt2/@parent_name", "yt")

        set("//sys/accounts/yt/@resource_limits/disk_space_per_medium", {"default": 2, "nvme1": 1})
        set("//sys/account_tree/yt2/@parent_name", "yt")

    @authors("shakurov")
    def test_recursive_violated_resource_limits(self):
        create_domestic_medium("hdd7")
        limits_x = self._build_resource_limits(
            node_count=1000,
            chunk_count=1000,
            tablet_count=1000,
            tablet_static_memory=100500,
            disk_space=100500,
        )
        limits_2x = self._build_resource_limits(
            node_count=2000,
            chunk_count=2000,
            tablet_count=2000,
            tablet_static_memory=201000,
            disk_space=201000,
        )
        create_account("yt", attributes={"resource_limits": limits_2x})
        create_account("yt-dev", "yt", attributes={"resource_limits": limits_x})
        create_account("yt-dyntables", "yt-dev", attributes={"resource_limits": limits_x})
        create_account("yt-prod", "yt", attributes={"resource_limits": limits_x})

        no_violated_limits = self._build_resource_limits()
        no_violated_limits["disk_space_per_medium"] = {"default": 0}

        assert get("//sys/accounts/yt/@recursive_violated_resource_limits") == no_violated_limits
        assert get("//sys/accounts/yt-dev/@recursive_violated_resource_limits") == no_violated_limits
        assert get("//sys/accounts/yt-dyntables/@recursive_violated_resource_limits") == no_violated_limits
        assert get("//sys/accounts/yt-prod/@recursive_violated_resource_limits") == no_violated_limits

        create("map_node", "//tmp/yt", attributes={"account": "yt"})
        create("map_node", "//tmp/yt-dev", attributes={"account": "yt-dev"})
        create("map_node", "//tmp/yt-prod", attributes={"account": "yt-prod"})

        # yt-dyntables
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/yt-dev/dynamic",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            account="yt-dyntables",
        )
        sync_mount_table("//tmp/yt-dev/dynamic")
        insert_rows("//tmp/yt-dev/dynamic", [{"key": 0, "value": "0"}])
        sync_unmount_table("//tmp/yt-dev/dynamic")

        set("//tmp/yt-dev/dynamic/@in_memory_mode", "uncompressed")
        sync_mount_table("//tmp/yt-dev/dynamic")
        wait(lambda: get("//sys/accounts/yt-dyntables/@resource_usage/tablet_static_memory") != 0)

        # violate tablet count and tablet static memory limits for yt-dyntables
        set("//sys/accounts/yt-dyntables/@resource_limits/tablet_count", 0)
        set("//sys/accounts/yt-dyntables/@resource_limits/tablet_static_memory", 0)

        assert get("//sys/accounts/yt-dyntables/@recursive_violated_resource_limits") == {
            "node_count": 0,
            "chunk_count": 0,
            "tablet_count": 1,
            "tablet_static_memory": 1,
            "disk_space_per_medium": {"default": 0},
            "master_memory":
            {
                "total": 0,
                "chunk_host": 0,
                "per_cell": {}
            }
        }

        # yt-dev

        # violate node, chunk and default medium disk space limits for yt-dev
        create("table", "//tmp/yt-dev/table")
        write_table("//tmp/yt-dev/table", {"a": "b"})
        self._set_account_node_count_limit("yt-dyntables", 0)
        self._set_account_node_count_limit("yt-dev", 0)
        self._set_account_chunk_count_limit("yt-dyntables", 0)
        self._set_account_chunk_count_limit("yt-dyntables", 0)
        set_account_disk_space_limit("yt-dyntables", 0)
        set_account_disk_space_limit("yt-dev", 0)

        wait(
            lambda: get("//sys/accounts/yt-dev/@recursive_violated_resource_limits")
            == {
                "node_count": 2,
                "chunk_count": 1,
                "tablet_count": 1,
                "tablet_static_memory": 1,
                "disk_space_per_medium": {"default": 2},
                "master_memory":
                {
                    "total": 0,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            }
        )

        # yt-prod

        create("file", "//tmp/yt-prod/file")
        write_file("//tmp/yt-prod/file", b"abacaba")
        set("//tmp/yt-prod/file/@primary_medium", "hdd7")

        # violate hdd7 disk space limit for yt-prod
        set_account_disk_space_limit("yt-prod", 0, "hdd7")

        wait(
            lambda: get("//sys/accounts/yt-prod/@recursive_violated_resource_limits")
            == {
                "node_count": 0,
                "chunk_count": 0,
                "tablet_count": 0,
                "tablet_static_memory": 0,
                "disk_space_per_medium": {"hdd7": 1, "default": 0},
                "master_memory":
                {
                    "total": 0,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            }
        )

        # yt

        # violate hdd7 disk space limit for yt
        copy("//tmp/yt-prod/file", "//tmp/yt/file", pessimistic_quota_check=False)
        # violate node count limit for yt
        self._set_account_node_count_limit("yt-prod", 0)
        self._set_account_node_count_limit("yt", 0)

        wait(
            lambda: get("//sys/accounts/yt/@recursive_violated_resource_limits")
            == {
                "node_count": 4,
                "chunk_count": 1,
                "tablet_count": 1,
                "tablet_static_memory": 1,
                "disk_space_per_medium": {"default": 2, "hdd7": 2},
                "master_memory":
                {
                    "total": 0,
                    "chunk_host": 0,
                    "per_cell": {}
                }
            }
        )

    @authors("shakurov")
    @pytest.mark.parametrize("allow_overcommit", [False, True])
    def test_single_child_overcommit_impossible(self, allow_overcommit):
        create_account(
            "yt",
            attributes={
                "resource_limits": {"node_count": 1},
                "allow_children_limit_overcommit": allow_overcommit,
            },
        )
        with pytest.raises(YtError):
            create_account("yt-dev", "yt", attributes={"resource_limits": {"node_count": 2}})
        create_account("yt-dev", "yt", attributes={"resource_limits": {"node_count": 1}})
        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt-dev", 2)
        set("//sys/accounts/yt/@allow_children_limit_overcommit", not allow_overcommit)
        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt-dev", 2)

    @authors("shakurov")
    def test_allow_children_limit_overcommit_validation(self):
        limits = {"node_count": 1}
        create_account(
            "yt",
            attributes={
                "resource_limits": limits,
                "allow_children_limit_overcommit": True,
            },
        )
        create_account("yt-dev", "yt", attributes={"resource_limits": limits})
        create_account("yt-front", "yt", attributes={"resource_limits": limits})
        with pytest.raises(YtError):
            set("//sys/accounts/yt/@allow_children_limit_overcommit", False)

        self._set_account_node_count_limit("yt-front", 0)
        set("//sys/accounts/yt/@allow_children_limit_overcommit", True)

    @authors("shakurov")
    @pytest.mark.parametrize("allow_overcommit", [False, True])
    def test_move_overcommit_impossible_single_child(self, allow_overcommit):
        create_account(
            "yt",
            attributes={
                "resource_limits": {"node_count": 1},
                "allow_children_limit_overcommit": allow_overcommit,
            },
        )
        create_account("yt2", attributes={"resource_limits": {"node_count": 2}})

        with pytest.raises(YtError):
            set("//sys/account_tree/yt2/@parent_name", "yt")

        self._set_account_node_count_limit("yt2", 1)

        set("//sys/account_tree/yt2/@parent_name", "yt")

    @authors("shakurov")
    def test_move_overcommit_impossible_children_sum(self):
        create_account("yt", attributes={"resource_limits": {"node_count": 2}})
        create_account("yt-dev", "yt", attributes={"resource_limits": {"node_count": 1}})
        create_account("yt2", attributes={"resource_limits": {"node_count": 3}})

        with pytest.raises(YtError):
            move("//sys/account_tree/yt2", "//sys/account_tree/yt")
        set("//sys/accounts/yt/@allow_children_limit_overcommit", True)
        with pytest.raises(YtError):
            set("//sys/account_tree/yt2/@parent_name", "yt")

        self._set_account_node_count_limit("yt2", 2)
        set("//sys/account_tree/yt2/@parent_name", "yt")

    @authors("shakurov")
    def test_parent_overdraft(self):
        limits = {"node_count": 1}
        create_account(
            "yt",
            attributes={
                "resource_limits": limits,
                "allow_children_limit_overcommit": True,
            },
        )
        create_account("yt-dev", "yt", attributes={"resource_limits": limits})
        create_account("yt-front", "yt", attributes={"resource_limits": limits})
        create("map_node", "//tmp/d1", attributes={"account": "yt-front"})

        with pytest.raises(YtError):
            create("map_node", "//tmp/d2", attributes={"account": "yt-dev"})

    @authors("shakurov")
    def test_ancestor_overdraft(self):
        limits = {"node_count": 1}
        create_account(
            "yt",
            attributes={
                "resource_limits": limits,
                "allow_children_limit_overcommit": True,
            },
        )
        create_account("yt-dev", "yt", attributes={"resource_limits": limits})
        create_account("yt-dt", "yt-dev", attributes={"resource_limits": limits})
        create_account("yt-front", "yt", attributes={"resource_limits": limits})
        create("map_node", "//tmp/d1", attributes={"account": "yt-front"})

        with pytest.raises(YtError):
            create("map_node", "//tmp/d2", attributes={"account": "yt-dt"})

    @authors("shakurov")
    def test_change_limits_overcommit_impossible(self):
        create_account("yt", attributes={"resource_limits": {"node_count": 4}})
        create_account("yt-dev", "yt", attributes={"resource_limits": {"node_count": 2}})
        create_account("yt-front", "yt", attributes={"resource_limits": {"node_count": 2}})

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt-dev", 5)

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt-dev", 3)

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt", 1)

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt", 2)

        set("//sys/accounts/yt/@allow_children_limit_overcommit", True)

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt-dev", 5)

        self._set_account_node_count_limit("yt-dev", 3)

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt", 1)

        self._set_account_node_count_limit("yt", 3)

    @authors("aleksandra-zh")
    def test_enable_master_memory_overcommit(self):
        set(
            "//sys/@config/security_manager/enable_master_memory_usage_account_overcommit_validation",
            False,
        )

        create_account("a", attributes={"resource_limits": {"master_memory": {"total": 10000}}})
        create_account("b", "a", attributes={"resource_limits": {"master_memory": {"total": 5000}}})
        create_account("c", "a", attributes={"resource_limits": {"master_memory": {"total": 6000}}})

        with pytest.raises(YtError):
            self._set_account_master_memory("a", 1000)

        self._set_account_master_memory("b", 10000)

        self._set_account_master_memory("b", 5000)
        self._set_account_master_memory("c", 5000)

        set(
            "//sys/@config/security_manager/enable_master_memory_usage_account_overcommit_validation",
            True,
        )

        with pytest.raises(YtError):
            self._set_account_master_memory("b", 10000)

    @authors("shakurov")
    def test_negative_limits(self):
        with pytest.raises(YtError):
            create_account(
                "yt",
                attributes={"resource_limits": self._build_resource_limits(node_count=-1)},
            )

        create_account("yt", attributes={"resource_limits": {"node_count": 1}})

        with pytest.raises(YtError):
            self._set_account_node_count_limit("yt", -1)

    @authors("kiselyovp")
    def test_overcommit_disk_space_with_zero_limit(self):
        create_domestic_medium("hdd8")
        limits = {"disk_space_per_medium": {"hdd8": 1024}}
        create_account("yt", attributes={"resource_limits": {"disk_space": 1000}})

        with pytest.raises(YtError):
            create_account("yt-dev", "yt", attributes={"resource_limits": limits})

        create_account("yt-dev", attributes={"resource_limits": limits})
        with pytest.raises(YtError):
            set("//sys/accounts/yt-dev/@parent_name", "yt")

        create_account("yt-prod", "yt", empty=True)
        with pytest.raises(YtError):
            set_account_disk_space_limit("yt-prod", 1024, "hdd8")

    @authors("kiselyovp")
    def test_transfer_account_resources1(self):
        limits1 = self._build_resource_limits(
            node_count=10,
            chunk_count=40,
            disk_space=9001,
            tablet_count=5,
            tablet_static_memory=1000,
            master_memory=50000,
            include_disk_space=True,
        )
        limits2 = self._build_resource_limits(
            node_count=5,
            chunk_count=20,
            disk_space=1000,
            master_memory=40000,
            include_disk_space=True,
        )
        limits = add_recursive(limits1, limits2)
        create_account("yt-dev", attributes={"resource_limits": limits})
        create_account("yt-prod", attributes={"resource_limits": limits2})

        transfer_account_resources("yt-dev", "yt-prod", limits1)
        assert get("//sys/accounts/yt-dev/@resource_limits") == limits2
        assert get("//sys/accounts/yt-prod/@resource_limits") == limits

        transfer_account_resources("yt-prod", "yt-prod", {"node_count": 5, "chunk_count": 10})
        assert get("//sys/accounts/yt-prod/@resource_limits") == limits

        transfer_account_resources("yt-prod", "yt-dev", limits1)
        assert get("//sys/accounts/yt-dev/@resource_limits") == limits
        assert get("//sys/accounts/yt-prod/@resource_limits") == limits2

    @authors("kiselyovp")
    def test_transfer_account_resources2(self):
        limits = self._build_resource_limits(
            node_count=10,
            chunk_count=40,
            disk_space=9001,
            tablet_count=5,
            tablet_static_memory=1000,
            include_disk_space=True,
        )
        create_account("yt-dev", attributes={"resource_limits": limits})
        create_account("yt-prod", empty=True)

        with pytest.raises(YtError):
            transfer_account_resources("yt-dev", "yt-prod", "entire_hahn")
        for resource in (
            "node_count",
            "chunk_count",
            "tablet_count",
            "tablet_static_memory",
            "disk_space",
        ):
            with pytest.raises(YtError):
                transfer_account_resources("yt-dev", "yt-prod", self._build_resource_limits(**{resource: -1}))
            limit = get("//sys/accounts/yt-dev/@resource_limits/{0}".format(resource))
            with pytest.raises(YtError):
                transfer_account_resources(
                    "yt-dev",
                    "yt-prod",
                    self._build_resource_limits(**{resource: limit + 1}),
                )
            transfer_account_resources("yt-dev", "yt-prod", self._build_resource_limits(**{resource: limit}))

        with pytest.raises(YtError):
            create("map_node", "//tmp/test", attributes={"account": "yt-dev"})
        assert cluster_resources_equal(
            get("//sys/accounts/yt-dev/@resource_limits"),
            self._build_resource_limits(include_disk_space=True),
        )
        assert cluster_resources_equal(get("//sys/accounts/yt-prod/@resource_limits"), limits)

    @authors("kiselyovp")
    def test_transfer_account_resources3(self):
        create_account("yt")
        create_account("parent")
        create_account("child", "parent")

        with pytest.raises(YtError):
            transfer_account_resources("fake", "parent", {})
        with pytest.raises(YtError):
            transfer_account_resources("child", "fake", {})
        with pytest.raises(YtError):
            transfer_account_resources("parent/child", "yt", {})

    @authors("kiselyovp")
    def test_transfer_account_resources4(self):
        create_account("parent", attributes={"resource_limits": {"node_count": 15}})
        create_account("child", "parent", attributes={"resource_limits": {"node_count": 10}})
        create_account("grandchild", "child", attributes={"resource_limits": {"node_count": 5}})

        def validate_node_counts(parent, child, grandchild):
            for account, node_count in [
                ("parent", parent),
                ("child", child),
                ("grandchild", grandchild),
            ]:
                assert cluster_resources_equal(
                    get("//sys/accounts/{0}/@resource_limits".format(account)),
                    self._build_resource_limits(node_count=node_count, include_disk_space=True),
                )

        with pytest.raises(YtError):
            transfer_account_resources("grandchild", "parent", {"node_count": 6})
        with pytest.raises(YtError):
            transfer_account_resources("child", "parent", {"node_count": 6})
        with pytest.raises(YtError):
            transfer_account_resources("parent", "child", {"node_count": 6})
        with pytest.raises(YtError):
            transfer_account_resources("parent", "grandchild", {"node_count": 6})
        validate_node_counts(15, 10, 5)

        transfer_account_resources("parent", "child", {"node_count": 2})
        validate_node_counts(15, 12, 5)
        transfer_account_resources("parent", "grandchild", {"node_count": 3})
        validate_node_counts(15, 15, 8)
        transfer_account_resources("grandchild", "child", {"node_count": 2})
        validate_node_counts(15, 15, 6)
        transfer_account_resources("grandchild", "parent", {"node_count": 5})
        validate_node_counts(15, 10, 1)

    @authors("kiselyovp")
    def test_transfer_account_resources5(self):
        create_account("yt-dev", attributes={"resource_limits": {"node_count": 5}})
        create_account("yt-prod", attributes={"resource_limits": {"node_count": 5}})

        create("map_node", "//tmp/test", attributes={"account": "yt-dev"})
        remove_account("yt-dev", sync=False)

        with pytest.raises(YtError):
            transfer_account_resources("yt-dev", "yt-prod", {"node_count": 4})

        remove("//tmp/test")
        wait(lambda: not exists("//sys/accounts/yt-dev"))
        with pytest.raises(YtError):
            transfer_account_resources("yt-dev", "yt-prod", {})
        with pytest.raises(YtError):
            transfer_account_resources("yt-prod", "yt-dev", {})

    @authors("kiselyovp")
    def test_transfer_account_resources6(self):
        create_account(
            "yt",
            attributes={
                "resource_limits": {"node_count": 16},
                "allow_children_limit_overcommit": True,
            },
        )
        create_account(
            "yt-dev",
            "yt",
            attributes={
                "resource_limits": {"node_count": 12},
                "allow_children_limit_overcommit": True,
            },
        )
        create_account(
            "yt-master",
            "yt-dev",
            attributes={
                "resource_limits": {"node_count": 8},
                "allow_children_limit_overcommit": True,
            },
        )
        create_account(
            "yt-cypress-server",
            "yt-master",
            attributes={"resource_limits": {"node_count": 6}},
        )
        create_account(
            "yt-security-server",
            "yt-master",
            attributes={"resource_limits": {"node_count": 6}},
        )
        create_account(
            "yt-scheduler",
            "yt-dev",
            attributes={
                "resource_limits": {"node_count": 10},
                "allow_children_limit_overcommit": True,
            },
        )
        create_account("yt-front", "yt", attributes={"resource_limits": {"node_count": 10}})
        create_account("yt-morda", "yt-front", attributes={"resource_limits": {"node_count": 8}})

        with pytest.raises(YtError):
            transfer_account_resources("yt-master", "yt-morda", {"node_count": 3})

        self._set_account_node_count_limit("yt-cypress-server", 4)
        self._set_account_node_count_limit("yt-security-server", 4)
        set("//sys/accounts/yt-master/@allow_children_limit_overcommit", False)
        with pytest.raises(YtError):
            transfer_account_resources("yt-master", "yt-morda", {"node_count": 1})

        limits_backup = get("//sys/account_tree", attributes=["name", "resource_limits"])
        with pytest.raises(YtError):
            transfer_account_resources("yt-security-server", "yt-front", {"node_count": 3})
        assert get("//sys/account_tree", attributes=["name", "resource_limits"]) == limits_backup

        self._set_account_node_count_limit("yt-front", 15)
        limits_backup = get("//sys/account_tree", attributes=["name", "resource_limits"])
        with pytest.raises(YtError):
            transfer_account_resources("yt-security-server", "yt-morda", {"node_count": 2})
        assert get("//sys/account_tree", attributes=["name", "resource_limits"]) == limits_backup

        self._set_account_node_count_limit("yt-security-server", 3)
        transfer_account_resources("yt-master", "yt-morda", {"node_count": 1})
        for account, node_count in [
            ("yt", 16),
            ("yt-dev", 11),
            ("yt-master", 7),
            ("yt-cypress-server", 4),
            ("yt-security-server", 3),
            ("yt-scheduler", 10),
            ("yt-front", 16),
            ("yt-morda", 9),
        ]:
            assert cluster_resources_equal(
                get("//sys/accounts/{0}/@resource_limits".format(account)),
                self._build_resource_limits(node_count=node_count, include_disk_space=True),
            )

    @authors("kiselyovp")
    def test_transfer_account_resources7(self):
        create_user("babenko")
        create_user("andozer")
        create_user("shakurov")
        create_user("kiselyovp")
        create_user("tsufiev")

        limits = self._build_resource_limits(
            node_count=10,
            chunk_count=40,
            disk_space=9001,
            tablet_count=5,
            tablet_static_memory=1000,
            master_memory=50000,
            include_disk_space=True,
        )
        limits_x2 = multiply_recursive(limits, 2)
        limits_x5 = multiply_recursive(limits, 5)
        limits_delta = self._build_resource_limits(
            node_count=5,
            chunk_count=20,
            disk_space=1000,
            master_memory=20000,
            include_disk_space=True,
        )

        def create_write_acl(users):
            return [make_ace("allow", users, ["write"])]

        create_account(
            "yt",
            attributes={
                "resource_limits": limits_x5,
                "acl": self._create_account_acl("babenko"),
            },
        )
        create_account(
            "yt-dev",
            "yt",
            attributes={
                "resource_limits": limits_x2,
                "acl": create_write_acl("shakurov"),
            },
        )
        create_account(
            "yt-master",
            "yt-dev",
            attributes={
                "resource_limits": limits,
                "acl": create_write_acl(["kiselyovp", "andozer"]),
            },
        )
        create_account(
            "yt-front",
            "yt",
            attributes={
                "resource_limits": limits_x2,
                "acl": create_write_acl("andozer"),
            },
        )
        create_account(
            "yt-morda",
            "yt-front",
            attributes={
                "resource_limits": limits,
                "acl": create_write_acl(["tsufiev", "shakurov"]),
            },
        )

        transfer_account_resources("yt-morda", "yt-master", limits_delta, authenticated_user="babenko")

        with pytest.raises(YtError):
            transfer_account_resources("yt-master", "yt-morda", limits_delta, authenticated_user="tsufiev")
        with pytest.raises(YtError):
            transfer_account_resources("yt-master", "yt-morda", limits_delta, authenticated_user="kiselyovp")
        with pytest.raises(YtError):
            transfer_account_resources("yt-master", "yt-morda", limits_delta, authenticated_user="andozer")
        with pytest.raises(YtError):
            transfer_account_resources("yt-master", "yt-morda", limits_delta, authenticated_user="shakurov")

        assert get("//sys/accounts/yt-morda/@resource_limits") == subtract_recursive(limits, limits_delta)
        assert get("//sys/accounts/yt-front/@resource_limits") == subtract_recursive(limits_x2, limits_delta)
        assert get("//sys/accounts/yt/@resource_limits") == limits_x5
        assert get("//sys/accounts/yt-dev/@resource_limits") == add_recursive(limits_x2, limits_delta)
        assert get("//sys/accounts/yt-master/@resource_limits") == add_recursive(limits, limits_delta)

        transfer_account_resources("yt-master", "yt-front", limits_delta, authenticated_user="babenko")
        assert get("//sys/accounts/yt-master/@resource_limits") == limits
        assert get("//sys/accounts/yt-dev/@resource_limits") == limits_x2
        assert get("//sys/accounts/yt/@resource_limits") == limits_x5
        assert get("//sys/accounts/yt-front/@resource_limits") == limits_x2

    @authors("shakurov")
    def test_total_children_resource_limits(self):
        limits_x1 = self._build_resource_limits(
            node_count=10,
            chunk_count=40,
            disk_space=9001,
            tablet_count=5,
            tablet_static_memory=1000,
            master_memory=50000,
            include_disk_space=True,
        )
        limits_x2 = multiply_recursive(limits_x1, 2)
        limits_x3 = multiply_recursive(limits_x1, 3)
        limits_x5 = multiply_recursive(limits_x1, 5)

        create_account(
            "a",
            attributes={"resource_limits": limits_x5},
        )
        create_account(
            "b",
            "a",
            attributes={"resource_limits": limits_x2},
        )
        create_account(
            "c",
            "a",
            attributes={"resource_limits": limits_x1},
        )
        create_account(
            "d",
            "b",
            attributes={"resource_limits": limits_x1},
        )

        limits_x0 = self._build_resource_limits(include_disk_space=True)

        assert cluster_resources_equal(
            get("//sys/account_tree/a/@total_children_resource_limits"),
            limits_x3)
        assert cluster_resources_equal(
            get("//sys/account_tree/a/b/@total_children_resource_limits"),
            limits_x1)
        assert cluster_resources_equal(
            get("//sys/account_tree/a/c/@total_children_resource_limits"),
            limits_x0)
        assert cluster_resources_equal(
            get("//sys/account_tree/a/b/d/@total_children_resource_limits"),
            limits_x0)

    @authors("cookiedoth")
    def test_subtree_size_limits(self):
        set("//sys/@config/security_manager/max_account_subtree_size", 3)
        create_account("a", empty=True)
        create_account("aa", "a", empty=True)
        create_account("ab", "a", empty=True)
        with pytest.raises(YtError):
            create_account("ac", "a", empty=True)
        with pytest.raises(YtError):
            create_account("aba", "ab", empty=True)
        create_account("b", empty=True)
        create_account("c", empty=True)
        create_account("d", empty=True)
        create_account("da", "d", empty=True)
        create_account("db", "d", empty=True)
        create_account("ca", "c", empty=True)
        with pytest.raises(YtError):
            move("//sys/account_tree/d", "//sys/account_tree/c/ca/caa")
        set("//sys/@config/security_manager/max_account_subtree_size", 5)
        move("//sys/account_tree/d", "//sys/account_tree/c/ca/caa")


##################################################################


class TestAccountsMulticell(TestAccounts):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1

    @authors("babenko", "kiselyovp")
    def test_requisitions2(self):
        create_account("a1")
        create_account("a2")

        create("table", "//tmp/t1", attributes={"account": "a1", "external_cell_tag": 11})
        write_table("//tmp/t1", {"a": "b"})

        create("table", "//tmp/t2", attributes={"account": "a2", "external_cell_tag": 12})
        merge(mode="unordered", in_=["//tmp/t1", "//tmp/t1"], out="//tmp/t2")

        chunk_id = get_singular_chunk_id("//tmp/t1")

        wait(lambda: len(get("#" + chunk_id + "/@requisition")) == 2)

        remove("//tmp/t1")

        wait(lambda: len(get("#" + chunk_id + "/@owning_nodes")) == 1)
        wait(lambda: len(get("#" + chunk_id + "/@requisition")) == 1)

    @authors("aleksandra-zh")
    def test_master_memory_per_cell(self):
        set("//sys/@config/security_manager/enable_master_memory_usage_validation", True)

        create_account("a")
        set("//sys/accounts/a/@resource_limits/master_memory/total", 1000000)
        set("//sys/accounts/a/@resource_limits/master_memory/chunk_host", 1000000)
        set("//sys/accounts/a/@resource_limits/master_memory/per_cell", {"11": 0})
        master_memory_sleep()

        create("table", "//tmp/t1", attributes={"account": "a", "external_cell_tag": 11})
        with pytest.raises(YtError):
            write_table("//tmp/t1", {"a": "b"})

        create("table", "//tmp/t2", attributes={"account": "a", "external_cell_tag": 12})
        write_table("//tmp/t2", {"a": "b"})

    @authors("cookiedoth")
    def test_master_memory_usage_per_cell1(self):
        create_account("a")
        master_memory_sleep()
        total = get("//sys/accounts/a/@resource_usage/master_memory/total")
        mem1 = get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11")

        create("table", "//tmp/t1", attributes={"account": "a"})
        write_table("//tmp/t1", {"a": "b"})
        master_memory_sleep()
        assert get("//sys/accounts/a/@resource_usage/master_memory/total") > total
        mem2 = get("//sys/accounts/a/@resource_usage/master_memory/per_cell/12")

        create("table", "//tmp/t2", attributes={"account": "a", "external_cell_tag": 11})
        write_table("//tmp/t2", {"a": "b"})
        master_memory_sleep()
        assert get("//sys/accounts/a/@resource_usage/master_memory/total") > total
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11") > mem1
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/12") == mem2
        assert get("//sys/accounts/a/@multicell_statistics/11/resource_usage/master_memory") > mem1
        assert get("//sys/accounts/a/@multicell_statistics/12/resource_usage/master_memory") == mem2
        mem1 = get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11")

        create("table", "//tmp/t3", attributes={"account": "a", "external_cell_tag": 12})
        write_table("//tmp/t3", {"a": "b"})
        master_memory_sleep()
        assert get("//sys/accounts/a/@resource_usage/master_memory/total") > total
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11") == mem1
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/12") > mem2
        assert get("//sys/accounts/a/@multicell_statistics/11/resource_usage/master_memory") == mem1
        assert get("//sys/accounts/a/@multicell_statistics/12/resource_usage/master_memory") > mem2

    @authors("cookiedoth")
    def test_master_memory_usage_per_cell2(self):
        create_account("a")
        master_memory_sleep()

        set("//sys/@config/multicell_manager/cell_descriptors", {
            "11": {"roles": ["chunk_host"]},
            "12": {"roles": ["chunk_host"]}})
        assert get("//sys/accounts/a/@resource_usage/master_memory/chunk_host") == 0

        create("table", "//tmp/t1", attributes={"account": "a", "external_cell_tag": 12})
        write_table("//tmp/t1", {"a": "b"})
        master_memory_sleep()
        assert get("//sys/accounts/a/@resource_usage/master_memory/chunk_host") > 0
        assert get("//sys/accounts/a/@resource_usage/master_memory/chunk_host") == \
            get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11") + \
            get("//sys/accounts/a/@resource_usage/master_memory/per_cell/12")

        create("table", "//tmp/t2", attributes={"account": "a", "external_cell_tag": 11})
        write_table("//tmp/t2", {"a": "b"})
        master_memory_sleep()
        assert get("//sys/accounts/a/@resource_usage/master_memory/chunk_host") > 0
        assert get("//sys/accounts/a/@resource_usage/master_memory/chunk_host") == \
            get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11") + \
            get("//sys/accounts/a/@resource_usage/master_memory/per_cell/12")

    @authors("kvk1920")
    def test_transfer_master_memory_limits_per_cell(self):
        create_account("a")
        create_account("b")

        set("//sys/accounts/a/@resource_limits/master_memory/per_cell", {"11": 0})
        set("//sys/accounts/b/@resource_limits/master_memory/per_cell", {"12": 1})

        with raises_yt_error("infinity-related"):
            transfer_account_resources("a", "b", self._build_resource_limits(master_memory_per_cell={"12": 1}))

        with raises_yt_error("infinity-related"):
            transfer_account_resources("b", "a", self._build_resource_limits(master_memory_per_cell={"12": 1}))

        transfer_account_resources("b", "a", self._build_resource_limits(master_memory_per_cell={"11": 0}))
        assert get("//sys/accounts/a/@resource_limits/master_memory/per_cell") == {"11": 0}
        assert get("//sys/accounts/b/@resource_limits/master_memory/per_cell") == {"12": 1}

        set("//sys/accounts/a/@resource_limits/master_memory/per_cell", {"11": 1000, "12": 1000})
        set("//sys/accounts/b/@resource_limits/master_memory/per_cell", {"11": 2000})
        transfer_account_resources("b", "a", self._build_resource_limits(master_memory_per_cell={"11": 1000}))
        assert get("//sys/accounts/a/@resource_limits/master_memory/per_cell") == {"11": 2000, "12": 1000}
        assert get("//sys/accounts/b/@resource_limits/master_memory/per_cell") == {"11": 1000}

    @authors("aleksandra-zh")
    def test_master_memory_per_cell_overcommit_validation(self):
        set("//sys/@config/security_manager/enable_master_memory_usage_validation", True)
        set(
            "//sys/@config/security_manager/enable_master_memory_usage_account_overcommit_validation",
            True,
        )

        create_account("a")
        create_account("b", "a")

        with pytest.raises(YtError):
            set("//sys/accounts/a/@resource_limits/master_memory/per_cell", {"11": 0})

        set("//sys/accounts/b/@resource_limits/master_memory/per_cell", {"11": 0})
        set("//sys/accounts/a/@resource_limits/master_memory/per_cell", {"11": 0})

    @authors("aleksandra-zh")
    def test_chunk_host_master_memory1(self):
        set("//sys/@config/security_manager/enable_master_memory_usage_validation", True)

        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["chunk_host"]}})

        create_account("a")
        set("//sys/accounts/a/@resource_limits/master_memory/total", 1000000)
        set("//sys/accounts/a/@resource_limits/master_memory/chunk_host", 0)
        master_memory_sleep()

        create("table", "//tmp/t1", attributes={"account": "a", "external_cell_tag": 11})
        with pytest.raises(YtError):
            write_table("//tmp/t1", {"a": "b"})

    @authors("aleksandra-zh")
    def test_chunk_host_master_memory2(self):
        set("//sys/@config/security_manager/enable_master_memory_usage_validation", True)

        set("//sys/@config/multicell_manager/cell_descriptors", {
            "11": {"roles": ["chunk_host"]},
            "12": {"roles": ["chunk_host"]}})

        create_account("a")
        set("//sys/accounts/a/@resource_limits/master_memory/total", 1000000)
        set("//sys/accounts/a/@resource_limits/master_memory/chunk_host", 100)
        master_memory_sleep()

        create("table", "//tmp/t1", attributes={"account": "a", "external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"account": "a", "external_cell_tag": 12})

        def quota_is_over():
            try:
                write_table("<append=true>//tmp/t1", {"a": "b"})
            except YtError:
                return True
            master_memory_sleep()
            return False

        wait(quota_is_over)

        master_memory_sleep()
        with pytest.raises(YtError):
            write_table("<append=true>//tmp/t2", {"a": "b"})

    @authors("aleksandra-zh")
    def test_master_cell_names(self):
        set("//sys/@config/multicell_manager/cell_descriptors/10", {"name": "Julia"})

        with pytest.raises(YtError):
            set("//sys/@config/multicell_manager/cell_descriptors/12", {"name": "Julia"})

        with pytest.raises(YtError):
            set("//sys/@config/multicell_manager/cell_descriptors/11", {"name": "12"})

        create_account("a")
        set("//sys/accounts/a/@resource_limits/master_memory/total", 1000000)
        set("//sys/accounts/a/@resource_limits/master_memory/chunk_host", 1000000)

        set("//sys/accounts/a/@resource_limits/master_memory/per_cell", {"Julia": 100, "11": 200, "12": 300})

        set("//sys/@config/multicell_manager/cell_descriptors/12", {"name": "George"})

        assert get("//sys/accounts/a/@resource_limits/master_memory/per_cell") ==\
            {"Julia": 100, "11": 200, "George": 300}

        assert sorted(ls("//sys/accounts/a/@multicell_statistics")) == ["11", "George", "Julia"]

    @authors("h0pless")
    def test_chunk_host_tag_switch(self):
        create_account("a")
        master_memory_sleep()

        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["chunk_host"]}, "12": {"roles": ["chunk_host"]}})

        create("table", "//tmp/t1", attributes={"account": "a", "external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"account": "a", "external_cell_tag": 12})
        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t2", {"a": "b"})
        write_table("<append=true>//tmp/t2", {"a": "c"})
        master_memory_sleep()

        assert get("//sys/accounts/a/@resource_usage/master_memory", driver=get_driver(1)) > 0
        assert get("//sys/accounts/a/@multicell_statistics/11/resource_usage/chunk_host_cell_master_memory", driver=get_driver(1)) > 0

        set("//sys/@config/multicell_manager/cell_descriptors/11/roles", ["transaction_coordinator"])
        master_memory_sleep()
        write_table("<append=true>//tmp/t1", {"a": "d"})
        write_table("<append=true>//tmp/t1", {"a": "c"})
        master_memory_sleep()

        assert get("//sys/accounts/a/@multicell_statistics/11/resource_usage/chunk_host_cell_master_memory", driver=get_driver(1)) == 0
        wait(lambda: get("//sys/accounts/a/@resource_usage/master_memory/chunk_host") ==
             get("//sys/accounts/a/@multicell_statistics/12/resource_usage/master_memory", driver=get_driver(2)))

        set("//sys/@config/multicell_manager/cell_descriptors/11/roles", ["chunk_host"])
        master_memory_sleep()

        assert get("//sys/accounts/a/@multicell_statistics/11/resource_usage/chunk_host_cell_master_memory", driver=get_driver(1)) > 0
        wait(lambda: get("//sys/accounts/a/@resource_usage/master_memory/chunk_host") ==
             get("//sys/accounts/a/@multicell_statistics/11/resource_usage/master_memory", driver=get_driver(1)) +
             get("//sys/accounts/a/@multicell_statistics/12/resource_usage/master_memory", driver=get_driver(2)))

    @authors("h0pless")
    def test_non_chunk_host_secondary_cell_multicell_statistics(self):
        create_account("a")
        master_memory_sleep()

        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["chunk_host"]}, "12": {"roles": ["chunk_host"]}})
        create("table", "//tmp/t1", attributes={"account": "a", "external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"account": "a", "external_cell_tag": 12})
        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t2", {"a": "b"})
        write_table("<append=true>//tmp/t2", {"a": "c"})
        master_memory_sleep()

        assert len(ls("//sys/accounts/a/@multicell_statistics")) == self.NUM_SECONDARY_MASTER_CELLS + 1
        assert len(ls("//sys/accounts/a/@multicell_statistics", driver=get_driver(1))) == 1
        assert len(ls("//sys/accounts/a/@multicell_statistics", driver=get_driver(2))) == 1


class TestAccountTreeMulticell(TestAccountTree):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1

    @authors("cookiedoth")
    def test_master_memory_usage_per_cell3(self):
        create_account("a")
        create_account("aa", "a")

        create("table", "//tmp/t1", attributes={"account": "aa", "external_cell_tag": 12})
        write_table("//tmp/t1", {"a": "b"})
        master_memory_sleep()

        mem = get("//sys/accounts/aa/@resource_usage/master_memory/total")
        mem2 = get("//sys/accounts/aa/@resource_usage/master_memory/per_cell/12")
        assert get("//sys/accounts/a/@resource_usage/master_memory/total") == 0
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11") == 0
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/12") == 0
        assert get("//sys/accounts/aa/@resource_usage/master_memory/total") > 0
        assert get("//sys/accounts/aa/@resource_usage/master_memory/per_cell/11") == 0
        assert get("//sys/accounts/aa/@resource_usage/master_memory/per_cell/12") > 0
        assert get("//sys/accounts/a/@recursive_resource_usage/master_memory/total") == mem
        assert get("//sys/accounts/a/@recursive_resource_usage/master_memory/per_cell/11") == 0
        assert get("//sys/accounts/a/@recursive_resource_usage/master_memory/per_cell/12") == mem2

        create("table", "//tmp/t2", attributes={"account": "a", "external_cell_tag": 11})
        write_table("//tmp/t2", {"a": "b"})
        master_memory_sleep()

        assert get("//sys/accounts/a/@resource_usage/master_memory/total") > 0
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/11") > 0
        assert get("//sys/accounts/a/@resource_usage/master_memory/per_cell/12") == 0
        assert get("//sys/accounts/aa/@resource_usage/master_memory/total") == mem
        assert get("//sys/accounts/aa/@resource_usage/master_memory/per_cell/11") == 0
        assert get("//sys/accounts/aa/@resource_usage/master_memory/per_cell/12") == mem2
        assert get("//sys/accounts/a/@recursive_resource_usage/master_memory/total") > mem
        assert get("//sys/accounts/a/@recursive_resource_usage/master_memory/per_cell/11") > 0
        assert get("//sys/accounts/a/@recursive_resource_usage/master_memory/per_cell/12") == mem2


##################################################################


class TestAccountsProfiling(YTEnvSetup):
    NUM_MASTERS = 3

    def _get_leader_address(self):
        for master in ls("//sys/primary_masters"):
            address = f"//sys/primary_masters/{master}/orchid"
            if get(f"{address}/monitoring/hydra/state") == "leading":
                return master

    def _get_follower_addresses(self):
        followers = []

        for master in ls("//sys/primary_masters"):
            address = f"//sys/primary_masters/{master}/orchid"
            if get(f"{address}/monitoring/hydra/state") == "following":
                followers.append(master)

        return followers

    def _check_profiler_values(self, profiler, gauge_name, should_report):
        values = profiler.gauge(gauge_name).get_all()
        assert (values == []) != should_report

    def _switch_leader_back_and_forth(self):
        old_leader_rpc_address = get_active_primary_master_leader_address(self)
        new_leader_rpc_address = get_active_primary_master_follower_address(self)
        cell_id = get("//sys/@cell_id")

        switch_leader(cell_id, new_leader_rpc_address)
        wait(lambda: is_active_primary_master_leader(new_leader_rpc_address))
        wait(lambda: is_active_primary_master_follower(old_leader_rpc_address))

        sleep(5)

        switch_leader(cell_id, old_leader_rpc_address)
        wait(lambda: is_active_primary_master_leader(old_leader_rpc_address))
        wait(lambda: is_active_primary_master_follower(new_leader_rpc_address))

    @authors("h0pless")
    def test_account_profiling_buckets(self):
        set("//sys/@config/incumbent_manager/scheduler/incumbents/security_manager/use_followers", False)
        set("//sys/@config/security_manager/accounts_profiling_period", 1)

        leader_address = get_active_primary_master_leader_address(self)
        leader_profiler = profiler_factory().at_primary_master(leader_address)
        sleep(1.5)
        gauge = leader_profiler.gauge("accounts/node_count_limit", fixed_tags={"account": "sys"})
        gauge_value = gauge.get()

        create_account("industrial_chimney_lover")
        sleep(1.5)
        assert gauge_value == gauge.get()

        create_account("industrial_furnace_hater")
        sleep(1.5)
        assert gauge_value == gauge.get()

        self._switch_leader_back_and_forth()

        sleep(1.5)
        leader_address = get_active_primary_master_leader_address(self)
        leader_profiler = profiler_factory().at_primary_master(leader_address)

        sleep(1.5)
        gauge = leader_profiler.gauge("accounts/node_count_limit", fixed_tags={"account": "sys"})
        assert gauge_value == gauge.get()

    @authors("h0pless")
    @flaky(max_runs=3)
    def test_accounts_profiling(self):
        set("//sys/@config/incumbent_manager/scheduler/incumbents/security_manager/use_followers", True)
        set("//sys/@config/security_manager/accounts_profiling_period", 1)

        # Ensuring that both peers have an account in one of their shards
        for name in ["juan", "gregorz", "john", "johna", "johnson"]:
            create_account(f"{name}_the_1st")
            create_account(f"{name}_the_2nd")
            create_account(f"{name}_the_3rd")
            for generation in range(4, 21):
                create_account(f"{name}_the_{generation}th")

        leader_address = get_active_primary_master_leader_address(self)
        leader_profiler = profiler_factory().at_primary_master(leader_address)

        follower_addresses = get_currently_active_pirmary_master_follower_addresses(self)
        follower_profilers = [profiler_factory().at_primary_master(address) for address in follower_addresses]

        secondary_cell_tags = get("//sys/secondary_masters")
        secondary_profilers = []
        for tag in secondary_cell_tags:
            addrs = [address for address in secondary_cell_tags[tag]]
            secondary_profilers += [profiler_factory().at_secondary_master(tag, master_address) for master_address in addrs]

        gauge_name = "accounts/chunk_count"
        sleep(2)

        # Check that followers actually report something.
        for follower_profiler in follower_profilers:
            self._check_profiler_values(follower_profiler, gauge_name, True)

        # Not the leader or secondary cells.
        self._check_profiler_values(leader_profiler, gauge_name, False)
        for profiler in secondary_profilers:
            self._check_profiler_values(profiler, gauge_name, False)

        set("//sys/@config/incumbent_manager/scheduler/incumbents/security_manager/use_followers", False)
        sleep(2)

        # Check that leader now is the one reporting.
        self._check_profiler_values(leader_profiler, gauge_name, True)

        # Not the followers or secondary cells.
        for follower_profiler in follower_profilers:
            self._check_profiler_values(follower_profiler, gauge_name, False)
        for profiler in secondary_profilers:
            self._check_profiler_values(profiler, gauge_name, False)

        set("//sys/@config/security_manager/enable_accounts_profiling", False)
        # Check that nobody is reporting now.
        sleep(2)
        self._check_profiler_values(leader_profiler, gauge_name, False)
        for follower_profiler in follower_profilers:
            self._check_profiler_values(follower_profiler, gauge_name, False)
        for profiler in secondary_profilers:
            self._check_profiler_values(profiler, gauge_name, False)
