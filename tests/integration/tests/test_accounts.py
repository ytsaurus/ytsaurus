import pytest
from time import sleep

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestAccounts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 16

    def _get_disk_space_for_medium(self, disk_space_map, medium_name = "default"):
        return disk_space_map.get(medium_name, 0)

    def _get_account_node_count(self, account):
        return get("//sys/accounts/{0}/@resource_usage/node_count".format(account))

    def _get_account_chunk_count(self, account):
        return get("//sys/accounts/{0}/@resource_usage/chunk_count".format(account))

    def _get_account_node_count_limit(self, account):
        return get("//sys/accounts/{0}/@resource_limits/node_count".format(account))

    def _set_account_node_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/node_count".format(account), value)

    def _get_account_chunk_count_limit(self, account):
        return get("//sys/accounts/{0}/@resource_limits/chunk_count".format(account))

    def _set_account_chunk_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/chunk_count".format(account), value)

    def _set_account_zero_limits(self, account):
        set("//sys/accounts/{0}/@resource_limits".format(account),
            {"disk_space_per_medium": {"default" : 0}, "chunk_count": 0, "node_count": 0})

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

    def test_init(self):
        assert sorted(ls("//sys/accounts")) == sorted(["sys", "tmp", "intermediate"])
        assert get("//@account") == "sys"
        assert get("//sys/@account") == "sys"
        assert get("//tmp/@account") == "tmp"

    def test_account_create1(self):
        create_account("max")
        assert sorted(ls("//sys/accounts")) == sorted(["sys", "tmp", "intermediate", "max"])
        assert get_account_disk_space("max") == 0
        assert self._get_account_node_count("max") == 0
        assert self._get_account_chunk_count("max") == 0

    def test_account_create2(self):
        with pytest.raises(YtError): create_account("sys")
        with pytest.raises(YtError): create_account("tmp")

    def test_account_remove_builtin(self):
        with pytest.raises(YtError): remove_account("sys")
        with pytest.raises(YtError): remove_account("tmp")

    def test_account_create3(self):
        create_account("max")
        with pytest.raises(YtError): create_account("max")

    def test_empty_name_fail(self):
        with pytest.raises(YtError): create_account("")

    def test_account_attr1(self):
        set("//tmp/a", {})
        assert get("//tmp/a/@account") == "tmp"

    def test_account_attr2(self):
        # should not crash
        get("//sys/accounts/tmp/@")

    def test_account_attr3(self):
        set("//tmp/a", {"x" : 1, "y" : 2})
        assert get("//tmp/a/@account") == "tmp"
        assert get("//tmp/a/x/@account") == "tmp"
        assert get("//tmp/a/y/@account") == "tmp"
        copy("//tmp/a", "//tmp/b")
        assert get("//tmp/b/@account") == "tmp"
        assert get("//tmp/b/x/@account") == "tmp"
        assert get("//tmp/b/y/@account") == "tmp"

    def test_account_attr4(self):
        create_account("max")
        assert self._get_account_node_count("max") == 0
        assert self._get_account_chunk_count("max") == 0

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        tmp_node_count = self._get_account_node_count("tmp")
        tmp_chunk_count = self._get_account_chunk_count("tmp")

        set("//tmp/t/@account", "max")

        multicell_sleep()
        assert self._get_account_node_count("tmp") == tmp_node_count - 1
        assert self._get_account_node_count("max") == 1

    def test_account_attr5(self):
        create_account("max")
        set("//tmp/a", {})
        tx = start_transaction()
        with pytest.raises(YtError): set("//tmp/a/@account", "max", tx=tx)

    def test_remove1(self):
        create_account("max")
        remove_account("max")

    def test_remove2(self):
        create_account("max")
        set("//tmp/a", {})
        set("//tmp/a/@account", "max")
        set("//tmp/a/@account", "sys")
        remove_account("max")

    def test_remove3(self):
        create_account("max")
        set("//tmp/a", {})
        set("//tmp/a/@account", "max")
        with pytest.raises(YtError): remove_account("max")

    def test_file1(self):
        assert get_account_disk_space("tmp") == 0

        content = "some_data"
        create("file", "//tmp/f1")
        write_file("//tmp/f1", content)

        multicell_sleep()
        space = get_account_disk_space("tmp")
        assert space > 0

        create("file", "//tmp/f2")
        write_file("//tmp/f2", content)

        multicell_sleep()
        assert get_account_disk_space("tmp") == 2 * space

        remove("//tmp/f1")

        gc_collect()
        multicell_sleep()
        assert get_account_disk_space("tmp") == space

        remove("//tmp/f2")

        gc_collect()
        multicell_sleep()
        assert get_account_disk_space("tmp") == 0

    def test_file2(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)

        multicell_sleep()
        space = get_account_disk_space("tmp")

        create_account("max")
        set("//tmp/f/@account", "max")

        multicell_sleep()
        assert get_account_disk_space("tmp") == 0
        assert get_account_disk_space("max") == space

        remove("//tmp/f")

        gc_collect()
        multicell_sleep()
        assert get_account_disk_space("max") == 0

    def test_file3(self):
        create_account("max")

        assert get_account_disk_space("max") == 0

        content = "some_data"
        create("file", "//tmp/f", attributes={"account": "max"})
        write_file("//tmp/f", content)

        multicell_sleep()
        assert get_account_disk_space("max") > 0

        remove("//tmp/f")

        gc_collect()
        multicell_sleep()
        assert get_account_disk_space("max") == 0

    def test_file4(self):
        create_account("max")

        content = "some_data"
        create("file", "//tmp/f", attributes={"account": "max"})
        write_file("//tmp/f", content)

        multicell_sleep()
        space = get_account_disk_space("max")
        assert space > 0

        rf  = get("//tmp/f/@replication_factor")
        set("//tmp/f/@replication_factor", rf * 2)

        multicell_sleep()
        assert get_account_disk_space("max") == space * 2

    def test_table1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        assert get_account_disk_space("tmp") > 0

    def test_table2(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        for i in xrange(0, 5):
            write_table("//tmp/t", {"a" : "b"}, tx=tx)
            multicell_sleep()
            account_space = get_account_disk_space("tmp")
            tx_space = self._get_tx_disk_space(tx, "tmp")
            assert account_space > 0
            assert account_space == tx_space
            assert get_chunk_owner_disk_space("//tmp/t") == 0
            assert get_chunk_owner_disk_space("//tmp/t", tx=tx) == tx_space
            last_space = tx_space

        commit_transaction(tx)

        multicell_sleep()
        assert get_chunk_owner_disk_space("//tmp/t") == last_space

    def test_table3(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        space1 = get_account_disk_space("tmp")
        assert space1 > 0

        tx = start_transaction()
        write_table("//tmp/t", {"xxxx" : "yyyy"}, tx=tx)

        multicell_sleep()
        space2 = self._get_tx_disk_space(tx, "tmp")
        assert space1 != space2
        assert get_account_disk_space("tmp") == space1 + space2
        assert get_chunk_owner_disk_space("//tmp/t") == space1
        assert get_chunk_owner_disk_space("//tmp/t", tx=tx) == space2

        commit_transaction(tx)

        multicell_sleep()
        assert get_account_disk_space("tmp") == space2
        assert get_chunk_owner_disk_space("//tmp/t") == space2

    def test_table4(self):
        tx = start_transaction()
        create("table", "//tmp/t", tx=tx)
        write_table("//tmp/t", {"a" : "b"}, tx=tx)

        multicell_sleep()
        assert get_account_disk_space("tmp") > 0

        abort_transaction(tx)

        multicell_sleep()
        assert get_account_disk_space("tmp") == 0

    def test_table5(self):
        tmp_node_count = self._get_account_node_count("tmp")
        tmp_chunk_count = self._get_account_chunk_count("tmp")

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        assert self._get_account_node_count("tmp") == tmp_node_count + 1
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count + 1
        space = get_account_disk_space("tmp")
        assert space > 0

        create_account("max")

        set("//tmp/t/@account", "max")

        multicell_sleep()
        assert self._get_account_node_count("tmp") == tmp_node_count
        assert self._get_account_node_count("max") == 1
        assert self._get_account_chunk_count("max") == 1
        assert get_account_disk_space("tmp") == 0
        assert get_account_disk_space("max") == space

        set("//tmp/t/@account", "tmp")

        multicell_sleep()
        assert self._get_account_node_count("tmp") == tmp_node_count + 1
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count + 1
        assert self._get_account_node_count("max") == 0
        assert self._get_account_chunk_count("max") == 0
        assert get_account_disk_space("tmp") == space
        assert get_account_disk_space("max") == 0

    def test_table6(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        write_table("//tmp/t", {"a" : "b"}, tx=tx)

        multicell_sleep()
        space = get_chunk_owner_disk_space("//tmp/t", tx=tx)
        assert space > 0
        assert get_account_disk_space("tmp") == space

        tx2 = start_transaction(tx=tx)

        multicell_sleep()
        assert get_chunk_owner_disk_space("//tmp/t", tx=tx2) == space

        write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx2)

        multicell_sleep()
        assert get_chunk_owner_disk_space("//tmp/t", tx=tx2) == space * 2
        assert get_account_disk_space("tmp") == space * 2

        commit_transaction(tx2)

        multicell_sleep()
        assert get_chunk_owner_disk_space("//tmp/t", tx=tx) == space * 2
        assert get_account_disk_space("tmp") == space * 2
        commit_transaction(tx)

        multicell_sleep()
        assert get_chunk_owner_disk_space("//tmp/t") == space * 2
        assert get_account_disk_space("tmp") == space * 2

    def test_node_count_limits1(self):
        create_account("max")
        assert not self._is_account_node_count_limit_violated("max")
        self._set_account_node_count_limit("max", 1000)
        self._set_account_node_count_limit("max", 2000)
        self._set_account_node_count_limit("max", 0)
        assert not self._is_account_node_count_limit_violated("max")
        with pytest.raises(YtError): self._set_account_node_count_limit("max", -1)

    def test_node_count_limits2(self):
        create_account("max")
        assert self._get_account_node_count("max") == 0
        
        create("table", "//tmp/t")
        set("//tmp/t/@account", "max")
        
        multicell_sleep()
        assert self._get_account_node_count("max") == 1

    def test_node_count_limits3(self):
        create_account("max")
        create("table", "//tmp/t")
        
        self._set_account_node_count_limit("max", 0)
        
        multicell_sleep()
        with pytest.raises(YtError): set("//tmp/t/@account", "max")

    def test_node_count_limits4(self):
        create_account("max")
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        
        self._set_account_node_count_limit("max", 0)
        with pytest.raises(YtError): set("//tmp/t/@account", "max")

    def test_chunk_count_limits1(self):
        create_account("max")
        assert not self._is_account_chunk_count_limit_violated("max")
        self._set_account_chunk_count_limit("max", 1000)
        self._set_account_chunk_count_limit("max", 2000)
        self._set_account_chunk_count_limit("max", 0)
        assert not self._is_account_chunk_count_limit_violated("max")
        with pytest.raises(YtError): self._set_account_chunk_count_limit("max", -1)

    def test_chunk_count_limits2(self):
        create_account("max")
        assert self._get_account_chunk_count("max") == 0

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        set("//tmp/t/@account", "max")

        multicell_sleep()
        assert self._get_account_chunk_count("max") == 1

    def test_disk_space_limits1(self):
        create_account("max")
        assert not self._is_account_disk_space_limit_violated("max")
        set_account_disk_space_limit("max", 1000)
        set_account_disk_space_limit("max", 2000)
        set_account_disk_space_limit("max", 0)
        assert not self._is_account_disk_space_limit_violated("max")
        with pytest.raises(YtError): set_account_disk_space_limit("max", -1)

    def test_disk_space_limits2(self):
        create_account("max")
        set_account_disk_space_limit("max", 1000000)

        create("table", "//tmp/t")
        set("//tmp/t/@account", "max")

        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()

        assert not self._is_account_disk_space_limit_violated("max")

        set_account_disk_space_limit("max", 0)
        multicell_sleep()

        assert self._is_account_disk_space_limit_violated("max")
        with pytest.raises(YtError): write_table("//tmp/t", {"a" : "b"})

        set_account_disk_space_limit("max", get_account_disk_space("max") + 1)
        multicell_sleep()
        assert not self._is_account_disk_space_limit_violated("max")

        write_table("<append=true>//tmp/t", {"a" : "b"})

        multicell_sleep()
        assert self._is_account_disk_space_limit_violated("max")

    def test_disk_space_limits3(self):
        create_account("max")
        set_account_disk_space_limit("max", 1000000)

        content = "some_data"

        create("file", "//tmp/f1", attributes={"account": "max"})
        write_file("//tmp/f1", content)
        
        multicell_sleep()
        assert not self._is_account_disk_space_limit_violated("max")

        set_account_disk_space_limit("max", 0)
        assert self._is_account_disk_space_limit_violated("max")

        create("file", "//tmp/f2", attributes={"account": "max"})
        with pytest.raises(YtError): write_file("//tmp/f2", content)

        set_account_disk_space_limit("max", get_account_disk_space("max") + 1)
        assert not self._is_account_disk_space_limit_violated("max")

        create("file", "//tmp/f3", attributes={"account": "max"})
        write_file("//tmp/f3", content)
        
        multicell_sleep()
        assert self._is_account_disk_space_limit_violated("max")

    def test_disk_space_limits4(self):
        content = "some_data"

        create("map_node", "//tmp/a")
        create("file", "//tmp/a/f1")
        write_file("//tmp/a/f1", content)
        create("file", "//tmp/a/f2")
        write_file("//tmp/a/f2", content)

        multicell_sleep()
        disk_space = get_chunk_owner_disk_space("//tmp/a/f1")
        disk_space_2 = get_chunk_owner_disk_space("//tmp/a/f2")
        assert disk_space == disk_space_2

        create_account("max")
        create("map_node", "//tmp/b")
        set("//tmp/b/@account", "max")

        set_account_disk_space_limit("max", disk_space * 2 + 1)
        copy("//tmp/a", "//tmp/b/a")

        multicell_sleep()
        assert get_account_disk_space("max") == disk_space * 2
        assert exists("//tmp/b/a")

        remove("//tmp/b/a")

        gc_collect()
        multicell_sleep()
        assert get_account_disk_space("max") == 0
        assert self._get_account_node_count("max") == 1

        assert not exists("//tmp/b/a")

    def test_committed_usage(self):
        multicell_sleep()
        assert get_account_committed_disk_space("tmp") == 0

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        space = get_chunk_owner_disk_space("//tmp/t")
        assert space > 0
        assert get_account_committed_disk_space("tmp") == space

        tx = start_transaction()
        write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx)

        multicell_sleep()
        assert get_account_committed_disk_space("tmp") == space

        commit_transaction(tx)

        multicell_sleep()
        assert get_account_committed_disk_space("tmp") == space * 2

    def test_nested_tx_uncommitted_usage(self):
        create("table", "//tmp/t")
        write_table("<append=true>//tmp/t", {"a" : "b"})
        write_table("<append=true>//tmp/t", {"a" : "b"})

        multicell_sleep()
        assert self._get_account_chunk_count("tmp") == 2

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx2)

        multicell_sleep()
        assert self._get_account_chunk_count("tmp") == 3
        
        assert get("//tmp/t/@update_mode") == "none"
        assert get("//tmp/t/@update_mode", tx=tx1) == "none"
        assert get("//tmp/t/@update_mode", tx=tx2) == "append"
        
        assert self._get_tx_chunk_count(tx1, "tmp") == 0
        assert self._get_tx_chunk_count(tx2, "tmp") == 1
        
        commit_transaction(tx2)

        multicell_sleep()
        assert get("//tmp/t/@update_mode") == "none"
        assert get("//tmp/t/@update_mode", tx=tx1) == "append"
        assert self._get_account_chunk_count("tmp") == 3
        assert self._get_tx_chunk_count(tx1, "tmp") == 1

        commit_transaction(tx1)

        multicell_sleep()
        assert get("//tmp/t/@update_mode") == "none"
        assert self._get_account_chunk_count("tmp") == 3
        
    def test_copy(self):
        create_account("a1")
        create_account("a2")

        create("map_node", "//tmp/x1", attributes={"account": "a1"})
        assert get("//tmp/x1/@account") == "a1"

        create("map_node", "//tmp/x2", attributes={"account": "a2"})
        assert get("//tmp/x2/@account") == "a2"

        create("table", "//tmp/x1/t")
        assert get("//tmp/x1/t/@account") == "a1"

        write_table("//tmp/x1/t", {"a" : "b"})

        multicell_sleep()
        space = get_account_disk_space("a1")
        assert space > 0
        assert space == get_account_committed_disk_space("a1")

        copy("//tmp/x1/t", "//tmp/x2/t")
        assert get("//tmp/x2/t/@account") == "a2"

        multicell_sleep()
        assert space == get_account_disk_space("a2")
        assert space == get_account_committed_disk_space("a2")

    def test_move_preserve_account_success(self):
        # setup
        create_account("a")
        set_account_disk_space_limit("a", 100000)
        create("map_node", "//tmp/x")
        set("//tmp/x/@account", "a")
        create("table", "//tmp/x/t")
        write_table("//tmp/x/t", {"a" : "b"})
        
        # make "a" overcommitted
        self._set_account_zero_limits("a")

        # move must succeed
        move("//tmp/x", "//tmp/y", preserve_account=True)
        
    def test_move_dont_preserve_account_success(self):
        # setup
        create_account("a")
        set_account_disk_space_limit("a", 100000)
        create("map_node", "//tmp/x")
        set("//tmp/x/@account", "a")
        create("table", "//tmp/x/t")
        write_table("//tmp/x/t", {"a" : "b"})
        create("map_node", "//tmp/for_y")
        set("//tmp/for_y/@account", "a")
        
        # make "a" overcommitted
        self._set_account_zero_limits("a")
        
        # move must succeed
        move("//tmp/x", "//tmp/for_y/y", preserve_account=False)

    def test_move_dont_preserve_account_fail(self):
        # setup
        create("map_node", "//tmp/x")
        create_account("a")
        create("map_node", "//tmp/for_y")
        set("//tmp/for_y/@account", "a")
        
        # make "a" overcommitted
        self._set_account_zero_limits("a")
        
        # move must fail
        with pytest.raises(YtError): move("//tmp/x", "//tmp/for_y/y", preserve_account=False)

    def test_copy_preserve_account_fail(self):
        # setup
        create_account("a")
        create("map_node", "//tmp/x")
        set("//tmp/x/@account", "a")
        
        # make "a" overcommitted
        self._set_account_zero_limits("a")
        
        # copy must fail
        with pytest.raises(YtError): copy("//tmp/x", "//tmp/y", preserve_account=True)
        
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
        with pytest.raises(YtError): copy("//tmp/x", "//tmp/for_y/y", preserve_account=False)
        
    def test_rename_success(self):
        create_account("a1")
        set("//sys/accounts/a1/@name", "a2")
        assert get("//sys/accounts/a2/@name") == "a2"

    def test_rename_fail(self):
        create_account("a1")
        create_account("a2")
        with pytest.raises(YtError): set("//sys/accounts/a1/@name", "a2")


    def test_set_account_fail_yt_6207(self):
        create_account("a")
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        assert get("//tmp/t/@account") == "tmp"
        multicell_sleep()
        assert get("//sys/accounts/tmp/@resource_usage/disk_space") > 0
        assert get("//sys/accounts/a/@resource_usage/disk_space") == 0
        create_user("u")
        with pytest.raises(YtError):  set("//tmp/t/@account", "a", authenticated_user="u")
        multicell_sleep()
        assert get("//sys/accounts/tmp/@resource_usage/disk_space") > 0
        assert get("//sys/accounts/a/@resource_usage/disk_space") == 0


    def test_change_account_with_snapshot_lock(self):
        multicell_sleep()
        tmp_nc = get("//sys/accounts/tmp/@resource_usage/node_count")
        tmp_rc = get("//sys/accounts/tmp/@ref_counter")
        create("table", "//tmp/t")
        create_account("a")
        multicell_sleep()
        assert get("//sys/accounts/tmp/@ref_counter") == tmp_rc + 1
        assert get("//sys/accounts/a/@ref_counter") == 1
        assert get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc + 1
        assert get("//sys/accounts/a/@resource_usage/node_count") == 0
        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        multicell_sleep()
        assert get("//sys/accounts/tmp/@ref_counter") == tmp_rc + 2
        assert get("//sys/accounts/a/@ref_counter") == 1
        assert get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc + 2
        assert get("//sys/accounts/a/@resource_usage/node_count") == 0
        set("//tmp/t/@account", "a")
        multicell_sleep()
        assert get("//sys/accounts/tmp/@ref_counter") == tmp_rc + 1
        assert get("//sys/accounts/a/@ref_counter") == 2
        assert get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc + 1
        assert get("//sys/accounts/a/@resource_usage/node_count") == 1
        abort_transaction(tx)
        multicell_sleep()
        assert get("//sys/accounts/tmp/@resource_usage/node_count") == tmp_nc
        assert get("//sys/accounts/a/@resource_usage/node_count") == 1
        assert get("//sys/accounts/tmp/@ref_counter") == tmp_rc
        assert get("//sys/accounts/a/@ref_counter") == 2


    def test_regular_disk_usage(self):
        create("table", "//tmp/t")
        set("//tmp/t/@replication_factor", 5)
        write_table("//tmp/t", {"a" : "b"})
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        assert get("//tmp/t/@resource_usage/disk_space_per_medium/default") == \
               get("#{0}/@statistics/regular_disk_space".format(chunk_list_id)) * 5

    def test_erasure_disk_usage(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        set("//tmp/t/@replication_factor", 5)
        write_table("//tmp/t", {"a" : "b"})
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        assert get("//tmp/t/@resource_usage/disk_space_per_medium/default") == \
               get("#{0}/@statistics/erasure_disk_space".format(chunk_list_id))

    
    def test_create_with_invalid_attrs_yt_7093(self):
        with pytest.raises(YtError):create_account("x", attributes={"resource_limits": 123})
        assert not exists("//sys/accounts/x")

##################################################################

class TestAccountsMulticell(TestAccounts):
    NUM_SECONDARY_MASTER_CELLS = 2
    
