import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestAccounts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def _get_account_disk_space(self, account):
        return get("//sys/accounts/{0}/@resource_usage/disk_space".format(account))

    def _get_account_node_count(self, account):
        return get("//sys/accounts/{0}/@resource_usage/node_count".format(account))

    def _get_account_chunk_count(self, account):
        return get("//sys/accounts/{0}/@resource_usage/chunk_count".format(account))

    def _get_account_committed_disk_space(self, account):
        return get("//sys/accounts/{0}/@committed_resource_usage/disk_space".format(account))

    def _get_account_disk_space_limit(self, account):
        return get("//sys/accounts/{0}/@resource_limits/disk_space".format(account))

    def _set_account_disk_space_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/disk_space".format(account), value)

    def _get_account_node_count_limit(self, account):
        return get("//sys/accounts/{0}/@resource_limits/node_count".format(account))

    def _set_account_node_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/node_count".format(account), value)

    def _get_account_chunk_count_limit(self, account):
        return get("//sys/accounts/{0}/@resource_limits/chunk_count".format(account))

    def _set_account_chunk_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/chunk_count".format(account), value)

    def _is_account_disk_space_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/disk_space".format(account))

    def _is_account_node_count_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/node_count".format(account))
    
    def _is_account_chunk_count_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/chunk_count".format(account))
    
    def _get_tx_disk_space(self, tx, account):
        return get("#{0}/@resource_usage/{1}/disk_space".format(tx, account))

    def _get_node_disk_space(self, path, *args, **kwargs):
        return get("{0}/@resource_usage/disk_space".format(path), *args, **kwargs)


    def test_init(self):
        self.assertItemsEqual(sorted(ls("//sys/accounts")), sorted(["sys", "tmp", "intermediate"]))
        assert get("//@account") == "sys"
        assert get("//sys/@account") == "sys"
        assert get("//tmp/@account") == "tmp"
        assert get("//home/@account") == "tmp"

    def test_account_create1(self):
        create_account("max")
        self.assertItemsEqual(sorted(ls("//sys/accounts")), sorted(["sys", "tmp", "intermediate", "max"]))
        assert self._get_account_disk_space("max") == 0
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
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count - 1
        assert self._get_account_chunk_count("max") == 1

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
        assert self._get_account_disk_space("tmp") == 0

        content = "some_data"
        create("file", "//tmp/f1")
        write_file("//tmp/f1", content)

        multicell_sleep()
        space = self._get_account_disk_space("tmp")
        assert space > 0

        create("file", "//tmp/f2")
        write_file("//tmp/f2", content)
        
        multicell_sleep()
        assert self._get_account_disk_space("tmp") == 2 * space

        remove("//tmp/f1")

        gc_collect()
        multicell_sleep()
        assert self._get_account_disk_space("tmp") == space

        remove("//tmp/f2")

        gc_collect()
        multicell_sleep()
        assert self._get_account_disk_space("tmp") == 0

    def test_file2(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)
        space = self._get_account_disk_space("tmp")

        create_account("max")
        set("//tmp/f/@account", "max")

        multicell_sleep()
        assert self._get_account_disk_space("tmp") == 0
        assert self._get_account_disk_space("max") == space

        remove("//tmp/f")
        
        gc_collect()
        multicell_sleep()
        assert self._get_account_disk_space("max") == 0

    def test_file3(self):
        create_account("max")

        assert self._get_account_disk_space("max") == 0

        content = "some_data"
        create("file", "//tmp/f", attributes={"account": "max"})
        write_file("//tmp/f", content)
        
        multicell_sleep()
        assert self._get_account_disk_space("max") > 0

        remove("//tmp/f")
        
        gc_collect()
        multicell_sleep()
        assert self._get_account_disk_space("max") == 0

    def test_file4(self):
        create_account("max")

        content = "some_data"
        create("file", "//tmp/f", attributes={"account": "max"})
        write_file("//tmp/f", content)
        
        multicell_sleep()
        space = self._get_account_disk_space("max")
        assert space > 0

        rf  = get("//tmp/f/@replication_factor")
        set("//tmp/f/@replication_factor", rf * 2)

        multicell_sleep()
        assert self._get_account_disk_space("max") == space * 2

    def test_table1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        assert self._get_account_disk_space("tmp") > 0

    def test_table2(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        for i in xrange(0, 5):
            write_table("//tmp/t", {"a" : "b"}, tx=tx)
            multicell_sleep()
            account_space = self._get_account_disk_space("tmp")
            tx_space = self._get_tx_disk_space(tx, "tmp")
            assert account_space > 0
            assert account_space == tx_space
            assert self._get_node_disk_space("//tmp/t") == 0
            assert self._get_node_disk_space("//tmp/t", tx=tx) == tx_space
            last_space = tx_space

        commit_transaction(tx)

        multicell_sleep()
        assert self._get_node_disk_space("//tmp/t") == last_space

    def test_table3(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        space1 = self._get_account_disk_space("tmp")
        assert space1 > 0

        tx = start_transaction()
        write_table("//tmp/t", {"xxxx" : "yyyy"}, tx=tx)

        multicell_sleep()
        space2 = self._get_tx_disk_space(tx, "tmp")
        assert space1 != space2
        assert self._get_account_disk_space("tmp") == space1 + space2
        assert self._get_node_disk_space("//tmp/t") == space1
        assert self._get_node_disk_space("//tmp/t", tx=tx) == space2

        commit_transaction(tx)

        multicell_sleep()
        assert self._get_account_disk_space("tmp") == space2
        assert self._get_node_disk_space("//tmp/t") == space2

    def test_table4(self):
        tx = start_transaction()
        create("table", "//tmp/t", tx=tx)
        write_table("//tmp/t", {"a" : "b"}, tx=tx)
        
        multicell_sleep()
        assert self._get_account_disk_space("tmp") > 0
        
        abort_transaction(tx)

        multicell_sleep()
        assert self._get_account_disk_space("tmp") == 0

    def test_table5(self):
        tmp_node_count = self._get_account_node_count("tmp")
        tmp_chunk_count = self._get_account_chunk_count("tmp")

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        multicell_sleep()
        assert self._get_account_node_count("tmp") == tmp_node_count + 1
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count + 1
        space = self._get_account_disk_space("tmp")
        assert space > 0

        create_account("max")

        set("//tmp/t/@account", "max")

        multicell_sleep()
        assert self._get_account_node_count("tmp") == tmp_node_count
        assert self._get_account_node_count("max") == 1
        assert self._get_account_chunk_count("max") == 1
        assert self._get_account_disk_space("tmp") == 0
        assert self._get_account_disk_space("max") == space

        set("//tmp/t/@account", "tmp")

        multicell_sleep()
        assert self._get_account_node_count("tmp") == tmp_node_count + 1
        assert self._get_account_chunk_count("tmp") == tmp_chunk_count + 1
        assert self._get_account_node_count("max") == 0
        assert self._get_account_chunk_count("max") == 0
        assert self._get_account_disk_space("tmp") == space
        assert self._get_account_disk_space("max") == 0

    def test_table6(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        write_table("//tmp/t", {"a" : "b"}, tx=tx)

        multicell_sleep()
        space = self._get_node_disk_space("//tmp/t", tx=tx)
        assert space > 0
        assert self._get_account_disk_space("tmp") == space

        tx2 = start_transaction(tx=tx)

        multicell_sleep()
        assert self._get_node_disk_space("//tmp/t", tx=tx2) == space

        write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx2)

        multicell_sleep()
        assert self._get_node_disk_space("//tmp/t", tx=tx2) == space * 2
        assert self._get_account_disk_space("tmp") == space * 2

        commit_transaction(tx2)

        multicell_sleep()
        assert self._get_node_disk_space("//tmp/t", tx=tx) == space * 2
        assert self._get_account_disk_space("tmp") == space * 2
        commit_transaction(tx)

        multicell_sleep()
        assert self._get_node_disk_space("//tmp/t") == space * 2
        assert self._get_account_disk_space("tmp") == space * 2

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
        self._set_account_disk_space_limit("max", 1000)
        self._set_account_disk_space_limit("max", 2000)
        self._set_account_disk_space_limit("max", 0)
        assert not self._is_account_disk_space_limit_violated("max")
        with pytest.raises(YtError): self._set_account_disk_space_limit("max", -1)

    def test_disk_space_limits2(self):
        create_account("max")
        self._set_account_disk_space_limit("max", 1000000)

        create("table", "//tmp/t")
        set("//tmp/t/@account", "max")

        write_table("//tmp/t", {"a" : "b"})
        
        multicell_sleep()
        assert not self._is_account_disk_space_limit_violated("max")

        self._set_account_disk_space_limit("max", 0)
        multicell_sleep()
        assert self._is_account_disk_space_limit_violated("max")
        with pytest.raises(YtError): write_table("//tmp/t", {"a" : "b"})

        self._set_account_disk_space_limit("max", self._get_account_disk_space("max") + 1)
        multicell_sleep()
        assert not self._is_account_disk_space_limit_violated("max")
        
        write_table("<append=true>//tmp/t", {"a" : "b"})
        
        multicell_sleep()
        assert self._is_account_disk_space_limit_violated("max")

    def test_disk_space_limits3(self):
        create_account("max")
        self._set_account_disk_space_limit("max", 1000000)

        content = "some_data"

        create("file", "//tmp/f1", attributes={"account": "max"})
        write_file("//tmp/f1", content)
        
        multicell_sleep()
        assert not self._is_account_disk_space_limit_violated("max")

        self._set_account_disk_space_limit("max", 0)
        assert self._is_account_disk_space_limit_violated("max")

        create("file", "//tmp/f2", attributes={"account": "max"})
        with pytest.raises(YtError): write_file("//tmp/f2", content)

        self._set_account_disk_space_limit("max", self._get_account_disk_space("max") + 1)
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
        disk_space = get("//tmp/a/f1/@resource_usage/disk_space")
        assert get("//tmp/a/f2/@resource_usage/disk_space") == disk_space

        create_account("max")
        create("map_node", "//tmp/b")
        set("//tmp/b/@account", "max")

        self._set_account_disk_space_limit("max", disk_space * 2 + 1)
        copy("//tmp/a", "//tmp/b/a")
        
        multicell_sleep()
        assert self._get_account_disk_space("max") == disk_space * 2
        assert exists("//tmp/b/a")

        remove("//tmp/b/a")
        
        gc_collect()
        multicell_sleep()
        assert self._get_account_disk_space("max") == 0
        assert self._get_account_node_count("max") == 1
        
        assert not exists("//tmp/b/a")

    def test_committed_usage(self):
        multicell_sleep()
        assert self._get_account_committed_disk_space("tmp") == 0

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        
        multicell_sleep()
        space = get("//tmp/t/@resource_usage/disk_space")
        assert space > 0
        assert self._get_account_committed_disk_space("tmp") == space

        tx = start_transaction()
        write_table("<append=true>//tmp/t", {"a" : "b"}, tx=tx)
        
        multicell_sleep()
        assert self._get_account_committed_disk_space("tmp") == space

        commit_transaction(tx)
        
        multicell_sleep()
        assert self._get_account_committed_disk_space("tmp") == space * 2

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
        space = self._get_account_disk_space("a1")
        assert space > 0
        assert space == self._get_account_committed_disk_space("a1")

        copy("//tmp/x1/t", "//tmp/x2/t")
        assert get("//tmp/x2/t/@account") == "a2"

        multicell_sleep()
        assert space == self._get_account_disk_space("a2")
        assert space == self._get_account_committed_disk_space("a2")

    def test_rename_success(self):
        create_account("a1")
        set("//sys/accounts/a1/@name", "a2")
        assert get("//sys/accounts/a2/@name") == "a2"

    def test_rename_fail(self):
        create_account("a1")
        create_account("a2")
        with pytest.raises(YtError): set("//sys/accounts/a1/@name", "a2")

##################################################################

class TestAccountsMulticell(TestAccounts):
    NUM_SECONDARY_MASTER_CELLS = 2
    