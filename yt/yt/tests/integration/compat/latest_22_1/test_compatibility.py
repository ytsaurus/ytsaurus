from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, get, create, write_table, sort, read_table, create_account, create_user,
    make_ace, write_file, internalize, wait, exists, read_file, set)

"""
This file contains the most basic checks that are run in all possible combinations that appear
when we update production clusters in standard component order:
- RPC + HTTP proxies;
- Scheduler + CA;
- Node and all its stuff;
- Masters.

Checks in the base class correspond to the most basic checks that verify that cluster is sane (similar
to the basic Odin checks).

Try not to pollute this test suite with too many checks, if you want to
test particular compatibility (i.e. old nodes + new CA), instantiate a separate test suite with
custom component configuration.

So, save the trees by not spawning too many redundant tests.

PS. For now this test suite is supposed to be copy & pasted to each new target. Maybe it is possible to
make it generic and eliminate copy-paste need, but I do not want to think about that for now.
"""


class ClusterSetupTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SECONDARY_MASTER_CELLS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    @authors("max42")
    def test_cluster_start(self):
        # Just check that cluster is able to start and stop properly.
        pass

    @authors("ignat")
    def test_simple(self):
        # Similar to Odin's sort result.

        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key")

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]


class TestEverythingIsOld(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "node", "job-proxy", "exec", "tools", "scheduler",
                 "controller-agent", "proxy", "http-proxy"]
    }


class TestNewUpToProxies(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "node", "job-proxy", "exec", "tools", "scheduler",
                 "controller-agent"],
        "trunk": ["proxy", "http-proxy"],
    }


class TestNewUpToSchedulerAndCA(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestNewUpToNodes(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master"],
        "trunk": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestNewNodesOldSchedulerAndCA(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }


class TestPortalSynchronization(TestNewUpToNodes):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_SCHEDULERS = 1

    @authors("kvk1920")
    def test_master_feature_portal_synchronization(self):
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        create_account("a")
        create_account("b")
        create_user("u")

        create("portal_entrance", "//tmp/m", attributes={"exit_cell_tag": 11})
        set("//tmp/m/@attr", "value")
        set("//tmp/m/@acl", [make_ace("allow", "u", "write")])
        shard_id = get("//tmp/m/@shard_id")

        TABLE_PAYLOAD = [{"key": "value"}]
        create(
            "table",
            "//tmp/m/t",
            attributes={
                "external": True,
                "external_cell_tag": 13,
                "account": "a",
                "attr": "t",
            },
        )
        write_table("//tmp/m/t", TABLE_PAYLOAD)

        FILE_PAYLOAD = b"PAYLOAD"
        create(
            "file",
            "//tmp/m/f",
            attributes={
                "external": True,
                "external_cell_tag": 13,
                "account": "b",
                "attr": "f",
            },
        )
        write_file("//tmp/m/f", FILE_PAYLOAD)

        create("document", "//tmp/m/d", attributes={"value": {"hello": "world"}})
        ct = get("//tmp/m/d/@creation_time")
        mt = get("//tmp/m/d/@modification_time")

        create(
            "map_node",
            "//tmp/m/m",
            attributes={"account": "a", "compression_codec": "brotli_8"},
        )

        create(
            "table",
            "//tmp/m/et",
            attributes={
                "external_cell_tag": 13,
                "expiration_time": "2100-01-01T00:00:00.000000Z",
            },
        )

        create(
            "map_node",
            "//tmp/m/acl1",
            attributes={"inherit_acl": True, "acl": [make_ace("deny", "u", "read")]},
        )
        create(
            "map_node",
            "//tmp/m/acl2",
            attributes={"inherit_acl": False, "acl": [make_ace("deny", "u", "read")]},
        )

        root_acl = get("//tmp/m/@effective_acl")
        acl1 = get("//tmp/m/acl1/@acl")
        acl2 = get("//tmp/m/acl2/@acl")

        ORCHID_MANIFEST = {"address": "someaddress"}
        create("orchid", "//tmp/m/orchid", attributes={"manifest": ORCHID_MANIFEST})

        internalize("//tmp/m")

        wait(lambda: not exists("#" + shard_id))

        assert not get("//tmp/m/@inherit_acl")
        assert get("//tmp/m/@effective_acl") == root_acl

        assert get("//tmp/m/acl1/@inherit_acl")
        assert get("//tmp/m/acl1/@acl") == acl1

        assert not get("//tmp/m/acl2/@inherit_acl")
        assert get("//tmp/m/acl2/@acl") == acl2

        assert get("//tmp/m/@type") == "map_node"
        assert get("//tmp/m/@attr") == "value"

        assert read_table("//tmp/m/t") == TABLE_PAYLOAD
        assert get("//tmp/m/t/@account") == "a"
        assert get("//tmp/m/t/@attr") == "t"

        assert read_file("//tmp/m/f") == FILE_PAYLOAD
        assert get("//tmp/m/f/@account") == "b"
        assert get("//tmp/m/f/@attr") == "f"

        assert get("//tmp/m/d") == {"hello": "world"}
        assert get("//tmp/m/d/@creation_time") == ct
        assert get("//tmp/m/d/@modification_time") == mt

        assert get("//tmp/m/m/@account") == "a"
        assert get("//tmp/m/m/@compression_codec") == "brotli_8"

        assert get("//tmp/m/et/@expiration_time") == "2100-01-01T00:00:00.000000Z"

        assert get("//tmp/m/orchid/@type") == "orchid"
        assert get("//tmp/m/orchid/@manifest") == ORCHID_MANIFEST

        assert get("//tmp/m/t/@shard_id") == get("//tmp/@shard_id")
