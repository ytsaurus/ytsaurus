from yt_env_setup import YTEnvSetup
from yt_commands import (authors, get, create, sort, write_table, read_table)

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

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

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
        "22_4": ["master", "node", "job-proxy", "exec", "tools", "scheduler",
                 "controller-agent", "proxy", "http-proxy"]
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False


class TestNewUpToProxies(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "node", "job-proxy", "exec", "tools", "scheduler",
                 "controller-agent"],
        "trunk": ["proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False


class TestNewUpToSchedulerAndCA(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False


class TestNewUpToNodes(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master"],
        "trunk": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False


class TestNewNodesOldSchedulerAndCA(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False
