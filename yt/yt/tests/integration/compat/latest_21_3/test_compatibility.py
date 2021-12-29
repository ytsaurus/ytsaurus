from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE
from yt_commands import (
    ls, update_nodes_dynamic_config, set, print_debug,
    create_medium, wait, exists, get_singular_chunk_id,
    authors, get, create, write_table, sort, read_table)

from copy import deepcopy

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
        "21_3": ["master", "node", "job-proxy", "exec", "tools", "scheduler",
                 "controller-agent", "proxy", "http-proxy"]
    }


class TestNewUpToProxies(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "21_3": ["master", "node", "job-proxy", "exec", "tools", "scheduler",
                 "controller-agent"],
        "trunk": ["proxy", "http-proxy"],
    }


class TestNewUpToSchedulerAndCA(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "21_3": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestNewUpToNodes(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "21_3": ["master"],
        "trunk": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestNewNodesOldSchedulerAndCA(ClusterSetupTestBase):
    ARTIFACT_COMPONENTS = {
        "21_3": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }

# There is no need in test with new masters, such test should not belong to this test suite.


class TestMediumUpdaterLegacyBehavior(TestNewUpToNodes):
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100,
        },
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 1
            }
        }
    }

    @classmethod
    def modify_node_config(cls, config):
        assert len(config["data_node"]["store_locations"]) == 1
        location_prototype = config["data_node"]["store_locations"][0]

        location0 = deepcopy(location_prototype)
        location0["path"] += "_0"
        location0["medium_name"] = "default"

        location1 = deepcopy(location_prototype)
        location1["path"] += "_1"
        location1["medium_name"] = "default"

        config["data_node"]["store_locations"] = [
            location0,
            location1,
        ]

    @authors("kvk1920")
    def test_medium_change_simple(self):
        update_nodes_dynamic_config({"data_node": {"medium_updater": {"period": 1}}})

        node = "//sys/data_nodes/" + ls("//sys/data_nodes")[0]

        def get_locations():
            return {
                location["location_uuid"]: location
                for location in get(node + "/@statistics/locations")
            }

        medium_name = "testmedium"
        if not exists("//sys/media/" + medium_name):
            create_medium(medium_name)

        table = "//tmp/t"
        create("table", table, attributes={"replication_factor": 1})
        write_table(table, {"foo": "bar"}, table_writer={"upload_replication_factor": 1})

        print_debug(get_singular_chunk_id(table))

        location1, location2 = get_locations().keys()

        wait(lambda: sum(map(lambda location: location["chunk_count"], get_locations().values())) == 1)

        set(node + "/@config", {
            "medium_overrides": {
                location1: medium_name
            }
        })

        wait(lambda: get_locations()[location1]["medium_name"] == medium_name)
        assert get_locations()[location2]["medium_name"] == "default"

        wait(lambda: get_locations()[location1]["chunk_count"] == 0)
        assert get_locations()[location2]["chunk_count"] == 1

        with Restarter(self.Env, NODES_SERVICE):
            pass

        assert get_locations()[location1]["medium_name"] == medium_name
        assert get_locations()[location2]["medium_name"] == "default"

        set(node + "/@config", {
            "medium_overrides": {
                location2: medium_name
            }
        })

        wait(lambda: get_locations()[location1]["medium_name"] == "default")
        assert get_locations()[location2]["medium_name"] == medium_name

        wait(lambda: get_locations()[location2]["chunk_count"] == 0)
        assert get_locations()[location1]["chunk_count"] == 1

        with Restarter(self.Env, NODES_SERVICE):
            pass

        assert get_locations()[location1]["medium_name"] == "default"
        assert get_locations()[location2]["medium_name"] == medium_name
