from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, run_sleeping_vanilla, wait, get, set, exists, ls, check_permission,
    create_pool, create_pool_tree, get_driver, raises_yt_error)

from yt_scheduler_helpers import (
    scheduler_orchid_operations_by_pool_path, scheduler_orchid_operation_path,
    scheduler_orchid_operation_by_pool_path, scheduler_new_orchid_pool_tree_path, scheduler_orchid_pool_tree_path)

from yt.wrapper.client import create_client_with_command_params
from yt.wrapper import YtClient

from yt.common import YtError

from yt import yson

import builtins
import time

##################################################################


class TestSchedulerOperationsByPoolOrchid(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {"heartbeat_period": 100},  # 100 msec
            "controller_agent_connector": {"heartbeat_period": 100},  # 100 msec
        },
    }

    @authors("pogorelov")
    def test_pool_tree_orchid(self):
        create_pool_tree("pool_tree", config={"nodes_filter": "tag"})
        wait(
            lambda: exists(scheduler_new_orchid_pool_tree_path("pool_tree")))

    @authors("pogorelov")
    def test_pool(self):
        create_pool("parent_pool")
        create_pool("child_pool", parent_name="parent_pool")
        wait(lambda: exists(scheduler_orchid_operations_by_pool_path("parent_pool", "default")))
        wait(lambda: exists(scheduler_orchid_operations_by_pool_path("child_pool", "default")))

    @authors("pogorelov")
    def test_operations(self):
        create_pool("pool")
        op1 = run_sleeping_vanilla(spec={"pool": "pool"}, job_count=1)
        op2 = run_sleeping_vanilla(spec={"pool": "pool"}, job_count=1)

        pool_path = scheduler_orchid_operations_by_pool_path("pool")
        wait(lambda: len(ls(pool_path)) == 2)

        assert builtins.set([op1.id, op2.id]) == builtins.set(ls(pool_path))

        wait(lambda: exists(scheduler_orchid_operation_path(op1.id)))

        wait(lambda: get(scheduler_orchid_operation_by_pool_path(op1.id, "pool")) == get(scheduler_orchid_operation_path(op1.id)))

        wait(lambda: exists(scheduler_orchid_operation_path(op2.id)))

        wait(lambda: get(scheduler_orchid_operation_by_pool_path(op2.id, "pool")) == get(scheduler_orchid_operation_path(op2.id)))

    @authors("pogorelov")
    def test_pools(self):
        create_pool("pool")
        create_pool("child", parent_name="pool", attributes={"weight": 3.0})

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/pools") == get(
                scheduler_orchid_pool_tree_path("default") + "/pools"))

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/pool_count") == get(
                scheduler_orchid_pool_tree_path("default") + "/pool_count"))

    @authors("pogorelov")
    def test_list_pools(self):
        create_pool("pool")
        create_pool("child", parent_name="pool", attributes={"weight": 3.0})

        def equal_list_results():
            list_pools_result = ls(scheduler_orchid_pool_tree_path("default") + "/pools").sort()
            list_pools_new_orchid_result = ls(scheduler_new_orchid_pool_tree_path("default") + "/pools").sort()
            return list_pools_result == list_pools_new_orchid_result
        wait(equal_list_results)

    @authors("omgronny")
    def test_pools_exist(self):
        create_pool("valid_pool")

        assert exists("//sys/scheduler/orchid/scheduler/pool_trees/default/pools")
        assert exists("//sys/scheduler/orchid/scheduler/pool_trees/default/pools/valid_pool")
        assert not exists("//sys/scheduler/orchid/scheduler/pool_trees/default/pools/invalid_pool")

    @authors("omgronny")
    def test_child_pools_by_pool(self):
        create_pool("pool1")
        create_pool("pool2")
        create_pool("pool3", parent_name="pool1")
        create_pool("pool4", parent_name="pool1")
        create_pool("pool5", parent_name="pool2")

        client = create_client_with_command_params(
            YtClient(config={
                "enable_token": False,
                "backend": "native",
                "driver_config": get_driver().get_config(),
            }),
            fields=["full_path"],
        )

        def child_pools_by_pool_orchid_path(pool):
            return "//sys/scheduler/orchid/scheduler/pool_trees/default/child_pools_by_pool" + pool

        assert client.get(child_pools_by_pool_orchid_path("")) == {
            "<Root>": yson.YsonEntity(),
            "pool3": yson.YsonEntity(),
            "pool2": yson.YsonEntity(),
            "pool5": yson.YsonEntity(),
            "pool4": yson.YsonEntity(),
            "pool1": yson.YsonEntity(),
        }

        assert client.get(child_pools_by_pool_orchid_path("/pool1")) == {
            "pool3": {"full_path": "/pool1/pool3"},
            "pool4": {"full_path": "/pool1/pool4"},
        }
        assert client.get(child_pools_by_pool_orchid_path("/pool2")) == {
            "pool5": {"full_path": "/pool2/pool5"},
        }
        assert client.get(child_pools_by_pool_orchid_path("/pool5")) == {}

        assert client.list(child_pools_by_pool_orchid_path("")) == ["<Root>", "pool1", "pool2", "pool3", "pool4", "pool5"]
        assert client.list(child_pools_by_pool_orchid_path("/pool1")) == ["pool3", "pool4"]
        assert client.list(child_pools_by_pool_orchid_path("/pool2")) == ["pool5"]
        assert client.list(child_pools_by_pool_orchid_path("/pool5")) == []

        assert client.exists(child_pools_by_pool_orchid_path(""))
        assert client.exists(child_pools_by_pool_orchid_path("/pool2"))
        assert not client.exists(child_pools_by_pool_orchid_path("/invalid_pool"))

        assert client.list(child_pools_by_pool_orchid_path("/pool1/@")) == []
        assert client.get(child_pools_by_pool_orchid_path("/pool1/@")) == {}
        with raises_yt_error():
            client.get(child_pools_by_pool_orchid_path("/pool1/@full_path"))

    @authors("pogorelov")
    def test_pools_single_pool(self):
        create_pool("pool")
        create_pool("child", parent_name="pool", attributes={"weight": 3.0})

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/pools/pool") == get(
                scheduler_orchid_pool_tree_path("default") + "/pools/pool"))

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/pools/pool/mode") == get(
                scheduler_orchid_pool_tree_path("default") + "/pools/pool/mode"))

    @authors("ignat")
    def test_weird_requests(self):
        # See details in YT-16814
        assert get(scheduler_new_orchid_pool_tree_path("default") + "/pools/@") == {}
        assert ls(scheduler_new_orchid_pool_tree_path("default") + "/pools/@") == []
        with raises_yt_error("\"CheckPermission\" method is not supported"):
            check_permission("root", "read", scheduler_new_orchid_pool_tree_path("default") + "/pools")
        with raises_yt_error("Error parsing attribute \"fields\""):
            get(scheduler_new_orchid_pool_tree_path("default") + "/pools", fields=10)

    @authors("pogorelov")
    def test_resource_distribution_info(self):
        set("//sys/pool_trees/default/@config/main_resource", "cpu")
        create_pool("pool", attributes={"strong_guarantee_resources": {"cpu": 72}})

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/resource_distribution_info") == get(
                scheduler_orchid_pool_tree_path("default") + "/resource_distribution_info"))

    @authors("pogorelov")
    def test_orchid_filter(self):
        create_pool("pool")
        create_pool("child", parent_name="pool", attributes={"weight": 3.0})

        client = create_client_with_command_params(
            YtClient(config={
                "enable_token": False,
                "backend": "native",
                "driver_config": get_driver().get_config(),
            }),
            fields=["running_operation_count", "mode", "is_ephemeral", "full_path"],
        )

        assert client.get(scheduler_new_orchid_pool_tree_path("default") + "/pools") == {
            'child': {
                'mode': 'fair_share',
                'is_ephemeral': False,
                'running_operation_count': 0,
                'full_path': '/pool/child',
            },
            'pool': {
                'mode': 'fair_share',
                'is_ephemeral': False,
                'running_operation_count': 0,
                'full_path': '/pool',
            },
            '<Root>': {
                'running_operation_count': 0,
            },
        }

        assert client.get(scheduler_new_orchid_pool_tree_path("default") + "/pools/child") == {
            'mode': 'fair_share',
            'is_ephemeral': False,
            'running_operation_count': 0,
            'full_path': '/pool/child',
        }

        assert client.get(scheduler_new_orchid_pool_tree_path("default") + "/pools/<Root>") == {
            'running_operation_count': 0,
        }


class TestOrchidOnSchedulerRestart(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 500,
            "testing_options": {
                "enable_random_master_disconnection": False,
                "random_master_disconnection_max_backoff": 2000,
                "finish_operation_transition_delay": {
                    "duration": 1000,
                },
            },
        },
    }

    @authors("pogorelov")
    def test_orchid_on_scheduler_restarts(self):
        create_pool("pool")
        try:
            set("//sys/scheduler/config/testing_options", {"enable_random_master_disconnection": True})
            for _ in range(50):
                try:
                    get(scheduler_new_orchid_pool_tree_path("default") + "/resource_usage", verbose=False)
                    get(scheduler_new_orchid_pool_tree_path("default") + "/resource_distribution_info", verbose=False)
                    get(scheduler_new_orchid_pool_tree_path("default") + "/pools", verbose=False)
                    get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits", verbose=False)
                    get(scheduler_new_orchid_pool_tree_path("default") + "/config", verbose=False)
                    get(scheduler_new_orchid_pool_tree_path("default") + "/operations", verbose=False)
                    get(scheduler_new_orchid_pool_tree_path("default") + "/operations_by_pool", verbose=False)
                except YtError:
                    pass
                time.sleep(0.1)
        finally:
            set("//sys/scheduler/config/testing_options", {})
            time.sleep(2)
