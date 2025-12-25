import sys
import os
from yt_commands import authors
from typing import Any, Generator
import logging

import requests
import yatest.common
import pytest

from yatest.common import network

from yt_env_setup import YTEnvSetup
from yt_commands import (
    create_account,
    create_domestic_medium,
    create_network_project,
    create_pool,
    create_user,
    create,
    exists,
    link,
    mount_table,
    set,
    sync_create_cells,
)

from native_snapshot_exporter import NativeSnapshotRunner

import subprocess
from yt.wrapper import YtClient

from yt.wrapper.testlib.helpers import wait

RESOURCE_USAGE_ROREN: str = yatest.common.binary_path("yt/microservices/resource_usage_roren/resource_usage_roren")
RESOURCE_USAGE_JSON_API_GO: str = yatest.common.binary_path(
    "yt/microservices/resource_usage/json_api_go/cmd/resource_usage_api/resource_usage_api"
)

NETWORK_PROJECT_NAME = "yt_microservices"


class TestResourceUsage(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    NUM_SCHEDULERS = 1

    preprocessing_previously_runned = False
    already_initialized = False

    NUM_MASTERS = 1
    NUM_NODES = 6

    STORE_LOCATION_COUNT = 2
    DEFAULT_MEDIUM = "default"
    TEST_MEDIUM = "test_medium"

    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

    NUM_TEST_PARTITIONS = 2
    CLASS_TEST_LIMIT = 12 * 60

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 2
        config["data_node"]["store_locations"][0]["medium_name"] = cls.DEFAULT_MEDIUM
        config["data_node"]["store_locations"][1]["medium_name"] = cls.TEST_MEDIUM

    def setup_method(self, method: str):
        super(TestResourceUsage, self).setup_method(method)
        self._define_ports()
        create("map_node", "//home", recursive=True, ignore_existing=True)
        create("map_node", "//sys/admin/yt-microservices/resource_usage", recursive=True, ignore_existing=True)
        link(
            "//sys/admin/yt-microservices/resource_usage",
            "//sys/admin/snapshots/resource_usage_dump",
            force=True,
            recursive=True,
        )
        if not exists(f"//sys/network_projects/{NETWORK_PROJECT_NAME}"):
            create_network_project(NETWORK_PROJECT_NAME)
        sync_create_cells(1)
        create_pool("yt-microservices")
        self.snapshot_runner = NativeSnapshotRunner(self.path_to_run, self.bin_path)
        self.snapshot_runner.build_and_export_master_snapshot(YtClient(self.Env.get_http_proxy_address()), None)
        self.run_preprocessing()
        self.wait_for_services(
            self.resource_usage_json_api_go_process(),
        )
        if not exists("//sys/users/test_user"):
            create_user("test_user")
        if not exists(f"//sys/media/{self.TEST_MEDIUM}"):
            create_domestic_medium(self.TEST_MEDIUM)
        if not exists("//sys/accounts/test_account"):
            create_account("test_account")
        set(
            "//sys/accounts/test_account/@resource_limits/disk_space_per_medium",
            {self.DEFAULT_MEDIUM: 1024 * 1024 * 1024, self.TEST_MEDIUM: 1024 * 1024 * 1024},
        )

        if not exists("//sys/accounts/test_account2"):
            create_account("test_account2")
        set(
            "//sys/accounts/test_account2/@resource_limits/disk_space_per_medium",
            {self.DEFAULT_MEDIUM: 1024 * 1024 * 1024, self.TEST_MEDIUM: 1024 * 1024 * 1024},
        )

        if not exists("//sys/admin/yt-microservices/node_id_dict/data"):
            schema = [
                {
                    "name": "cluster",
                    "required": False,
                    "sort_order": "ascending",
                    "type": "string"
                },
                {
                    "name": "node_id",
                    "required": False,
                    "sort_order": "ascending",
                    "type": "string"
                },
                {
                    "name": "path",
                    "required": False,
                    "type": "string"
                }
            ]
            create("table", "//sys/admin/yt-microservices/node_id_dict/data", attributes={"schema": schema, "dynamic": True}, recursive=True)
            mount_table("//sys/admin/yt-microservices/node_id_dict/data")

    def teardown_method(self, method, wait_for_nodes=True):
        super(TestResourceUsage, self).teardown_method(method, wait_for_nodes)
        self._resource_usage_json_api_go_process.kill()

    @pytest.fixture
    def home_ypath(self, request: pytest.FixtureRequest):
        yield f"//home/{request.node.originalname}"

    @pytest.fixture(scope="module")
    def yt_proxy(self) -> Generator[str, None, None]:
        yield self.Env.get_http_proxy_address()

    @pytest.fixture
    def yt_client(self, home_ypath: str) -> Generator[YtClient, None, None]:
        yt_client = YtClient(self.Env.get_http_proxy_address())
        yt_client.create("map_node", home_ypath, recursive=True)
        yield yt_client
        yt_client.remove(home_ypath, recursive=True, force=True)

    def run_resource_usage_preprocessing(
        self,
    ):
        env = {
            "YT_PROXY": self.Env.get_http_proxy_address(),
            "YT_RESOURCE_USAGE_TOKEN": "ANYTOKEN",
        }

        args = [
            RESOURCE_USAGE_ROREN,
            "import-snapshot",
            "--snapshot-id",
            "latest",
            "--node-id-dict-cluster",
            self.Env.get_http_proxy_address(),
        ]
        subprocess.run(args, env=env, stdout=sys.stderr, stderr=sys.stderr, check=True, timeout=3600)

    def run_preprocessing(self):
        self.run_resource_usage_preprocessing()
        self.preprocessing_previously_runned = True

    def _define_ports(self) -> None:
        with network.PortManager() as pm:
            self.resource_usage_port = pm.get_port()
            self.resource_usage_debug_port = pm.get_port()

    def resource_usage_json_api_go_process(self) -> subprocess.Popen:
        config = f"""
http_addr: "localhost:{self.resource_usage_port}"
debug_http_addr: "localhost:{self.resource_usage_debug_port}"
http_handler_timeout: "1m"
snapshot_root: "//sys/admin/snapshots/resource_usage_dump"
included_clusters:
  - cluster_name: "local"
    proxy: "{self.Env.get_http_proxy_address()}"
debug_login: "test_user"
disable_tvm: true
disable_acl: true
update_snapshots_on_every_request: true
logs_dir: "/tmp/resource_usage_json_api_go_logs"
bulk_acl_checker_base_url: "https://yt-bulk-acl-checker.ytsaurus.tech/"
"""
        config_path = os.path.join(self.path_to_run, "config.yaml")
        with open(config_path, "w") as f:
            f.write(config)

        args = [
            RESOURCE_USAGE_JSON_API_GO,
            "--config",
            config_path,
        ]
        env = {
            "YT_PROXY": self.Env.get_http_proxy_address(),
            "CRYPTO_SECRET": "ANYSECRET",
        }
        return subprocess.Popen(args, env=env)

    def wait_for_services(
        self,
        resource_usage_json_api_go_process: subprocess.Popen,
    ):
        if not (resource_usage_json_api_go_process.returncode is None):
            stdout, stderr = resource_usage_json_api_go_process.communicate()
            raise Exception(
                "Resource usage json api go process exited with code {}\nstdout: {}\nstderr: {}".format(
                    resource_usage_json_api_go_process.returncode, stdout, stderr
                )
            )

        wait(
            lambda: requests.get(f"http://localhost:{self.resource_usage_port}/ready").json()["ready"],
            timeout=120,
            ignore_exceptions=True,
        )

        self._resource_usage_json_api_go_process = resource_usage_json_api_go_process

    def _run_request(self, handler: str, payload: dict) -> dict[str, Any]:
        response = requests.post(f"http://localhost:{self.resource_usage_port}/{handler}", json=payload).json()
        logging.info("Handler %s, payload %s, response %s", handler, payload, response)
        return response

    @authors("ilyaibraev")
    def test_get_resource_usage(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        created_tables = []
        created_nodes = []
        for i in range(10):
            table_name = home_ypath + f"/test_table_{i}"
            yt_client.create(
                "table", table_name, attributes={"primary_medium": self.DEFAULT_MEDIUM if i % 2 else self.TEST_MEDIUM}
            )
            yt_client.write_table(table_name, [{"id": j, "name": f"test_{j}"} for j in range((i + 1) * 10)])
            node_name = home_ypath + f"/test_node_{i}"
            yt_client.create("map_node", node_name)
            created_tables.append(table_name)
            created_nodes.append(node_name)

        yt_client.create("map_node", home_ypath + "/different_account_node", attributes={"account": "test_account2"})

        self.snapshot_runner.build_and_export_master_snapshot(yt_client, None)
        self.run_preprocessing()

        # check if all tables are present
        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
            },
        )
        paths = [item["path"] for item in response["items"]]
        assert len(paths) == 10
        assert sorted(created_tables) == sorted(paths)

        # check if all tables and nodes are present
        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "page": {"index": 0, "size": 200},
                "row_filter": {
                    "exclude_map_nodes": False,
                },
            },
        )
        parent_nodes = ["/", "//home", "//home/test_get_resource_usage"]
        paths = [item["path"] for item in response["items"]]
        assert len(paths) == 23
        assert sorted(created_tables + created_nodes + parent_nodes) == sorted(paths)

        for item in response["items"]:
            if item["path"] == home_ypath + "/test_table":
                assert item["disk_space"] == yt_client.get(home_ypath + "/test_table/@resource_usage/disk_space")

        # check pagination
        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": []},
                "page": {"index": 0, "size": 1},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_0"
        assert len(response["items"]) == 1

        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": []},
                "page": {"index": 0, "size": 2},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_0"
        assert response["items"][1]["path"] == home_ypath + "/test_table_1"
        assert len(response["items"]) == 2

        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": []},
                "page": {"index": 1, "size": 2},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_2"
        assert response["items"][1]["path"] == home_ypath + "/test_table_3"
        assert len(response["items"]) == 2

        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": []},
                "page": {"index": 3, "size": 3},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_9"
        assert len(response["items"]) == 1

        # check sort
        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": []},
                "sort_order": [{"field": "path", "desc": False}],
            },
        )
        i = 0
        for item in response["items"]:
            assert item["path"] == home_ypath + f"/test_table_{i}"
            i += 1

        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": []},
                "sort_order": [{"field": "path", "desc": True}],
            },
        )
        i = 9
        for item in response["items"]:
            assert item["path"] == home_ypath + f"/test_table_{i}"
            i -= 1

        response = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": []},
                "sort_order": [{"field": f"medium:{self.TEST_MEDIUM}", "desc": True}],
            },
        )
        current_value = float("inf")
        for item in response["items"]:
            new_val = item["medium:test_medium"] if item["medium:test_medium"] is not None else 0
            assert new_val <= current_value
            current_value = new_val

        supported_comparisons = {
            "<": lambda x, y: x < y,
            "<=": lambda x, y: x <= y,
            "=": lambda x, y: x == y,
            ">=": lambda x, y: x >= y,
            ">": lambda x, y: x > y,
        }
        compare_table_size = yt_client.get(home_ypath + "/test_table_5/@resource_usage/disk_space")
        for comparison, func in supported_comparisons.items():
            response = self._run_request(
                "get-resource-usage",
                {
                    "cluster": "local",
                    "account": "test_account",
                    "timestamp": 9999999999999,
                    "timestamp_rounding_policy": "closest",
                    "row_filter": {
                        "field_filters": [{"field": "disk_space", "comparison": comparison, "value": compare_table_size}]
                    },
                },
            )
            for item in response["items"]:
                assert func(item["disk_space"], compare_table_size)

    def test_get_children_and_resource_usage(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        created_tables = []
        created_nodes = []
        for i in range(10):
            table_name = home_ypath + f"/test_table_{i}"
            yt_client.create(
                "table", table_name, attributes={"primary_medium": self.DEFAULT_MEDIUM if i % 2 else self.TEST_MEDIUM}
            )
            yt_client.write_table(table_name, [{"id": j, "name": f"test_{j}"} for j in range((i + 1) * 10)])
            node_name = home_ypath + f"/test_node_{i}"
            yt_client.create("map_node", node_name)
            created_tables.append(table_name)
            created_nodes.append(node_name)

        yt_client.create("map_node", home_ypath + "/different_account_node", attributes={"account": "test_account2"})

        self.snapshot_runner.build_and_export_master_snapshot(yt_client, None)
        self.run_preprocessing()

        # check if all tables are present
        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"base_path": home_ypath},
            },
        )
        paths = [item["path"] for item in response["items"]]
        assert len(paths) == 10
        assert sorted(created_tables) == sorted(paths)

        # check if all tables and nodes are present
        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "page": {"index": 0, "size": 200},
                "row_filter": {"exclude_map_nodes": False, "base_path": home_ypath},
            },
        )
        paths = [item["path"] for item in response["items"]]
        assert len(paths) == 20
        assert sorted(created_tables + created_nodes) == sorted(paths)

        for item in response["items"]:
            if item["path"] == home_ypath + "/test_table":
                assert item["disk_space"] == yt_client.get(home_ypath + "/test_table/@resource_usage/disk_space")

        # check pagination
        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": [], "base_path": home_ypath},
                "page": {"index": 0, "size": 1},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_0"
        assert len(response["items"]) == 1

        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": [], "base_path": home_ypath},
                "page": {"index": 0, "size": 2},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_0"
        assert response["items"][1]["path"] == home_ypath + "/test_table_1"
        assert len(response["items"]) == 2

        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": [], "base_path": home_ypath},
                "page": {"index": 1, "size": 2},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_2"
        assert response["items"][1]["path"] == home_ypath + "/test_table_3"
        assert len(response["items"]) == 2

        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": [], "base_path": home_ypath},
                "page": {"index": 3, "size": 3},
            },
        )
        assert response["items"][0]["path"] == home_ypath + "/test_table_9"
        assert len(response["items"]) == 1

        # check sort
        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": [], "base_path": home_ypath},
                "sort_order": [{"field": "path", "desc": False}],
            },
        )
        i = 0
        for item in response["items"]:
            assert item["path"] == home_ypath + f"/test_table_{i}"
            i += 1

        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": [], "base_path": home_ypath},
                "sort_order": [{"field": "path", "desc": True}],
            },
        )
        i = 9
        for item in response["items"]:
            assert item["path"] == home_ypath + f"/test_table_{i}"
            i -= 1

        response = self._run_request(
            "get-children-and-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamp": 9999999999999,
                "timestamp_rounding_policy": "closest",
                "row_filter": {"exclude_map_nodes": True, "field_filters": [], "base_path": home_ypath},
                "sort_order": [{"field": f"medium:{self.TEST_MEDIUM}", "desc": True}],
            },
        )
        current_value = float("inf")
        for item in response["items"]:
            new_val = item["medium:test_medium"] if item["medium:test_medium"] is not None else 0
            assert new_val <= current_value
            current_value = new_val

        supported_comparisons = {
            "<": lambda x, y: x < y,
            "<=": lambda x, y: x <= y,
            "=": lambda x, y: x == y,
            ">=": lambda x, y: x >= y,
            ">": lambda x, y: x > y,
        }
        compare_table_size = yt_client.get(home_ypath + "/test_table_5/@resource_usage/disk_space")
        for comparison, func in supported_comparisons.items():
            response = self._run_request(
                "get-children-and-resource-usage",
                {
                    "cluster": "local",
                    "account": "test_account",
                    "timestamp": 9999999999999,
                    "timestamp_rounding_policy": "closest",
                    "row_filter": {
                        "field_filters": [{"field": "disk_space", "comparison": comparison, "value": compare_table_size}],
                        "base_path": home_ypath,
                    },
                },
            )
            for item in response["items"]:
                assert func(item["disk_space"], compare_table_size)

    def test_get_resource_usage_diff(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")

        content = [{"foo": "bar"} for _ in range(5)]

        def _find_item_by_path(response: dict[str, Any], path: str):
            for item in response["items"]:
                if item["path"] == path:
                    return item
            raise ValueError(f"Item with path {path} not found")

        # Initial state
        table_recreated = home_ypath + "/test_table_recreated"
        yt_client.write_table(table_recreated, content)
        table_recreated_initial_size = yt_client.get(table_recreated + "/@resource_usage/disk_space")

        node_recreated = home_ypath + "/test_node_recreated"
        yt_client.create("map_node", node_recreated)
        yt_client.write_table(node_recreated + "/test_table", content)

        table_untouched = home_ypath + "/test_table_untouched"
        yt_client.write_table(table_untouched, content)

        node_untouched = home_ypath + "/test_node_untouched"
        yt_client.create("map_node", node_untouched)
        yt_client.write_table(node_untouched + "/test_table", content)

        table_removed = home_ypath + "/test_table_removed"
        yt_client.write_table(table_removed, content)
        table_removed_size = yt_client.get(table_removed + "/@resource_usage/disk_space")

        node_removed = home_ypath + "/test_node_removed"
        yt_client.create("map_node", node_removed)
        yt_client.write_table(node_removed + "/test_table", content)
        node_removed_size = yt_client.get(node_removed + "/@resource_usage/disk_space")

        table_modified = home_ypath + "/test_table_modified"
        yt_client.write_table(table_modified, content)
        old_table_modified_size = yt_client.get(table_modified + "/@resource_usage/disk_space")

        node_modified = home_ypath + "/test_node_modified"
        yt_client.create("map_node", node_modified)
        yt_client.write_table(node_modified + "/test_table", content)

        older_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(yt_client, None)
        self.run_preprocessing()

        # Modified state
        yt_client.remove(table_recreated)
        yt_client.write_table(table_recreated, content + content)
        table_recreated_after_size = yt_client.get(table_recreated + "/@resource_usage/disk_space")

        yt_client.remove(node_recreated, recursive=True)
        yt_client.create("map_node", node_recreated)
        yt_client.write_table(node_recreated + "/test_table", content + content)

        yt_client.remove(table_removed)
        yt_client.remove(node_removed, recursive=True)

        table_created = home_ypath + "/test_table_created"
        yt_client.write_table(table_created, content)
        table_created_size = yt_client.get(table_created + "/@resource_usage/disk_space")

        node_created = home_ypath + "/test_node_created"
        yt_client.create("map_node", node_created)
        yt_client.write_table(node_created + "/test_table", content)

        yt_client.write_table("<append=%true>" + table_modified, content)
        new_table_modified_size = yt_client.get(table_modified + "/@resource_usage/disk_space")
        yt_client.write_table(node_modified + "/test_table2", content)

        newer_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(yt_client, None)
        self.run_preprocessing()

        # API calls
        response_without_children = self._run_request(
            "get-resource-usage-diff",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamps": {
                    "older": {"timestamp": older_snapshot_timestamp, "timestamp_rounding_policy": "closest"},
                    "newer": {"timestamp": newer_snapshot_timestamp, "timestamp_rounding_policy": "closest"},
                },
                "row_filter": {"exclude_map_nodes": False},
                "page": {"index": 0, "size": 200},
            },
        )

        response_with_children_inside_test_dir = self._run_request(
            "get-children-and-resource-usage-diff",
            {
                "cluster": "local",
                "account": "test_account",
                "timestamps": {
                    "older": {"timestamp": older_snapshot_timestamp, "timestamp_rounding_policy": "closest"},
                    "newer": {"timestamp": newer_snapshot_timestamp, "timestamp_rounding_policy": "closest"},
                },
                "row_filter": {"exclude_map_nodes": False, "base_path": home_ypath},
                "page": {"index": 0, "size": 200},
            },
        )

        # Assertions
        for response in [response_without_children, response_with_children_inside_test_dir]:
            # Recreated
            item = _find_item_by_path(response, table_recreated)
            assert item["recreation_status"] == "recreated"
            assert item["disk_space"] == table_recreated_after_size - table_recreated_initial_size

            item = _find_item_by_path(response, node_recreated)
            assert item["recreation_status"] == "recreated"
            assert item["disk_space"] == table_recreated_after_size - table_recreated_initial_size

            # Untouched
            item = _find_item_by_path(response, table_untouched)
            assert item["recreation_status"] == "untouched"
            assert item["disk_space"] == 0

            item = _find_item_by_path(response, node_untouched)
            assert item["recreation_status"] == "untouched"
            assert item["disk_space"] == 0

            # Removed
            # TODO(ilyaibraev): support removed state (currently cant work because of LEFT JOIN query)
            # item = _find_item_by_path(response, table_removed)
            # assert item["recreation_status"] == "removed"
            # assert item["disk_space"] == -table_removed_size
            _ = table_removed_size

            # item = _find_item_by_path(response, node_removed)
            # assert item["recreation_status"] == "removed"
            # assert item["disk_space"] == -node_removed_size
            _ = node_removed_size

            # Created
            # TODO(ilyaibraev): Enable after YT-26732s
            # item = _find_item_by_path(response, table_created)
            # assert item["recreation_status"] == "created"
            # assert item["disk_space"] == table_created_size

            # item = _find_item_by_path(response, node_created)
            # assert item["recreation_status"] == "created"
            # assert item["disk_space"] == table_created_size
            _ = table_created_size

            # Modified
            item = _find_item_by_path(response, table_modified)
            assert item["recreation_status"] == "untouched"
            assert item["disk_space"] == new_table_modified_size - old_table_modified_size
            assert item["node_count"] == 0
            assert item["chunk_count"] == 1

            item = _find_item_by_path(response, node_modified)
            assert item["recreation_status"] == "untouched"
            assert item["disk_space"] == new_table_modified_size - old_table_modified_size
            assert item["chunk_count"] == 1
            assert item["node_count"] == 1

    def test_get_versioned_resource_usage_table_test_medium(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        content = [{"foo": "bar"} for _ in range(5)]
        table_path = home_ypath + "/test_table_versioned_test"

        with yt_client.Transaction() as tx:
            yt_client.create(
                "table",
                table_path,
                attributes={"primary_medium": self.TEST_MEDIUM},
            )
            yt_client.write_table(table_path, content)
            table_size = yt_client.get(table_path + "/@resource_usage/disk_space")

            newer_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(YtClient(self.Env.get_http_proxy_address()), None)
            self.run_preprocessing()

            response = self._run_request(
                "get-versioned-resource-usage",
                {
                    "cluster": "local",
                    "account": "test_account",
                    "path": table_path,
                    "timestamp": newer_snapshot_timestamp,
                    "timestamp_rounding_policy": "closest",
                    "type": "table",
                },
            )

            tx_str = tx.transaction_id
            assert tx_str in response["transactions"], str(response)
            assert response["transactions"][tx_str]["per_medium"][self.TEST_MEDIUM] == round(table_size / 3)

    def test_get_versioned_resource_usage_table_default_medium(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        content = [{"foo": "bar"} for _ in range(5)]
        table_path = home_ypath + "/test_table_versioned_default"

        with yt_client.Transaction() as tx:
            yt_client.create(
                "table",
                table_path,
                attributes={"primary_medium": self.DEFAULT_MEDIUM},
            )
            yt_client.write_table(table_path, content)
            table_size = yt_client.get(table_path + "/@resource_usage/disk_space")

            newer_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(YtClient(self.Env.get_http_proxy_address()), None)
            self.run_preprocessing()

            response = self._run_request(
                "get-versioned-resource-usage",
                {
                    "cluster": "local",
                    "account": "test_account",
                    "path": table_path,
                    "timestamp": newer_snapshot_timestamp,
                    "timestamp_rounding_policy": "closest",
                    "type": "table",
                },
            )

            tx_str = tx.transaction_id
            assert tx_str in response["transactions"]
            assert response["transactions"][tx_str]["per_medium"][self.DEFAULT_MEDIUM] == round(table_size / 3)

    def test_get_versioned_resource_usage_node_test_medium(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        content = [{"foo": "bar"} for _ in range(5)]
        node_path = home_ypath + "/test_node_versioned_test"
        table_path = node_path + "/test_table_versioned_test"

        with yt_client.Transaction() as _:
            yt_client.create("map_node", node_path)
            yt_client.create(
                "table",
                table_path,
                attributes={"primary_medium": self.TEST_MEDIUM},
            )
            yt_client.write_table(table_path, content)
            node_size = yt_client.get(table_path + "/@resource_usage/disk_space")

            newer_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(YtClient(self.Env.get_http_proxy_address()), None)
            self.run_preprocessing()

            response = self._run_request(
                "get-versioned-resource-usage",
                {
                    "cluster": "local",
                    "account": "test_account",
                    "path": node_path,
                    "timestamp": newer_snapshot_timestamp,
                    "timestamp_rounding_policy": "closest",
                    "type": "map_node",
                },
            )

            tx_str = "child"
            assert tx_str in response["transactions"]
            assert response["transactions"][tx_str]["per_medium"][self.TEST_MEDIUM] == round(node_size / 3)

    def test_get_versioned_resource_usage_node_default_medium(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        content = [{"foo": "bar"} for _ in range(5)]
        node_path = home_ypath + "/test_node_versioned_default"
        table_path = node_path + "/test_table_versioned_default"

        with yt_client.Transaction() as _:
            yt_client.create("map_node", node_path)
            yt_client.create(
                "table",
                table_path,
                attributes={"primary_medium": self.DEFAULT_MEDIUM},
            )
            yt_client.write_table(table_path, content)
            node_size = yt_client.get(table_path + "/@resource_usage/disk_space")

            newer_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(YtClient(self.Env.get_http_proxy_address()), None)
            self.run_preprocessing()

            response = self._run_request(
                "get-versioned-resource-usage",
                {
                    "cluster": "local",
                    "account": "test_account",
                    "path": node_path,
                    "timestamp": newer_snapshot_timestamp,
                    "timestamp_rounding_policy": "closest",
                    "type": "map_node",
                },
            )

            tx_str = "child"
            assert tx_str in response["transactions"]
            assert response["transactions"][tx_str]["per_medium"][self.DEFAULT_MEDIUM] == round(node_size / 3)

    def test_get_versioned_resource_usage_not_versioned(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        content = [{"foo": "bar"} for _ in range(5)]
        table_path = home_ypath + "/test_table_not_versioned_default"

        yt_client.create_table(table_path, attributes={"primary_medium": self.DEFAULT_MEDIUM})
        yt_client.write_table(table_path, content)

        newer_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(YtClient(self.Env.get_http_proxy_address()), None)
        self.run_preprocessing()

        response = self._run_request(
            "get-versioned-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "path": table_path,
                "timestamp": newer_snapshot_timestamp,
                "timestamp_rounding_policy": "closest",
                "type": "table",
            },
        )
        assert len(response["transactions"]) == 0

    def test_continuation_token(self, yt_client: YtClient, home_ypath: str):
        yt_client.set(home_ypath + "/@account", "test_account")
        content = [{"foo": "bar"} for _ in range(5)]
        for i in range(10):
            table_path = home_ypath + "/test_table_versioned_default_" + str(i)
            yt_client.create_table(table_path, attributes={"primary_medium": self.DEFAULT_MEDIUM})
            yt_client.write_table(table_path, content)

        newer_snapshot_timestamp = self.snapshot_runner.build_and_export_master_snapshot(
            YtClient(self.Env.get_http_proxy_address()), None
        )
        self.run_preprocessing()

        response_0_5 = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "row_filter": {"exclude_map_nodes": False, "base_path": home_ypath},
                "page": {"index": 0, "size": 5, "enable_continuation_token": True},
                "timestamp": newer_snapshot_timestamp,
            },
        )

        assert len(response_0_5["items"]) == 5

        response_1_5 = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "row_filter": {"exclude_map_nodes": False, "base_path": home_ypath},
                "page": {"index": 1, "size": 5, "enable_continuation_token": True},
                "timestamp": newer_snapshot_timestamp,
            },
        )

        assert len(response_1_5["items"]) == 5

        response_1_5_ct = self._run_request(
            "get-resource-usage",
            {
                "cluster": "local",
                "account": "test_account",
                "row_filter": {"exclude_map_nodes": False, "base_path": home_ypath},
                "page": {"index": 1e10, "size": 5, "continuation_token": response_0_5["continuation_token"]},
                "timestamp": newer_snapshot_timestamp,
            },
        )

        assert len(response_1_5_ct["items"]) == 5

        assert response_1_5["items"] == response_1_5_ct["items"]
