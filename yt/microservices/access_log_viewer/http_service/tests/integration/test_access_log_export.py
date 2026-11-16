from base import ClickHouseTestBase, Clique

from yt.common import YtError
from yt.wrapper import YtClient
from yt.wrapper.testlib.helpers import wait
from yt_commands import (
    authors,
    create,
    create_account,
    create_access_control_object,
    create_access_control_object_namespace,
    create_domestic_medium,
    create_pool,
    create_user,
    get,
    issue_token,
    make_ace,
    set,
)

from library.python.port_manager import PortManager

import pytest
import requests
import yatest.common

import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
from typing import Iterator
from urllib.parse import quote

ACCESS_LOG_VIEWER_BINARY_PATH = yatest.common.binary_path(
    "yt/microservices/access_log_viewer/http_service/access_log_viewer"
)

ACCESS_LOG_SNAPSHOT_ROOT = "//tmp/access_log_export_snapshots"
EXPORT_OUTPUT_ROOT = "//tmp/access_log_exports"
DEFAULT_EXPORT_POOL = "access-log-export-pool"
NESTED_EXPORT_POOL = "nested-export-pool"
EXPORT_USER = "alice"
OTHER_EXPORT_USER = "bob"
EXPORT_ROBOT = "access-log-export-robot"
EXPORT_TIMEZONE_ATTRIBUTE = "_access_log_viewer_export_timezone"
FULL_READ_PRINCIPAL = "//sys/access_control_object_namespaces/access_log_viewer/full_read/principal"
CHYT_ACO_NAMESPACE = "//sys/access_control_object_namespaces/chyt"
EXPORT_OPERATION_CLUSTER = "ytserver"


class TestAccessLogExport(ClickHouseTestBase):
    DELTA_HTTP_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
        },
    }

    CLASS_TEST_LIMIT = 20 * 60

    @pytest.fixture(autouse=True)
    def export_services(self) -> Iterator[None]:
        """Manage the viewer, clique, and reserved port for one test."""
        self._root_token, _ = issue_token("root")
        create_user(EXPORT_ROBOT)
        self._robot_token, _ = issue_token(EXPORT_ROBOT)
        assert "superusers" not in get(f"//sys/users/{EXPORT_ROBOT}/@member_of_closure")
        proxy = self.Env.get_http_proxy_address()
        response = requests.post(
            f"http://{proxy}/auth/whoami",
            headers={"Authorization": f"OAuth {self._robot_token}"},
            timeout=5,
        )
        response.raise_for_status()
        assert response.json()["login"] == EXPORT_ROBOT
        self._access_log_viewer_process = None
        with ExitStack() as resources:
            ports = resources.enter_context(PortManager())
            self._access_log_viewer_port = ports.get_port()
            self._access_log_viewer_log = resources.enter_context(
                open(yatest.common.test_output_path("access_log_viewer.log"), "w"),
            )
            self._clique_resources = resources.enter_context(ExitStack())
            self._clique = Clique(1)
            set(
                f"{CHYT_ACO_NAMESPACE}/{self._clique.alias}/principal/@acl/end",
                make_ace("allow", EXPORT_ROBOT, "use"),
            )
            self._clique_resources.enter_context(self._clique)
            resources.callback(self._stop_access_log_viewer)
            yield

    def _prepare_cluster(self, yt_client, cluster_proxy_address):
        create_user(EXPORT_USER)
        create_pool(DEFAULT_EXPORT_POOL)
        create_pool("export-parent")
        create_pool(NESTED_EXPORT_POOL, parent_name="export-parent")
        for pool in [DEFAULT_EXPORT_POOL, f"export-parent/{NESTED_EXPORT_POOL}"]:
            set(
                f"//sys/pool_trees/default/{pool}/@acl/end",
                make_ace("allow", EXPORT_ROBOT, "use"),
            )
        set(
            f"//sys/pool_trees/default/export-parent/{NESTED_EXPORT_POOL}/@acl/end",
            make_ace("allow", EXPORT_USER, "use"),
        )
        create("map_node", EXPORT_OUTPUT_ROOT, recursive=True, ignore_existing=True)
        set(f"{EXPORT_OUTPUT_ROOT}/@acl/end", make_ace("allow", EXPORT_USER, "write"))
        set(
            f"{EXPORT_OUTPUT_ROOT}/@acl/end",
            make_ace("allow", EXPORT_ROBOT, ["read", "write"]),
        )
        create_access_control_object_namespace("access_log_viewer", ignore_existing=True)
        create_access_control_object("full_read", "access_log_viewer", ignore_existing=True)

        begin = int(datetime(2024, 1, 2, 12, tzinfo=timezone.utc).timestamp())
        access_log_table_path = f"{ACCESS_LOG_SNAPSHOT_ROOT}/{cluster_proxy_address}/1d/2024-01-02"
        create(
            "table",
            access_log_table_path,
            recursive=True,
            attributes={
                "schema": [
                    {"name": "path", "type": "string"},
                    {"name": "instant", "type": "string"},
                    {"name": "target_path", "type": "string"},
                    {"name": "type", "type": "string"},
                    {"name": "method", "type": "string"},
                    {"name": "user", "type": "string"},
                    {"name": "timestamp", "type": "int64"},
                ],
            },
        )
        paths = [
            "//home/allowed",
            "//home/allowed/child/@acl",
            "//home/allowed-x",
            "#1-2-3-4",
            "/@acl/0",
            "",
        ]
        rows = []
        for index, path in enumerate(paths):
            rows.append(
                {
                    "path": path,
                    "instant": f"2024-01-02 15:00:{index:02}",
                    "method": "Get",
                    "type": "table",
                    "user": EXPORT_USER,
                    # Preprocessing in UTC interprets a Moscow wall-clock time three hours late.
                    "timestamp": begin + 3 * 3600 + index,
                }
            )
        # A row outside the requested interval.
        rows.append(dict(rows[0], instant="2024-01-02 16:00:00", timestamp=begin + 4 * 3600))
        yt_client.write_table(access_log_table_path, rows)
        # ALV preprocessing sorts its output tables by path and instant.
        yt_client.run_sort(access_log_table_path, sort_by=["path", "instant"])
        return begin, begin + 60

    def _start_access_log_viewer(
        self,
        cluster_proxy_address,
        export_operation_cluster=None,
        export_default_pool=DEFAULT_EXPORT_POOL,
        export_pool_tree="default",
        debug_login=EXPORT_USER,
    ):
        environment = os.environ.copy()
        environment["YT_ACCESS_LOG_VIEWER_TOKEN"] = self._robot_token
        environment["YT_PROXY"] = cluster_proxy_address
        command = [
            ACCESS_LOG_VIEWER_BINARY_PATH,
            "--port",
            str(self._access_log_viewer_port),
            "--proxy",
            cluster_proxy_address,
            "--chyt-cluster-name",
            cluster_proxy_address,
            "--chyt-alias",
            self._clique.alias,
            "--include",
            cluster_proxy_address,
            "--snapshot-root",
            ACCESS_LOG_SNAPSHOT_ROOT,
            "--logs-timezone",
            "Europe/Moscow",
            "--debug-login",
            debug_login,
        ]
        if export_operation_cluster is not None:
            aliasing_config = f'{{{export_operation_cluster}="{cluster_proxy_address}"}}'
            environment["YT_PROXY_URL_ALIASING_CONFIG"] = aliasing_config
            command.extend(["--export-operation-cluster", export_operation_cluster])
        if export_default_pool is not None:
            command.extend(["--export-default-pool", export_default_pool])
        if export_pool_tree is not None:
            command.extend(["--export-pool-tree", export_pool_tree])
        self._access_log_viewer_process = subprocess.Popen(
            command,
            env=environment,
            stdout=self._access_log_viewer_log,
            stderr=subprocess.STDOUT,
        )

        last_response = None

        def is_access_log_viewer_ready():
            nonlocal last_response
            process = self._access_log_viewer_process
            if process.poll() is not None:
                raise RuntimeError(f"Access Log Viewer exited with code {process.returncode}")
            try:
                response = requests.get(self._url("readiness"), timeout=2)
            except requests.RequestException as error:
                last_response = error
                return False
            last_response = response.text
            return response.ok

        wait(
            is_access_log_viewer_ready,
            lambda: f"readiness is not ok: {last_response}",
            timeout=120,
            ignore_exceptions=False,
        )

    def _stop_access_log_viewer(self):
        if self._access_log_viewer_process is None:
            return
        self._access_log_viewer_process.terminate()
        try:
            self._access_log_viewer_process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self._access_log_viewer_process.kill()
            self._access_log_viewer_process.wait(timeout=30)
        self._access_log_viewer_process = None

    def _url(self, endpoint):
        return f"http://127.0.0.1:{self._access_log_viewer_port}/{endpoint}"

    def _request(self, method, endpoint, expected_status=200, **kwargs):
        process = self._access_log_viewer_process
        assert process.poll() is None, process.returncode
        # The first export uploads the binary of Access Log Viewer to the cluster.
        response = requests.request(method, self._url(endpoint), timeout=180, **kwargs)
        assert response.status_code == expected_status, response.text
        return response

    def _wait_for_access_log_unavailability(self, cluster):
        last_response = None

        def matches_status():
            nonlocal last_response
            last_response = self._request("POST", "served-clusters").json()
            return not last_response["statuses"][cluster]["access_log"]["available"]

        wait(
            matches_status,
            lambda: f"access_log.available is still true: {last_response}",
            timeout=90,
            ignore_exceptions=False,
        )
        return last_response

    def _wait_for_completed_export(self, export_id, endpoint="export-access-log"):
        last_status = None

        def is_export_completed():
            nonlocal last_status
            last_status = self._request("GET", f"{endpoint}/{quote(export_id, safe='')}").json()
            assert last_status["state"] not in {"failed", "aborted"}, last_status
            return last_status["state"] == "completed"

        wait(
            is_export_completed,
            lambda: f"export is not completed: {last_status}",
            timeout=180,
            ignore_exceptions=False,
        )
        return last_status

    @authors("ilyaibraev")
    def test_export_permissions_filters_and_lifecycle(self) -> None:
        cluster_proxy_address = self.Env.get_http_proxy_address()
        yt_client = YtClient(cluster_proxy_address, token=self._root_token)
        begin, end = self._prepare_cluster(yt_client, cluster_proxy_address)
        self._start_access_log_viewer(cluster_proxy_address)

        def export_request(path, output_name, **fields):
            return dict(
                cluster=cluster_proxy_address,
                begin=begin,
                end=end,
                output_path=f"{EXPORT_OUTPUT_ROOT}/{output_name}",
                path=path,
                **fields,
            )

        # Configuring CHYT alone must not enable exports.
        for endpoint in ["export-access-log", "v1/export_access_log"]:
            self._request(
                "POST",
                endpoint,
                expected_status=404,
                json=export_request("/", "disabled"),
            )
            for method in ["GET", "DELETE"]:
                self._request(method, f"{endpoint}/not-an-export-id", expected_status=404)
        self._request("GET", "metrics")
        assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/disabled")
        response = self._request("POST", "served-clusters")
        cluster_status = response.json()["statuses"][cluster_proxy_address]
        assert cluster_status["access_log"]["available"]
        assert cluster_status["export"] == {
            "enabled": False,
            "input_tables_available": False,
            "reason": "disabled",
        }

        self._stop_access_log_viewer()
        self._start_access_log_viewer(
            cluster_proxy_address,
            export_operation_cluster=EXPORT_OPERATION_CLUSTER,
        )

        # Without the use permission on the full_read ACO the export is denied and leaves no output.
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "denied"),
        ).json()
        assert error["message"] == (
            "Access log export requires the use permission on the full_read access control object"
        ), error
        assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/denied")

        set(f"{FULL_READ_PRINCIPAL}/@acl/end", make_ace("allow", EXPORT_USER, "use"))

        # The user's access does not compensate for missing robot permissions on the output
        # directory. The response contains the YT access denial of the failed robot request.
        for denied_permissions, expected_message in [
            (["read"], "Could not read the parent directory of output_path"),
            (["write", "modify_children"], "Could not create the output static table"),
        ]:
            directory = f"robot_{denied_permissions[0]}_denied"
            create(
                "map_node",
                f"{EXPORT_OUTPUT_ROOT}/{directory}",
                attributes={"acl": [make_ace("deny", EXPORT_ROBOT, denied_permissions)]},
            )
            error = self._request(
                "POST",
                "export-access-log",
                expected_status=400,
                json=export_request("/", f"{directory}/output"),
            ).json()
            assert error["message"] == expected_message, error
            assert error["code"] == 1, error
            assert YtError.from_dict(error).is_access_denied(), error
            assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/{directory}/output")

        # A check that fails after the creation aborts the creation transaction, so the output
        # table does not remain.
        create(
            "map_node",
            f"{EXPORT_OUTPUT_ROOT}/robot_table_write_denied",
            attributes={
                "acl": [
                    make_ace("deny", EXPORT_ROBOT, "write", inheritance_mode="descendants_only"),
                ],
            },
        )
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "robot_table_write_denied/output"),
        ).json()
        assert error["message"] == "Service robot lacks permissions required for the export", error
        assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/robot_table_write_denied/output")

        create_pool("robot-denied")
        set(
            "//sys/pool_trees/default/robot-denied/@acl",
            [make_ace("allow", EXPORT_USER, "use"), make_ace("deny", EXPORT_ROBOT, "use")],
        )
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "robot_pool_denied", pool="robot-denied"),
        ).json()
        assert "Service robot lacks permissions" in error["message"], error
        assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/robot_pool_denied")

        # The scope is compared as a path prefix: //home/allowed-x is in the key range read from
        # the sorted input, but it is out of scope, as is the object ID.
        response = self._request(
            "POST",
            "export-access-log",
            json=export_request("//home/allowed", "allowed", strict=True),
        ).json()
        assert response["output_cluster"] == EXPORT_OPERATION_CLUSTER
        assert response["output_path"] == f"{EXPORT_OUTPUT_ROOT}/allowed"
        assert (response["path"], response["begin"], response["end"]) == (
            "//home/allowed",
            begin,
            end,
        )
        unexpected_fields = {"strict", "acl_snapshot_id", "access_transitions", "truncated"}
        assert not unexpected_fields & response.keys()

        status = self._wait_for_completed_export(response["export_id"])
        assert status["path"] == "//home/allowed"
        assert status["output_cluster"] == EXPORT_OPERATION_CLUSTER
        assert status["jobs"]["completed"] >= 1
        assert "error" not in status
        assert [row["path"] for row in yt_client.read_table(f"{EXPORT_OUTPUT_ROOT}/allowed")] == [
            "//home/allowed",
            "//home/allowed/child/@acl",
        ]
        output_schema = yt_client.get(f"{EXPORT_OUTPUT_ROOT}/allowed/@schema")
        assert output_schema.attributes["strict"]
        assert [column["name"] for column in output_schema] == [
            "path",
            "instant",
            "target_path",
            "original_path",
            "destination_path",
            "original_destination_path",
            "source_path",
            "original_source_path",
            "type",
            "method",
            "user",
            "transaction_info",
            "transaction_id",
        ]

        allowed_operation_id = response["export_id"].split(":", 1)[1]
        operation = yt_client.get_operation(
            allowed_operation_id,
            attributes=["spec", "runtime_parameters"],
        )
        annotation = operation["runtime_parameters"]["annotations"]["access_log_export"]
        assert annotation["owner"] == EXPORT_USER
        assert annotation["effective_parameters"]["pool"] == DEFAULT_EXPORT_POOL
        input_ranges = [path.attributes["ranges"] for path in operation["spec"]["input_table_paths"]]
        assert input_ranges == [
            [
                {
                    "lower_limit": {"key": ["//home/allowed"]},
                    "upper_limit": {"key": ["//home/allowed0"]},
                },
            ],
        ]

        # The metadata=false filter must survive the SDK's gob serialization, including in a
        # nested pool.
        filtered_response = self._request(
            "POST",
            "export-access-log",
            json=export_request(
                "//home/allowed",
                "without_metadata",
                metadata=False,
                pool=NESTED_EXPORT_POOL,
            ),
        ).json()
        self._wait_for_completed_export(filtered_response["export_id"])
        filtered_rows = list(yt_client.read_table(f"{EXPORT_OUTPUT_ROOT}/without_metadata"))
        assert [row["path"] for row in filtered_rows] == ["//home/allowed"]
        # The interval is selected by instant in --logs-timezone, not by the shifted input
        # timestamp.
        assert filtered_rows[0]["instant"] == "2024-01-02 15:00:00"
        assert "timestamp" not in filtered_rows[0]

        metadata_response = self._request(
            "POST",
            "export-access-log",
            json=export_request("/", "metadata", metadata=True),
        ).json()
        self._wait_for_completed_export(metadata_response["export_id"])
        assert [row["path"] for row in yt_client.read_table(f"{EXPORT_OUTPUT_ROOT}/metadata")] == [
            "//home/allowed/child/@acl",
            "/@acl/0",
        ]

        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "pool_path", pool=f"export-parent/{NESTED_EXPORT_POOL}"),
        ).json()
        pool_message = "Could not read the access log export pool from the scheduler orchid"
        assert error["message"] == pool_message, error
        assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/pool_path")

        # The export of the whole Cypress keeps every row, including object IDs and empty paths.
        root_response = self._request(
            "POST",
            "v1/export_access_log",
            json=export_request("/", "root"),
        ).json()
        self._wait_for_completed_export(root_response["export_id"], endpoint="v1/export_access_log")
        assert [row["path"] for row in yt_client.read_table(f"{EXPORT_OUTPUT_ROOT}/root")] == [
            "",
            "#1-2-3-4",
            "//home/allowed",
            "//home/allowed-x",
            "//home/allowed/child/@acl",
            "/@acl/0",
        ]
        # A repeated request with the same parameters receives the export that has written the
        # output table.
        repeated_response = self._request(
            "POST",
            "export-access-log",
            json=export_request("/", "root"),
        ).json()
        assert repeated_response == root_response, repeated_response

        # The abort of a finished export is a no-op.
        for _ in range(2):
            abort_response = self._request(
                "DELETE", f"export-access-log/{quote(root_response['export_id'], safe='')}"
            ).json()
            assert (abort_response["state"], abort_response["path"]) == (
                "completed",
                "/",
            ), abort_response

        missing_export_id = f"{root_response['export_id'].split(':', 1)[0]}:1-2-3-4"
        for method in ["GET", "DELETE"]:
            error = self._request(
                method,
                f"export-access-log/{missing_export_id}",
                expected_status=400,
            ).json()
            assert error["message"] == "Could not read the access log export operation", error
        error = self._request(
            "GET",
            "export-access-log/not-an-export-id",
            expected_status=400,
        ).json()
        assert error["message"] == "Invalid export_id", error
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("//home/", "invalid_path"),
        ).json()
        path_message = "Path must be / or an absolute Cypress path in the canonical YPath escaping"
        assert error["message"] == path_message, error

        create_account(
            "export-output",
            attributes={"acl": [make_ace("allow", EXPORT_ROBOT, "use")]},
        )
        resource_parent = f"{EXPORT_OUTPUT_ROOT}/resource_permissions"
        create("map_node", resource_parent, attributes={"account": "export-output"})
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "resource_permissions/account_denied"),
        ).json()
        assert error["message"] == "Use access to the output account is denied", error
        assert not yt_client.exists(f"{resource_parent}/account_denied")
        set("//sys/accounts/export-output/@acl/end", make_ace("allow", EXPORT_USER, "use"))
        create_domestic_medium(
            "export-restricted",
            attributes={"acl": [make_ace("allow", EXPORT_ROBOT, "use")]},
        )
        set(f"{EXPORT_OUTPUT_ROOT}/@primary_medium", "export-restricted")
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "resource_permissions/medium_denied"),
        ).json()
        assert error["message"] == "Use access to the output primary medium is denied", error
        assert not yt_client.exists(f"{resource_parent}/medium_denied")
        set(f"{EXPORT_OUTPUT_ROOT}/@primary_medium", "default")
        resource_export = self._request(
            "POST",
            "export-access-log",
            json=export_request("/", "resource_permissions/allowed"),
        ).json()
        self._wait_for_completed_export(resource_export["export_id"])
        assert yt_client.get(f"{resource_parent}/allowed/@account") == "export-output"
        assert yt_client.get(f"{resource_parent}/allowed/@primary_medium") == "default"

        self._clique_resources.close()
        last_cluster_status = self._wait_for_access_log_unavailability(cluster_proxy_address)
        assert cluster_proxy_address not in last_cluster_status["clusters"]
        assert last_cluster_status["statuses"][cluster_proxy_address]["export"] == {
            "enabled": True,
            "input_tables_available": True,
        }
        self._request("GET", "readiness", expected_status=200)
        export_without_chyt = self._request(
            "POST", "export-access-log", json=export_request("/", "without_chyt")
        ).json()
        self._wait_for_completed_export(export_without_chyt["export_id"])

        self._stop_access_log_viewer()
        self._start_access_log_viewer(
            cluster_proxy_address,
            export_operation_cluster=EXPORT_OPERATION_CLUSTER,
            export_default_pool=None,
            export_pool_tree=None,
        )
        # Without a pool in the request or in the configuration the export is rejected before the
        # output is created.
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "without_pool"),
        ).json()
        missing_pool_message = "Pool is required when no default export pool is configured"
        assert error["message"] == missing_pool_message, error
        assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/without_pool")

        # An explicit pool still requires the user's permission in the cluster's default tree.
        default_pool_path = f"//sys/pool_trees/default/{DEFAULT_EXPORT_POOL}"
        set(f"{default_pool_path}/@acl/end", make_ace("deny", EXPORT_USER, "use"))
        last_action = None

        def is_use_denied():
            nonlocal last_action
            permission = yt_client.check_permission(EXPORT_USER, "use", default_pool_path)
            last_action = permission["action"]
            return last_action == "deny"

        wait(is_use_denied, lambda: f"use is not denied: {last_action}", timeout=30)
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("/", "default_tree_denied", pool=DEFAULT_EXPORT_POOL),
        ).json()
        assert error["message"] == "Use access to the requested pool is denied", error
        assert not yt_client.exists(f"{EXPORT_OUTPUT_ROOT}/default_tree_denied")
        export_in_default_tree = self._request(
            "POST",
            "export-access-log",
            json=export_request("/", "default_tree", pool=NESTED_EXPORT_POOL),
        ).json()
        self._wait_for_completed_export(export_in_default_tree["export_id"])
        operation = yt_client.get_operation(
            export_in_default_tree["export_id"].split(":", 1)[1],
            attributes=["spec", "runtime_parameters"],
        )
        assert operation["spec"]["pool_trees"] == ["default"]
        scheduling_options = operation["runtime_parameters"]["scheduling_options_per_pool_tree"]
        assert scheduling_options["default"]["pool"] == NESTED_EXPORT_POOL
        assert yt_client.get(f"{EXPORT_OUTPUT_ROOT}/default_tree/@row_count") == 6

    @authors("ilyaibraev")
    def test_export_to_existing_output_path(self) -> None:
        cluster_proxy_address = self.Env.get_http_proxy_address()
        yt_client = YtClient(cluster_proxy_address, token=self._root_token)
        begin, end = self._prepare_cluster(yt_client, cluster_proxy_address)
        set(f"{FULL_READ_PRINCIPAL}/@acl/end", make_ace("allow", EXPORT_USER, "use"))
        self._start_access_log_viewer(
            cluster_proxy_address,
            export_operation_cluster=EXPORT_OPERATION_CLUSTER,
        )

        def export_request(output_name: str, path: str = "/") -> dict:
            return {
                "cluster": cluster_proxy_address,
                "begin": begin,
                "end": end,
                "path": path,
                "output_path": f"{EXPORT_OUTPUT_ROOT}/{output_name}",
            }

        def start_export(output_name: str) -> dict:
            return self._request(
                "POST",
                "export-access-log",
                json=export_request(output_name),
            ).json()

        first_response = start_export("first")
        self._wait_for_completed_export(first_response["export_id"])

        # A repeated request with other parameters names the export that has written the output
        # table.
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("first", path="//home/allowed"),
        ).json()
        other_parameters_message = "The output_path is used by an access log export with other parameters"
        assert error["message"] == other_parameters_message, error
        assert error["attributes"]["export_id"] == first_response["export_id"], error

        # A request without the end differs from the first export, whose request has the end.
        request_without_end = export_request("first")
        del request_without_end["end"]
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=request_without_end,
        ).json()
        assert error["message"] == other_parameters_message, error
        assert error["attributes"]["export_id"] == first_response["export_id"], error

        # An empty table without an export operation, as after a failed start, receives a new
        # export.
        export_schema = yt_client.get(f"{EXPORT_OUTPUT_ROOT}/first/@schema")
        export_table_attributes = {
            "schema": export_schema,
            EXPORT_TIMEZONE_ATTRIBUTE: "Europe/Moscow",
        }
        create("table", f"{EXPORT_OUTPUT_ROOT}/empty", attributes=export_table_attributes)
        empty_table_id = get(f"{EXPORT_OUTPUT_ROOT}/empty/@id")
        restarted_response = start_export("empty")
        self._wait_for_completed_export(restarted_response["export_id"])
        assert get(f"{EXPORT_OUTPUT_ROOT}/empty/@id") == empty_table_id
        assert get(f"{EXPORT_OUTPUT_ROOT}/empty/@row_count") == 6

        # Of concurrent starts to the same empty table, the scheduler runs one operation with the
        # alias of the table at a time. Both requests usually receive its export; a second
        # operation may take the alias after the first one has finished.
        def start_concurrent_export(output_name: str) -> dict:
            response = None

            # A request fails while the operation of the other request is starting, and its retry
            # receives the export.
            def is_export_started():
                nonlocal response
                response = requests.post(
                    self._url("export-access-log"),
                    json=export_request(output_name),
                    timeout=180,
                )
                if response.status_code == 200:
                    return True
                error = response.json()
                retry_message = "An access log export to output_path is starting, retry the request"
                assert error["message"] == retry_message, error
                return False

            wait(is_export_started, lambda: f"export is not started: {response.text}", timeout=60)
            return response.json()

        create("table", f"{EXPORT_OUTPUT_ROOT}/concurrent", attributes=export_table_attributes)
        concurrent_table_id = get(f"{EXPORT_OUTPUT_ROOT}/concurrent/@id")
        concurrent_alias = f"*access_log_viewer_export:{concurrent_table_id}"
        with ThreadPoolExecutor(max_workers=2) as requests_pool:
            concurrent_responses = list(
                requests_pool.map(start_concurrent_export, ["concurrent"] * 2),
            )
        for export_id in {response["export_id"] for response in concurrent_responses}:
            self._wait_for_completed_export(export_id)
            operation = yt_client.get_operation(export_id.split(":", 1)[1], attributes=["spec"])
            assert operation["spec"]["alias"] == concurrent_alias, operation["spec"]

        # Without an export operation, only a table that the service would create is reused.
        create(
            "table",
            f"{EXPORT_OUTPUT_ROOT}/other_schema",
            attributes={
                "schema": [{"name": "path", "type": "string"}],
                EXPORT_TIMEZONE_ATTRIBUTE: "Europe/Moscow",
            },
        )
        create(
            "table",
            f"{EXPORT_OUTPUT_ROOT}/without_timezone",
            attributes={"schema": export_schema},
        )
        create(
            "table",
            f"{EXPORT_OUTPUT_ROOT}/other_timezone",
            attributes={"schema": export_schema, EXPORT_TIMEZONE_ATTRIBUTE: "UTC"},
        )
        create("table", f"{EXPORT_OUTPUT_ROOT}/not_empty", attributes=export_table_attributes)
        yt_client.write_table(f"{EXPORT_OUTPUT_ROOT}/not_empty", [{"path": "//home/allowed"}])
        timezone_message = (
            f"The {EXPORT_TIMEZONE_ATTRIBUTE} attribute of the table differs from the logs timezone of the service"
        )
        for output_name, reason in [
            ("not_empty", "The table is not empty"),
            ("other_schema", "The table schema differs from the access log export schema"),
            ("without_timezone", timezone_message),
            ("other_timezone", timezone_message),
        ]:
            error = self._request(
                "POST",
                "export-access-log",
                expected_status=400,
                json=export_request(output_name),
            ).json()
            assert error["message"] == "The table at output_path does not satisfy the export requirements", error
            assert error["inner_errors"][0]["message"] == reason, error

        # Only a static table is used as the output.
        create("map_node", f"{EXPORT_OUTPUT_ROOT}/map_node")
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("map_node"),
        ).json()
        assert error["message"] == "The node at output_path is not a static table", error

        # The export of another user is not named, also to a user who may create the output table.
        # The user inherits the write permission from //tmp.
        create_user(OTHER_EXPORT_USER)
        self._stop_access_log_viewer()
        self._start_access_log_viewer(
            cluster_proxy_address,
            export_operation_cluster=EXPORT_OPERATION_CLUSTER,
            debug_login=OTHER_EXPORT_USER,
        )
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("first"),
        ).json()
        other_user_message = "The output_path is used by an access log export of another user"
        assert error["message"] == other_user_message, error
        assert "export_id" not in error.get("attributes", {}), error

        # A user who may write the existing table but may not create a child in its parent
        # directory learns nothing about the table and its export.
        set(
            f"{EXPORT_OUTPUT_ROOT}/@acl/end",
            make_ace("deny", OTHER_EXPORT_USER, "write", inheritance_mode="object_only"),
        )
        last_action = None

        def is_write_denied():
            nonlocal last_action
            permission = yt_client.check_permission(OTHER_EXPORT_USER, "write", EXPORT_OUTPUT_ROOT)
            last_action = permission["action"]
            return last_action == "deny"

        wait(is_write_denied, lambda: f"write is not denied: {last_action}", timeout=30)
        table_permission = yt_client.check_permission(
            OTHER_EXPORT_USER,
            "write",
            f"{EXPORT_OUTPUT_ROOT}/first",
        )
        assert table_permission["action"] == "allow", table_permission
        error = self._request(
            "POST",
            "export-access-log",
            expected_status=400,
            json=export_request("first"),
        ).json()
        assert error["message"] == "Write access to output_path is denied", error
        assert "export_id" not in error.get("attributes", {}), error

    @authors("ilyaibraev")
    def test_concurrent_exports(self) -> None:
        cluster_proxy_address = self.Env.get_http_proxy_address()
        yt_client = YtClient(cluster_proxy_address, token=self._root_token)
        begin, end = self._prepare_cluster(yt_client, cluster_proxy_address)
        set(f"{FULL_READ_PRINCIPAL}/@acl/end", make_ace("allow", EXPORT_USER, "use"))
        self._start_access_log_viewer(
            cluster_proxy_address,
            export_operation_cluster=EXPORT_OPERATION_CLUSTER,
        )

        def start_export(name: str) -> dict:
            return self._request(
                "POST",
                "export-access-log",
                json={
                    "cluster": cluster_proxy_address,
                    "begin": begin,
                    "end": end,
                    "path": "//home/allowed",
                    "output_path": f"{EXPORT_OUTPUT_ROOT}/{name}",
                },
            ).json()

        with ThreadPoolExecutor(max_workers=2) as requests_pool:
            responses = list(
                requests_pool.map(start_export, ["concurrent_first", "concurrent_second"]),
            )
        for response in responses:
            self._wait_for_completed_export(response["export_id"])
            rows = list(yt_client.read_table(response["output_path"]))
            assert [row["instant"] for row in rows] == [
                "2024-01-02 15:00:00",
                "2024-01-02 15:00:01",
            ]
            assert (
                yt_client.get(
                    f"{response['output_path']}/@_access_log_viewer_export_timezone",
                )
                == "Europe/Moscow"
            )
