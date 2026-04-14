from yt_env_setup import (YTEnvSetup, Restarter, NODES_SERVICE)

from yt_helpers import profiler_factory

from yt_commands import (
    ls, get, set, print_debug, authors, wait, wait_no_assert, raises_yt_error,
    wait_breakpoint, with_breakpoint, release_breakpoint,
    run_test_vanilla, create_user, create, remove, read_table,
    get_driver, update_nodes_dynamic_config, get_allocation_id_from_job_id,
)

from yt.common import update_inplace
from yt.wrapper import YtClient

import yt.yson

from yt_proto.yt.client.job_proxy.proto.job_api_service_pb2 import TReqOnProgressSaved, TRspOnProgressSaved

import grpc

import pytest

import datetime
from hashlib import sha1
import os.path
import re
import shutil
import time

import requests
import httpx

##################################################################


class JobProxyHider(object):
    def __init__(self, env_setup):
        self.bin_path = env_setup.bin_path

    def _job_proxy_path(self):
        return os.path.join(self.bin_path, "ytserver-job-proxy")

    def __enter__(self):
        job_proxy_path_hidden = self._job_proxy_path() + ".hidden"
        print_debug("Hiding {} to {}".format(self._job_proxy_path(), job_proxy_path_hidden))
        shutil.move(self._job_proxy_path(), job_proxy_path_hidden)

    def __exit__(self, exc_type, exc_val, exc_tb):
        job_proxy_path_hidden = self._job_proxy_path() + ".hidden"
        print_debug("Unhiding {} to {}".format(job_proxy_path_hidden, self._job_proxy_path()))
        shutil.move(job_proxy_path_hidden, self._job_proxy_path())


class JobProxyTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "safe_online_node_count": 2,
        },
    }


class TestJobProxyBinary(JobProxyTestBase):
    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_proxy_build_info_update_period": 300,
                },
            },
        },
    }

    @authors("galtsev")
    def test_orchid_build_info(self):
        def check_iso8601_date(string):
            date = datetime.datetime.fromisoformat(string.rstrip("Z"))
            assert date > datetime.datetime.fromisoformat("2010-02-02")

        n = ls("//sys/cluster_nodes")[0]
        service_path = f"//sys/cluster_nodes/{n}/orchid/service/"

        assert get(service_path + "name") == "node"
        assert len(get(service_path + "build_host")) > 0
        assert re.match(r"^[0-9]+\.[0-9]", get(service_path + "version"))

        for time_name in ("build_time", "start_time"):
            check_iso8601_date(get(service_path + time_name))

    @authors("galtsev")
    def test_job_proxy_build_attribute(self):
        n = ls("//sys/cluster_nodes")[0]
        path = f"//sys/cluster_nodes/{n}"
        attribute_name = "job_proxy_build_version"
        attribute_path = f"{path}/@{attribute_name}"
        orchid_path = f"{path}/orchid/exec_node/job_controller/job_proxy_build/version"

        assert attribute_name in ls(f"{path}/@")

        assert get(attribute_path) == get(orchid_path)

        assert re.match(r"^[0-9]+\.[0-9]", get(attribute_path))

    @authors("max42")
    @pytest.mark.ignore_in_opensource_ci
    def test_job_proxy_build_info(self):
        n = ls("//sys/cluster_nodes")[0]
        orchid_path = "//sys/cluster_nodes/{}/orchid".format(n)

        expected_version = get(orchid_path + "/service/version")

        def check_build(build, expect_error):
            if expect_error:
                return "error" in build
            else:
                return "error" not in build and "version" in build and \
                       build["version"] == expected_version

        def check_direct(expect_error):
            job_proxy_build = get(orchid_path + "/exec_node/job_controller/job_proxy_build")
            return check_build(job_proxy_build, expect_error)

        def check_discover_versions(expect_error):
            url = "http://" + self.Env.get_proxy_address() + "/internal/discover_versions/v2"
            print_debug("HTTP GET", url)
            rsp = requests.get(url).json()
            print_debug(rsp)
            job_proxies = [instance for instance in rsp["details"] if instance["type"] == "job_proxy"]
            assert len(job_proxies) == 1
            return check_build(job_proxies[0], expect_error)

        # At the beginning job proxy version should be immediately visible.
        assert check_direct(False)
        assert check_discover_versions(False)

        with JobProxyHider(self):
            wait(lambda: check_direct(True))
            wait(lambda: check_discover_versions(True))

        wait(lambda: check_direct(False))
        wait(lambda: check_discover_versions(False))

    @authors("ermolovd")
    def test_slot_disabling_on_unavailable_job_proxy(self):
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/total_node_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/user_slots") == 1)

        with JobProxyHider(self):
            with Restarter(self.Env, NODES_SERVICE):
                wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/total_node_count") == 0)
                wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/user_slots") == 0)
            wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/total_node_count") == 1)
            wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/user_slots") == 0)

        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/total_node_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/user_slots") == 1)

    @authors("ermolovd")
    @pytest.mark.parametrize("spec,expected_env_presence", [
        ({"enable_rpc_proxy_in_job_proxy": False}, False),
        ({"enable_rpc_proxy_in_job_proxy": True}, True),
    ])
    def test_rpc_proxy_socket_path_env_variable(self, spec: dict, expected_env_presence: bool):
        op = run_test_vanilla(
            with_breakpoint("echo $YT_JOB_PROXY_SOCKET_PATH >&2; BREAKPOINT"),
            task_patch=spec,
        )
        job_id = wait_breakpoint()[0]
        content = op.read_stderr(job_id).decode("ascii").strip()

        if expected_env_presence:
            assert re.match(r"^.*/pipes/.*-job-proxy-[0-9]+$", content)
        else:
            assert content == ""

        release_breakpoint()


class TestJobProxyUserJobFlagRedirectStdoutToStderr(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    @authors("apachee")
    @pytest.mark.parametrize("env_variable_value,spec", [
        ("1", {}),
        ("1", {"redirect_stdout_to_stderr": False}),
        ("4", {"redirect_stdout_to_stderr": True}),
    ])
    def test_first_output_table_fd_env_variable_value(self, env_variable_value: str, spec: dict):
        command = "echo $YT_FIRST_OUTPUT_TABLE_FD >&2; BREAKPOINT"

        op = run_test_vanilla(
            with_breakpoint(command),
            task_patch=spec,
        )

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1
        job_id = job_ids[0]
        content = op.read_stderr(job_id).decode("ascii").strip()

        # check $YT_FIRST_OUTPUT_TABLE_FD value
        assert content == env_variable_value

        release_breakpoint()

    @authors("apachee")
    @pytest.mark.parametrize("tables_data_yson,spec", [
        (["{a=1};", "{b=2};"], {}),
        (["{a=3};", "{b=4};"], {"redirect_stdout_to_stderr": False}),
        (["{a=5};", "{b=6};"], {"redirect_stdout_to_stderr": True}),
    ])
    def test_output_pipes(self, tables_data_yson: list[str], spec: dict):
        tables_data = [
            yt.yson.loads(f"[{table_data_yson}]".encode("ascii"))
            for table_data_yson in tables_data_yson
        ]

        tables = [f"//tmp/t{i}" for i in range(len(tables_data))]
        for table in tables:
            create("table", table)

        commands = [
            f"echo {table_data_yson!r} >&$(( $YT_FIRST_OUTPUT_TABLE_FD + {3*i} ));"
            for i, table_data_yson in enumerate(tables_data_yson)
        ]
        command = " ".join(commands)

        run_test_vanilla(command, track=True, task_patch={
            "output_table_paths": tables,
            **spec
        })

        # check contents of the tables
        for table, table_data in zip(tables, tables_data):
            assert read_table(table) == table_data

        for table in tables:
            remove(table)

    @authors("apachee")
    def test_stdout(self):
        op = run_test_vanilla(with_breakpoint("echo content; BREAKPOINT"), task_patch={
            "redirect_stdout_to_stderr": True
        })

        job_ids = wait_breakpoint()

        assert len(job_ids) == 1
        job_id = job_ids[0]
        content = op.read_stderr(job_id).decode("ascii").strip()
        assert content == "content"

        release_breakpoint()


class TestRpcProxyInJobProxyBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    ENABLE_HTTP_PROXY = False

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_authentication_manager": {
                    "enable_authentication": True,
                    "cypress_token_authenticator": {
                        "secure": True,
                    },
                },
            },
            "job_proxy_solomon_exporter": {
                "host": "node.yt.test",
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 3,
                "cpu": 3,
            },
        },
        "solomon_exporter": {
            "host": "node.yt.test",
        },
    }

    def setup_method(self, method):
        super(TestRpcProxyInJobProxyBase, self).setup_method(method)
        self.work_dir = os.getcwd()
        create_user("tester_name")
        set("//sys/tokens/" + sha1(b"tester_token").hexdigest(), "tester_name")

    def teardown_method(self, method):
        os.chdir(self.work_dir)
        super(TestRpcProxyInJobProxyBase, self).teardown_method(method)

    def create_client_from_uds(self, socket_file, config=None):
        default_config = {
            "backend": "rpc",
            "driver_config": {
                "connection_type": "rpc",
                "proxy_unix_domain_socket": socket_file,
            },
        }
        if config:
            update_inplace(default_config, config)
        return YtClient(proxy=None, config=default_config)

    def run_job_proxy(
        self,
        enable_rpc_proxy,
        rpc_proxy_thread_pool_size=None,
        enable_monitoring=False,
        time_limit=2000,
        env_variable="YT_JOB_PROXY_SOCKET_PATH",
    ):
        task_patch = {
            "enable_rpc_proxy_in_job_proxy": enable_rpc_proxy,
            "monitoring": {
                "enable": enable_monitoring,
            }
        }
        if rpc_proxy_thread_pool_size is not None:
            task_patch["rpc_proxy_worker_thread_pool_size"] = rpc_proxy_thread_pool_size
        op = run_test_vanilla(
            with_breakpoint(f"echo ${env_variable} >&2; BREAKPOINT; sleep {time_limit}"),
            task_patch=task_patch
        )
        job_id = wait_breakpoint()[0]
        socket_file = op.read_stderr(job_id).decode("ascii").strip()
        release_breakpoint()

        # NB, we return path of unix domain socket. Its path cannot be longer than ~108 chars.
        # So we go to directory of this socket and use short relative path.
        socket_directory = os.path.dirname(socket_file)
        os.chdir(socket_directory)
        return os.path.basename(socket_file)


class TestRpcProxyInJobProxySingleCluster(TestRpcProxyInJobProxyBase):
    @authors("ermolovd")
    def test_rpc_proxy_simple_query(self):
        socket_file = self.run_job_proxy(enable_rpc_proxy=True)
        client = self.create_client_from_uds(socket_file, config={"token": "tester_token"})
        root_listing = client.list("/")
        assert root_listing == ["sys", "tmp"]

    @authors("ermolovd")
    def test_failed_rpc_proxy_auth(self):
        with raises_yt_error("Request authentication failed"):
            socket_file = self.run_job_proxy(enable_rpc_proxy=True)
            client = self.create_client_from_uds(socket_file, config={"token": "wrong_token"})
            client.list("/")

    @authors("ermolovd")
    def test_incorrect_thread_count(self):
        with raises_yt_error("Error parsing vanilla operation spec"):
            self.run_job_proxy(enable_rpc_proxy=True, rpc_proxy_thread_pool_size=0)

    @authors("ermolovd")
    def test_metrics(self):
        def check_job_descriptor_sensor_values(projections):
            job_descriptor_projections = [
                projection for projection in projections
                if "job_descriptor" in projection["tags"]
            ]

            assert any(projection["value"] > 0 for projection in job_descriptor_projections)
            assert all(projection["tags"]["host"] == "" for projection in job_descriptor_projections)
            assert all(
                "slot_index" not in projection["tags"]
                for projection in job_descriptor_projections)

        def check_slot_index_sensor_values(projections):
            slot_index_projections = [
                projection for projection in projections
                if "slot_index" in projection["tags"]
            ]

            assert any(projection["value"] > 0 for projection in slot_index_projections)
            assert all(
                projection["tags"].get("host", "") == "node.yt.test" and "job_descriptor" not in projection["tags"]
                for projection in slot_index_projections)

        socket_file = self.run_job_proxy(enable_rpc_proxy=True, enable_monitoring=True)
        client = self.create_client_from_uds(socket_file, config={"token": "tester_token"})
        client.list("/")
        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_job_proxy(node, fixed_tags={"yt_service": "ApiService", "method": "ListNode"})
        wait_no_assert(lambda: check_job_descriptor_sensor_values(profiler.get_all("rpc/server/request_count")))
        wait_no_assert(lambda: check_slot_index_sensor_values(profiler.get_all("rpc/server/request_count")))


class TestRpcProxyInJobProxySingleClusterSeveralNodes(TestRpcProxyInJobProxyBase):
    NUM_NODES = 2

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_authentication_manager": {
                    "enable_authentication": True,
                    "cypress_token_authenticator": {
                        "secure": True,
                    },
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 4,
                "cpu": 4,
            },
        },
        "solomon_exporter": {
            "host": "node.yt.test",
        },
    }

    @authors("hiddenpath", "nadya73")
    def test_rpc_proxy_count_metrics(self):
        nodes = ls("//sys/cluster_nodes")
        rpc_proxy_in_job_proxy_gauges = [
            profiler_factory()
            .at_node(nodes[node_index])
            .gauge(name="exec_node/rpc_proxy_in_job_proxy_count")
            for node_index in range(self.NUM_NODES)
        ]

        create_user("u1")
        create_user("u2")

        task_patch = {"enable_rpc_proxy_in_job_proxy": True, "monitoring": {"enable": True}}

        run_test_vanilla(
            with_breakpoint("BREAKPOINT", breakpoint_name="op1"),
            job_count=5,
            task_patch=task_patch,
            authenticated_user="u1",
        )
        job_ids1 = wait_breakpoint(breakpoint_name="op1", job_count=5)
        assert len(job_ids1) == 5

        run_test_vanilla(
            with_breakpoint("BREAKPOINT", breakpoint_name="op2"),
            job_count=3,
            task_patch=task_patch,
            authenticated_user="u2",
        )
        job_ids2 = wait_breakpoint(breakpoint_name="op2", job_count=3)
        assert len(job_ids2) == 3

        wait(lambda: rpc_proxy_in_job_proxy_gauges[0].get(tags={"user": "u1"}) is not None)

        wait(lambda: sum([gauge.get(tags={"user": "u1"}, default=0.0) for gauge in rpc_proxy_in_job_proxy_gauges]) == 5)
        wait(lambda: sum([gauge.get(tags={"user": "u2"}, default=0.0) for gauge in rpc_proxy_in_job_proxy_gauges]) == 3)

        for node_index in range(self.NUM_NODES):
            monitoring_port = self.Env.configs["node"][node_index]["monitoring_port"]
            sensors = requests.get(f"http://localhost:{monitoring_port}/solomon/all").json()["sensors"]
            sensors = [sensor for sensor in sensors if sensor.get("labels", {}).get("sensor") == "yt.exec_node.rpc_proxy_in_job_proxy_count"]
            assert sensors[0]["labels"]["host"] == "node.yt.test"

        release_breakpoint(breakpoint_name="op1", job_id=job_ids1[0])

        wait(lambda: sum([gauge.get(tags={"user": "u1"}, default=0.0) for gauge in rpc_proxy_in_job_proxy_gauges]) == 4)
        wait(lambda: sum([gauge.get(tags={"user": "u2"}, default=0.0) for gauge in rpc_proxy_in_job_proxy_gauges]) == 3)

        release_breakpoint(breakpoint_name="op1")
        release_breakpoint(breakpoint_name="op2")

        for gauge in rpc_proxy_in_job_proxy_gauges:
            wait(lambda: gauge.get(tags={"user": "u1"}) is None)
            wait(lambda: gauge.get(tags={"user": "u2"}) is None)


class TestRpcProxyInJobProxyMultiCluster(TestRpcProxyInJobProxyBase):
    NUM_REMOTE_CLUSTERS = 1
    REMOTE_CLUSTER_NAME = "remote_0"

    @classmethod
    def setup_class(cls):
        super(TestRpcProxyInJobProxyMultiCluster, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)
        create_user("tester_name", driver=cls.remote_driver)

    @authors("ermolovd")
    def test_exe_node_multiproxy_mode(self):
        config = {
            "%true": {
                "exec_node":  {
                    "job_controller": {
                        "job_proxy": {
                            "job_proxy_api_service": {
                                "multiproxy": {
                                    "presets": {
                                        "default": {
                                            "enabled_methods": "read",
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            },
        }
        set("//sys/cluster_nodes/@config", config)
        time.sleep(5)

        set("//tmp/name", "remote", driver=self.remote_driver)
        set("//tmp/name", "local")

        assert get("//tmp/name", driver=self.remote_driver) == "remote"
        assert get("//tmp/name") == "local"

        socket_file = self.run_job_proxy(enable_rpc_proxy=True)

        local_client = self.create_client_from_uds(socket_file, config={"token": "tester_token"})
        remote_client = self.create_client_from_uds(socket_file, config={
            "token": "tester_token",
            "driver_config": {"multiproxy_target_cluster": "remote_0"}
        })
        assert local_client.get("//tmp/name") == "local"
        assert remote_client.get("//tmp/name") == "remote"


class TestUnavailableJobProxy(JobProxyTestBase):
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "testing": {
                    "skip_job_proxy_unavailable_alert": True,
                }
            }
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_proxy_build_info_update_period": 300,
                },
            },
        },
    }

    @authors("max42")
    @pytest.mark.ignore_in_opensource_ci
    def test_job_abort_on_unavailable_job_proxy(self):
        # JobProxyUnavailable alert is racy by its nature, so we still must ensure
        # that whenever job is scheduled to a node that does not have ytserver-job-proxy now,
        # job is simply aborted instead of accounting as failed. We do so
        # by forcefully skipping aforementioned alert and checking that operation successfully
        # terminates despite job proxy being unavailable for some period.

        job_count = 4
        op = run_test_vanilla("sleep 0.6", job_count=job_count, spec={"max_failed_job_count": 0}, track=False)

        wait(lambda: op.get_job_count("completed") > 0)

        with JobProxyHider(self):
            wait(lambda: op.get_job_count("aborted") > 2)

        op.track()


class TestJobProxyProfiling(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    ENABLE_RESOURCE_TRACKING = True

    @authors("prime")
    def test_sensors(self):
        op = run_test_vanilla("sleep 100", job_count=10, track=False, task_patch={"monitoring": {"enable": True}})

        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_job_proxy(node)

        wait(lambda: sum(projection["value"] for projection in profiler.get_all("resource_tracker/thread_count")) > 0)

        op.abort()

        wait(lambda: profiler.get_all("resource_tracker/thread_count") == [])


@pytest.mark.enabled_multidaemon
class TestJobProxySignatures(YTEnvSetup):
    OWNERS_PATH = "//sys/public_keys/by_owner"
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "signature_components": {
                "validation": {
                    "cypress_key_reader": dict(),
                },
                "generation": {
                    "cypress_key_writer": dict(),
                    "generator": dict(),
                    "key_rotator": {
                        "key_rotation_options": {
                            "period": "1s",
                        },
                    },
                },
            },
            "job_proxy": {
                "job_proxy_authentication_manager": {
                    "enable_authentication": True,
                    "cypress_token_authenticator": {
                        "secure": True,
                    },
                },
            },
        }
    }

    @authors("pavook")
    def test_key_rotates(self):
        wait(lambda: ls(self.OWNERS_PATH))
        owner = ls(self.OWNERS_PATH)[0]
        wait(lambda: len(ls(f"{self.OWNERS_PATH}/{owner}")) > 1)

    @authors("pavook")
    def test_dynamic_config(self):
        wait(lambda: ls(self.OWNERS_PATH))
        new_path = "//tmp/dynamic_test_public_keys"
        create("map_node", new_path)
        new_config = self.DELTA_NODE_CONFIG["exec_node"]["signature_components"]
        new_config["generation"]["cypress_key_writer"]["path"] = new_path
        new_config["validation"]["cypress_key_reader"]["path"] = new_path
        update_nodes_dynamic_config(path="exec_node/signature_components", value=new_config)
        wait(lambda: ls(new_path))


@authors("khlebnikov")
class TestJobProxyTls(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    ENABLE_TLS = True

    def test_smoke(self):
        run_test_vanilla("true", track=True)


@authors("khlebnikov")
class TestJobProxyTlsCri(TestJobProxyTls):
    JOB_ENVIRONMENT_TYPE = "cri"


class TestJobProxyJobApi(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "grpc_dispatcher": {
            # This is not necessary for the test, but helpful with debugging
            "grpc_internal_min_log_level": "debug",
        },
    }

    def setup_method(self, method):
        super().setup_method(method)
        self._work_dir = os.getcwd()

    def teardown_method(self, method):
        os.chdir(self._work_dir)
        super().teardown_method(method)

    def run_job_proxy(
        self,
        time_limit=2000,
        env_variable="YT_JOB_PROXY_SOCKET_PATH",
    ):
        op = run_test_vanilla(
            with_breakpoint(f"echo ${env_variable} >&2; BREAKPOINT; sleep {time_limit}"),
            task_patch={},
        )
        job_id = wait_breakpoint()[0]
        socket_file = op.read_stderr(job_id).decode("ascii").strip()
        release_breakpoint()

        # NB, we return path of unix domain socket. Its path cannot be longer than ~108 chars.
        # So we go to directory of this socket and use short relative path.
        socket_directory = os.path.dirname(socket_file)
        os.chdir(socket_directory)
        return os.path.basename(socket_file), job_id

    def try_get_last_save_time(self, job_id) -> datetime.datetime | None:
        node = ls("//sys/cluster_nodes")[0]
        orchid_path = f"//sys/cluster_nodes/{node}/orchid/exec_node/job_controller/active_jobs/{job_id}/job_proxy"
        orchid_key = "last_progress_save_time"

        if orchid_key not in get(orchid_path):
            return None

        return datetime.datetime.fromisoformat(get(f"{orchid_path}/{orchid_key}"))

    def get_preemptible_progress_time(self, job_id):
        allocation_id = get_allocation_id_from_job_id(job_id)
        orchid_path = f"//sys/scheduler/orchid/scheduler/allocations/{allocation_id}/preemptible_progress_start_time"
        return datetime.datetime.fromisoformat(get(orchid_path))

    @authors("dann239")
    def test_grpc_disabled_by_default(self):
        socket_file, _ = self.run_job_proxy(
            env_variable="YT_JOB_PROXY_GRPC_SOCKET_PATH",
        )
        assert not os.path.exists(socket_file)

    @authors("dann239")
    def test_http_disabled_by_default(self):
        socket_file, _ = self.run_job_proxy(
            env_variable="YT_JOB_PROXY_HTTP_SOCKET_PATH",
        )
        assert not os.path.exists(socket_file)

    @authors("dann239")
    def test_progress_saved_grpc(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "enable_grpc_server": True,
                    },
                },
            },
        })

        t0 = datetime.datetime.now(tz=datetime.timezone.utc)

        socket_file, job_id = self.run_job_proxy(
            env_variable="YT_JOB_PROXY_GRPC_SOCKET_PATH",
        )

        t1 = datetime.datetime.now(tz=datetime.timezone.utc)

        assert self.try_get_last_save_time(job_id) is None
        assert t0 < self.get_preemptible_progress_time(job_id) < t1

        channel = grpc.insecure_channel(f"unix:{socket_file}")
        endpoint = channel.unary_unary(
            "/JobApiService/OnProgressSaved",
            TReqOnProgressSaved.SerializeToString,
            TRspOnProgressSaved.FromString,
        )
        endpoint(TReqOnProgressSaved())

        t2 = datetime.datetime.now(tz=datetime.timezone.utc)

        assert t1 < self.try_get_last_save_time(job_id) < t2
        wait(lambda: self.try_get_last_save_time(job_id) == self.get_preemptible_progress_time(job_id))

    @authors("dann239")
    def test_progress_saved_http(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "enable_http_server": True,
                    },
                },
            },
        })

        t0 = datetime.datetime.now(tz=datetime.timezone.utc)

        socket_file, job_id = self.run_job_proxy(
            env_variable="YT_JOB_PROXY_HTTP_SOCKET_PATH",
        )

        assert self.try_get_last_save_time(job_id) is None

        t1 = datetime.datetime.now(tz=datetime.timezone.utc)

        assert self.try_get_last_save_time(job_id) is None
        assert t0 < self.get_preemptible_progress_time(job_id) < t1

        # Since we're using a UDS, hostname can be whatever
        hostname = "foo.bar"

        client = httpx.Client(transport=httpx.HTTPTransport(uds=socket_file))
        client.headers.clear()
        response = client.post(f"http://{hostname}/JobApiService/OnProgressSaved")
        assert response.is_success, (response.headers, response.text)

        t2 = datetime.datetime.now(tz=datetime.timezone.utc)

        assert t1 < self.try_get_last_save_time(job_id) < t2
        wait(lambda: self.try_get_last_save_time(job_id) == self.get_preemptible_progress_time(job_id))
