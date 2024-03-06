from yt_env_setup import (YTEnvSetup, Restarter, NODES_SERVICE)

from yt_helpers import profiler_factory

from yt_commands import (
    ls, get, set, print_debug, authors, wait, run_test_vanilla, create_user,
    wait_breakpoint, with_breakpoint, release_breakpoint, create, remove, read_table)

from yt.common import YtError, update_inplace
from yt.wrapper import YtClient

import yt.yson

import pytest

import datetime
from hashlib import sha1
import os.path
import re
import shutil
import requests

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

        for time in ("build_time", "start_time"):
            check_iso8601_date(get(service_path + time))

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

    @authors("max42")
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

    @authors("alex-shishkin")
    def test_rpc_proxy_socket_path_env_variable(self):
        op = run_test_vanilla(with_breakpoint("echo $YT_JOB_PROXY_SOCKET_PATH >&2; BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        content = op.read_stderr(job_id).decode("ascii").strip()
        release_breakpoint()
        assert re.match(r"^.*/pipes/.*-job-proxy-[0-9]+$", content)


class TestJobProxyUserJobFlagRedirectStdoutToStderr(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    @authors("apachee")
    @pytest.mark.parametrize('env_variable_value,spec', [
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
        content = op.read_stderr(job_id).decode('ascii').strip()
        assert content == "content"

        release_breakpoint()


class TestRpcProxyInJobProxy(YTEnvSetup):
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
        }
    }

    def setup_method(self, method):
        super(TestRpcProxyInJobProxy, self).setup_method(method)
        create_user("tester_name")
        set("//sys/tokens/" + sha1(b"tester_token").hexdigest(), "tester_name")

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

    def run_job_proxy(self, enable_rpc_proxy, rpc_proxy_thread_pool_size=None,  time_limit=2000):
        task_patch = {
            "enable_rpc_proxy_in_job_proxy": enable_rpc_proxy,
        }
        if rpc_proxy_thread_pool_size is not None:
            task_patch["rpc_proxy_worker_thread_pool_size"] = rpc_proxy_thread_pool_size
        op = run_test_vanilla(
            with_breakpoint("echo $YT_JOB_PROXY_SOCKET_PATH >&2; BREAKPOINT; sleep {}".format(time_limit)),
            task_patch=task_patch
        )
        job_id = wait_breakpoint()[0]
        socket_file = op.read_stderr(job_id).decode("ascii").strip()
        release_breakpoint()
        return socket_file

    @authors("alex-shishkin")
    def test_disabled_rpc_proxy(self):
        with pytest.raises(YtError):
            socket_file = self.run_job_proxy(enable_rpc_proxy=False)
            client = self.create_client_from_uds(socket_file, config={"token": "tester_token"})
            client.list("/")

    @authors("alex-shishkin")
    def test_rpc_proxy_simple_query(self):
        socket_file = self.run_job_proxy(enable_rpc_proxy=True)
        client = self.create_client_from_uds(socket_file, config={"token": "tester_token"})
        root_listing = client.list("/")
        assert root_listing == ["sys", "tmp"]

    @authors("alex-shishkin")
    def test_failed_rpc_proxy_auth(self):
        with pytest.raises(YtError):
            socket_file = self.run_job_proxy(enable_rpc_proxy=True)
            client = self.create_client_from_uds(socket_file, config={"token": "wrong_token"})
            client.list("/")

    @authors("alex-shishkin")
    def test_incorrect_thread_count(self):
        with pytest.raises(YtError):
            self.run_job_proxy(enable_rpc_proxy=True, rpc_proxy_thread_pool_size=0)


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
        op = run_test_vanilla("sleep 100", job_count=10, track=False)

        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_job_proxy(node)

        thread_count = profiler.gauge("resource_tracker/thread_count")

        wait(lambda: thread_count.get() and thread_count.get() > 0)

        op.abort()

        wait(lambda: thread_count.get() is None)
