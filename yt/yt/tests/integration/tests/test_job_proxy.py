from yt_env_setup import (YTEnvSetup, Restarter, NODES_SERVICE)
from yt_commands import (ls, get, print_debug, authors, wait, run_test_vanilla)

import os.path
import shutil
import requests

##################################################################


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

    def _job_proxy_path(self):
        return os.path.join(self.bin_path, "ytserver-job-proxy")

    def _hide_job_proxy(self):
        job_proxy_path_hidden = self._job_proxy_path() + ".hidden"
        print_debug("Moving {} to {}".format(self._job_proxy_path(), job_proxy_path_hidden))
        shutil.move(self._job_proxy_path(), job_proxy_path_hidden)

    def _unhide_job_proxy(self):
        job_proxy_path_hidden = self._job_proxy_path() + ".hidden"
        print_debug("Moving {} to {}".format(job_proxy_path_hidden, self._job_proxy_path()))
        shutil.move(job_proxy_path_hidden, self._job_proxy_path())


class TestJobProxyBinary(JobProxyTestBase):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "job_proxy_build_info_update_period": 300,
            }
        }
    }

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
            job_proxy_build = get(orchid_path + "/job_controller/job_proxy_build")
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

        self._hide_job_proxy()

        wait(lambda: check_direct(True))
        wait(lambda: check_discover_versions(True))

        self._unhide_job_proxy()

        wait(lambda: check_direct(False))
        wait(lambda: check_discover_versions(False))

    @authors("max42")
    def test_slot_disabling_on_unavailable_job_proxy(self):
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/total_node_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 1)

        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/total_node_count") == 0)
            wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 0)
            self._hide_job_proxy()

        wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/total_node_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 0)

        self._unhide_job_proxy()

        wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/total_node_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 1)


class TestUnavailableJobProxy(JobProxyTestBase):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "job_proxy_build_info_update_period": 300,
            },
            "slot_manager": {
                "testing": {
                    "skip_job_proxy_unavailable_alert": True,
                }
            }
        }
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

        self._hide_job_proxy()

        wait(lambda: op.get_job_count("aborted") > 2)

        self._unhide_job_proxy()

        op.track()
