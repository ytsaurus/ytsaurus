from yt_env_setup import YTEnvSetup
from yt_commands import (ls, get, print_debug, authors, wait)

import os.path
import shutil
import requests

##################################################################


class TestJobProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

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

        job_proxy_path = os.path.join(self.bin_path, "ytserver-job-proxy")
        job_proxy_path_hidden = job_proxy_path + ".hide"

        print_debug("Moving {} to {}".format(job_proxy_path, job_proxy_path_hidden))
        shutil.move(job_proxy_path, job_proxy_path_hidden)

        wait(lambda: check_direct(True))
        wait(lambda: check_discover_versions(True))

        print_debug("Moving {} to {}".format(job_proxy_path_hidden, job_proxy_path))
        shutil.move(job_proxy_path_hidden, job_proxy_path)

        wait(lambda: check_direct(False))
        wait(lambda: check_discover_versions(False))
