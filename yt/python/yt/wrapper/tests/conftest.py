from yt.wrapper.constants import UI_ADDRESS_PATTERN
from yt.wrapper.testlib.helpers import (  # noqa
    get_tests_location, get_tests_sandbox, get_test_file_path,
    wait, sync_create_cell, create_job_events, TEST_DIR)
from yt.wrapper.testlib.conftest_helpers import *  # noqa

import os
import pytest
import re
import socket


@pytest.hookimpl
def pytest_enter_pdb(config, pdb):

    if config and config.current_test_log_path and os.path.exists(config.current_test_log_path):
        yt_host, yt_port = None, None
        cur_host = socket.gethostname()

        for line, _ in zip(open(config.current_test_log_path, "r"), range(1000)):
            # http proxy
            res = re.search(r"Perform HTTP get request https?:\/\/(.+?):(\d+)\/api", line)
            if res:
                yt_host, yt_port = res.groups()
                break
            # backup (for rpc)
            res = re.search(r"(localhost):(\d+)", line)
            if res:
                yt_host, yt_port = res.groups()

        if yt_host:
            print("")
            print("Local YT cluster is still available for this test.")
            print("  Use: `yt --proxy \"http://{}:{}\" list /` to connect".format(yt_host, yt_port))
            print("   or: `yt --proxy \"http://{}:{}\" list /`".format(cur_host, yt_port))
            print("   or: \"{}\" (take care about network availability (f.e. port range))".format(UI_ADDRESS_PATTERN.format(cluster_name=cur_host + ":" + yt_port)))
            print("\n" + "-" * 128)
