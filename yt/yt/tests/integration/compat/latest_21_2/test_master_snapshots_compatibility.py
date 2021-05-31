from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import *

from original_tests.yt.yt.tests.integration.tests.test_master_snapshots \
    import MASTER_SNAPSHOT_CHECKER_LIST, check_master_memory, check_proxy_roles

import os
import sys
import pytest
import random
import yatest.common

##################################################################


class TestMasterSnapshotsCompatibility(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    ARTIFACT_COMPONENTS = {
        "21_2": ["master"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    @authors("gritukan")
    def test(self):
        CHECKER_LIST = [
        ] + MASTER_SNAPSHOT_CHECKER_LIST

        # Remove tests that are not supported in 20.3 masters.
        CHECKER_LIST.remove(check_master_memory)
        CHECKER_LIST.remove(check_proxy_roles)

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_master_snapshots(set_read_only=True)

        with Restarter(self.Env, MASTERS_SERVICE):
            master_path = os.path.join(self.bin_path, "ytserver-master")
            ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/server/all/ytserver-all")
            print_debug("Removing {}".format(master_path))
            os.remove(master_path)
            print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, master_path))
            os.symlink(ytserver_all_trunk_path, master_path)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
