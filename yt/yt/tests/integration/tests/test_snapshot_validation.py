from yt_env_setup import YTEnvSetup
from yt_commands import *

from distutils.spawn import find_executable

import subprocess
import os

##################################################################

# The primary purpose of this test is to check that the snapshot validation process runs at all
# and to check anything specific. The usual reason of failure is accidental network usage
# in TBootstrap::Initialize.

##################################################################


class TestSnapshotValidation(YTEnvSetup):
    @authors("ifsmirnov")
    def test_master_snapshot_validation(self):
        create("table", "//tmp/t")
        build_master_snapshots()

        snapshot_dir = os.path.join(self.path_to_run, "runtime_data", "master", "0", "snapshots")
        snapshots = os.listdir(snapshot_dir)
        assert len(snapshots) > 0
        snapshot_path = os.path.join(snapshot_dir, snapshots[0])

        config_path = os.path.join(self.path_to_run, "configs", "master-0-0.yson")

        binary = find_executable("ytserver-master")

        # NB: Sleep after initialize is required since the main thread otherwise can halt before
        # some other thread uses network.
        command = [
            binary,
            "--validate-snapshot",
            snapshot_path,
            "--config",
            config_path,
            "--sleep-after-initialize",
        ]

        ret = subprocess.run(command)
        assert ret.returncode == 0
