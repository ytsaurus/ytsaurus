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
    NUM_MASTERS = 1

    @authors("ifsmirnov")
    def test_master_snapshot_validation(self):
        create("table", "//tmp/t")
        build_master_snapshots()

        snapshot_dir = os.path.join(self.path_to_run, "runtime_data", "master", "0", "snapshots")
        snapshots = [name for name in os.listdir(snapshot_dir) if name.endswith("snapshot")]
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

    @authors("akozhikhov")
    def test_tablet_cell_snapshot_validation(self):
        [cell_id] = sync_create_cells(1)

        snapshot_path = "//sys/tablet_cells/{}/snapshots".format(cell_id)
        assert not ls(snapshot_path)
        build_snapshot(cell_id=cell_id)
        snapshots = ls(snapshot_path)
        assert snapshots

        binary = find_executable("ytserver-node")

        config_path = os.path.join(self.path_to_run, "configs", "node-0.yson")

        config = None
        with open(config_path, "r") as fh:
            config = " ".join(fh.read().splitlines())
            config = yson.loads(config)

        config["data_node"]["store_locations"][0]["path"] = "."

        with open(config_path, "w") as fh:
            fh.write(yson.dumps(config, yson_format="pretty"))

        snapshot = read_file("{}/{}".format(snapshot_path, snapshots[0]))
        with open("snapshot_file", "w") as fh:
            fh.write(snapshot)

        command = [
            binary,
            "--validate-snapshot",
            "./snapshot_file",
            "--config",
            config_path,
            "--sleep-after-initialize",
        ]
        ret = subprocess.run(command)
        assert ret.returncode == 0
