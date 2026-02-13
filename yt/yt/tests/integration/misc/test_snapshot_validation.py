from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, create_account, create_dynamic_table, execute_command, sync_mount_table,
    ls, read_file, sync_create_cells, write_table, insert_rows, build_snapshot,
    get, build_master_snapshots, print_debug)

import yt.yson as yson

import pytest

from pytest import fail

import subprocess
import os
import os.path

##################################################################

# The primary purpose of this test is to check that the snapshot validation process runs at all
# and to check anything specific. The usual reason of failure is accidental network usage
# in TBootstrap::Initialize.

##################################################################


@pytest.mark.enabled_multidaemon
class TestSnapshotValidation(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    USE_DYNAMIC_TABLES = True

    def _update_config(self, config_path):
        config = None
        with open(config_path, "rb") as fh:
            config = yson.loads(fh.read())
        config["data_node"]["store_locations"] = []
        config["exec_node"]["slot_manager"]["locations"] = []
        with open(config_path, "wb") as fh:
            fh.write(yson.dumps(config, yson_format="pretty"))

    def _populate_master_changelog(self):
        create_account("cool_account")
        create("table", "//tmp/table1")
        create("table", "//tmp/table2", attributes={"account": "cool_account", "schema": [
            {"name": "title", "type": "string"},
            {"name": "score", "type": "int64"},
            {"name": "is_funny", "type": "any"}]})
        write_table("//tmp/table2", {"title": "AHIT", "score": 79, "is_funny": "yes"})
        write_table("<append=%true>//tmp/table2", {"title": "TBOI", "score": 86, "is_funny": "yes"})
        write_table("//tmp/table1", {"misery": "a little line", "happiness": "a little cross"})

    def _run_validation(
            self,
            binary,
            snapshot_path,
            build_snapshot_dir,
            config_path,
            changelog_path=None,
            cell_id=None,
            snapshot_meta=None):
        # NB: Sleep after initialize is required since the main thread otherwise can halt before
        # some other thread uses network.
        command = [
            binary,
            "--validate-snapshot", snapshot_path,
            "--build-snapshot", build_snapshot_dir,
            "--config", config_path,
        ]

        if changelog_path is not None:
            command += ["--replay-changelogs", changelog_path]

        if cell_id is not None:
            command += ["--cell-id", cell_id]

        if snapshot_meta is not None:
            command += ["--snapshot-meta", yson.dumps(snapshot_meta)]

        timeout = 30
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            fail("Snapshot validation timed out (Timeout: {})".format(timeout))
        print_debug("Snapshot validation finished (stdout: {}, stderr: {})".format(stdout, stderr))
        return proc.returncode

    @authors("h0pless")
    def test_master_dry_run(self):
        build_master_snapshots()
        self._populate_master_changelog()
        build_master_snapshots()

        result_dir = os.path.join(self.path_to_run, "runtime_data", "master", "0")
        assert os.path.exists(result_dir)

        snapshot_dir = os.path.join(result_dir, "snapshots")
        changelog_dir = os.path.join(result_dir, "changelogs")

        snapshots = [name for name in os.listdir(snapshot_dir) if name.endswith("snapshot")]
        snapshots.sort()
        assert len(snapshots) > 1

        snapshot_id = snapshots[-2].split(".")[0]
        print_debug("Using snapshot and changelog with id ", snapshot_id)
        snapshot_path = os.path.join(snapshot_dir, snapshot_id + ".snapshot")
        changelog_path = os.path.join(changelog_dir, snapshot_id + ".log")

        config_path = os.path.join(self.path_to_run, "configs", "master-0-0.yson")
        binary = os.path.join(self.bin_path, "ytserver-master")

        assert self._run_validation(
            binary,
            snapshot_path,
            result_dir,
            config_path,
            changelog_path=changelog_path) == 0, "Snapshot validation failed"

    def _populate_tablet_cell_changelog(self, lower_key, upper_key):
        format = yson.YsonString(b"yson")
        format.attributes["enable_null_to_yson_entity_conversion"] = False
        for key in range(lower_key, upper_key):
            rows = [{
                "key": key,
                "value": "test" * (key % 5),
                "arbitrary": (None, 2.0, "three")[key % 3],
            }]
            format.attributes["enable_null_to_yson_entity_conversion"] = key % 2 == 0
            insert_rows(
                "//tmp/dyntable",
                rows,
                input_format=format,
            )

    @authors("akozhikhov", "h0pless", "sabdenovch")
    @pytest.mark.parametrize("sorted", [False, True])
    def test_tablet_cell_dry_run(self, sorted):
        [cell_id] = sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
            {"name": "arbitrary", "type": "any"},
        ]
        if sorted:
            schema[0]["sort_order"] = "ascending"

        create_dynamic_table("//tmp/dyntable", schema=schema)
        sync_mount_table("//tmp/dyntable")

        self._populate_tablet_cell_changelog(1, 7)
        build_snapshot(cell_id=cell_id)
        self._populate_tablet_cell_changelog(8, 14)
        build_snapshot(cell_id=cell_id)

        snapshot_dir = "//sys/tablet_cells/{}/snapshots".format(cell_id)
        changelog_dir = "//sys/tablet_cells/{}/changelogs".format(cell_id)

        snapshots = ls(snapshot_dir)
        snapshots.sort()
        assert len(snapshots) > 1

        binary = os.path.join(self.bin_path, "ytserver-node")
        config_path = os.path.join(self.path_to_run, "configs", "node-0.yson")
        self._update_config(config_path)

        snapshot_id = snapshots[-2]
        next_snapshot_id = snapshots[-1]
        print_debug("Using snapshot and changelog with id ", snapshot_id)
        snapshot_path = f"{snapshot_dir}/{snapshot_id}"
        next_snapshot_path = f"{snapshot_dir}/{next_snapshot_id}"
        snapshot = read_file(snapshot_path)
        next_snapshot = read_file(next_snapshot_path)
        with open("snapshot_file", "wb") as fh:
            fh.write(snapshot)

        snapshot_meta = get(snapshot_path + "/@", attributes=[
            "last_mutation_term",
            "timestamp",
            "last_segment_id",
            "random_seed",
            "last_record_id",
            "sequence_number",
            "state_hash",
            "logical_time",
            "last_logical_record_id",
        ])

        changelog_path = f"{changelog_dir}/{snapshot_id}"
        with open("changelog_file", 'wb') as fh:
            fh.write(execute_command("read_journal", {"path": changelog_path}))

        suffix = "sorted" if sorted else "ordered"
        replay_dir = f"with_replay_{suffix}"
        resave_dir = f"with_resave_{suffix}"
        if not os.path.exists(replay_dir):
            os.mkdir(replay_dir)
        if not os.path.exists(resave_dir):
            os.mkdir(resave_dir)

        assert self._run_validation(
            binary,
            "snapshot_file",
            f"{replay_dir}",
            config_path,
            changelog_path="./changelog_file",
            cell_id=cell_id,
            snapshot_meta=snapshot_meta) == 0, "Snapshot validation with replay failed"

        with open(f"{replay_dir}/{next_snapshot_id}.snapshot", "rb") as f:
            replay_snapshot = f.read()

        assert replay_snapshot == next_snapshot

        next_snapshot_meta = get(next_snapshot_path + "/@", attributes=[
            "last_mutation_term",
            "timestamp",
            "last_segment_id",
            "random_seed",
            "last_record_id",
            "sequence_number",
            "state_hash",
            "logical_time",
            "last_logical_record_id",
        ])

        assert self._run_validation(
            binary,
            f"{replay_dir}/{next_snapshot_id}.snapshot",
            f"{resave_dir}",
            config_path,
            cell_id=cell_id,
            snapshot_meta=next_snapshot_meta) == 0, "Snapshot validation with resave failed"

        with open(f"{resave_dir}/{next_snapshot_id}.snapshot", "rb") as f:
            resave_snapshot = f.read()

        assert resave_snapshot == next_snapshot
