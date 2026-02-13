from datetime import datetime, timezone
import os
import shlex
import subprocess
import tempfile
from types import NoneType
from typing import Any

from yt.wrapper import YtClient

from yt_commands import build_master_snapshots
import yt.yson as yson


PROC_TIMEOUT = 30

# by standard, this scheme should be equivalent to schema from
# yt/admin/snapshot_processing/export_master_snapshot/export_snapshot.py
SCHEMA_COLUMNS = [
    # -------------COLUMN NAME-----------------|---TYPE---|---REQUIRED---|--ATTRIBUTE--#
    ("_read_schema_attributes",                 "any",          False,      False),
    ("_read_schema",                            "any",          False,      False),
    ("_yql_read_udf",                           "string",       False,      False),
    ("_yql_row_spec_attributes",                "any",          False,      False),
    ("_yql_row_spec",                           "any",          False,      False),
    ("access_time",                             "string",       True,       True),
    ("account",                                 "string",       True,       True),
    ("acl",                                     "any",          False,      True),
    ("children_held",                           "any",          False,      False),
    ("chunk_count",                             "int64",        False,      True),
    ("chunk_format",                            "string",       False,      True),
    ("chunk_row_count",                         "int64",        False,      True),
    ("compressed_data_size",                    "int64",        False,      True),
    ("compression_codec",                       "string",       False,      True),
    ("creation_time",                           "string",       True,       True),
    ("cypress_transaction_id",                  "string",       False,      False),
    ("cypress_transaction_timestamp",           "any",          False,      False),
    ("cypress_transaction_title",               "string",       False,      False),
    ("data_weight",                             "int64",        False,      True),
    ("dynamic",                                 "any",          False,      True),
    ("erasure_codec",                           "string",       False,      True),
    ("external",                                "boolean",      True,       True),
    ("id",                                      "string",       True,       True),
    ("in_memory_mode",                          "string",       False,      True),
    ("inherit_acl",                             "boolean",      True,       True),
    ("media",                                   "any",          False,      True),
    ("modification_time",                       "string",       True,       True),
    ("monitor_table_statistics",                "any",          False,      True),
    ("nightly_compression_settings",            "any",          False,      True),
    ("optimize_for",                            "string",       False,      True),
    ("original_owner",                          "string",       False,      True),
    ("originator_cypress_transaction_id",       "string",       False,      False),
    ("orphaned",                                "boolean",      False,      False),
    ("owner",                                   "string",       False,      True),
    ("parent_id",                               "string",       False,      True),
    ("path",                                    "string",       True,       False),
    ("primary_medium",                          "string",       False,      True),
    ("ref_counter",                             "int64",        True,       True),
    ("replication_factor",                      "int64",        False,      True),
    ("resource_usage",                          "any",          False,      True),
    ("revision",                                "uint64",       False,      True),
    ("row_count",                               "int64",        False,      True),
    ("schema_attributes",                       "any",          False,      True),
    ("schema_mode",                             "string",       False,      True),
    ("schema",                                  "any",          False,      True),
    ("sorted",                                  "any",          False,      True),
    ("security_tags",                           "any",          False,      True),
    ("tablet_cell_bundle",                      "string",       False,      True),
    ("target_path",                             "string",       False,      True),
    ("type",                                    "string",       True,       True),
    ("uncompressed_data_size",                  "int64",        False,      True),
    ("user_attribute_keys",                     "any",          False,      True),
    ("versioned_resource_usage",                "any",          False,      True),
    ("last_compression_time",                   "string",       False,      True),
    ("force_nightly_compress",                  "any",          False,      True),
    ("nightly_compressed",                      "boolean",      False,      True),
    ("last_compression_desired_chunk_size",     "int64",        False,      True),
    ("nightly_compression_select_timestamp",    "int64",        False,      True),
    ("nightly_compression_user_time",           "string",       False,      True),
    ("tablet_state",                            "string",       False,      True),
    ("expiration_time",                         "string",       False,      True),
    ("expiration_timeout",                      "int64",        False,      True),
]


class SnapshotBuilderBase:
    def __init__(self, path_to_run: str, bin_path: str) -> None:
        self.path_to_run = path_to_run
        self.bin_path = bin_path
        self.binary = os.path.join(self.bin_path, "ytserver-master")
        self.config_path = os.path.join(self.path_to_run, "configs", "master-0-0.yson")

    def build_master_snapshot(self, set_read_only=False) -> str:
        build_master_snapshots(set_read_only=set_read_only)

        self.snapshot_result_dir = os.path.join(self.path_to_run, "runtime_data", "master", "0")
        assert os.path.exists(self.snapshot_result_dir)

        snapshot_dir = os.path.join(self.snapshot_result_dir, "snapshots")

        snapshots = [name for name in os.listdir(snapshot_dir) if name.endswith(".snapshot")]
        snapshots.sort()

        self.snapshot_id = snapshots[-1].split(".")[0]
        self.snapshot_path = os.path.join(snapshot_dir, self.snapshot_id + ".snapshot")
        return self.snapshot_id

    def validate_master_snapshot(self):
        changelog_dir = os.path.join(self.snapshot_result_dir, "changelogs")
        changelog_path = os.path.join(changelog_dir, self.snapshot_id + ".log")

        assert self._run_validation(
            self.binary,
            self.snapshot_path,
            changelog_path,
            self.snapshot_result_dir,
            self.config_path,
        ) == 0
        return self.snapshot_id

    def _run_validation(
            self,
            binary,
            snapshot_path,
            changelog_path,
            build_snapshot_dir,
            config_path,
            cell_id=None,
            snapshot_meta=None):
        # NB: Sleep after initialize is required since the main thread otherwise can halt before
        # some other thread uses network.
        command = [
            binary,
            "--validate-snapshot", snapshot_path,
            "--replay-changelogs", changelog_path,
            "--build-snapshot", build_snapshot_dir,
            "--config", config_path,
        ]

        if cell_id is not None:
            command += ["--cell-id", cell_id]

        if snapshot_meta is not None:
            command += ["--snapshot-meta", yson.dumps(snapshot_meta)]

        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            _, _ = proc.communicate(timeout=PROC_TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
        return proc.returncode


def create_schema():
    schema = yson.YsonList()
    schema.attributes["unique_keys"] = False
    schema.attributes["strict"] = True
    for column_name, type, required, _ in SCHEMA_COLUMNS:
        schema.append({"name": column_name, "type": type, "required": required})
    return schema


class NativeSnapshotRunner(SnapshotBuilderBase):
    def create_export_config(self):
        _attributes = [column for column, _, _, is_attribute in SCHEMA_COLUMNS if is_attribute is True]
        export_users = "%true"
        export_config = "{{export_users={};attributes=[{}]}}".format(export_users, ";".join(_attributes))
        return export_config

    def build_and_export_master_snapshot(self, yt_client: YtClient, append_to_snapshot: list[dict[str, Any]] | None = None):
        snapshot_time = datetime.now(tz=timezone.utc)

        snapshot_id = self.build_master_snapshot()
        self.validate_master_snapshot()

        export_config = self.create_export_config()
        snapshot_exports_destination = "//sys/admin/snapshots/snapshot_exports/" + snapshot_id + "_abcdabcd_unified_export"
        user_exports_destination = "//sys/admin/snapshots/user_exports/" + snapshot_id + "_abcdabcd_user_export"

        self._run_export(
            yt_client,
            self.binary,
            self.snapshot_path,
            self.config_path,
            export_config,
            snapshot_exports_destination,
            user_exports_destination,
            append_to_snapshot,
            snapshot_time
        )

        return int(snapshot_time.timestamp())

    def _run_export(
            self,
            yt_client: YtClient,
            binary,
            snapshot_path,
            config_path,
            export_config,
            snapshot_exports_destination,
            user_exports_destination,
            append_to_snapshot,
            snapshot_time):
        command = [
            binary,
            "--export-snapshot", snapshot_path,
            "--config", config_path,
            "--export-config", export_config,
        ]

        with tempfile.NamedTemporaryFile(mode="rb+") as tmp_file1:
            with tempfile.NamedTemporaryFile(mode="rb+") as tmp_file4:
                command = shlex.join(command)
                subprocess.run(
                    f"{command} 1>{tmp_file1.name} 4>{tmp_file4.name}",
                    shell=True,
                    check=True,
                    timeout=PROC_TIMEOUT
                )

                tmp_file1.seek(0)
                fd1_output = tmp_file1.read()

                tmp_file4.seek(0)
                fd4_output = tmp_file4.read()

        export_table_schema = create_schema()

        yt_client.create(
            "map_node",
            os.path.dirname(snapshot_exports_destination),
            recursive=True,
            ignore_existing=True,
        )
        yt_client.create(
            "map_node",
            os.path.dirname(user_exports_destination),
            recursive=True,
            ignore_existing=True,
        )

        yt_client.create(
            "table",
            snapshot_exports_destination,
            recursive=True,
            ignore_existing=True,
            attributes={
                "schema": export_table_schema,
                "snapshot_creation_time": "{}".format(snapshot_time),
            },
        )
        yt_client.create(
            "table",
            user_exports_destination,
            recursive=True,
            ignore_existing=True,
            attributes={
                "snapshot_creation_time": "{}".format(snapshot_time),
            },
        )

        yt_client.write_table(
            snapshot_exports_destination,
            yson.loads(fd1_output, yson_type="list_fragment"),
        )
        if not isinstance(append_to_snapshot, NoneType):
            yt_client.write_table(f"<append=%true;>{snapshot_exports_destination}", append_to_snapshot)

        yt_client.write_table(
            user_exports_destination,
            yson.loads(fd4_output, yson_type="list_fragment"),
        )
        yt_client.link(snapshot_exports_destination, "//sys/admin/snapshots/snapshot_exports/latest", force=True)
        yt_client.link(
            user_exports_destination,
            "//sys/admin/snapshots/user_exports/latest",
            force=True,
        )
