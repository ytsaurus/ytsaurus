import os
import subprocess
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
]


def create_schema():
    schema = yson.YsonList()
    schema.attributes["unique_keys"] = False
    schema.attributes["strict"] = True
    for column_name, type, required, _ in SCHEMA_COLUMNS:
        schema.append({"name": column_name, "type": type, "required": required})
    return schema


class NativeSnapshotRunner:
    def __init__(self, path_to_run: str, bin_path: str) -> None:
        self.path_to_run = path_to_run
        self.bin_path = bin_path

    def create_export_config(self):
        _attributes = [column for column, _, _, is_attribute in SCHEMA_COLUMNS if is_attribute is True]

        export_config = "{{attributes=[{}]}}".format(
                        ";".join(_attributes))

        return export_config

    def build_and_export_master_snapshot(self, yt_client: YtClient, append_to_snapshot: list[dict[str, Any]] | None = None):

        build_master_snapshots()

        result_dir = os.path.join(self.path_to_run, "runtime_data", "master", "0")
        assert os.path.exists(result_dir)

        snapshot_dir = os.path.join(result_dir, "snapshots")
        changelog_dir = os.path.join(result_dir, "changelogs")

        snapshots = [name for name in os.listdir(snapshot_dir) if name.endswith("snapshot")]
        snapshots.sort()

        snapshot_id = snapshots[-1].split(".")[0]
        snapshot_path = os.path.join(snapshot_dir, snapshot_id + ".snapshot")
        changelog_path = os.path.join(changelog_dir, snapshot_id + ".log")
        binary = os.path.join(self.bin_path, "ytserver-master")

        config_path = os.path.join(self.path_to_run, "configs", "master-0-0.yson")
        assert self._run_validation(
            binary,
            snapshot_path,
            changelog_path,
            result_dir,
            config_path,
        ) == 0

        export_config = self.create_export_config()
        export_destination = "//sys/admin/snapshots/snapshot_exports/latest"

        self._run_export(
            yt_client,
            binary,
            snapshot_path,
            config_path,
            export_config,
            export_destination,
            append_to_snapshot
        )

    def _run_export(
            self,
            yt_client: YtClient,
            binary,
            snapshot_path,
            config_path,
            export_config,
            export_destination,
            append_to_snapshot):
        command = [
            binary,
            "--export-snapshot", snapshot_path,
            "--config", config_path,
            "--export-config", export_config,
        ]

        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, _ = proc.communicate(timeout=PROC_TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
        assert proc.returncode == 0

        export_table_schema = create_schema()
        yt_client.remove(os.path.dirname(export_destination), recursive=True, force=True)
        yt_client.create("map_node", os.path.dirname(export_destination), recursive=True)
        yt_client.create("table", export_destination, recursive=True, ignore_existing=True, attributes={"schema": export_table_schema})
        yt_client.write_table(
            export_destination,
            yson.loads(stdout, yson_type="list_fragment"),
        )
        if not isinstance(append_to_snapshot, NoneType):
            yt_client.write_table(f"<append=%true;>{export_destination}", append_to_snapshot)

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
