#!/usr/bin/env python

from yt.admin.snapshot_processing.helpers import (
    cypress_helpers,
    master_config_helpers,
    portal_cell_helpers,
    client_helpers)

import yt.logger as logger
import yt.yson as yson

from yt.wrapper import raw_io, OperationsTracker, FilePath
from yt.wrapper import YtOperationFailedError, YtError
from yt.wrapper import format as yt_format

from datetime import datetime, timedelta

import os
import sys
import logging
import argparse
import subprocess

schema_columns = [
    # -------------COLUMN NAME-----------------|---TYPE---|---REQUIRED---|--ATTRIBUTE--#
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
    ("expiration_time",                         "string",       False,      True),
    ("expiration_timeout",                      "int64",        False,      True),
    ("external",                                "boolean",      True,       True),
    ("force_nightly_compress",                  "any",          False,      True),
    ("id",                                      "string",       True,       True),
    ("immediate_annotation",                    "string",       False,      True),
    ("in_memory_mode",                          "string",       False,      True),
    ("inherit_acl",                             "boolean",      True,       True),
    ("last_compression_desired_chunk_size",     "int64",        False,      True),
    ("last_compression_time",                   "string",       False,      True),
    ("media",                                   "any",          False,      True),
    ("modification_time",                       "string",       True,       True),
    ("monitor_table_statistics",                "any",          False,      True),
    ("mount_config",                            "any",          False,      True),
    ("nightly_compressed",                      "boolean",      False,      True),
    ("nightly_compression_select_timestamp",    "int64",        False,      True),
    ("nightly_compression_settings",            "any",          False,      True),
    ("nightly_compression_user_time",           "string",       False,      True),
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
    ("schema",                                  "any",          False,      True),
    ("schema_attributes",                       "any",          False,      True),
    ("schema_mode",                             "string",       False,      True),
    ("security_tags",                           "any",          False,      True),
    ("sorted",                                  "any",          False,      True),
    ("tablet_cell_bundle",                      "string",       False,      True),
    ("tablet_state",                            "string",       False,      True),
    ("target_path",                             "string",       False,      True),
    ("type",                                    "string",       True,       True),
    ("uncompressed_data_size",                  "int64",        False,      True),
    ("user_attribute_keys",                     "any",          False,      True),
    ("versioned_resource_usage",                "any",          False,      True),
]


def create_schema():
    schema = yson.YsonList()
    schema.attributes["unique_keys"] = False
    schema.attributes["strict"] = True

    for column_name, type, required, _ in schema_columns:
        schema.append({"name": column_name, "type": type, "required": required})

    return schema


class MasterSnapshotExporter():
    def _configure_client(self):
        self._yt_client.config["mount_sandbox_in_tmpfs"] = {
            "enable": True,
            "additional_tmpfs_size": 2 ** 30,
        }
        self._yt_client.config["write_parallel"]["enable"] = True

        # Otherwise operations tracker could deadlock upon cancellation
        self._yt_client.config["ping_failed_mode"] = "pass"

    def _get_config_path(self, cell_id):
        return cypress_helpers.join_path(self._meta_path, cell_id + "_export")

    def _get_fresh_valid_snapshot(self, cell_id):
        # Lexicographical order of snapshot names corresponds to their creation time.
        validated_snapshots = self._yt_client.list(self._validation_result_paths[cell_id])[-self._number_of_considered_snapshots:]
        validated_snapshots.reverse()

        uploaded_snapshots = set(self._yt_client.list(self._snapshot_paths[cell_id]))
        exported_snapshots = set(self._yt_client.list(self._export_paths[cell_id]))

        for snapshot in validated_snapshots:
            validation_result_path = cypress_helpers.join_path(self._validation_result_paths[cell_id], snapshot)
            if snapshot in uploaded_snapshots and \
                    self._get_export_name(snapshot) not in exported_snapshots and \
                    self._yt_client.get("{}/@return_code".format(validation_result_path)) == 0:
                return snapshot

        return None

    def _get_op_title(self, snapshot_path):
        return "Export {}".format(snapshot_path)

    def _get_export_name(self, snapshot_name):
        return "{}_export".format(snapshot_name)

    def _get_unified_export_name(self, primary_snapshot_name):
        return "{}_unified_export".format(primary_snapshot_name)

    def _get_user_export_name(self, primary_snapshot_name):
        return "{}_user_export".format(primary_snapshot_name)

    def _get_export_table_path(self, cell_id, snapshot_name):
        return cypress_helpers.join_path(self._export_paths[cell_id], self._get_export_name(snapshot_name))

    def _init_tmp_table(self, tmp_table, input_list):
        self._yt_client.write_table(tmp_table, input_list, format=yt_format.YsonFormat(format="text"))

    def _get_last_export_serial_number(self, export_dir):
        if not self._yt_client.exists(export_dir):
            return -1

        existing_exports = self._yt_client.list(export_dir)
        if len(existing_exports) == 0:
            return -1

        latest_snapshot = cypress_helpers.join_path(export_dir, existing_exports[-1])
        if not self._yt_client.exists(latest_snapshot + "/@export_serial_number"):
            return -1

        return self._yt_client.get(latest_snapshot + "/@export_serial_number")

    def _calculate_ttl_for_export(self, export_dir):
        export_serial_number = self._get_last_export_serial_number(export_dir) + 1
        if export_serial_number == 0:
            return 0, None

        power = (export_serial_number & -export_serial_number).bit_length() - 1
        return export_serial_number, 2 ** ((power + 1) // 2)

    @raw_io
    def _mapper(self):
        for line in sys.stdin.buffer:
            input_yson = list(yson.loads(line, yson_type="list_fragment"))[0]
            job_index, job_count, export_users = input_yson["job_index"], input_yson["job_count"], input_yson["export_users"]
            os.mkdir("./changelogs")
            os.mkdir("./snapshots")

            export_config = "{{job_index={}; job_count={}; export_users={}; attributes=[{}]}}".format(
                job_index,
                job_count,
                export_users,
                ";".join(self._attributes))

            command_line = [
                "./ytserver-master",
                "--config", cypress_helpers.basename(self._get_config_path(input_yson["cell_id"])),
                "--export-snapshot", input_yson["snapshot_name"],
                "--export-config", export_config
            ]
            if self._skip_invariants_check:
                command_line.append("--skip-invariants-check")

            proc = subprocess.Popen(command_line, stderr=subprocess.PIPE, close_fds=False)
            _, stderr = proc.communicate()
            retcode = proc.returncode

            if retcode:
                raise RuntimeError(stderr)

    def run(self):
        self._configure_client()

        export_table_schema = create_schema()
        if self._attributes is None:
            self._attributes = [column for column, _, _, is_attribute in schema_columns if is_attribute is True]

        input_list = [{"job_index": job, "job_count": self._job_count} for job in range(self._job_count)]

        export_part_paths = []
        unified_export_name = None
        cell_id_to_snapshot = {}

        for cell_id in self._cell_ids:
            snapshot_name = self._get_fresh_valid_snapshot(cell_id)
            if snapshot_name is None:
                logger.info("Will not export snapshots: no fresh snapshots found to export at cell {}".format(cell_id))
                return
            cell_id_to_snapshot[cell_id] = snapshot_name

        tracker = OperationsTracker()
        for cell_id, snapshot_name in cell_id_to_snapshot.items():
            snapshot_path = cypress_helpers.join_path(self._snapshot_paths[cell_id], snapshot_name)

            if cell_id == self._primary_cell_id:
                unified_export_name = self._get_unified_export_name(snapshot_name)
                snapshot_creation_time = self._yt_client.get(snapshot_path + "/@creation_time")
                user_export_name = self._get_user_export_name(snapshot_name)
                user_export_path = cypress_helpers.join_path(self._user_export_path, user_export_name)
                is_primary_cell = True
            else:
                is_primary_cell = False

            master_binary_path = cypress_helpers.get_master_binary_path(
                self._yt_client,
                self._master_binary_path,
                snapshot_path)
            logger.info("Starting export of {} with binary {}".format(
                snapshot_path,
                cypress_helpers.basename(master_binary_path)))

            export_table_path = self._get_export_table_path(cell_id, snapshot_name)
            self._yt_client.create("table", export_table_path,
                                   attributes={"expiration_time": "{}".format(datetime.today() + timedelta(days=7)),
                                               "schema": export_table_schema,
                                               "optimize_for": "scan"})
            export_users = is_primary_cell  # export users from primary cell only
            if export_users:
                self._yt_client.create("table", user_export_path, attributes={"optimize_for": "scan"})

            self._yt_client.write_file(
                self._get_config_path(cell_id),
                self._master_configs[cell_id])

            for job_input in input_list:
                job_input["export_users"] = export_users
                job_input["cell_id"] = cell_id
                job_input["snapshot_name"] = snapshot_name

            tmp_table = self._yt_client.create_temp_table()
            self._init_tmp_table(tmp_table, input_list)

            try:
                op = self._yt_client.run_map(
                    self._mapper,
                    tmp_table,
                    ([export_table_path, user_export_path] if export_users else export_table_path),
                    yt_files=[
                        FilePath(master_binary_path, file_name="ytserver-master", attributes={"bypass_artifact_cache": True}),
                        self._get_config_path(cell_id),
                        FilePath(snapshot_path, attributes={"bypass_artifact_cache": True}),
                    ],
                    spec={
                        "title": self._get_op_title(snapshot_path),
                        "pool": self._scheduler_pool,
                        "max_failed_job_count": self._max_failed_job_count,
                        "mapper": {
                            "copy_files": True,
                            "cpu_limit": self._job_cpu_limit,
                            "job_time_limit": self._job_execution_time_limit,
                            "user_job_memory_digest_default_value": 1,
                        },
                    },
                    memory_limit=self._memory_limit_gbs * 1024 ** 3,
                    input_format=yt_format.YsonFormat(format="text"),
                    output_format=yt_format.YsonFormat(),
                    job_count=self._job_count,
                    sync=False,
                )

                tracker.add(op)
                export_part_paths.append(export_table_path)

            except YtOperationFailedError as error:
                raise error

        tracker.wait_all()

        if len(export_part_paths) != len(self._cell_ids):
            raise RuntimeError("Unexpectedly number of exported snapshots differs from number of cells: {} != {}".format(
                len(export_part_paths),
                len(self._cell_id)))
        if unified_export_name is None:
            raise RuntimeError("Unexpectedly 'unified_export_name' is None")

        unified_export_path = cypress_helpers.join_path(self._unified_export_path, unified_export_name)
        unified_export_serial_number, unified_export_ttl = self._calculate_ttl_for_export(self._unified_export_path)
        logger.info("Creating unified export with serial number {} and a ttl of {} week(s)".format(unified_export_serial_number, unified_export_ttl))
        self._yt_client.create("table", unified_export_path,
                               attributes={"optimize_for": "scan",
                                           "export_serial_number": unified_export_serial_number})
        if unified_export_ttl is not None:
            self._yt_client.set(unified_export_path + "/@expiration_time", "{}".format(datetime.today() + timedelta(days=7 * unified_export_ttl)))
            if export_users:
                self._yt_client.set(user_export_path + "/@expiration_time", "{}".format(datetime.today() + timedelta(days=7 * unified_export_ttl)))

        logger.info("Merging snapshot export parts into {}".format(unified_export_path))
        self._yt_client.run_merge(
            export_part_paths,
            unified_export_path,
            spec={
                "title": "Merge snapshot exports into a unified snapshot",
                "pool": self._scheduler_pool,
            })

        self._yt_client.set(unified_export_path + "/@snapshot_creation_time", snapshot_creation_time)
        latest_link_path = cypress_helpers.join_path(self._unified_export_path, "latest")
        logger.info("Creating link from export table to {}".format(latest_link_path))
        self._yt_client.link(
            unified_export_path,
            latest_link_path,
            force=True)

    def __init__(self, **kwargs):
        for key in kwargs:
            new_key = "_" + key
            assert not hasattr(self, new_key)
            setattr(self, new_key, kwargs[key])

        self._snapshot_name = None


def run():
    parser = argparse.ArgumentParser(description="Export validated master snapshot to Cypress.")

    parser.add_argument("--proxy")
    parser.add_argument("--token-env-variable")
    parser.add_argument("--cypress-root-path", default="//sys/admin/snapshots")
    parser.add_argument("--snapshots-cypress-subpath", default="snapshots")
    parser.add_argument("--validation-results-cypress-subpath", default="results")
    parser.add_argument("--export-cypress-subpath", default="exports")
    parser.add_argument("--master-binary-subpath-prefix", default="master_binary")
    parser.add_argument("--attributes", default=None)
    parser.add_argument("--job-count", type=int, default=10)
    parser.add_argument("--job-cpu-limit", type=int, default=2)
    parser.add_argument("--memory-limit-gbs", type=float, default=3)  # Most small clusters use 3 GiB
    parser.add_argument("--job-execution-time-limit", type=int, default=90 * 60 * 1000)  # 90m
    parser.add_argument("--scheduler-pool", type=str, default=None)
    parser.add_argument("--max-failed-job-count", type=int, default=3)
    parser.add_argument("--transaction-timeout", type=int, default=360 * 1000)
    # NB: This option is set to 1 because we want to consider only the newest snapshot of each cell
    # so table statistics monitor will provide consistent statistics.
    parser.add_argument("--number-of-considered-snapshots", type=int, default=1)
    parser.add_argument("--wait-lock-for", type=int, default=60 * 1000)
    parser.add_argument("--unified-export-subpath", type=str, default="snapshot_exports")
    parser.add_argument("--users-export-subpath", type=str, default="user_exports")
    parser.add_argument("--skip-invariants-check", action="store_true")

    args = parser.parse_args()

    yt_client = client_helpers.create_client(args.proxy, args.token_env_variable)

    logger.set_formatter(logging.Formatter('%(asctime)-15s\t{}\t%(message)s'.format(yt_client.config["proxy"]["url"])))

    if not yt_client.exists(args.cypress_root_path):
        raise RuntimeError("Invalid cypress root path {}".format(args.cypress_root_path))

    primary_cell_id = str(yt_client.get("//sys/@primary_cell_tag"))
    portal_cell_ids = portal_cell_helpers.get_portal_cell_ids(yt_client)
    cell_ids = list(set([primary_cell_id] + portal_cell_ids))

    snapshot_paths = {}
    validation_result_paths = {}
    export_paths = {}
    for cell_id in cell_ids:
        snapshot_paths[cell_id] = cypress_helpers.join_path(args.cypress_root_path, cell_id, args.snapshots_cypress_subpath)
        validation_result_paths[cell_id] = cypress_helpers.join_path(args.cypress_root_path, cell_id, args.validation_results_cypress_subpath)
        export_paths[cell_id] = cypress_helpers.join_path(args.cypress_root_path, cell_id, args.export_cypress_subpath)

    meta_path = cypress_helpers.join_path(args.cypress_root_path, "meta")
    master_binary_path = cypress_helpers.join_path(meta_path, args.master_binary_subpath_prefix)

    for path in list(snapshot_paths.values()) + list(validation_result_paths.values()):
        if not yt_client.exists(path):
            raise RuntimeError("Directory \"{}\" must exist".format(path))

    for path in export_paths.values():
        if not yt_client.exists(path):
            yt_client.create("map_node", path)

    unified_export_path = cypress_helpers.join_path(args.cypress_root_path, args.unified_export_subpath)
    if not yt_client.exists(unified_export_path):
        yt_client.create("map_node", unified_export_path)

    user_export_path = cypress_helpers.join_path(args.cypress_root_path, args.users_export_subpath)
    if not yt_client.exists(user_export_path):
        yt_client.create("map_node", user_export_path)

    master_configs = {}
    master_configs[primary_cell_id] = master_config_helpers.get_patched_primary(yt_client)
    # FIXME wait another PR with specification of secondary cell tags
    secondary_master_configs = master_config_helpers.get_patched_secondary(yt_client)
    for cell_id in portal_cell_ids:
        if cell_id == primary_cell_id:
            continue
        master_configs[cell_id] = secondary_master_configs[cell_id]

    logger.info("Starting snapshots export (attributes: {})".format(args.attributes))

    master_snapshot_exporter = MasterSnapshotExporter(
        yt_client=yt_client,
        primary_cell_id=primary_cell_id,
        cell_ids=cell_ids,
        snapshot_paths=snapshot_paths,
        validation_result_paths=validation_result_paths,
        export_paths=export_paths,
        meta_path=meta_path,
        master_configs=master_configs,
        master_binary_path=master_binary_path,
        attributes=args.attributes,
        job_count=args.job_count,
        job_cpu_limit=args.job_cpu_limit,
        memory_limit_gbs=args.memory_limit_gbs,
        job_execution_time_limit=args.job_execution_time_limit,
        scheduler_pool=args.scheduler_pool,
        max_failed_job_count=args.max_failed_job_count,
        number_of_considered_snapshots=args.number_of_considered_snapshots,
        unified_export_path=unified_export_path,
        user_export_path=user_export_path,
        skip_invariants_check=args.skip_invariants_check)

    with yt_client.Transaction(timeout=args.transaction_timeout):
        try:
            # TODO(akozhikhov): use shared locks for each cell
            yt_client.lock(export_paths[primary_cell_id], mode="exclusive", waitable=True, wait_for=args.wait_lock_for)
        except YtError:
            logger.info("Failed to lock {}".format(export_paths[primary_cell_id]))
            return
        except Exception as e:
            raise e
        master_snapshot_exporter.run()

    logger.info("Finished exporting snapshots")
