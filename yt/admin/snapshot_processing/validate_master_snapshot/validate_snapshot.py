#!usr/bin/env python

from yt.admin.snapshot_processing.helpers import (
    binary_janitor,
    cypress_helpers,
    master_config_helpers,
    client_helpers)

import yt.logger as logger

from yt.wrapper.errors import YtCypressTransactionLockConflict

from yt.wrapper import FilePath
from yt.wrapper import format as yt_format
from yt.common import YT_NULL_TRANSACTION_ID

from datetime import datetime, timedelta

import sys
import time
import logging
import argparse
import subprocess
import os.path
import shutil


class MasterSnapshotsValidator():
    def _configure_client(self):
        self._yt_client.config["mount_sandbox_in_tmpfs"] = {
            "enable": True,
            "additional_tmpfs_size": 2 ** 30,
        }
        # self._yt_client.config["pickling"]["encrypt_pickle_files"] = 2

    def _get_config_path(self, cell_tag):
        return cypress_helpers.join_path(self._meta_path, cell_tag + "_validation")

    def _init_input_table(self, input_table, snapshot_path, cell_tag):
        row = [{"cell_tag": cell_tag, "snapshot_name": cypress_helpers.basename(snapshot_path)}]
        self._yt_client.write_table(input_table, row, format=yt_format.YsonFormat())

    def _get_operation_title(self, snapshot_path):
        return "Check snapshot {0}".format(snapshot_path)

    def _get_result_file_path(self, cell_tag, snapshot_name):
        results_path = cypress_helpers.join_path(self._cypress_root_path, cell_tag, self._results_cypress_subpath)
        return cypress_helpers.join_path(results_path, snapshot_name)

    def _create_result_file(self, result_file_path, return_code):
        """
        :type snapshot_name: str, unicode
        :type return_code: int
        """
        try:
            self._yt_client.create("file", result_file_path,
                                   ignore_existing=True,
                                   attributes={"expiration_time": "{}".format(datetime.today() + timedelta(days=14)),
                                               "return_code": return_code})
            return True
        except YtCypressTransactionLockConflict:
            # Someone creates the same file. It should be concurrent run of this
            # script. Seems ok.
            logger.info("Failed to write result file " + result_file_path + " due to lock conflict. Skipping")
            return False

    def _try_lock_node(self, path, mode):
        try:
            self._yt_client.lock(path, mode=mode)
            return True
        except:  # noqa
            return False

    def _pick_snapshots_by_cell_tag(self, cell_tag):
        snapshots_path = cypress_helpers.join_path(self._cypress_root_path, cell_tag, self._snapshots_cypress_subpath)
        results_path = cypress_helpers.join_path(self._cypress_root_path, cell_tag, self._results_cypress_subpath)

        # Lexicographical order of snapshot names corresponds to their creation time.
        snapshots_in_cypress = self._yt_client.list(snapshots_path)[-self._number_of_snapshots_to_consider:]
        snapshots_in_cypress.reverse()
        results_in_cypress = set(self._yt_client.list(results_path))

        snapshot_paths_for_validation = []
        for snapshot in snapshots_in_cypress:
            snapshot_path = cypress_helpers.join_path(snapshots_path, snapshot)
            snapshot_result_path = cypress_helpers.join_path(results_path, snapshot)

            if self._custom_snapshot_validation:
                return [snapshot_path]

            if snapshot not in results_in_cypress and \
                    self._try_lock_node(snapshot_path, "snapshot"):
                if self._yt_client.exists(snapshot_result_path):
                    continue

                snapshot_paths_for_validation.append(snapshot_path)
                if len(snapshot_paths_for_validation) >= self._number_of_snapshots_to_validate_per_cell:
                    break

        return snapshot_paths_for_validation

    def _pick_snapshots(self):
        snapshots_in_cell_tags = {}
        for cell_tag in self._cell_tags:
            cell_tag_snapshots = self._pick_snapshots_by_cell_tag(cell_tag)
            if cell_tag_snapshots:
                snapshots_in_cell_tags[cell_tag] = cell_tag_snapshots

        return snapshots_in_cell_tags

    def _mapper(self, row):
        cell_tag = row["cell_tag"]
        snapshot_name = row["snapshot_name"]

        command_line = [
            "./ytserver-master",
            "--config", cypress_helpers.basename(self._get_config_path(cell_tag)),
            "--validate-snapshot", snapshot_name,
        ]

        if self._abort_on_alert:
            command_line.append("--abort-on-alert")

        proc = subprocess.Popen(command_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = proc.communicate(timeout=self._validation_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()

        retcode = proc.returncode

        result = {"return_code": retcode, "stderr": stderr if retcode else ""}
        yield result

    def _start_map(self, input_table, output_table, snapshot_path, cell_tag, master_binary_path):
        self._init_input_table(input_table, snapshot_path, cell_tag)

        return self._yt_client.run_map(
            self._mapper,
            input_table,
            output_table,
            yt_files=[
                FilePath(snapshot_path, attributes={"bypass_artifact_cache": True}),
                FilePath(master_binary_path, file_name="ytserver-master", attributes={"bypass_artifact_cache": True}),
                self._get_config_path(cell_tag),
            ],
            spec={
                "title": self._get_operation_title(snapshot_path),
                "pool": self._scheduler_pool,
                "max_failed_job_count": self._max_failed_jobs,
                "mapper": {
                    "copy_files": True,
                    "job_time_limit": self._job_time_limit * 1000,
                    "cpu_limit": self._job_cpu_limit,
                    "user_job_memory_digest_default_value": 1,
                    "use_porto_memory_tracking": self._use_porto_memory_tracking,
                },
            },
            memory_limit=self._memory_limit_gbs * 1024 ** 3,
            format=yt_format.YsonFormat(),
            sync=False,
        )

    def _process_mapper_result(self, result_file_path, output_table):
        rows = self._yt_client.read_table(output_table, format=yt_format.YsonFormat())

        with self._yt_client.Transaction(
            transaction_id=YT_NULL_TRANSACTION_ID,
        ):
            for row in rows:
                return_code = row.get("return_code")
                # We don't want Odin to know about custom checks.
                displayed_return_code = return_code if not self._custom_snapshot_validation else 0

                if not self._create_result_file(result_file_path, displayed_return_code):
                    # Looks like some concurrent process is validating the same snapshot.
                    return True

                stderr = row.get("stderr")
                if return_code and stderr:
                    self._yt_client.write_file(result_file_path, stderr.encode())
                return return_code == 0

    def _check_is_operation_finished(self, operation, snapshot_path):
        state = operation.get_state()
        if state.is_finished():
            if not state.is_unsuccessfully_finished():
                return True
            stderr = operation.get_jobs_with_error_or_stderr()
            sys.stderr.write("Map stderrs: {0}".format(stderr))
            raise RuntimeError("Map operation '{}' for '{}' was unsuccessful".format(
                operation.id,
                snapshot_path))

        return False

    def _monitor_operations(self, operations):
        wait_time = 0
        all_ops_successful = True
        while wait_time < self._total_validation_timeout:
            unfinished_operations = []
            for operation_info in operations:
                operation, output_table, snapshot_path, cell_tag = operation_info

                if self._check_is_operation_finished(operation, snapshot_path):
                    result_file_path = self._get_result_file_path(cell_tag, cypress_helpers.basename(snapshot_path))
                    if self._custom_snapshot_validation:
                        result_file_path += "_by_{binary}".format(binary=cypress_helpers.basename(
                            self._master_binary_path))

                    map_success = self._process_mapper_result(result_file_path, output_table)
                    all_ops_successful = all_ops_successful and map_success

                    if map_success:
                        logger.info("Operation for {0} has successfully finished".format(snapshot_path))
                    else:
                        logger.error("Operation for cell tag {0} failed. Error is written to {1}".format(cell_tag, result_file_path))
                else:
                    unfinished_operations.append(operation_info)

            if unfinished_operations:
                operations = unfinished_operations
                time.sleep(self._sleep_between_checks)
                wait_time += self._sleep_between_checks
            else:
                if self._custom_snapshot_validation:
                    if all_ops_successful:
                        logger.info("Custom validation finished successfully")
                    else:
                        raise RuntimeError("Custom validation failed. Check abovementioned errors")
                return

        raise RuntimeError("Max total validation time exceeded")

    def guarded_run(self):
        self._configure_client()

        if self._cypress_lock_path is not None:
            if not self._try_lock_node(self._cypress_lock_path, "exclusive"):
                logger.info("A concurrent execution detected; backing off.")
                return

        operations = []

        picked_snapshots_in_all_cells = self._pick_snapshots()

        for cell_tag in sorted(picked_snapshots_in_all_cells):
            # This call may fail in case of concurrent execution, but configs rarely change and it's fine to use
            # a slightly older config here.
            try:
                self._yt_client.write_file(
                    self._get_config_path(cell_tag),
                    self._master_configs[cell_tag],
                )
            except Exception as ex:  # noqa
                logger.info(
                    "Error writing config for file cell %s (type: %s): %s",
                    cell_tag,
                    type(ex).__name__,
                    str(ex))

        for cell_tag, cell_tag_snapshots in picked_snapshots_in_all_cells.items():
            for snapshot_path in cell_tag_snapshots:
                input_tmp_table = self._yt_client.create_temp_table()
                output_tmp_table = self._yt_client.create_temp_table()

                master_binary_path = self._master_binary_path
                if not self._custom_snapshot_validation:
                    master_binary_path = cypress_helpers.get_master_binary_path(
                        self._yt_client,
                        master_binary_path,
                        snapshot_path)

                logger.info("Starting validation of {} with binary {}".format(
                    snapshot_path,
                    cypress_helpers.basename(master_binary_path)))
                operations.append(
                    (self._start_map(input_tmp_table, output_tmp_table, snapshot_path, cell_tag, master_binary_path),
                     output_tmp_table,
                     snapshot_path,
                     cell_tag))

        if self._custom_snapshot_validation:
            if len(operations) != len(self._cell_tags):
                raise RuntimeError("Custom snapshot validation operation count mismatch (Expected: {}, Actual:{})".format(len(self._cell_tags), len(operations)))

        if not operations:
            logger.info("Nothing to validate.")
            return

        logger.info("Waiting for master snapshot validation operations to finish.")
        self._monitor_operations(operations)

    def run(self):
        if self._cypress_lock_path is not None:
            self._yt_client.create("map_node", self._cypress_lock_path, ignore_existing=True)

        with self._yt_client.Transaction(
            timeout=self._transaction_timeout,
            ping_period=self._transaction_ping_period,
        ) as tx:
            logger.info("Started transaction '{}'".format(tx.transaction_id))
            self.guarded_run()

    def __init__(self, **kwargs):
        for key in kwargs:
            new_key = "_" + key
            assert not hasattr(self, new_key)
            setattr(self, new_key, kwargs[key])


def _parse_arguments():
    parser = argparse.ArgumentParser(description="Validate master snapshots from Cypress.")

    parser.add_argument("--proxy")
    parser.add_argument("--token-env-variable")

    # Paths
    parser.add_argument("--cypress-root-path", default="//sys/admin/snapshots")
    parser.add_argument("--snapshots-cypress-subpath", default="snapshots")
    parser.add_argument("--results-cypress-subpath", default="results")
    parser.add_argument("--master-binary-subpath-prefix", default="master_binary")
    parser.add_argument("--cypress-lock-path", required=False)

    # Operation settings
    parser.add_argument("--job-cpu-limit", type=int, default=1)
    parser.add_argument("--memory-limit-gbs", type=float, default=3)  # Most small clusters use 3 GiB
    parser.add_argument("--job-time-limit", type=int, default=90 * 60)  # 90m
    parser.add_argument("--validation-timeout", type=int, default=75 * 60)  # 75m
    parser.add_argument("--scheduler-pool", type=str, default=None)
    parser.add_argument("--max-failed-jobs", type=int, default=3)
    parser.add_argument("--transaction-timeout", type=int, default=600 * 1000)  # 10m
    parser.add_argument("--transaction-ping-period", type=int, default=30 * 1000)  # 30s
    parser.add_argument("--wait-lock-for", type=int, default=60 * 1000)
    parser.add_argument("--sleep-between-checks", type=int, default=60)  # 1m
    parser.add_argument("--total-validation-timeout", type=int, default=150 * 60)  # 2.5h
    parser.add_argument("--use-porto-memory-tracking", type=bool, default=False)

    # Custom master versions
    parser.add_argument("--custom-master-version", type=str, default=None)
    parser.add_argument("--custom-master-skynet-id", type=str, default=None)
    parser.add_argument("--custom-master-path", type=str, default=None)
    parser.add_argument("--validate-via-trunk", action="store_true")

    # Misc
    parser.add_argument("--number-of-snapshots-to-validate-per-cell", type=int, default=2)
    parser.add_argument("--number-of-snapshots-to-consider", type=int, default=30)
    parser.add_argument("--abort-on-alert", action="store_true")

    return parser.parse_args()


def _prepare_snapshots_validator(yt_client, args):
    if not yt_client.exists(args.cypress_root_path):
        raise RuntimeError("Invalid cypress root")

    use_custom_yt_version = (
        args.custom_master_version is not None or
        args.custom_master_skynet_id is not None or
        args.custom_master_path is not None)

    master_binary_upload_is_needed = False
    meta_path = cypress_helpers.join_path(args.cypress_root_path, "meta")
    master_binary_path = cypress_helpers.join_path(meta_path, args.master_binary_subpath_prefix)

    if args.validate_via_trunk:
        if use_custom_yt_version:
            raise RuntimeError("Option validate-via-trunk can not be used with a custom master binary")

        master_binary_upload_is_needed = binary_janitor.check_trunk_binary_is_needed(yt_client, meta_path)

    primary_cell_tag = str(yt_client.get("//sys/@primary_cell_tag"))
    secondary_cell_tags = list(map(str, yt_client.get("//sys/@registered_master_cell_tags")))
    cell_tags = [primary_cell_tag] + secondary_cell_tags

    for cell_tag in cell_tags:
        master_snapshots_path = cypress_helpers.join_path(args.cypress_root_path, cell_tag, args.snapshots_cypress_subpath)
        results_path = cypress_helpers.join_path(args.cypress_root_path, cell_tag, args.results_cypress_subpath)
        if not yt_client.exists(master_snapshots_path):
            raise RuntimeError("Snapshots path {} does not exist".format(master_snapshots_path))
        if not yt_client.exists(results_path):
            yt_client.create("map_node", results_path)

    master_configs = {}
    master_configs[primary_cell_tag] = master_config_helpers.get_patched_primary(yt_client)
    secondary_master_configs = master_config_helpers.get_patched_secondary(yt_client)
    if sorted(secondary_master_configs.keys()) != sorted(secondary_cell_tags):
        raise RuntimeError("Unexpectedly list of secondary cell tags differs")
    master_configs.update(secondary_master_configs)

    if use_custom_yt_version or master_binary_upload_is_needed:
        from yt.admin.snapshot_processing.helpers import master_binary_fetcher_and_uploader

        master_binary_path = master_binary_fetcher_and_uploader.fetch_and_upload(
            yt_client=yt_client,
            version=args.custom_master_version,
            skynet_id=args.custom_master_skynet_id,
            master_binary_path=args.custom_master_path,
            master_binary_cypress_dir=meta_path,
            master_binary_cypress_basename=args.master_binary_subpath_prefix,
            validate_via_trunk=args.validate_via_trunk,
            token_env_variable=args.token_env_variable)

    if args.validate_via_trunk and not master_binary_upload_is_needed:
        master_binary_path, is_trunk_binary_present = binary_janitor.get_trunk_binary_cypress_path(yt_client, meta_path)
        if is_trunk_binary_present is False:
            raise RuntimeError("Trunk was marked as uploaded, but the binary was not found")

    master_snapshots_validator = MasterSnapshotsValidator(
        yt_client=yt_client,
        cell_tags=cell_tags,
        cypress_root_path=args.cypress_root_path,
        snapshots_cypress_subpath=args.snapshots_cypress_subpath,
        results_cypress_subpath=args.results_cypress_subpath,
        meta_path=meta_path,
        master_binary_path=master_binary_path,
        master_configs=master_configs,
        job_cpu_limit=args.job_cpu_limit,
        memory_limit_gbs=args.memory_limit_gbs,
        job_time_limit=args.job_time_limit,
        validation_timeout=args.validation_timeout,
        scheduler_pool=args.scheduler_pool,
        max_failed_jobs=args.max_failed_jobs,
        transaction_timeout=args.transaction_timeout,
        transaction_ping_period=args.transaction_ping_period,
        number_of_snapshots_to_validate_per_cell=args.number_of_snapshots_to_validate_per_cell,
        number_of_snapshots_to_consider=args.number_of_snapshots_to_consider,
        sleep_between_checks=args.sleep_between_checks,
        total_validation_timeout=args.total_validation_timeout,
        use_porto_memory_tracking=args.use_porto_memory_tracking,
        custom_snapshot_validation=use_custom_yt_version or args.validate_via_trunk,
        abort_on_alert=args.abort_on_alert,
        cypress_lock_path=args.cypress_lock_path)

    return master_snapshots_validator


def run():
    args = _parse_arguments()

    yt_client = client_helpers.create_client(args.proxy, args.token_env_variable)

    logger.set_formatter(logging.Formatter('%(asctime)-15s\t{}\t%(message)s'.format(yt_client.config["proxy"]["url"])))

    try:
        logger.info("Starting master snapshots validation")

        master_snapshots_validator = _prepare_snapshots_validator(yt_client, args)
        master_snapshots_validator.run()
    finally:
        # This is done to make sure we don't leave random binary lying around if the process fails.
        if args.validate_via_trunk:
            binary_dir = binary_janitor.get_local_master_trunk_binary_dir()
            if os.path.exists(binary_dir):
                logger.info("Clearing {}".format(binary_dir))
                shutil.rmtree(binary_dir)

    logger.info("Finished master snapshots validation")
