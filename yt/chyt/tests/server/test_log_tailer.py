from base import Clique, ClickHouseTestBase, get_current_test_name

from yt_commands import (authors, create_user, sync_mount_table, add_member, remove, create, sync_create_cells,
                         create_tablet_cell_bundle, freeze_table, wait_for_tablet_state, write_file, read_table,
                         write_table)

from yt.environment import arcadia_interop

import copy
import subprocess
import os.path

import yatest.common.network
import yt.packages.requests as requests

from yt_env_setup import YTEnvSetup

from yt.clickhouse.test_helpers import get_log_tailer_config, get_host_paths

from yt.common import wait

import yt.yson as yson

HOST_PATHS = get_host_paths(arcadia_interop, ["dummy-logger", "ytserver-log-tailer"])

#################################################################


class TestLogTailer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("gritukan")
    def test_log_rotation(self):
        log_tailer_config = get_log_tailer_config(mock_tvm_id=self.Env.configs["driver"].get("tvm_id"), inject_secret=True)
        log_path = os.path.join(self.path_to_run, "logs", "dummy_logger", "log")

        log_tailer_config["log_tailer"]["log_files"] = [
            {"path": log_path, "tables": [{"path": "//sys/log1"}, {"path": "//sys/log2", "require_trace_id": True}]}
        ]

        log_tailer_config["log_tailer"]["log_files"] = log_tailer_config["log_tailer"]["log_files"][:1]

        log_tailer_config["logging"]["writers"]["debug"]["file_name"] = os.path.join(
            self.path_to_run, "logs", "dummy_logger", "log_tailer.debug.log"
        )
        log_tailer_config["cluster_connection"] = copy.deepcopy(self.Env.configs["driver"])
        if "tvm_service" in log_tailer_config["cluster_connection"]:
            log_tailer_config["cluster_connection"].pop("tvm_service")

        os.mkdir(os.path.join(self.path_to_run, "logs", "dummy_logger"))
        log_tailer_config_file = os.path.join(self.path_to_run, "logs", "dummy_logger", "log_tailer_config.yson")

        with open(log_tailer_config_file, "wb") as config:
            config.write(yson.dumps(log_tailer_config, yson_format="pretty"))

        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        create(
            "table",
            "//sys/log1",
            attributes={
                "dynamic": True,
                "schema": [
                    {
                        "name": "job_id_shard",
                        "type": "uint64",
                        "expression": "farm_hash(job_id) % 123",
                        "sort_order": "ascending",
                    },
                    {"name": "timestamp", "type": "string", "sort_order": "ascending"},
                    {"name": "increment", "type": "uint64", "sort_order": "ascending"},
                    {"name": "job_id", "type": "string", "sort_order": "ascending"},
                    {"name": "category", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "log_level", "type": "string"},
                    {"name": "thread_id", "type": "string"},
                    {"name": "fiber_id", "type": "string"},
                    {"name": "trace_id", "type": "string"},
                    {"name": "operation_id", "type": "string"},
                ],
                "tablet_cell_bundle": "sys",
                "atomicity": "none",
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )

        create(
            "table",
            "//sys/log2",
            attributes={
                "dynamic": True,
                "schema": [
                    {
                        "name": "trace_id_hash",
                        "type": "uint64",
                        "expression": "farm_hash(trace_id)",
                        "sort_order": "ascending",
                    },
                    {"name": "trace_id", "type": "string", "sort_order": "ascending"},
                    {"name": "timestamp", "type": "string", "sort_order": "ascending"},
                    {"name": "job_id", "type": "string", "sort_order": "ascending"},
                    {"name": "increment", "type": "uint64", "sort_order": "ascending"},
                    {"name": "category", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "log_level", "type": "string"},
                    {"name": "thread_id", "type": "string"},
                    {"name": "fiber_id", "type": "string"},
                    {"name": "operation_id", "type": "string"},
                ],
                "tablet_cell_bundle": "sys",
                "atomicity": "none",
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )

        log_tables = ["//sys/log1", "//sys/log2"]

        for log_table in log_tables:
            sync_mount_table(log_table)

        create_user("yt-log-tailer")
        add_member("yt-log-tailer", "superusers")

        log_tailer_monitoring_port = yatest.common.network.PortManager().get_port()

        dummy_logger = subprocess.Popen([HOST_PATHS["dummy-logger"], log_path, "5", "1000", "2000"])
        log_tailer = subprocess.Popen(
            [
                HOST_PATHS["ytserver-log-tailer"],
                str(dummy_logger.pid),
                "--config",
                log_tailer_config_file,
                "--monitoring-port",
                str(log_tailer_monitoring_port),
            ]
        )

        def cleanup():
            # NB(gritukan): some of the processes are already terminated.
            # Calling `terminate` on them will result in OSError.
            try:
                dummy_logger.terminate()
            except OSError:
                pass

            try:
                log_tailer.terminate()
            except OSError:
                pass

            for log_table in log_tables:
                remove(log_table)

        try:

            def check_rows_written_profiling():
                try:
                    url = f"http://localhost:{log_tailer_monitoring_port}/orchid/sensors"
                    params = {
                        "verb": "get",
                        # "name" parameter is a yson-string, so we need to add extra ".
                        "name": '"yt/log_tailer/rows_written"',
                    }
                    rsp = requests.get(url, params=params)
                    return rsp.json()
                except:  # noqa
                    return False

            wait(check_rows_written_profiling)

            os.wait()

            wait(lambda: dummy_logger.poll() is not None)
            wait(lambda: log_tailer.poll() is not None)

            for log_table in log_tables:
                freeze_table(log_table)
                wait_for_tablet_state(log_table, "frozen")

                rows = read_table(log_table)
                assert len(rows) == 1000
            cleanup()
        except:  # noqa
            cleanup()
            raise


class TestClickHouseWithLogTailer(ClickHouseTestBase):
    @authors("gritukan")
    def test_log_tailer(self):
        # Prepare log tailer config and upload it to Cypress.
        log_tailer_config = get_log_tailer_config(mock_tvm_id=self.Env.configs["driver"].get("tvm_id"))
        log_file_path = os.path.join(
            self.path_to_run, "logs", "clickhouse-{}-0".format(get_current_test_name()), "clickhouse-{}.debug.log".format(0)
        )

        log_table = "//sys/clickhouse/logs/log"
        log_tailer_config["log_tailer"]["log_files"] = [{"path": log_file_path, "tables": [{"path": log_table}]}]

        log_tailer_config["logging"]["writers"]["debug"]["file_name"] = os.path.join(
            self.path_to_run, "logs", "clickhouse-{}-0".format(get_current_test_name()), "log_tailer-{}.debug.log".format(0)
        )
        log_tailer_config["cluster_connection"] = self.Env.configs["driver"]
        if "tvm_service" in log_tailer_config["cluster_connection"]:
            log_tailer_config["cluster_connection"].pop("tvm_service")
        log_tailer_config_filename = "//sys/clickhouse/log_tailer_config.yson"
        create("file", log_tailer_config_filename)

        log_tailer_config_str = yson.dumps(log_tailer_config, yson_format="pretty")
        assert log_tailer_config_str.find(b"TestSecret") == -1

        write_file(log_tailer_config_filename, log_tailer_config_str)

        # Create dynamic tables for logs.
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        create("map_node", "//sys/clickhouse/logs")

        create(
            "table",
            log_table,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "timestamp", "type": "string", "sort_order": "ascending"},
                    {"name": "increment", "type": "uint64", "sort_order": "ascending"},
                    {"name": "category", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "log_level", "type": "string"},
                    {"name": "thread_id", "type": "string"},
                    {"name": "fiber_id", "type": "string"},
                    {"name": "trace_id", "type": "string"},
                    {"name": "job_id", "type": "string"},
                    {"name": "operation_id", "type": "string"},
                ],
                "tablet_cell_bundle": "sys",
                "atomicity": "none",
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )

        sync_mount_table(log_table)

        # Create log tailer user and make it superuser.
        create_user("yt-log-tailer")
        add_member("yt-log-tailer", "superusers")

        # Create clique with log tailer enabled.
        with Clique(
                instance_count=1,
                cypress_ytserver_log_tailer_config_path=log_tailer_config_filename,
                host_ytserver_log_tailer_path=HOST_PATHS["ytserver-log-tailer"],
                enable_log_tailer=True,
        ) as clique:

            # Make some queries.
            create(
                "table",
                "//tmp/t",
                attributes={
                    "schema": [
                        {"name": "key1", "type": "string"},
                        {"name": "key2", "type": "string"},
                        {"name": "value", "type": "int64"},
                    ]
                },
            )
            for i in range(5):
                write_table(
                    "<append=%true>//tmp/t",
                    [{"key1": "dream", "key2": "theater", "value": i * 5 + j} for j in range(5)],
                )
            total = 24 * 25 // 2

            for _ in range(10):
                result = clique.make_query('select key1, key2, sum(value) from "//tmp/t" group by key1, key2')
                assert result == [{"key1": "dream", "key2": "theater", "sum(value)": total}]

        # Freeze table to flush logs.
        freeze_table(log_table)
        wait_for_tablet_state(log_table, "frozen")

        # Check whether log was written.
        try:
            assert len(read_table(log_table)) > 0
        except:  # noqa
            remove(log_table)
            raise
        remove(log_table)
