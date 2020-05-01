from yt_commands import *

import pytest

import subprocess
import sys
import os.path

import yt.packages.requests as requests

from yt_env_setup import YTEnvSetup

from yt.environment.helpers import OpenPortIterator

from distutils.spawn import find_executable

if arcadia_interop.yatest_common is None:
    TEST_DIR = os.path.join(os.path.dirname(__file__))

    YT_LOG_TAILER_BINARY = os.environ.get("YT_LOG_TAILER_BINARY")
    if YT_LOG_TAILER_BINARY is None:
        YT_LOG_TAILER_BINARY = find_executable("ytserver-log-tailer")

    YT_DUMMY_LOGGER_BINARY = os.environ.get("YT_DUMMY_LOGGER_BINARY")
    if YT_DUMMY_LOGGER_BINARY is None:
        YT_DUMMY_LOGGER_BINARY = find_executable("dummy_logger")
else:
    test_dir = os.environ.get("YT_ROOT") + "/yt/tests/integration/tests"
    TEST_DIR = arcadia_interop.yatest_common.source_path(test_dir)
    assert os.path.exists(TEST_DIR)

    YT_LOG_TAILER_BINARY = arcadia_interop.search_binary_path("ytserver-log-tailer")
    YT_DUMMY_LOGGER_BINARY = arcadia_interop.search_binary_path("dummy_logger")

#################################################################


class TestLogTailer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def _read_local_config_file(self, name):
        return open(os.path.join(TEST_DIR, "test_clickhouse", name)).read()

    def setup(self):
        if YT_LOG_TAILER_BINARY is None:
            pytest.skip("This test requires log_tailer binary being built")
        if YT_DUMMY_LOGGER_BINARY is None:
            pytest.skip("This test requires dummy_logger binary being built")

    @authors("gritukan")
    def test_log_rotation(self):
        log_tailer_config = yson.loads(self._read_local_config_file("log_tailer_config.yson"))
        log_path = \
            os.path.join(self.path_to_run,
            "logs",
            "dummy_logger",
            "log")

        log_tables = ["//sys/clickhouse/logs/log1", "//sys/clickhouse/logs/log2"]

        log_tailer_config["log_tailer"]["log_files"][0]["path"] = log_path
        log_tailer_config["log_tailer"]["log_files"][0]["table_paths"] = log_tables

        log_tailer_config["logging"]["writers"]["debug"]["file_name"] = \
            os.path.join(self.path_to_run,
            "logs",
            "dummy_logger",
            "log_tailer.debug.log")
        log_tailer_config["cluster_connection"] = self.__class__.Env.configs["driver"]

        os.mkdir(os.path.join(self.path_to_run, "logs", "dummy_logger"))
        log_tailer_config_file = \
            os.path.join(self.path_to_run,
            "logs",
            "dummy_logger",
            "log_tailer_config.yson")

        with open(log_tailer_config_file, "w") as config:
            config.write(yson.dumps(log_tailer_config, yson_format="pretty"))

        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        create("map_node", "//sys/clickhouse")
        create("map_node", "//sys/clickhouse/logs")

        create("table", "//sys/clickhouse/logs/log1", attributes={
                "dynamic": True,
                "schema": [
                    {"name": "job_id_shard", "type": "uint64", "expression": "farm_hash(job_id) % 123", "sort_order": "ascending"},
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
            })

        create("table", "//sys/clickhouse/logs/log2", attributes={
                "dynamic": True,
                "schema": [
                    {"name": "trace_id_hash", "type": "uint64", "expression": "farm_hash(trace_id)", "sort_order": "ascending"},
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
            })

        for log_table in log_tables:
            sync_mount_table(log_table)

        create_user("yt-log-tailer")
        add_member("yt-log-tailer", "superusers")

        port_iterator = OpenPortIterator(
            port_locks_path=self.Env.port_locks_path,
            local_port_range=self.Env.local_port_range)
        log_tailer_monitoring_port = next(port_iterator)

        dummy_logger = subprocess.Popen([YT_DUMMY_LOGGER_BINARY, log_path, "5", "1000", "2000"])
        log_tailer = subprocess.Popen([
            YT_LOG_TAILER_BINARY,
            str(dummy_logger.pid),
            "--config",
            log_tailer_config_file,
            "--monitoring-port",
            str(log_tailer_monitoring_port)])

        def cleanup():
            # NB(gritukan): some of the processes are already terminated.
            # Calling `terminate` on them will result in OSError.
            try:
                dummy_logger.terminate()
            except:
                pass

            try:
                log_tailer.terminate()
            except:
                pass

            for log_table in log_tables:
                remove(log_table)

        try:
            def check_rows_written_profiling():
                try:
                    r = requests.get(url="http://localhost:{}/orchid/profiling/log_tailer/rows_written".format(log_tailer_monitoring_port))
                    rsp = r.json()
                    if len(rsp) == 0:
                        return False
                    if "value" not in rsp[-1]:
                        return False
                    return rsp[-1]["value"] == 1000
                except:
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
        except:
            cleanup()
            raise
