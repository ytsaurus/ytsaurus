from yt.common import update

import yt.yson as yson

import os.path

# Helpers in this file are used both in Python API tests and integration tests for clickhouse server and log tailer.

INIT_CLUSTER_CONFIG_FILENAME = "init_cluster_config.yson"
RUN_CONFIG_FILENAME = "run_config.yson"
STRAWBERRY_ROOT = "//sys/clickhouse/strawberry"
SPECLET_FILENAME = "speclet.yson"


# TODO(max42): join with similar helpers for regular ytserver binary discovery.
# Path may be overridden via env var like YTSERVER_CLICKHOUSE_PATH=/path/to/bin.
def get_host_paths(arcadia_interop, bins):
    result = {}
    if arcadia_interop.yatest_common is None:
        from distutils.spawn import find_executable
        get_host_path = find_executable
    else:
        get_host_path = arcadia_interop.search_binary_path

    for bin in bins:
        result[bin] = os.environ.get(bin.replace("-", "_").upper() + "_PATH") or get_host_path(bin)

    return result


def get_logging_config():
    result = {
        "rules": [
            {"min_level": "debug", "writers": ["debug"]},
            {"min_level": "info", "writers": ["info"]},
            {"min_level": "error", "writers": ["stderr"]},
        ],
        "writers": {
            "info": {"file_name": "./clickhouse-$YT_JOB_INDEX.log", "type": "file"},
            "debug": {"file_name": "./clickhouse-$YT_JOB_INDEX.debug.log", "type": "file"},
            "stderr": {"type": "stderr"},
        },
        "flush_period": 1000,
        "watch_period": 5000,
    }
    if "YT_ENABLE_TRACE_LOG" in os.environ:
        result["rules"].append({"min_level": "trace", "writers": ["trace"]})
        result["writers"]["trace"] = {"file_name": "./clickhouse-$YT_JOB_INDEX.trace.log", "type": "file"}
    return result


def get_clickhouse_server_config():
    return {
        "logging": get_logging_config(),
        "solomon_exporter": {"enable_core_profiling_compatibility": True},
        "address_resolver": {"localhost_fqdn": "localhost"},
        "validate_operation_access": False,
        "user": "root",
        # COMPAT(max42): rename to clickhouse and remove /subquery.
        "engine": {
            "settings": {
                "max_temporary_non_const_columns": 1234,
                "max_threads": 1,
                "max_distributed_connections": 1,
                "enable_optimize_predicate_expression": 0,
                "join_use_nulls": 1,
            },
            "subquery": {"min_data_weight_per_thread": 0},
        },
        "discovery": {
            "directory": "//sys/clickhouse/cliques",
            "update_period": 500,
            "transaction_timeout": 1000,
        },
        # COMPAT(max42): leave only new config format.
        "yt": {
            "user": "root",
            "security_manager": {"enable": False},
            "discovery": {"update_period": 500, "transaction_timeout": 10000},
            "subquery": {"min_data_weight_per_thread": 0},
        },
    }


def get_log_tailer_config(mock_tvm_id=None, inject_secret=False):
    config = {
        "log_tailer": {
            "log_rotation": {
                "enable": False,
                "rotation_delay": 15000,
                "log_segment_count": 100,
                "rotation_period": 900000,
            },
            "log_files": [
                {"ttl": 604800000, "path": "clickhouse.debug.log"},
                {"ttl": 604800000, "path": "clickhouse.log"},
            ],
            "log_writer_liveness_checker": {
                "enable": True,
                "liveness_check_period": 5000,
            },
        },
        "logging": get_logging_config(),
        "solomon_exporter": {"enable_core_profiling_compatibility": True},
    }
    if mock_tvm_id is not None:
        config["native_authentication_manager"] = {
            "tvm_service": {
                "enable_mock": True,
                "client_self_id": mock_tvm_id,
                "client_enable_service_ticket_fetching": True,
                "client_enable_service_ticket_checking": True,
            },
            "enable_validation": True,
        }
        if inject_secret:
            config["native_authentication_manager"]["tvm_service"]["client_self_secret"] = "TestSecret-" + str(mock_tvm_id)
    return config


def create_controller_init_cluster_config(proxy, work_dir, families=[]):
    config_filename = work_dir + "/" + INIT_CLUSTER_CONFIG_FILENAME
    with open(config_filename, "wb") as fout:
        yson.dump(
            {
                "proxy": proxy,
                "strawberry_root": STRAWBERRY_ROOT,
                "families": families,
            },
            fout
        )
    return config_filename


def create_controller_run_config(proxy, work_dir, api_port, monitoring_port):
    config_filename = work_dir + "/" + RUN_CONFIG_FILENAME
    with open(config_filename, "wb") as fout:
        yson.dump(
            {
                "location_proxies": [proxy],
                "coordination_proxy": proxy,
                "coordination_path": "//sys/clickhouse/controller/test",
                "strawberry": {
                    "root": STRAWBERRY_ROOT,
                    "pass_period": 100,
                    "controller_update_period": 500,
                    "revision_collect_period": 100,
                    "stage": "test",
                    "robot_username": "root",
                },
                "controller": {
                    "local_binaries_dir": work_dir,
                    "log_rotation_mode": "disabled",
                },
                "http_api_endpoint": ":{}".format(api_port),
                "disable_api_auth": True,
                "http_monitoring_endpoint": ":{}".format(monitoring_port),
            },
            fout)
    return config_filename


def create_controller_one_shot_run_config(proxy, work_dir):
    config_filename = work_dir + "/" + RUN_CONFIG_FILENAME
    with open(config_filename, "wb") as fout:
        yson.dump(
            {
                "proxy": proxy,
                "strawberry_root": STRAWBERRY_ROOT,
                "controller": {
                    "local_binaries_dir": work_dir,
                    "log_rotation_mode": "disabled",
                },
            },
            fout,
        )
    return config_filename


def create_clique_speclet(work_dir, speclet_patch):
    speclet = {
        "active": True,
        "pool": "chyt",
        "instance_count": 1,
        "enable_geodata": False,
        "yt_config": {
            "health_checker": {
                "queries": [],
            },
        },
        "instance_cpu": 1,
        "instance_memory": {
            "clickhouse": 2 * 1024 ** 3,
            "chunk_meta_cache": 0,
            "compressed_cache": 0,
            "uncompressed_cache": 0,
            "reader": 1024 ** 3,
            "clickhouse_watermark": 0,
            "watchdog_oom_watermark": 0,
            "watchdog_oom_window_watermark": 0,
            "footprint": 0,
        },
    }
    speclet = update(speclet, speclet_patch)
    speclet_filename = work_dir + "/" + SPECLET_FILENAME
    with open(speclet_filename, "wb") as fout:
        yson.dump(
            speclet,
            fout
        )
    return speclet_filename


def create_symlink_for_binary(host_paths, yt_work_dir, binary_name):
    symlink = yt_work_dir + "/" + binary_name
    os.symlink(host_paths[binary_name], symlink)
    return symlink


def create_symlinks_for_chyt_binaries(host_paths, yt_work_dir):
    symlinks = {}
    for binary in host_paths:
        symlinks[binary] = create_symlink_for_binary(host_paths, yt_work_dir, binary)
    return symlinks
