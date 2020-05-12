import os.path

# Helpers in this file are used both in Python API tests and integration tests for clickhouse server and log tailer.


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
    return {
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


def get_clickhouse_server_config():
    return {
        "logging": get_logging_config(),
        "address_resolver": {"localhost_fqdn": "localhost"},
        "validate_operation_access": False,
        "user": "root",
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
    }


def get_log_tailer_config():
    return {
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
    }

