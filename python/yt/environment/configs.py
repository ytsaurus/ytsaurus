import yt.yson as yson
import simplejson as json

"""This module provides default ytserver configs"""

def get_logging_pattern():
    return yson.loads(
"""
{
    rules = [
        {
            min_level = info;
            writers = [ info ];
        };
        {
            min_level = debug;
            writers = [ debug ];
        };
        {
            min_level = error;
            writers = [ stderr ];
        };
    ];
    writers = {
        stderr = {
            type = stderr;
        };
        info = {
            type = file;
            file_name = "{path}/{name}.log";
        };
        debug = {
            type = file;
            file_name = "{path}/{name}.debug.log";
        };
    };
}
""")

def get_tracing_config():
    return {}

def get_master_config():
    return yson.loads(
"""
{
    enable_provision_lock = false;

    master = {
        addresses = [ ];
        cell_tag = 0;
        cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
    };

    timestamp_provider = {
        addresses = [ ];
        soft_backoff_time = 100;
        hard_backoff_time = 100;
    };

    changelogs = {
        path = "";
    };

    snapshots = {
        path = "";
    };

    cell_directory = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
    };

    transaction_manager = {
        default_transaction_timeout = 300000;
    };

    chunk_manager = {
        chunk_refresh_delay = 300;
        chunk_refresh_period = 10;
        chunk_properties_update_period = 10;
    };

    cypress_manager = {
        statistics_flush_period = 10;
    };

    security_manager = {
        statistics_flush_period = 10;
        request_rate_smoothing_period = 60000;
    };

    node_tracker = {
        online_node_timeout = 1000;
    };

    object_manager = {
        gc_sweep_period = 10;
    };

    hive_manager = {
        ping_period = 1000;
        rpc_timeout = 1000;
    };

    logging = { };

    tracing = { };
}
""")

def get_scheduler_config():
    return yson.loads(
"""
{
    cluster_connection = {
        master = {
            addresses = [ ];
            cell_tag = 0;
            cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
            rpc_timeout = 5000;
        };

        master_cache = {
            addresses = [ ];
            cell_tag = 0;
            cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
            soft_backoff_time = 100;
            hard_backoff_time = 100;
            rpc_timeout = 5000;
        };

        timestamp_provider = {
            addresses = [ ];
            soft_backoff_time = 100;
            hard_backoff_time = 100;
        };

        transaction_manager = {
            ping_period = 500;
        };
    };

    scheduler = {
        strategy = fair_share;
        max_failed_job_count = 10;
        snapshot_period = 100000000;
        connect_retry_period = 2000;
        lock_transaction_timeout = 2000;
        transactions_refresh_period = 500;
        operations_update_period = 500;
        watchers_update_period = 500;
        connect_grace_delay = 0;
        environment = {
             PYTHONUSERBASE = "/tmp"
        };

        enable_snapshot_loading = true;
        snapshot_timeout = 1000;
    };

    transaction_manager = {
        ping_period = 500;
    };

    logging = { };

    tracing = { };
}
""")

def get_node_config():
    return yson.loads(
"""
{
    orchid_cache_expiration_time = 0;

    cluster_connection = {
        master = {
            addresses = [ ];
            cell_tag = 0;
            cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
            rpc_timeout = 5000;
        };

        master_cache = {
            addresses = [ ];
            cell_tag = 0;
            cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
            soft_backoff_time = 100;
            hard_backoff_time = 100;
            rpc_timeout = 5000;
        };

        timestamp_provider = {
            addresses = [ ];
            soft_backoff_time = 100;
            hard_backoff_time = 100;
        };

        transaction_manager = {
            ping_period = 500;
        };
    };

    data_node = {
        cache_location = {
            path = "";
        };
        store_locations = [];
        multiplexed_changelog = {
            path = "";
        };
        incremental_heartbeat_period = 100;
    };

    exec_agent = {
        scheduler_connector = {
            heartbeat_period = 100;
        };

        environment_manager = {
            environments = {
                default = {
                    type = unsafe;
                };
            };
        };

        job_controller = {
            resource_limits = {
                memory = 8000000000;
                user_slots = 1;
            };
        };

        slot_manager = {
            enable_cgroups = false;
            path = "";
        };

        job_proxy_logging = {
            rules = [
                {
                    min_level = info;
                    writers = [ info ];
                };
                {
                    min_level = debug;
                    writers = [ debug ];
                };
            ];
            writers = {
                info = {
                    type = file;
                    file_name = "{path}/{name}.log";
                };
                debug = {
                    type = file;
                    file_name = "{path}/{name}.debug.log";
                };
            }
        };

        job_proxy_tracing = {
        };
    };

    tablet_node = {
        snapshots = {
            temp_path = "";
        };
    };

    query_agent = {
    };

    tracing = { };

    logging = {
        rules = [
            {
                min_level = info;
                writers = [ info ];
            };
            {
                min_level = debug;
                writers = [ debug ];
            };
        ];
        writers = {
            info = {
                type = file;
                file_name = "{path}/{name}.log";
            };
            debug = {
                type = file;
                file_name = "{path}/{name}.debug.log";
            };
        }
    };
}
""")

def get_driver_config():
    return yson.loads(
"""
{
    master = {
        addresses = [ ];
        cell_tag = 0;
        cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
        rpc_timeout = 5000;
    };

    master_cache = {
        addresses = [ ];
        cell_tag = 0;
        cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
        soft_backoff_time = 100;
        hard_backoff_time = 100;
        rpc_timeout = 5000;
    };

    timestamp_provider = {
        addresses = [ ];
        soft_backoff_time = 100;
        hard_backoff_time = 100;
    };

    transaction_manager = {
        ping_period = 500;
    };

    format_defaults = {
        structured = <
            format = text;
        > yson;
        tabular = <
            format = text;
        > yson
    };
}
""")

def get_console_driver_config():
    return yson.loads(
"""
{
    driver = { };
    logging = { };
    tracing = { };
}
""")

def get_proxy_config():
    return json.loads(
"""
{
    "port" : -1,
    "log_port" : -1,
    "address" : "localhost",
    "number_of_workers" : 1,
    "memory_limit" : 33554432,
    "thread_limit" : 64,
    "spare_threads" : 4,

    "neighbours" : [  ],

    "logging" : {
        "level": "debug",
        "silent": false,
        "colorize": false,
        "timestamp": true,
        "json": true,
        "filename" : "/dev/null"
    },

    "authentication" : {
        "enable" : false
    },

    "coordination" : {
        "enable" : false
    },

    "proxy" : {
        "driver" : { },
        "logging" : { },
        "tracing" : { }
    }
}
""")
