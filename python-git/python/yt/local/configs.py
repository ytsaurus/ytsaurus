import yt.yson as yson
import yt.packages.simplejson as json

"""This module provides default configs for local YT"""

def get_logging_config():
    return yson.loads(
"""
{
    rules = [
        {
            min_level = info;
            writers = [ info ];
        };
        {
            min_level = error;
            writers = [ error ];
        };
    ];
    writers = {
        error = {
            type = file;
            file_name = "{path}/{name}.error.log"
        };
        info = {
            type = file;
            file_name = "{path}/{name}.log";
        };
    };
}
""")

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

    hydra_manager = {
        leader_lease_timeout = 15000;
        control_rpc_timeout = 5000;
        disable_leader_lease_grace_delay = true;
        response_keeper = {
            expiration_time = 25000;
            warmup_time = 30000;
        };
    };

    node_tracker = {
        online_node_timeout = 20000;
        registered_node_timeout = 20000;
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
        enable_read_from_followers = true;

        master = {
            addresses = [ ];
            cell_tag = 0;
            cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
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

    response_keeper = {
        expiration_time = 25000;
        warmup_time = 30000;
    };

    scheduler = {
        strategy = fair_share;
        max_failed_job_count = 10;
        snapshot_period = 100000000;
        lock_transaction_timeout = 2000;
        environment = {
             PYTHONUSERBASE = "/tmp"
        };

        enable_snapshot_loading = true;
        snapshot_timeout = 300000;
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
        enable_read_from_followers = true;

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
        };

        timestamp_provider = {
            addresses = [ ];
        };
    };

    data_node = {
        cache_locations = [];
        store_locations = [];

        cache_location = {
            path = "";
        };

        multiplexed_changelog = {
            path = "";
        };
        block_cache = {
            compressed_data = {
                capacity = 0;
            };
            uncompressed_data = {
                capacity = 0;
            };
        };
    };

    exec_agent = {
        environment_manager = {
            environments = {
                default = {
                    type = unsafe;
                };
            };
        };

        job_controller = {
            resource_limits = {
                memory = 1073741824;
                user_slots = 1;
            };
        };

        enable_cgroups = false;

        slot_manager = {
            paths = [];
            path = "";
        };

        job_proxy_logging = { };

        job_proxy_tracing = { };
    };

    resource_limits = {
        memory = 2357198848;
    };

    tablet_node = { };

    query_agent = { };

    tracing = { };

    logging = { };
}
""")

def get_driver_config():
    return yson.loads(
"""
{
    enable_read_from_followers = true;

    master = {
        addresses = [ ];
        cell_tag = 0;
        cell_id = "ffffffff-ffffffff-ffffffff-ffffffff";
        rpc_timeout = 5000;
    };

    timestamp_provider = {
        addresses = [ ];
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
    "number_of_workers" : 1,
    "memory_limit" : 33554432,
    "thread_limit" : 64,
    "spare_threads" : 4,

    "neighbours" : [  ],

    "logging" : {
        "level": "info",
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
