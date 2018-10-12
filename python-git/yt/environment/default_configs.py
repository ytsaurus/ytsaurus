import yt.yson as yson
try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

"""This module provides default ytserver configs"""

def get_logging_config(enable_debug_logging=True, enable_structured_logging=False):
    config = {
        "rules": [
            {"min_level": "info", "writers": ["info"]},
            {"min_level": "error", "writers": ["stderr"]},
        ],
        "writers": {
            "stderr": {
                "type": "stderr",
            },
            "info": {
                "type": "file",
                "file_name": "{path}/{name}.log",
            }
        }
    }
    if enable_debug_logging:
        config["rules"].append({
            "min_level": "debug",
            "exclude_categories": ["Bus"],
            "writers": ["debug"],
        })
        config["writers"]["debug"] = {
            "type": "file",
            "file_name": "{path}/{name}.debug.log",
        }
    if enable_structured_logging:
        config["rules"].append({
            "min_level": "debug",
            "writers": ["json"],
            "message_format": "structured",
        })
        config["writers"]["json"] = {
            "type": "file",
            "file_name": "{path}/{name}.json.log",
            "accepted_message_format": "structured",
        }

    return yson.to_yson_type(config)

def get_master_config():
    return yson.loads(
b"""
{
    enable_provision_lock = %false;

    timestamp_provider = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
        update_period = 500;
    };

    changelogs = {
        flush_period = 10;
    };

    cell_directory = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
    };

    transaction_manager = {
        default_transaction_timeout = 300000;
    };

    timestamp_manager = {
        commit_advance = 2000;
        request_backoff_time = 10;
        calibration_period = 10;
    };

    chunk_manager = {
        chunk_refresh_delay = 300;
        chunk_refresh_period = 10;
        chunk_properties_update_period = 10;
    };

    cypress_manager = {
        statistics_flush_period = 50;
    };

    security_manager = {
        user_statistics_gossip_period = 150;
        account_statistics_gossip_period = 150;
        user_statistics_flush_period = 50;
        request_rate_smoothing_period = 60000;
    };

    node_tracker = {
        max_concurrent_node_registrations = 100;
        max_concurrent_node_unregistrations = 100;
    };

    object_manager = {
        gc_sweep_period = 10;
    };

    hive_manager = {
        ping_period = 1000;
        idle_post_period = 1000;
    };

    tablet_manager = {
        cell_scan_period = 100;

        tablet_balancer = {
            enable_tablet_balancer = %false;
        };
    };

    multicell_manager = {
        cell_statistics_gossip_period = 80;
    };

    cell_directory_synchronizer = {
        sync_period = 500;
    };
}
""")

# TODO(babenko): drop cluster_directory_synchronizer in the root
def get_scheduler_config():
    return yson.loads(
b"""
{
    cluster_connection = {
        enable_read_from_followers = %true;
        scheduler = {
            retry_backoff_time = 100;
        }
    };

    response_keeper = {
        enable_warmup = %false;
        expiration_time = 25000;
        warmup_time = 30000;
    };

    scheduler = {
        lock_transaction_timeout = 10000;
        operations_update_period = 500;
        fair_share_update_period = 500;
        watchers_update_period = 100;
        nodes_attributes_update_period = 100;
        scheduling_tag_filter_expire_timeout = 100;
        node_shard_exec_nodes_cache_update_period = 100;
        schedule_job_time_limit = 5000;

        static_orchid_cache_update_period = 100;
        orchid_keys_update_period = 100;

        min_needed_resources_update_period = 100;
    };
}
""")

def get_controller_agent_config():
    return yson.loads(
b"""
{
    node_directory_synchronizer = {
        sync_period = 100;
    };

    cluster_connection = {
        enable_read_from_followers = %true;
        scheduler = {
            retry_backoff_time = 100;
        }
    };

    controller_agent = {
        operations_update_period = 500;
        scheduling_tag_filter_expire_timeout = 100;
        safe_scheduler_online_time = 5000;

        static_orchid_cache_update_period = 300;

        controller_static_orchid_update_period = 0;

        operations_push_period = 10;
        operation_alerts_push_period = 100;
        suspicious_jobs_push_period = 100;

        config_update_period = 100;

        controller_exec_node_info_update_period = 100;

        exec_nodes_update_period = 100;

        environment = {
            PYTHONUSERBASE = "/tmp";
        };

        testing_options = {
            enable_snapshot_cycle_after_materialization = %true;
        };

        enable_snapshot_loading = %true;

        snapshot_period = 100000000;

        transactions_refresh_period = 500;

        max_archived_job_spec_count_per_operation = 10;

        operation_options = {
            spec_template = {
                max_failed_job_count = 10;
                locality_timeout = 100;
            }
        };
    };
}
""")


# TODO(babenko): drop cluster_directory_synchronizer in the root
def get_node_config(enable_debug_logging=True):
    config = yson.loads(
b"""
{
    orchid_cache_update_period = 0;

    node_directory_synchronizer = {
        sync_period = 100;
    };

    cluster_connection = {
        enable_read_from_followers = %true;

        transaction_manager = {
            default_transaction_timeout = 3000;
        };

        scheduler = {
            retry_backoff_time = 100;
        };

        enable_udf = %true;
    };

    data_node = {
        read_thread_count =  2;
        write_thread_count = 2;

        multiplexed_changelog = {
            flush_period = 10;
        };

        incremental_heartbeat_period = 200;
        register_retry_period = 100;

        block_cache = {
            compressed_data = {
                capacity = 0;
            };
            uncompressed_data = {
                capacity = 0;
            };
        };

        chunk_meta_cache = {
            capacity = 0;
        };

        sync_directories_on_connect = %true;
    };

    exec_agent = {
        slot_manager = {
            job_environment = {
                type = simple;
            };

            slot_initialization_failure_is_fatal = %true;
        };

        node_directory_prepare_backoff_time = 100;

        scheduler_connector = {
            failed_heartbeat_backoff_time = 50;
            unsuccess_heartbeat_backoff_time = 50;
            heartbeat_period = 200;
        };

        job_proxy_heartbeat_period = 200;

        job_proxy_logging = {
            rules = [
                {
                    min_level = info;
                    writers = [ info ];
                };
                {
                    min_level = debug;
                    exclude_categories = [ Bus ];
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

        job_controller = {
            total_confirmation_period = 5000;
            cpu_per_tablet_slot = 0.0;
        }
    };

    tablet_node = {
        slot_scan_period = 100;

        tablet_manager = {
            preload_backoff_time = 100;
            compaction_backoff_time = 100;
            flush_backoff_time = 100;
            replicator_soft_backoff_time = 100;
            replicator_hard_backoff_time = 100;
        };

        hive_manager = {
            ping_period = 1000;
            idle_post_period = 1000;
            rpc_timeout = 1000;
        };
    };

    logging = {
        rules = [
            {
                min_level = info;
                writers = [ info ];
            };
            {
                min_level = debug;
                writers = [ debug ];
                exclude_categories = ["Bus"];
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
    if not enable_debug_logging:
        del config["logging"]["rules"][1]
        del config["logging"]["writers"]["debug"]
        del config["exec_agent"]["job_proxy_logging"]["rules"][1]
        del config["exec_agent"]["job_proxy_logging"]["writers"]["debug"]

    return config

def get_driver_config():
    return yson.loads(
b"""
{
    enable_read_from_followers = %true;

    format_defaults = {
        structured = <
            format = text;
        > yson;
        tabular = <
            format = text;
        > yson
    };

    enable_udf = %true;
}
""")

def get_proxy_config():
    return json.loads(
"""
{
    "port" : -1,
    "address" : "::",
    "number_of_workers" : 1,
    "memory_limit" : 33554432,
    "thread_limit" : 64,
    "spare_threads" : 4,

    "neighbours" : [  ],

    "logging" : {
        "filename" : "/dev/null"
    },

    "disable_cors_check" : true,
    "authentication" : {
        "enable" : false
    },

    "coordination" : {
        "enable" : true,
        "announce" : true,
        "heartbeat_interval" : 500
    },

    "show_ports" : true,

    "static": []
}
""")

def get_ui_config():
    return """
tm.managers = {
    production: {
        url: '//transfer-manager.yt.yandex.net/api/v1',
        version: '1'
    }
};

YT.odinPath = '';

YT.clusters = {
    'ui' : {
        name: 'Local',
        proxy: %%proxy_address%%,
        oauthApplication: '',
        type: 'infrastructural',
        theme: 'grapefruit',
        description: 'Local',
        %%masters%%
    }
};
"""

def get_watcher_config():
    return {
        "logs_rotate_max_part_count": 100,
        "logs_rotate_size": "8M",
        "logs_rotate_interval": 120
    }
