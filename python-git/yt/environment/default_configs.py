import yt.yson as yson
try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

"""This module provides default ytserver configs"""

def get_logging_config(enable_debug_logging=True, enable_structured_logging=False):
    config = {
        "abort_on_alert": True,
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
                "file_name": "{path}/{name}.log.gz",
                "enable_compression": True,
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
            "file_name": "{path}/{name}.debug.log.gz",
            "enable_compression": True,
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

# TODO(babenko): drop settings mirrored in get_dynamic_master_config below
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
        success_expiration_time = 500;
        failure_expiration_time = 500;
    };
}
""")

def get_dynamic_master_config():
    return yson.loads(
b"""
{
    object_service = {
        enable_two_level_cache = %true;
    };

    chunk_manager = {
        chunk_refresh_delay = 300;
        chunk_refresh_period = 10;
        chunk_properties_update_period = 10;
    };

    node_tracker = {
        full_node_states_gossip_period = 1000;
        total_node_statistics_update_period = 1000;
    };

    object_manager = {
        gc_sweep_period = 10;
    };

    security_manager = {
        user_statistics_gossip_period = 150;
        account_statistics_gossip_period = 150;
        user_statistics_flush_period = 50;
        request_rate_smoothing_period = 60000;
        enable_delayed_membership_closure_recomputation = %false;
    };

    cypress_manager = {
        statistics_flush_period = 50;
        expiration_check_period = 100;
        expiration_backoff_time = 100;
        enable_unlock_command = %true;
    };

    multicell_manager = {
        cell_statistics_gossip_period = 80;
    };
}
""")

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

        job_revival_abort_timeout = 2000;
    };
}
""")

def get_clock_config():
    return yson.loads(
b"""
{
    timestamp_provider = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
        update_period = 500;
    };

    changelogs = {
        flush_period = 10;
    };

    timestamp_manager = {
        commit_advance = 2000;
        request_backoff_time = 10;
        calibration_period = 10;
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
        };
        node_directory_synchronizer = {
            sync_period = 100;
            success_expiration_time = 100;
            failure_expiration_time = 100;
        };
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
        snapshot_timeout = 5000;

        transactions_refresh_period = 500;

        max_archived_job_spec_count_per_operation = 10;

        operation_options = {
            spec_template = {
                max_failed_job_count = 10;
                locality_timeout = 100;
                intermediate_data_replication_factor = 1;
            }
        };
    };
}
""")

# COMPAT(babenko): drop unsuccess_heartbeat_backoff_time
def get_node_config(enable_debug_logging=True):
    config = yson.loads(
b"""
{
    orchid_cache_update_period = 0;

    master_cache_service = {
        capacity = 16777216;
    };

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

        node_directory_synchronizer = {
            sync_period = 100;
            success_expiration_time = 100;
            failure_expiration_time = 100;
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
        incremental_heartbeat_throttler_limit = 100;
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
            unsuccess_heartbeat_backoff_time  = 50;
            failed_heartbeat_backoff_start_time = 50;
            failed_heartbeat_backoff_max_time = 50;
            failed_heartbeat_backoff_multiplier = 1.0;
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
                    file_name = "{path}/{name}.log.gz";
                    enable_compression = %true;
                };
                debug = {
                    type = file;
                    file_name = "{path}/{name}.debug.log.gz";
                    enable_compression = %true;
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
            tablet_cell_decommission_check_period = 100;
        };

        security_manager = {
            table_permission_cache = {
                expire_after_successful_update_time = 0;
                expire_after_failed_update_time = 0;
                refresh_time = 0;
            };
            resource_limits_cache = {
                expire_after_successful_update_time = 0;
                expire_after_failed_update_time = 0;
                refresh_time = 0;
            };
        };

        hive_manager = {
            ping_period = 1000;
            idle_post_period = 1000;
            rpc_timeout = 1000;
        };
    };

    logging = {
        abort_on_alert = %true;
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
                file_name = "{path}/{name}.log.gz";
                enable_compression = %true;
            };
            debug = {
                type = file;
                file_name = "{path}/{name}.debug.log.gz";
                enable_compression = %true;
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
    return yson.loads(
b"""
{
    port = -1;
    address = "::";

    disable_cors_check = %true;
    auth = {
        enable_authentication = %false;
        blackbox_service = {};
        cypress_token_authenticator = {};
        blackbox_token_authenticator = {
            scope = "";
            enable_scope_check = %false;
        };
        blackbox_cookie_authenticator = {
            csrf_token = "";
        };
    };

    coordinator = {
        enable = %true;
        announce = %true;
        heartbeat_interval = 500;
        show_ports = %true;
    };
}
""")

def get_watcher_config():
    return {
        "logs_rotate_max_part_count": 1000,
        "logs_rotate_size": "1M",
        "logs_rotate_interval": 5,
    }
