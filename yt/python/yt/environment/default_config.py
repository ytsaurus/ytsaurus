import yt.yson as yson


# TODO(babenko): drop settings mirrored in get_dynamic_master_config below
def get_master_config():
    return yson.loads(b"""
{
    enable_provision_lock = %false;
    use_new_hydra = %true;

    timestamp_provider = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
        update_period = 500;
    };

    changelogs = {
        flush_period = 10;
        io_engine = {
            enable_sync = %false;
        }
    };

    cell_directory = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
    };

    object_service = {
        enable_local_read_executor = %true;
        enable_local_read_busy_wait = %false;
    };

    timestamp_manager = {
        commit_advance = 2000;
        request_backoff_time = 100;
        calibration_period = 200;
    };

    hive_manager = {
        ping_period = 1000;
        idle_post_period = 1000;
    };

    cell_directory_synchronizer = {
        sync_period = 500;
        sync_period_splay = 100;
    };

    hydra_manager = {
        snapshot_background_thread_count = 4;
        leader_sync_delay = 0;
        minimize_commit_latency = %true;
    };

    world_initializer = {
        init_retry_period = 100;
        update_period = 5000;
    };

    expose_testing_facilities = %true
}
""")


def get_dynamic_master_config():
    return yson.loads(b"""
{
    chunk_manager = {
        chunk_refresh_delay = 300;
        chunk_refresh_period = 200;
        chunk_seal_backoff_time = 1000;
        removal_job_schedule_delay = 0;
        replicator_enabled_check_period = 1000;
        enable_chunk_schemas = %true;
    };

    node_tracker = {
        master_cache_manager = {
            update_period = 1000;
        };
        timestamp_provider_manager = {
            update_period = 1000;
        };
        full_node_states_gossip_period = 1000;
        node_statistics_gossip_period = 1000;
        total_node_statistics_update_period = 1000;
        enable_node_cpu_statistics = %true;
        enable_real_chunk_locations = %true;
        forbid_maintenance_attribute_writes = %false;
        node_disposal_tick_period = 100;
    };

    object_manager = {
        gc_sweep_period = 10;
    };

    object_service = {
        enable_local_read_executor = %true;
    };

    security_manager = {
        account_statistics_gossip_period = 200;
        request_rate_smoothing_period = 60000;
        account_master_memory_usage_update_period = 500;
        enable_delayed_membership_closure_recomputation = %false;
    };

    cypress_manager = {
        statistics_flush_period = 200;
        expiration_check_period = 200;
        expiration_backoff_time = 200;
        scion_removal_period = 1000;
    };

    multicell_manager = {
        cell_statistics_gossip_period = 200;
    };

    tablet_manager = {
        cell_scan_period = 100;
        accumulate_preload_pending_store_count_correctly = %true;
        increase_upload_replication_factor = %true;

        replicated_table_tracker = {
            check_period = 100;
            cluster_directory_synchronizer = {
                sync_period = 500;
            };
        };

        tablet_balancer = {
            enable_tablet_balancer = %false;
        };

        tablet_cell_decommissioner = {
            decommission_check_period = 100;
            orphans_check_period = 100;
        };

        multicell_gossip = {
            table_statistics_gossip_period = 100;
            tablet_cell_statistics_gossip_period = 100;
            tablet_cell_status_full_gossip_period = 100;
            tablet_cell_status_incremental_gossip_period = 100;
            bundle_resource_usage_gossip_period = 100;
        };

        enable_bulk_insert = %true;
        enable_hunks = %true;
        enable_aggressive_tablet_statistics_validation = %true;
        forbid_arbitrary_data_versions_in_retention_config = %true;
    };

    sequoia_manager = {};

    incumbent_manager = {
        scheduler = {
            incumbents = {
                cell_janitor = {
                    use_followers = %true;
                };
                chunk_replicator = {
                    use_followers = %true;
                    weight = 1000000;
                };
            };
        };
    };
}
""")


def get_scheduler_config():
    return yson.loads(b"""
{
    cluster_connection = {
    };

    response_keeper = {
        enable_warmup = %false;
        expiration_time = 25000;
        warmup_time = 30000;
    };

    rpc_server = {
        tracing_mode = "force";
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
        exec_node_descriptors_update_period = 100;
        static_orchid_cache_update_period = 100;
        operation_to_agent_assignment_backoff = 100;
        orchid_keys_update_period = 100;

        always_send_controller_agent_descriptors = %false;
        send_full_controller_agent_descriptors_for_jobs = %false;

        min_needed_resources_update_period = 100;

        job_revival_abort_timeout = 2000;

        validate_node_tags_period = 100;
        parse_operation_attributes_batch_size = 2;

        operations_cleaner = {
            parse_operation_attributes_batch_size = 2;
            tablet_transaction_timeout = 5000;
        };
        controller_agent_tracker = {
            heartbeat_timeout = 10000;
            enable_response_keeper = %true;
        };
        crash_on_job_heartbeat_processing_exception = %true;
    };
}
""")


def get_clock_config():
    return yson.loads(b"""
{
    timestamp_provider = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
        update_period = 500;
    };

    changelogs = {
        flush_period = 10;
        io_engine = {
            enable_sync = %false;
        };
    };

    timestamp_manager = {
        commit_advance = 2000;
        request_backoff_time = 10;
        calibration_period = 10;
    };
}
""")


def get_timestamp_provider_config():
    return yson.loads(b"""
{
    timestamp_provider = {
        soft_backoff_time = 100;
        hard_backoff_time = 100;
        update_period = 500;
    };
}
""")


def get_cell_balancer_config():
    return yson.loads(b"""
{
    cluster_connection = {
    };

    election_manager = {
        lock_path = "//sys/cell_balancers/lock";
    };

    cell_balancer = {
        tablet_manager = {
            cell_scan_period = 100;
            tablet_cell_balancer = {
                enable_verbose_logging = %true;
            };
        };
    };
}
""")


def get_controller_agent_config():
    return yson.loads(b"""
{
    cluster_connection = {
    };

    rpc_server = {
        tracing_mode = "force";
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

        update_account_resource_usage_leases_period = 100;

        running_job_statistics_update_period = 100;

        exec_nodes_update_period = 100;

        environment = {
            TMPDIR = "$(SandboxPath)";
            PYTHON_EGG_CACHE = "$(SandboxPath)/.python-eggs";
            PYTHONUSERBASE = "$(SandboxPath)/.python-site-packages";
            PYTHONPATH = "$(SandboxPath)";
            HOME = "$(SandboxPath)";
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
                enable_trace_logging = %true;
            }
        };
        sorted_merge_operation_options = {
            spec_template = {
                use_new_sorted_pool = %true;
            };
        };
        reduce_operation_options = {
            spec_template = {
                use_new_sorted_pool = %true;
            };
        };
        join_reduce_operation_options = {
            spec_template = {
                use_new_sorted_pool = %true;
            };
        };
        sort_operation_options = {
            spec_template = {
                use_new_sorted_pool = %true;
            };
        };
        map_reduce_operation_options = {
            spec_template = {
                use_new_sorted_pool = %true;
            };
        };

        enable_bulk_insert_for_everyone = %true;

        running_job_time_statistics_updates_send_period = 10;

        job_tracker = {
            logging_job_sample_size = 1000;
            duration_before_job_considered_vanished = 1000;
        };

        set_committed_attribute_via_transaction_action = %true;
    };
}
""")


def get_node_config():
    return yson.loads(b"""
{
    orchid_cache_update_period = 0;

    master_cache_service = {
        capacity = 16777216;
        rate_limit = 100;
    };

    dynamic_config_manager = {
        update_period = 500;
    };

    cluster_connection = {
    };

    data_node = {
        multiplexed_changelog = {
            flush_period = 10;
        };
        low_latency_split_changelog = {
            flush_period = 10;
        };

        incremental_heartbeat_period = 200;
        incremental_heartbeat_period_splay = 100;
        incremental_heartbeat_throttler = {
            limit = 100;
            period = 1000;
        };
        register_retry_period = 100;

        master_connector = {
            incremental_heartbeat_period = 200;
            incremental_heartbeat_period_splay = 50;
            job_heartbeat_period = 200;
            job_heartbeat_period_splay = 50;
        };

        chunk_meta_cache = {
            capacity = 1000000;
            shard_count = 1;
        };

        block_meta_cache = {
            capacity = 1000000;
            shard_count = 1;
        };

        blocks_ext_cache = {
            capacity = 1000000;
            shard_count = 1;
        };

        block_cache = {
            compressed_data = {
                capacity = 1000000;
                shard_count = 1;
            };
            uncompressed_data = {
                capacity = 1000000;
                shard_count = 1;
            };
        };

        p2p = {
            enabled = %false;
        };

        volume_manager = {
            enable_layers_cache = %false;
        };

        sync_directories_on_connect = %true;
    };

    exec_node = {
        slot_manager = {
            job_environment = {
                type = simple;
                use_short_container_names = %true;
            };
        };

        node_directory_prepare_backoff_time = 100;

        job_prepare_time_limit = 60000;

        scheduler_connector = {
            failed_heartbeat_backoff_start_time = 50;
            failed_heartbeat_backoff_max_time = 50;
            failed_heartbeat_backoff_multiplier = 1.0;
            heartbeat_period = 100;
        };

        controller_agent_connector = {
            running_job_statistics_sending_backoff = 0;
            heartbeat_period = 100;
        };

        job_proxy_heartbeat_period = 200;

        job_controller = {
            cpu_per_tablet_slot = 0.0;
            unknown_operation_jobs_removal_delay = 5000;
        };

        master_connector = {
            heartbeat_period = 100;
            heartbeat_period_splay = 30;
        };
    };

    cellar_node = {
        master_connector = {
            heartbeat_period = 100;
            heartbeat_period_splay = 30;
        };
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
            resource_limits_cache = {
                expire_after_successful_update_time = 0;
                expire_after_failed_update_time = 0;
                refresh_time = 0;
            };
        };

        hive_manager = {
            ping_period = 1000;
            idle_post_period = 1000;
        };

        master_connector = {
            heartbeat_period = 100;
            heartbeat_period_splay = 30;
        };

        versioned_chunk_meta_cache = {
            capacity = 1000000;
            shard_count = 1;
        };

        changelogs = {
            lock_transaction_timeout = 3000;
        };

        hydra_manager = {
            use_new_hydra = %true;
        };

        table_config_manager = {
            update_period = 100;
        };
    };

    query_agent = {
        pool_weight_cache = {
            expire_after_successful_update_time = 0;
            expire_after_failed_update_time = 0;
            refresh_time = 0;
        };
    };

    master_connector = {
        heartbeat_period = 100;
        heartbeat_period_splay = 30;
    };

    use_new_heartbeats = %true;

    enable_fair_throttler = %true;
}
""")


def get_chaos_node_config():
    return yson.loads(b"""
{
    orchid_cache_update_period = 0;

    dynamic_config_manager = {
        update_period = 500;
    };

    cluster_connection = {
    };

    flavors = [
        "chaos";
    ];

    data_node = {
        incremental_heartbeat_period = 200;
        incremental_heartbeat_period_splay = 50;
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
    };

    exec_node = {
        job_controller = {
            resource_limits = {
                user_slots = 0;
                cpu = 0;
            };
        };

        node_directory_prepare_backoff_time = 100;
    };

    tablet_node = {
        resource_limits ={
            slots = 0;
        };

        versioned_chunk_meta_cache = {
            capacity = 0;
        };
    };

    chaos_node = {
        chaos_manager = {
            chaos_cell_synchronizer = {
                sync_period = 100;
            };
            replication_card_observer = {
                observation_period = 100;
            };
            era_commencing_period = 100;
        };
        snapshot_store_read_pool_size = 1;
        replicated_table_tracker_config_fetcher = {
            update_period = 100;
            enable_unrecognized_options_alert = %true;
        };
    };

    cellar_node = {
        master_connector = {
            heartbeat_period = 100;
            heartbeat_period_splay = 30;
        };

        cellar_manager = {
            cellars = {
                chaos = {
                    size = 4;
                    occupant = {
                        changelogs = {
                            lock_transaction_timeout = 3000;
                        };
                        use_new_hydra = %true;
                        hive_manager = {
                            ping_period = 1000;
                            idle_post_period = 1000;
                        };
                    };
                };
            };
        };
    };

    master_connector = {
        heartbeat_period = 100;
        heartbeat_period_splay = 30;
    };

    use_new_heartbeats = %true;
}
""")


def get_master_cache_config():
    return yson.loads(b"""
{
    cluster_connection = {
    };
}
""")


def get_dynamic_node_config():
    return yson.loads(b"""
{
    "%true" = {
        config_annotation = "default";
        exec_node = {
            abort_on_jobs_disabled = %true;

            job_controller = {
                operation_infos_request_period = 1000;
            };

            controller_agent_connector = {
                total_confirmation_period = 5000;
                send_waiting_jobs = %true;
                use_job_tracker_service_to_settle_jobs = %true;
            };
        };
    };
}
""")


def get_driver_config():
    return yson.loads(b"""
{
    format_defaults = {
        structured = <
            format = text;
        > yson;
        tabular = <
            format = text;
        > yson
    };

    force_tracing = %true;

    proxy_discovery_cache = {
        refresh_time = 1000;
        expire_after_successful_update_time = 1000;
        expire_after_failed_update_time = 1000;
    };

    enable_internal_commands = %true;
}
""")


def get_proxy_config():
    return yson.loads(b"""
{
    port = -1;

    api = {
        cors = {
            disable_cors_check = %true;
        };
    };

    auth = {
        enable_authentication = %false;
        blackbox_service = {};
        cypress_token_authenticator = {};
        blackbox_token_authenticator = {
            scope = "";
            enable_scope_check = %false;
        };
        blackbox_cookie_authenticator = {};
    };

    coordinator = {
        enable = %true;
        announce = %true;
        heartbeat_interval = 500;
        show_ports = %true;
    };

    dynamic_config_manager = {
        update_period = 500;
    };
}
""")


def get_watcher_config():
    return {
        "logs_rotate_max_part_count": 100,
        "logs_rotate_size": "1M",
        "logs_rotate_interval": 600,
    }


def get_queue_agent_config():
    return yson.loads(b"""
{
    dynamic_config_manager = {
        update_period = 100;
    };
}
""")


def get_tablet_balancer_config():
    return yson.loads(b"""
{
    dynamic_config_manager = {
        update_period = 100;
    };
}
""")


def get_cypress_proxy_config():
    return yson.loads(b"""
{
    dynamic_config_manager = {
        update_period = 100;
    };
}
""")
