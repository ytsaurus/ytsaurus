import yt.yson as yson


# TODO(babenko): drop settings mirrored in get_dynamic_master_config below
# COMPAT(aleksandra-zh): enable_secondary_master_registration should be removed after compat tests
# are bumped to 25.1.
def get_master_config():
    return {
        "enable_provision_lock": False,

        "enable_secondary_master_registration": True,

        "timestamp_provider": {
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
            "update_period": 500,
        },

        "changelogs": {
            "flush_period": 10,
            "io_engine": {
                "enable_sync": False,
            }
        },

        "cell_directory": {
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
        },

        "object_service": {
            "enable_local_read_busy_wait": False,
        },

        "cell_manager": {
            "create_virtual_cell_maps_by_default": True,
        },

        "table_manager": {
            "make_schema_attribute_opaque": True,
        },

        "timestamp_manager": {
            "commit_advance": 2000,
            "request_backoff_time": 100,
            "calibration_period": 200,
        },

        "hive_manager": {
            "use_new": True,
            "ping_period": 1000,
            "idle_post_period": 1000,
        },

        "cell_directory_synchronizer": {
            "sync_period": 500,
            "sync_period_splay": 100,
        },

        "hydra_manager": {
            "snapshot_background_thread_count": 4,
            "leader_sync_delay": 0,
            "minimize_commit_latency": True,
        },

        "world_initializer": {
            "init_retry_period": 100,
            "update_period": 1000000000,
        },

        "transaction_lease_tracker": {
            "thread_count": 2,
        },

        "expose_testing_facilities": True,
    }


# COMPAT(koloshmet) cypress_manager/enable_preserve_acl_during_move
def get_dynamic_master_config():
    return {
        "chunk_manager": {
            "chunk_refresh_delay": 300,
            "replica_approve_timeout": 1000,
            "chunk_refresh_period": 200,
            "chunk_seal_backoff_time": 1000,
            "removal_job_schedule_delay": 0,
            "replicator_enabled_check_period": 1000,
            "enable_chunk_schemas": True,

            "data_node_tracker": {
                "enable_per_location_full_heartbeats": True,
            },
        },

        "node_tracker": {
            "master_cache_manager": {
                "update_period": 1000,
            },
            "timestamp_provider_manager": {
                "update_period": 1000,
            },
            "full_node_states_gossip_period": 1000,
            "node_statistics_gossip_period": 1000,
            "total_node_statistics_update_period": 1000,
            "enable_node_cpu_statistics": True,
            "forbid_maintenance_attribute_writes": False,
            "node_disposal_tick_period": 100,
        },

        "object_manager": {
            "gc_sweep_period": 10,
        },

        "object_service": {
            "minimize_execute_latency": True,
        },

        "security_manager": {
            "account_statistics_gossip_period": 200,
            "request_rate_smoothing_period": 60000,
            "account_master_memory_usage_update_period": 500,
            "enable_delayed_membership_closure_recomputation": False,
        },

        "cypress_manager": {
            "statistics_flush_period": 200,
            "expiration_check_period": 200,
            "expiration_backoff_time": 200,
            "remove_expired_master_nodes_using_client": True,
            "scion_removal_period": 1000,
            "virtual_map_read_offload_batch_size": 2,
            "enable_preserve_acl_during_move": False,
        },

        "transaction_manager": {
            "forbid_transaction_actions_for_cypress_transactions": True,
        },

        "multicell_manager": {
            "cell_statistics_gossip_period": 200,
        },

        "tablet_manager": {
            "cell_scan_period": 100,
            "accumulate_preload_pending_store_count_correctly": True,
            "increase_upload_replication_factor": True,

            "replicated_table_tracker": {
                "check_period": 500,
                "update_period": 500,
                "cluster_directory_synchronizer": {
                    "sync_period": 500,
                },
            },

            "tablet_balancer": {
                "enable_tablet_balancer": False,
            },

            "tablet_cell_decommissioner": {
                "decommission_check_period": 100,
                "orphans_check_period": 100,
            },

            "extra_peer_drop_delay": 100,

            "multicell_gossip": {
                "table_statistics_gossip_period": 100,
                "tablet_cell_statistics_gossip_period": 100,
                "tablet_cell_status_full_gossip_period": 100,
                "tablet_cell_status_incremental_gossip_period": 100,
                "bundle_resource_usage_gossip_period": 100,
            },

            "enable_bulk_insert": True,
            "enable_aggressive_tablet_statistics_validation": True,
            "forbid_arbitrary_data_versions_in_retention_config": True,

            "cell_hydra_persistence_synchronizer": {
                "migrate_to_virtual_cell_maps": True,
                "synchronization_period": 100,
            },
        },

        "sequoia_manager": {},

        "cell_master": {
            "alert_update_period": 500,
        },
    }


def get_scheduler_config():
    return {
        "cluster_connection": {},

        "response_keeper": {
            "enable_warmup": False,
            "expiration_time": 25000,
            "warmup_time": 30000,
        },

        "rpc_server": {
            "tracing_mode": "force",
        },

        "scheduler": {
            "lock_transaction_timeout": 10000,
            "operations_update_period": 500,
            "fair_share_update_period": 100,
            "watchers_update_period": 100,
            "nodes_attributes_update_period": 100,
            "scheduling_tag_filter_expire_timeout": 100,
            "node_shard_exec_nodes_cache_update_period": 100,
            "schedule_allocation_time_limit": 5000,
            "exec_node_descriptors_update_period": 100,
            "static_orchid_cache_update_period": 100,
            "operation_to_agent_assignment_backoff": 100,
            "orchid_keys_update_period": 100,

            "min_needed_resources_update_period": 100,

            "validate_node_tags_period": 100,
            "parse_operation_attributes_batch_size": 2,

            "operations_cleaner": {
                "parse_operation_attributes_batch_size": 2,
                "tablet_transaction_timeout": 5000,
            },

            "controller_agent_tracker": {
                "heartbeat_timeout": 10000,
                "enable_response_keeper": True,
            },

            "crash_on_allocation_heartbeat_processing_exception": True,

            "template_pool_tree_config_map": {
                "common": {
                    "filter": ".*",
                    "priority": -117,
                    "config": {
                        "enable_fast_child_function_summation_in_fifo_pools": True,
                    },
                },
            },
        },
    }


def get_clock_config():
    return {
        "timestamp_provider": {
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
            "update_period": 500,
        },
        "changelogs": {
            "flush_period": 10,
            "io_engine": {
                "enable_sync": False,
            },
        },
        "timestamp_manager": {
            "commit_advance": 2000,
            "request_backoff_time": 10,
            "calibration_period": 10,
        },
    }


def get_timestamp_provider_config():
    return {
        "timestamp_provider": {
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
            "update_period": 500,
        },
    }


def get_cell_balancer_config():
    return {
        "cluster_connection": {},
        "election_manager": {
            "lock_path": "//sys/cell_balancers/lock",
        },
        "cell_balancer": {
            "tablet_manager": {
                "cell_scan_period": 100,
                "tablet_cell_balancer": {
                    "enable_verbose_logging": True,
                },
            },
        },
    }


def get_controller_agent_config():
    return {
        "cluster_connection": {},

        "rpc_server": {
            "tracing_mode": "force",
        },

        "controller_agent": {
            "operations_update_period": 500,
            "scheduling_tag_filter_expire_timeout": 100,
            "safe_scheduler_online_time": 5000,

            "static_orchid_cache_update_period": 300,
            "controller_orchid_keys_update_period": 100,
            "controller_static_orchid_update_period": 0,

            "operations_push_period": 10,
            "operation_alerts_push_period": 100,
            "suspicious_jobs_push_period": 100,

            "config_update_period": 100,
            "controller_exec_node_info_update_period": 100,
            "update_account_resource_usage_leases_period": 100,
            "running_job_statistics_update_period": 100,
            "exec_nodes_update_period": 100,

            "environment": {
                "TMPDIR": "$(SandboxPath)",
                "PYTHON_EGG_CACHE": "$(SandboxPath)/.python-eggs",
                "PYTHONUSERBASE": "$(SandboxPath)/.python-site-packages",
                "PYTHONPATH": "$(SandboxPath)",
                "HOME": "$(SandboxPath)",
            },

            "testing_options": {
                "enable_snapshot_cycle_after_materialization": True,
            },

            "enable_snapshot_loading": True,
            "snapshot_period": 100000000,
            "snapshot_timeout": 5000,

            "transactions_refresh_period": 500,
            "max_archived_job_spec_count_per_operation": 10,

            "operation_options": {
                "spec_template": {
                    "max_failed_job_count": 10,
                    "locality_timeout": 100,
                    "intermediate_data_replication_factor": 1,
                    "enable_trace_logging": True,
                    "job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        }
                    },
                }
            },

            "sorted_merge_operation_options": {
                "spec_template": {
                    "use_new_sorted_pool": True,
                }
            },

            "reduce_operation_options": {
                "spec_template": {
                    "use_new_sorted_pool": True,
                }
            },

            "join_reduce_operation_options": {
                "spec_template": {
                    "use_new_sorted_pool": True,
                }
            },

            "sort_operation_options": {
                "spec_template": {
                    "use_new_sorted_pool": True,
                    "merge_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        }
                    },
                    "partition_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        }
                    },
                    "sort_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        }
                    },
                }
            },

            "map_reduce_operation_options": {
                "spec_template": {
                    "use_new_sorted_pool": True,
                    "map_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        }
                    },
                    "sort_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        }
                    },
                    "reduce_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        }
                    },
                }
            },

            "enable_bulk_insert_for_everyone": True,
            "running_job_time_statistics_updates_send_period": 10,

            "job_tracker": {
                "logging_job_sample_size": 1000,
                "duration_before_job_considered_disappeared_from_node": 1000,
                "node_disconnection_timeout": 2000,
            },

            "set_committed_attribute_via_transaction_action": False,
            "commit_operation_cypress_node_changes_via_system_transaction": True,
            "job_id_unequal_to_allocation_id": True,
        },
    }


def get_node_config():
    return {
        "orchid_cache_update_period": 0,

        "resource_limits": {
            "cpu_per_tablet_slot": 0.0,
        },

        "master_cache_service": {
            "capacity": 16777216,
            "rate_limit": 100,
        },

        "dynamic_config_manager": {
            "update_period": 500,
        },

        "cluster_connection": {},

        "data_node": {
            "multiplexed_changelog": {
                "flush_period": 10,
            },
            "low_latency_split_changelog": {
                "flush_period": 10,
            },
            "incremental_heartbeat_period": 200,
            "incremental_heartbeat_period_splay": 100,
            "incremental_heartbeat_throttler": {
                "limit": 100,
                "period": 1000,
            },
            "register_retry_period": 100,
            "master_connector": {
                "incremental_heartbeat_period": 200,
                "incremental_heartbeat_period_splay": 50,
                "job_heartbeat_period": 200,
                "job_heartbeat_period_splay": 50,
            },
            "chunk_meta_cache": {
                "capacity": 1000000,
                "shard_count": 1,
            },
            "block_meta_cache": {
                "capacity": 1000000,
                "shard_count": 1,
            },
            "blocks_ext_cache": {
                "capacity": 1000000,
                "shard_count": 1,
            },
            "block_cache": {
                "compressed_data": {
                    "capacity": 1000000,
                    "shard_count": 1,
                },
                "uncompressed_data": {
                    "capacity": 1000000,
                    "shard_count": 1,
                },
            },
            "p2p": {
                "enabled": False,
            },
            "volume_manager": {
                # It is disabled by the issues of tests stability in Porto environment.
                # The layer unpacking is performed by Porto in dedicated cgroup. The layer cache (as well as all
                # test artifacts) is allocated on tmpfs. In such circumstances the unpacked layer
                # is accounted to this dedicated cgroup, it causes starvation in this cgroup and layer unpacking
                # is getting stuck.
                "enable_layers_cache": False,
            },
            "sync_directories_on_connect": True,
            "session_timeout": 20000,
        },

        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "simple",
                },
            },
            "job_proxy_heartbeat_period": 200,
            "job_proxy": {
                "job_proxy_heartbeat_period": 200,
            },
            "job_prepare_time_limit": 60000,
        },

        "cellar_node": {
            "master_connector": {
                "heartbeat_period": 100,
                "heartbeat_period_splay": 30,
            },
            "transaction_lease_tracker": {
                "thread_count": 2,
            },
        },

        "tablet_node": {
            "slot_scan_period": 100,
            "tablet_manager": {
                "preload_backoff_time": 100,
                "compaction_backoff_time": 100,
                "flush_backoff_time": 100,
                "replicator_soft_backoff_time": 100,
                "replicator_hard_backoff_time": 100,
                "tablet_cell_decommission_check_period": 100,
            },
            "security_manager": {
                "resource_limits_cache": {
                    "expire_after_successful_update_time": 0,
                    "expire_after_failed_update_time": 0,
                    "refresh_time": 0,
                },
            },
            "hive_manager": {
                "use_new": True,
                "ping_period": 1000,
                "idle_post_period": 1000,
            },
            "hydra_manager": {
                "alert_on_snapshot_failure": False,
            },
            "master_connector": {
                "heartbeat_period": 100,
                "heartbeat_period_splay": 30,
            },
            "versioned_chunk_meta_cache": {
                "capacity": 1000000,
                "shard_count": 1,
            },
            "changelogs": {
                "lock_transaction_timeout": 5000,
            },
            "table_config_manager": {
                "update_period": 100,
            },
        },

        "query_agent": {
            "pool_weight_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "refresh_time": 0,
            },
        },

        "master_connector": {
            "heartbeat_period": 100,
            "heartbeat_period_splay": 30,
        },

        "enable_fair_throttler": True,
    }


def get_chaos_node_config():
    return {
        "orchid_cache_update_period": 0,

        "dynamic_config_manager": {
            "update_period": 500,
        },

        "cluster_connection": {},

        "flavors": [
            "chaos",
        ],

        "data_node": {
            "incremental_heartbeat_period": 200,
            "incremental_heartbeat_period_splay": 50,
            "block_cache": {
                "compressed_data": {
                    "capacity": 0,
                },
                "uncompressed_data": {
                    "capacity": 0,
                },
            },
            "chunk_meta_cache": {
                "capacity": 0,
            },
        },

        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 0,
                "cpu": 0,
            },
        },

        "tablet_node": {
            "resource_limits": {
                "slots": 0,
            },
            "versioned_chunk_meta_cache": {
                "capacity": 0,
            },
        },

        "chaos_node": {
            "chaos_manager": {
                "chaos_cell_synchronizer": {
                    "sync_period": 100,
                },
                "replication_card_observer": {
                    "observation_period": 100,
                },
                "era_commencing_period": 100,
            },
            "snapshot_store_read_pool_size": 1,
            "replicated_table_tracker_config_fetcher": {
                "update_period": 100,
                "enable_unrecognized_options_alert": True,
            },
        },

        "cellar_node": {
            "master_connector": {
                "heartbeat_period": 100,
                "heartbeat_period_splay": 30,
            },
            "cellar_manager": {
                "cellars": {
                    "chaos": {
                        "size": 4,
                        "occupant": {
                            "changelogs": {
                                "lock_transaction_timeout": 5000,
                            },
                            "hive_manager": {
                                "use_new": True,
                                "ping_period": 1000,
                                "idle_post_period": 1000,
                            },
                        },
                    },
                },
            },
        },

        "master_connector": {
            "heartbeat_period": 100,
            "heartbeat_period_splay": 30,
        },
    }


def get_master_cache_config():
    return {
        "cluster_connection": {
        },
    }


# COMPAT(arkady-e1ppa) operation_infos_request_period, total_confirmation_period
def get_dynamic_node_config():
    return {
        "%true": {
            "config_annotation": "default",
            "cellar_node": {
                "master_connector": {
                    "heartbeat_executor": {
                        "period": 100,
                        "splay": 30,
                        "jitter": 0.3,
                    },
                },
            },
            "data_node": {
                "master_connector": {
                    "heartbeat_executor": {
                        "period": 200,
                        "splay": 100,
                        "jitter": 0.3,
                    },
                },
                "remove_chunk_job": {
                    "wait_for_incremental_heartbeat_barrier": False,
                },
            },
            "exec_node": {
                "job_controller": {
                    "operation_info_request_backoff_strategy": {
                        "backoff": 1000,
                    },
                    "job_common": {
                        "node_directory_prepare_backoff_time": 100,
                        "job_prepare_time_limit": 60000,
                    },
                    "operation_infos_request_period": 1000,
                    "unknown_operation_jobs_removal_delay": 5000,
                    "disable_legacy_allocation_preparation": True,
                },
                "controller_agent_connector": {
                    "total_confirmation_backoff_strategy": {
                        "backoff": 5000,
                    },
                    "total_confirmation_period": 5000,
                    "job_staleness_delay": 5000,
                    "running_job_statistics_sending_backoff": 0,
                    "send_waiting_jobs": True,
                    "heartbeat_period": 100,
                    "heartbeat_executor": {
                        "period": 100,
                        "splay": 100,
                    },
                    "enable_tracing": True,
                },
                "scheduler_connector": {
                    "failed_heartbeat_backoff_start_time": 50,
                    "failed_heartbeat_backoff_max_time": 50,
                    "failed_heartbeat_backoff_multiplier": 1.0,
                    "heartbeat_period": 100,
                    "heartbeat_executor": {
                        "period": 100,
                        "min_backoff": 200,
                        "max_backoff": 200,
                        "backoff_multiplier": 1.0,
                    },
                    "enable_tracing": True,
                },
                "master_connector": {
                    "heartbeat_executor": {
                        "period": 100,
                        "splay": 30,
                        "jitter": 0.3,
                    },
                },
                "slot_manager": {
                    "abort_on_jobs_disabled": True,
                },
            },
            "master_connector": {
                "heartbeat_executor": {
                    "period": 100,
                    "splay": 30,
                    "jitter": 0.3,
                },
            },
            "tablet_node": {
                "master_connector": {
                    "heartbeat_executor": {
                        "period": 100,
                        "splay": 30,
                        "jitter": 0.3,
                    },
                },
            },
        },
    }


def get_driver_config():
    return {
        "format_defaults": {
            "structured": yson.to_yson_type("text", attributes={"format": "text"}),
            "tabular": yson.to_yson_type("text", attributes={"format": "text"}),
        },

        "force_tracing": True,
        "proxy_discovery_cache": {
            "refresh_time": 1000,
            "expire_after_successful_update_time": 1000,
            "expire_after_failed_update_time": 1000,
        },
        "table_writer": {
            "enable_large_columnar_statistics": True,
        },
        "enable_internal_commands": True,
    }


def get_proxy_config():
    return {
        "port": -1,
        "api": {
            "cors": {
                "disable_cors_check": True,
            },
        },
        "auth": {
            "enable_authentication": False,
            "blackbox_service": {},
            "cypress_token_authenticator": {},
            "blackbox_token_authenticator": {
                "scope": "",
                "enable_scope_check": False,
            },
            "blackbox_cookie_authenticator": {},
        },
        "coordinator": {
            "enable": True,
            "announce": True,
            "heartbeat_interval": 500,
            "show_ports": True,
        },
        "dynamic_config_manager": {
            "update_period": 500,
        },
    }


def get_watcher_config():
    return {
        "logs_rotate_max_part_count": 100,
        "logs_rotate_size": "1M",
        "logs_rotate_interval": 600,
    }


def get_queue_agent_config():
    return {
        "dynamic_config_manager": {
            "update_period": 100,
        },
    }


def get_kafka_proxy_config():
    return {
        "dynamic_config_manager": {
            "update_period": 100,
        },
        "auth": {
            "cypress_token_authenticator": {},
        },
    }


def get_dynamic_queue_agent_config(yt_config):
    return {
        "queue_agent": {
            "controller": {
                "enable_automatic_trimming": True,
            },
            "handle_replicated_objects": True,
        },
        "native_authentication_manager": {
            "enable_validation": False,
        },
        "cypress_synchronizer": {
            "write_replicated_table_mapping": True,
            "poll_replicated_objects": True,
            "clusters": [yt_config.cluster_name],
            "policy": "watching",
            "chaos_replicated_table_queue_agent_stage": "production",
        },
    }


def get_tablet_balancer_config():
    return {
        "dynamic_config_manager": {
            "update_period": 100,
        },
    }


def get_cypress_proxy_config():
    return {
        "dynamic_config_manager": {
            "update_period": 100,
        },
    }


def get_dynamic_cypress_proxy_config():
    return {}


def get_replicated_table_tracker_config():
    return {
        "dynamic_config_manager": {
            "update_period": 100,
        },
    }
