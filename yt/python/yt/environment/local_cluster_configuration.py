from yt.wrapper.common import MB, GB

from yt.common import update_inplace

from .default_config import get_dynamic_node_config

try:
    from yt.packages.six import iteritems, itervalues
except ImportError:
    from six import iteritems, itervalues

MASTER_CONFIG_PATCHES = [
    {
        "cell_directory": None,
        "transaction_manager": None,
        "chunk_manager": None,
        "cypress_manager": None,
        "security_manager": None,
        "object_manager": None,
        # These option is required to decrease timeout for table mounting on local mode startup.
        "hive_manager": {
            "ping_period": 1000,
            "idle_post_period": 1000,
        },
        "cell_directory_synchronizer": None,
        "hydra_manager": {
            "max_changelog_data_size": 256 * MB
        }
    },
    {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
            "default_journal_replication_factor": 1,
            "default_journal_read_quorum": 1,
            "default_journal_write_quorum": 1,
            "default_hunk_storage_replication_factor": 1,
            "default_hunk_storage_read_quorum": 1,
            "default_hunk_storage_write_quorum": 1,
        }
    },
    {
        "timestamp_manager": {
            "commit_advance": 3000,
            "request_backoff_time": 10,
            "calibration_period": 10
        }
    },
    {
        "tablet_manager": None,
        "multicell_manager": None
    },
    {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }
]

CLUSTER_CONNECTION_CONFIG_PATCH = {
    "transaction_manager": None,
    # Reverting transaction manager config above also increases ping period to default 5 sec.
    # Thus we have to revert upload timeout as well.
    "upload_transaction_timeout": 15000,
    "scheduler": None
}

SCHEDULER_CONFIG_PATCH = {
    "cluster_connection": CLUSTER_CONNECTION_CONFIG_PATCH,
    "scheduler": {
        "operations_update_period": None,
        "watchers_update_period": 300,
        "lock_transaction_timeout": 30000
    }
}

CONTROLLER_AGENT_CONFIG_PATCH = {
    "controller_agent": {
        "transactions_refresh_period": None,
        "operations_update_period": None,
        "testing_options": None,
        "enable_tmpfs": False,
        "enable_locality": False,
        "snapshot_timeout": 300000,
        "sort_operation_options": {
            "spec_template": {
                "partition_data_size": 512 * MB
            }
        },

        "map_reduce_operation_options": {
            "spec_template": {
                "partition_data_size": 512 * MB,
                "enable_table_index_if_has_trivial_mapper": False,
            }
        },
    }
}

NODE_CONFIG_PATCHES = [
    {
        "cluster_connection": CLUSTER_CONNECTION_CONFIG_PATCH,
        "master_connector": {
            "heartbeat_period": 300,
            "heartbeat_period_splay": 50,
        },
        "data_node": {
            "max_bytes_per_read": 10 * GB,
            "multiplexed_changelog": None,
            "block_cache": {
                "compressed_data": {
                    "capacity": 200 * MB
                },
                "uncompressed_data": {
                    "capacity": 500 * MB
                }
            },
            "incremental_heartbeat_period": 300,
            "incremental_heartbeat_period_splay": 150,
            "master_connector": {
                "incremental_heartbeat_period": 200,
                "incremental_heartbeat_period_splay": 50,
                "job_heartbeat_period": 200,
                "job_heartbeat_period_splay": 50,
            },
        },
        "tablet_node": None
    },
    {
        "tablet_node": {
            "resource_limits": {
                "slots": 1,
                "tablet_dynamic_memory": 500 * MB,
                "tablet_static_memory": 1 * GB,
            },
            "hydra_manager": {
                "leader_lease_check_period": 100,
                "leader_lease_timeout": 20000,
                "disable_leader_lease_grace_delay": True,
                "response_keeper": {
                    "enable_warmup": False,
                    "expiration_time": 25000,
                    "warmup_time": 30000,
                }
            },
            "master_connector": {
                "heartbeat_period": 300,
                "heartbeat_period_splay": 50,
            },
            "versioned_chunk_meta_cache": {
                "capacity": 1000000,
            },
            "table_config_manager": {
                "update_period": 300,
            },
        },
        "cellar_node": {
            "master_connector": {
                "heartbeat_period": 300,
                "heartbeat_period_splay": 50,
            },
        },
    },
    {
        "cell_directory_synchronizer": None,
    }
]

# COMPAT(arkady-e1ppa): heartbeat_period, heartbeat_splay, failed_heartbeat_backoff_start_time, failed_heartbeat_backoff_max_time, failed_heartbeat_backoff_multiplier
DYNAMIC_NODE_CONFIG_PATCHES = [
    {
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
            "scheduler_connector": {
                "heartbeat_period": 100,
                "heartbeat_splay": 50,
                "failed_heartbeat_backoff_start_time": 50,
                "failed_heartbeat_backoff_max_time": 50,
                "failed_heartbeat_backoff_multiplier": 1.0,
                "heartbeats": {
                    "periodic": {
                        "period": 100,
                        "splay": 50,
                    },
                    "backoff_strategy": {
                        "min_backoff": 50,
                        "max_backoff": 50,
                        "backoff_multiplier": 1.0,
                    },
                },
            },
            "master_connector": {
                "heartbeat_period": 300,
                "heartbeat_splay": 50,
            },
        },
    },
]

NODE_STORE_LOCATION_PATCHES = [
    {
        "max_trash_ttl": 0,
    }
]

DRIVER_CONFIG_PATCH = CLUSTER_CONNECTION_CONFIG_PATCH


def _remove_none_fields(node):
    def process(key, value, keys_to_remove):
        if value is None:
            keys_to_remove.append(key)
        else:
            traverse(value)

    def traverse(node):
        keys_to_remove = []

        if isinstance(node, dict):
            for key, value in iteritems(node):
                process(key, value, keys_to_remove)
        elif isinstance(node, list):
            for i, value in enumerate(node):
                process(i, value, keys_to_remove)

        # Avoiding "dictionary/list changed size during iteration" error
        for key in keys_to_remove:
            del node[key]

    traverse(node)


def get_patched_dynamic_node_config(yt_config):
    dyn_node_config = get_dynamic_node_config()

    if yt_config.optimize_config:
        for patch in DYNAMIC_NODE_CONFIG_PATCHES:
            update_inplace(dyn_node_config["%true"], patch)

    return dyn_node_config


def modify_cluster_configuration(yt_config, cluster_configuration):
    master = cluster_configuration["master"]

    for tag in [master["primary_cell_tag"]] + master["secondary_cell_tags"]:
        for config in master[tag]:
            if yt_config.optimize_config:
                for patch in MASTER_CONFIG_PATCHES:
                    update_inplace(config, patch)

            if yt_config.delta_master_config:
                update_inplace(config, yt_config.delta_master_config)

    for config in itervalues(cluster_configuration["driver"]):
        if yt_config.optimize_config:
            update_inplace(config, DRIVER_CONFIG_PATCH)

        if yt_config.delta_driver_config:
            update_inplace(config, yt_config.delta_driver_config)

    for config in cluster_configuration["scheduler"]:
        if yt_config.optimize_config:
            update_inplace(config, SCHEDULER_CONFIG_PATCH)

        if yt_config.delta_scheduler_config:
            update_inplace(config, yt_config.delta_scheduler_config)

    for config in cluster_configuration["controller_agent"]:
        if yt_config.optimize_config:
            update_inplace(config, CONTROLLER_AGENT_CONFIG_PATCH)

        if yt_config.delta_controller_agent_config:
            update_inplace(config, yt_config.delta_controller_agent_config)

    for config in cluster_configuration["queue_agent"]:
        if yt_config.delta_queue_agent_config:
            update_inplace(config, yt_config.delta_queue_agent_config)

    for config in cluster_configuration["node"]:
        if yt_config.optimize_config:
            for patch in NODE_CONFIG_PATCHES:
                update_inplace(config, patch)

            for store_location in config["data_node"].get("store_locations", []):
                for patch in NODE_STORE_LOCATION_PATCHES:
                    update_inplace(store_location, patch)

        if yt_config.delta_node_config:
            update_inplace(config, yt_config.delta_node_config)

    for config in cluster_configuration["http_proxy"]:
        if yt_config.delta_http_proxy_config:
            update_inplace(config, yt_config.delta_http_proxy_config)

    for config in cluster_configuration["rpc_proxy"]:
        if yt_config.delta_rpc_proxy_config:
            update_inplace(config, yt_config.delta_rpc_proxy_config)

    if config in cluster_configuration["master_cache"]:
        if yt_config.delta_master_cache_config:
            update_inplace(config, yt_config.delta_master_cache_config)

    if yt_config.optimize_config:
        _remove_none_fields(cluster_configuration)
