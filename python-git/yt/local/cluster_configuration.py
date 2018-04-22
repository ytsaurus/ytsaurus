from yt.wrapper.common import MB, GB

# TODO(asaitgalin): Remove it when new version of yt.wrapper
# is built and deployed.
from copy import deepcopy
from yt.common import update
try:
    from yt.common import update_inplace
except ImportError:
    update_inplace = update
    del update

    def update(obj, patch):
        return update_inplace(deepcopy(obj), patch)

from yt.packages.six import iteritems, itervalues

# Local mode config patches (for all versions)
# None values mean config subtree removal (see _remove_none_fields function below)
# For more detailed info about how configs are generated see environment/configs_provider.py
MASTER_CONFIG_PATCHES = [
    {
        "changelogs": {
            "enable_sync": False
        },
        "node_tracker": {
            "max_concurrent_node_registrations": None,
            "max_concurrent_node_unregistrations": None
        },
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
        "node_tracker": {
            "node_states_gossip_period": None
        },
        "tablet_manager": None,
        "multicell_manager": None
    }
]

SCHEDULER_CONFIG_PATCH = {
    "cluster_connection": {
        "transaction_manager": None
    },
    "transaction_manager": None,
    "scheduler": {
        "transactions_refresh_period": None,
        "operations_update_period": None,
        "watchers_update_period": 300,
        "connect_grace_delay": None,
        "lock_transaction_timeout": 30000,
        "enable_locality": False,
        "sort_operation_options": {
            "spec_template": {
                "partition_data_size": 512 * MB
            }
        },

        "map_reduce_operation_options": {
            "spec_template": {
                "partition_data_size": 512 * MB
            }
        },
        "enable_tmpfs": False,
        "environment": {
            "TMPDIR": "$(SandboxPath)",
            "PYTHON_EGG_CACHE": "$(SandboxPath)/.python-eggs",
            "PYTHONUSERBASE": "$(SandboxPath)/.python-site-packages",
            "PYTHONPATH": "$(SandboxPath)",
            "HOME": "$(SandboxPath)",
        },
        "enable_snapshot_cycle_after_materialization": False,
        "testing_options": None,
    },
    "snapshot_timeout": 300000,
}

SCHEDULER_CONFIG_PATCH_19_3 = {
    "cluster_connection": {
        "transaction_manager": None
    },
    "transaction_manager": None,
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
        "environment": {
            "TMPDIR": "$(SandboxPath)",
            "PYTHON_EGG_CACHE": "$(SandboxPath)/.python-eggs",
            "PYTHONUSERBASE": "$(SandboxPath)/.python-site-packages",
            "PYTHONPATH": "$(SandboxPath)",
            "HOME": "$(SandboxPath)",
        },
        "snapshot_timeout": 300000,
        "sort_operation_options": {
            "spec_template": {
                "partition_data_size": 512 * MB
            }
        },

        "map_reduce_operation_options": {
            "spec_template": {
                "partition_data_size": 512 * MB
            }
        },
    }
}

NODE_CONFIG_PATCHES = [
    {
        "cluster_connection": {
            "transaction_manager": None,
            "master_cache": {
                "soft_backoff_time": None,
                "hard_backoff_time": None
            },
            "scheduler": None
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
            "incremental_heartbeat_period": 300
        },
        "exec_agent": {
            "scheduler_connector": None
        },
        "tablet_node": None
    },
    {
        "tablet_node": {
            "resource_limits": {
                "slots": 1,
                "tablet_dynamic_memory": 500 * MB,
                "tablet_static_memory": 0
            },
            "hydra_manager": {
                "leader_lease_check_period": 100,
                "leader_lease_timeout": 500,
                "leader_lease_grace_delay": 600,
            }
        },
        "exec_agent": {
            "job_proxy_heartbeat_period": 100,
            "scheduler_connector": {
                "heartbeat_period": 100,
                "heartbeat_splay": 50
            }
        }
    },
    {
        "cell_directory_synchronizer": None,
        "exec_agent": {
            "scheduler_connector": {
                "unsuccess_heartbeat_backoff_time": 50
            }
        }
    }
]

NODE_MEMORY_LIMIT_ADDITION = 500 * MB + 200 * MB + 500 * MB  # block_cache + tablet_node, see above

DRIVER_CONFIG_PATCH = {
    "transaction_manager": None
}

def _remove_none_fields(node):
    def process(key, value, keys_to_remove):
        if value is None:
            keys_to_remove.append(key)
        else:
            traverse(value)
            if isinstance(value, (list, dict)) and not value:
                keys_to_remove.append(key)

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

def modify_cluster_configuration(cluster_configuration, abi_version, master_config_patch=None, node_config_patch=None,
                                 scheduler_config_patch=None, controller_agent_config_patch=None, proxy_config_patch=None):
    master = cluster_configuration["master"]

    for tag in [master["primary_cell_tag"]] + master["secondary_cell_tags"]:
        for config in master[tag]:
            for patch in MASTER_CONFIG_PATCHES:
                update_inplace(config, patch)

            if master_config_patch:
                update_inplace(config, master_config_patch)

    for config in itervalues(cluster_configuration["driver"]):
        update_inplace(config, DRIVER_CONFIG_PATCH)

    for config in cluster_configuration["scheduler"]:
        if abi_version >= (19, 3):
            update_inplace(config, SCHEDULER_CONFIG_PATCH_19_3)
        else:
            update_inplace(config, SCHEDULER_CONFIG_PATCH)

        if scheduler_config_patch:
            update_inplace(config, scheduler_config_patch)

    if abi_version >= (19, 3):
        for config in cluster_configuration["controller_agent"]:
            update_inplace(config, CONTROLLER_AGENT_CONFIG_PATCH)

        if controller_agent_config_patch:
            update_inplace(config, controller_agent_config_patch)

    for config in cluster_configuration["node"]:
        for patch in NODE_CONFIG_PATCHES:
            update_inplace(config, patch)

        if node_config_patch:
            update_inplace(config, node_config_patch)

    if proxy_config_patch:
        update_inplace(cluster_configuration["proxy"], proxy_config_patch)

    _remove_none_fields(cluster_configuration)
