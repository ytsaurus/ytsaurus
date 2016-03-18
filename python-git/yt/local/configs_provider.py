from yt.common import YtError, update
from yt.environment.configs_provider import ConfigsProvider_17_3, ConfigsProvider_17_4, ConfigsProvider_18
from yt.environment.helpers import versions_cmp

class ConfigsProviderFactory(object):
    @staticmethod
    def create_for_version(version, ports, enable_debug_logging, fqdn):
        if versions_cmp(version, "0.17.3") <= 0:
            return LocalModeConfigsProvider_17_3(ports, enable_debug_logging, fqdn)
        elif versions_cmp(version, "0.17.4") >= 0 and versions_cmp(version, "0.18") < 0:
            return LocalModeConfigsProvider_17_4(ports, enable_debug_logging, fqdn)
        elif versions_cmp(version, "0.18") >= 0:
            return LocalModeConfigsProvider_18(ports, enable_debug_logging, fqdn)

        raise YtError("Cannot create configs provider for version: {0}".format(version))

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
            for key, value in node.iteritems():
                process(key, value, keys_to_remove)
        elif isinstance(node, list):
            for i, value in enumerate(node):
                process(i, value, keys_to_remove)

        # Avoiding "dictionary/list changed size during iteration" error
        for key in keys_to_remove:
            del node[key]

    traverse(node)

# Local mode config patches (for all versions)
# None values mean config subtree removal (see _remove_none_fields function)
MASTER_CONFIG_PATCHES = [
    {
        "changelogs": {
            "enable_sync": False
        },
        "node_tracker": {
            "online_node_timeout": 20000,
            "registered_node_timeout": 20000,
            "max_concurrent_node_registrations": None,
            "max_concurrent_node_unregistrations": None
        },
        "cell_directory": None,
        "transaction_manager": None,
        "chunk_manager": None,
        "cypress_manager": None,
        "security_manager": None,
        "object_manager": None,
        "hive_manager": None,
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
        "lock_transaction_timeout": 30000
    },
    "snapshot_timeout": 300000
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
            "multiplexed_changelog": None,
            "block_cache": {
                "compressed_data": {
                    "capacity": 209715200  # 200 MB
                },
                "uncompressed_data": {
                    "capacity": 524288000  # 500 MB
                }
            },
            "incremental_heartbeat_period": 300,
            "store_locations": [
                {
                    "enable_journals": True
                }
            ]
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
                "tablet_dynamic_memory": 524288000,  # 500 MB
                "tablet_static_memory": 0
            }
        },
        "exec_agent": {
            "scheduler_connector": {
                "failed_heartbeat_backoff_time": 50,
                "heartbeat_period": 300,
                "heartbeat_splay": 100
            }
        }
    }
]

DRIVER_CONFIG_PATCH = {
    "transaction_manager": None
}

def _tune_memory_limits(config):
    memory = config["resource_limits"]["memory"]
    # Add tablet resource limits
    tablet_resource_limits = config.get("tablet_node", {}).get("resource_limits", {})
    memory += tablet_resource_limits.get("tablet_dynamic_memory", 0)

    block_cache = config.get("data_node", {}).get("block_cache", {})
    memory += block_cache.get("compressed_data", {}).get("capacity", 0)
    memory += block_cache.get("uncompressed_data", {}).get("capacity", 0)

    config["resource_limits"]["memory"] = memory

class LocalModeConfigsProvider_17_3(ConfigsProvider_17_3):
    def get_master_configs(self, master_count, nonvoting_master_count, master_dirs,
                           tmpfs_master_dirs=None, secondary_master_cell_count=0, cell_tag=0):

        configs = super(LocalModeConfigsProvider_17_3, self)\
            .get_master_configs(master_count, nonvoting_master_count, master_dirs, tmpfs_master_dirs, secondary_master_cell_count, cell_tag)

        # In patches None values mean config subtree deletion (see _remove_none_fields function)
        for cell_index in xrange(secondary_master_cell_count + 1):
            for config in configs[cell_index]:
                for patch in MASTER_CONFIG_PATCHES:
                    update(config, patch)
                _remove_none_fields(config)

        return configs

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs = super(LocalModeConfigsProvider_17_3, self).get_scheduler_configs(scheduler_count, scheduler_dirs)

        for config in configs:
            update(config, SCHEDULER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

    def get_node_configs(self, node_count, node_dirs, operations_memory_limit=None):
        configs = super(LocalModeConfigsProvider_17_3, self)\
                .get_node_configs(node_count, node_dirs, operations_memory_limit)

        for config in configs:
            for patch in NODE_CONFIG_PATCHES:
                update(config, patch)
            _remove_none_fields(config)

        return configs

    def get_driver_configs(self):
        configs = super(LocalModeConfigsProvider_17_3, self).get_driver_configs()

        for config in configs:
            update(config, DRIVER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

class LocalModeConfigsProvider_17_4(ConfigsProvider_17_4):
    def get_master_configs(self, master_count, nonvoting_master_count, master_dirs,
                           tmpfs_master_dirs=None, secondary_master_cell_count=0, cell_tag=0):

        configs = super(LocalModeConfigsProvider_17_4, self)\
            .get_master_configs(master_count, nonvoting_master_count, master_dirs, tmpfs_master_dirs, secondary_master_cell_count, cell_tag)

        for cell_index in xrange(secondary_master_cell_count + 1):
            for config in configs[cell_index]:
                config["hydra_manager"] = {
                    "leader_lease_check_period": 100,
                    "leader_lease_timeout": 200,
                    "disable_leader_lease_grace_delay": True,
                    "response_keeper": {
                        "expiration_time": 25000,
                        "warmup_time": 30000,
                    }
                }

                for patch in MASTER_CONFIG_PATCHES:
                    update(config, patch)
                _remove_none_fields(config)

        return configs

    def get_node_configs(self, node_count, node_dirs, operations_memory_limit=None):
        configs = super(LocalModeConfigsProvider_17_4, self)\
                .get_node_configs(node_count, node_dirs, operations_memory_limit)

        for config in configs:
            for patch in NODE_CONFIG_PATCHES:
                update(config, patch)

            _remove_none_fields(config)
            _tune_memory_limits(config)

        return configs

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs = super(LocalModeConfigsProvider_17_4, self).get_scheduler_configs(scheduler_count, scheduler_dirs)

        for config in configs:
            update(config, SCHEDULER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

    def get_driver_configs(self):
        configs = super(LocalModeConfigsProvider_17_4, self).get_driver_configs()

        for config in configs:
            update(config, DRIVER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

class LocalModeConfigsProvider_18(ConfigsProvider_18):
    def get_master_configs(self, master_count, nonvoting_master_count, master_dirs,
                           tmpfs_master_dirs=None, secondary_master_cell_count=0, cell_tag=0):

        configs = super(LocalModeConfigsProvider_18, self)\
            .get_master_configs(master_count, nonvoting_master_count, master_dirs, tmpfs_master_dirs, secondary_master_cell_count, cell_tag)

        local_patch = {
            "node_tracker": {
                "node_states_gossip_period": None
            },
            "tablet_manager": None,
            "multicell_manager": None
        }

        for cell_index in xrange(secondary_master_cell_count + 1):
            for config in configs[cell_index]:
                for patch in MASTER_CONFIG_PATCHES:
                    update(config, patch)
                update(config, local_patch)
                _remove_none_fields(config)

        return configs

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs = super(LocalModeConfigsProvider_18, self).get_scheduler_configs(scheduler_count, scheduler_dirs)

        for config in configs:
            update(config, SCHEDULER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

    def get_node_configs(self, node_count, node_dirs, operations_memory_limit=None):
        configs = super(LocalModeConfigsProvider_18, self)\
                .get_node_configs(node_count, node_dirs, operations_memory_limit)

        local_patch = {
            "cell_directory_synchronizer": None
        }

        for config in configs:
            for patch in NODE_CONFIG_PATCHES:
                update(config, patch)
            update(config, local_patch)

            _remove_none_fields(config)
            _tune_memory_limits(config)

        return configs

    def get_driver_configs(self):
        configs = super(LocalModeConfigsProvider_18, self).get_driver_configs()

        for config in configs:
            update(config, DRIVER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

