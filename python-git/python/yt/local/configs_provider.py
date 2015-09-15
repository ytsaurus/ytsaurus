from yt.common import YtError, update
from yt.environment.configs_provider import ConfigsProvider_17_2, ConfigsProvider_17_3, ConfigsProvider_18
from yt.environment.helpers import versions_cmp

class ConfigsProviderFactory(object):
    @staticmethod
    def create_for_version(version, enable_debug_logging):
        if versions_cmp(version, "0.17.2") <= 0:
            return LocalModeConfigsProvider_17_2(enable_debug_logging)
        elif versions_cmp(version, "0.17.3") >= 0 and versions_cmp(version, "0.18") < 0:
            return LocalModeConfigsProvider_17_3(enable_debug_logging)
        elif versions_cmp(version, "0.18") >= 0:
            return LocalModeConfigsProvider_18(enable_debug_logging)

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
MASTER_CONFIG_PATCH = {
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
}

SCHEDULER_CONFIG_PATCH = {
    "cluster_connection": {
        "transaction_manager": None
    },
    "transaction_manager": None,
    "scheduler": {
        "transactions_refresh_period": None,
        "operations_update_period": None,
        "watchers_update_period": None,
        "connect_grace_delay": None
    },
    "snapshot_timeout": 300000
}

NODE_CONFIG_PATCH = {
    "cluster_connection": {
        "transaction_manager": None,
        "master_cache": {
            "soft_backoff_time": None,
            "hard_backoff_time": None
        },
        "scheduler": None
    },
    "data_node": {
        "split_changelog": None,
        "block_cache": {
            "compressed_data": {
                "capacity": 0
            },
            "uncompressed_data": {
                "capacity": 0
            }
        },
        "incremental_heartbeat_period": None
    },
    "exec_agent": {
        "scheduler_connector": None,
        "job_controller": {
            "resource_limits": {
                "memory": 1073741824
            }
        }
    },
    "tablet_node": None,
    "resource_limits": {
        "memory": 2357198848  # 2.2 GB
    }
}

DRIVER_CONFIG_PATCH = {
    "transaction_manager": None
}

class LocalModeConfigsProvider_17_2(ConfigsProvider_17_2):
    def get_master_configs(self, master_count, master_dirs, secondary_master_cell_count=0, cell_tag=0):
        configs, addresses = super(LocalModeConfigsProvider_17_2, self)\
            .get_master_configs(master_count, master_dirs, secondary_master_cell_count, cell_tag)

        # In patches None values mean config subtree deletion (see _remove_none_fields function)
        for cell_index in xrange(secondary_master_cell_count + 1):
            for config in configs[cell_index]:
                update(config, MASTER_CONFIG_PATCH)
                _remove_none_fields(config)

        return configs, addresses

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs, addresses = super(LocalModeConfigsProvider_17_2, self)\
            .get_scheduler_configs(scheduler_count, scheduler_dirs)

        for config in configs:
            update(config, SCHEDULER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs, addresses

    def get_node_configs(self, node_count, node_dirs):
        configs, addresses = super(LocalModeConfigsProvider_17_2, self)\
            .get_node_configs(node_count, node_dirs)

        for config in configs:
            update(config, NODE_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs, addresses

    def get_driver_configs(self):
        configs = super(LocalModeConfigsProvider_17_2, self).get_driver_configs()

        for config in configs:
            update(config, DRIVER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

class LocalModeConfigsProvider_17_3(ConfigsProvider_17_3):
    def get_master_configs(self, master_count, master_dirs, secondary_master_cell_count=0, cell_tag=0):
        configs, addresses = super(LocalModeConfigsProvider_17_3, self)\
            .get_master_configs(master_count, master_dirs, secondary_master_cell_count, cell_tag)

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

                update(config, MASTER_CONFIG_PATCH)
                _remove_none_fields(config)

        return configs, addresses

    def get_node_configs(self, node_count, node_dirs):
        configs, addresses = super(LocalModeConfigsProvider_17_3, self)\
            .get_node_configs(node_count, node_dirs)

        for config in configs:
            update(config, NODE_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs, addresses

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs, addresses = super(LocalModeConfigsProvider_17_3, self)\
            .get_scheduler_configs(scheduler_count, scheduler_dirs)

        for config in configs:
            update(config, SCHEDULER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs, addresses

    def get_driver_configs(self):
        configs = super(LocalModeConfigsProvider_17_3, self).get_driver_configs()

        for config in configs:
            update(config, DRIVER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

class LocalModeConfigsProvider_18(ConfigsProvider_18):
    def get_master_configs(self, master_count, master_dirs, secondary_master_cell_count=0, cell_tag=0):
        configs, addresses = super(LocalModeConfigsProvider_18, self)\
            .get_master_configs(master_count, master_dirs, secondary_master_cell_count, cell_tag)

        local_patch = {
            "node_tracker": {
                "node_states_gossip_period": None
            },
            "tablet_manager": None,
            "multicell_manager": None
        }

        for cell_index in xrange(secondary_master_cell_count + 1):
            for config in configs[cell_index]:
                update(config, MASTER_CONFIG_PATCH)
                update(config, local_patch)
                _remove_none_fields(config)

        return configs, addresses

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs, addresses = super(LocalModeConfigsProvider_18, self)\
            .get_scheduler_configs(scheduler_count, scheduler_dirs)

        for config in configs:
            update(config, SCHEDULER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs, addresses

    def get_node_configs(self, node_count, node_dirs):
        configs, addresses = super(LocalModeConfigsProvider_18, self)\
            .get_node_configs(node_count, node_dirs)

        local_patch = {
            "cell_directory_synchronizer": None
        }

        for config in configs:
            update(config, NODE_CONFIG_PATCH)
            update(config, local_patch)
            _remove_none_fields(config)

        return configs, addresses

    def get_driver_configs(self):
        configs = super(LocalModeConfigsProvider_18, self).get_driver_configs()

        for config in configs:
            update(config, DRIVER_CONFIG_PATCH)
            _remove_none_fields(config)

        return configs

