import default_configs
from helpers import get_open_port, versions_cmp

from yt.common import YtError, unlist

import os
import abc
import socket

def init_logging(node, path, name, enable_debug_logging):
    if not node:
        node = default_configs.get_logging_config(enable_debug_logging)

    def process(node, key, value):
        if isinstance(value, str):
            node[key] = value.format(path=path, name=name)
        else:
            node[key] = traverse(value)

    def traverse(node):
        if isinstance(node, dict):
            for key, value in node.iteritems():
                process(node, key, value)
        elif isinstance(node, list):
            for i, value in enumerate(node):
                process(node, i, value)
        return node

    return traverse(node)

class ConfigsProviderFactory(object):
    @staticmethod
    def create_for_version(version, enable_debug_logging):
        if versions_cmp(version, "0.17.2") <= 0:
            return ConfigsProvider_17_2(enable_debug_logging)
        elif versions_cmp(version, "0.17.3") >= 0 and versions_cmp(version, "0.18") < 0:
            return ConfigsProvider_17_3(enable_debug_logging)
        elif versions_cmp(version, "0.18") >= 0:
            return ConfigsProvider_18(enable_debug_logging)

        raise YtError("Cannot create configs provider for version: {0}".format(version))

class ConfigsProvider(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, enable_debug_logging=True):
        self.fqdn = socket.getfqdn()
        self.enable_debug_logging = enable_debug_logging
        get_open_port.busy_ports = set()
        # Generated addresses
        # _master_addresses["secondary"] is list of size secondary_master_cell_count
        self._master_addresses = {"primary": [], "secondary": []}
        self._node_addresses = []
        self._scheduler_addresses = []
        self._proxy_address = None

    @abc.abstractmethod
    def get_master_configs(self, master_count, master_dirs, secondary_master_cell_count=0, cell_tag=0):
        pass

    @abc.abstractmethod
    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        pass

    @abc.abstractmethod
    def get_node_configs(self, node_count, node_dirs):
        pass

    @abc.abstractmethod
    def get_proxy_config(self, proxy_dir, proxy_port=None):
        pass

    @abc.abstractmethod
    def get_driver_configs(self):
        pass

class ConfigsProvider_17(ConfigsProvider):
    def __init__(self, enable_debug_logging=True):
        super(ConfigsProvider_17, self).__init__(enable_debug_logging)
        self._master_cell_tag = 0

    def get_master_configs(self, master_count, master_dirs, secondary_master_cell_count=0, cell_tag=0):
        if secondary_master_cell_count > 0:
            raise YtError("Secondary master cells are not supported in YT version <= 0.18")
        master_dirs = unlist(master_dirs)

        ports = [get_open_port() for _ in xrange(master_count * 2)]
        addresses = ["{0}:{1}".format(self.fqdn, ports[2 * i]) for i in xrange(master_count)]

        configs = []

        for i in xrange(master_count):
            config = default_configs.get_master_config()

            config["rpc_port"] = ports[2 * i]
            config["monitoring_port"] = ports[2 * i + 1]

            config["master"] = {
                "cell_tag": cell_tag,
                "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
                "addresses": addresses
            }
            config["timestamp_provider"]["addresses"] = addresses
            config["changelogs"]["path"] = os.path.join(master_dirs[i], "changelogs")
            config["snapshots"]["path"] = os.path.join(master_dirs[i], "snapshots")
            config["logging"] = init_logging(config["logging"], master_dirs[i], "master-" + str(i),
                                             self.enable_debug_logging)

            config["node_tracker"]["online_node_timeout"] = 1000

            configs.append(config)

        self._master_addresses["primary"] = addresses
        self._master_cell_tag = cell_tag

        return [configs], [addresses]

    def _get_cache_addresses(self):
        if self._node_addresses:
            return self._node_addresses
        else:
            return self._master_addresses["primary"]

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        ports = [get_open_port() for _ in xrange(scheduler_count * 2)]
        addresses = ["{0}:{1}".format(self.fqdn, ports[2 * i]) for i in xrange(scheduler_count)]

        configs = []

        for i in xrange(scheduler_count):
            config = default_configs.get_scheduler_config()

            config["cluster_connection"]["master"] = {
                "addresses": self._master_addresses["primary"],
                "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
                "cell_tag": self._master_cell_tag,
                "rpc_timeout": 5000
            }
            config["cluster_connection"]["timestamp_provider"]["addresses"] = self._master_addresses["primary"]
            config["cluster_connection"]["transaction_manager"]["ping_period"] = 500

            config["rpc_port"] = ports[2 * i]
            config["monitoring_port"] = ports[2 * i + 1]
            config["scheduler"]["snapshot_temp_path"] = os.path.join(scheduler_dirs[i], "snapshots")

            config["transaction_manager"]["ping_period"] = 500

            config["logging"] = init_logging(config["logging"], scheduler_dirs[i], "scheduler-" + str(i),
                                             self.enable_debug_logging)

            configs.append(config)

        self._scheduler_addresses = addresses

        return configs, addresses

    def get_node_configs(self, node_count, node_dirs):
        ports = [get_open_port() for _ in xrange(node_count * 2)]
        addresses = ["{0}:{1}".format(self.fqdn, ports[2 * i]) for i in xrange(node_count)]

        current_user = 10000

        configs = []

        for i in xrange(node_count):
            config = default_configs.get_node_config()

            config["addresses"] = {
                "default": self.fqdn,
                "interconnect": self.fqdn
            }

            config["rpc_port"] = ports[2 * i]
            config["monitoring_port"] = ports[2 * i + 1]

            config["cluster_connection"]["master"] = {
                "addresses": self._master_addresses["primary"],
                "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
                "cell_tag": self._master_cell_tag,
                "rpc_timeout": 5000
            }
            config["cluster_connection"]["master_cache"] = {
                "addresses": self._master_addresses["primary"],
                "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
                "cell_tag": self._master_cell_tag
            }
            config["cluster_connection"]["timestamp_provider"]["addresses"] = self._master_addresses["primary"]
            config["cluster_connection"]["transaction_manager"]["ping_period"] = 500

            config["data_node"]["multiplexed_changelog"] = {}
            config["data_node"]["multiplexed_changelog"]["path"] = os.path.join(node_dirs[i], "multiplexed")

            config["data_node"]["store_locations"].append({
                "path": os.path.join(node_dirs[i], "chunk_store"),
                "low_watermark": 0,
                "high_watermark": 0
            })
            config["exec_agent"]["slot_manager"]["start_uid"] = current_user

            current_user += config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] + 1

            config["logging"] = init_logging(config["logging"], node_dirs[i], "node-{0}".format(i),
                                             self.enable_debug_logging)

            config["exec_agent"]["job_proxy_logging"] = init_logging(config["exec_agent"]["job_proxy_logging"],
                                                                     node_dirs[i], "job_proxy-{0}".format(i),
                                                                     self.enable_debug_logging)

            configs.append(config)

        self._node_addresses = addresses

        return configs, addresses

    def get_proxy_config(self, proxy_dir, proxy_port=None):
        if proxy_port is not None and isinstance(proxy_port, int):
            ports = [proxy_port, get_open_port()]
        else:
            ports = [get_open_port(), get_open_port()]

        driver_config = default_configs.get_driver_config()
        driver_config["master"] = {
            "addresses": self._master_addresses["primary"],
            "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
            "cell_tag": self._master_cell_tag
        }
        driver_config["timestamp_provider"]["addresses"] = self._master_addresses["primary"]

        proxy_config = default_configs.get_proxy_config()
        proxy_config["proxy"]["logging"] = init_logging(proxy_config["proxy"]["logging"], proxy_dir, "http_proxy",
                                                        self.enable_debug_logging)
        proxy_config["proxy"]["driver"] = driver_config
        proxy_config["port"] = ports[0]
        proxy_config["log_port"] = ports[1]

        self._proxy_address = "{0}:{1}".format(self.fqdn, ports[0])

        return proxy_config, self._proxy_address

    def get_driver_configs(self):
        config = default_configs.get_driver_config()
        config["master"] = {
            "addresses": self._master_addresses["primary"],
            "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
            "cell_tag": self._master_cell_tag
        }
        config["timestamp_provider"]["addresses"] = self._get_cache_addresses()
        config["transaction_manager"]["ping_period"] = 500

        return [config]

class ConfigsProvider_17_3(ConfigsProvider_17):
    def get_master_configs(self, master_count, master_dirs, secondary_master_cell_count=0, cell_tag=0):
        configs, addresses = super(ConfigsProvider_17_3, self).\
            get_master_configs(master_count, master_dirs, secondary_master_cell_count, cell_tag)

        for cell_index in xrange(secondary_master_cell_count + 1):
            for config in configs[cell_index]:
                config["hydra_manager"] = {
                    "leader_lease_check_period": 1000,
                    "leader_lease_timeout": 3000,
                    "disable_leader_lease_grace_delay": True,
                    "response_keeper": {
                        "expiration_time": 25000,
                        "warmup_time": 30000,
                    }
                }

        return configs, addresses

    def get_node_configs(self, node_count, node_dirs):
        configs, addresses = super(ConfigsProvider_17_3, self).\
                get_node_configs(node_count, node_dirs)

        for i, config in enumerate(configs):
            config["data_node"]["cache_locations"] = [{
                "path": os.path.join(node_dirs[i], "chunk_cache")
            }]
            config["exec_agent"]["slot_manager"] = {
                "paths": [os.path.join(node_dirs[i]), "slots"]
            }

        return configs, addresses

class ConfigsProvider_17_2(ConfigsProvider_17):
    def get_node_configs(self, node_count, node_dirs):
        configs, addresses = super(ConfigsProvider_17_2, self).\
                get_node_configs(node_count, node_dirs)

        for i, config in enumerate(configs):
            config["exec_agent"]["slot_manager"] = {"path": os.path.join(node_dirs[i], "slots")}
            config["data_node"]["cache_location"] = {"path": os.path.join(node_dirs[i], "chunk_cache")}

        return configs, addresses

class ConfigsProvider_18(ConfigsProvider):
    def __init__(self, enable_debug_logging=True):
        super(ConfigsProvider_18, self).__init__(enable_debug_logging)
        self._primary_master_cell_id = 0
        self._secondary_masters_cell_ids = []

    def get_master_configs(self, master_count, master_dirs, secondary_master_cell_count=0, cell_tag=0):
        ports = []
        addresses = []

        self._secondary_master_cells_count = secondary_master_cell_count
        self._primary_master_cell_id = "ffffffff-ffffffff-%x0259-ffffffff" % cell_tag
        self._secondary_masters_cell_ids = ["ffffffff-ffffffff-%x0259-ffffffff" % (cell_tag + index + 1)
                                            for index in xrange(secondary_master_cell_count)]

        # Primary masters cell index is 0
        for cell_index in xrange(secondary_master_cell_count + 1):
            ports.append([get_open_port() for _ in xrange(master_count * 2)])
            addresses.append(["{0}:{1}".format(self.fqdn, ports[cell_index][2 * i])
                              for i in xrange(master_count)])

        cell_configs = []

        for cell_index in xrange(secondary_master_cell_count + 1):
            configs = []

            for master_index in xrange(master_count):
                config = default_configs.get_master_config()

                config["hydra_manager"] = {
                    "leader_lease_check_period": 100,
                    "leader_lease_timeout": 200,
                    "disable_leader_lease_grace_delay": True,
                    "response_keeper": {
                        "expiration_time": 25000,
                        "warmup_time": 30000,
                    }
                }

                config["security_manager"]["user_statistics_gossip_period"] = 80
                config["security_manager"]["account_statistics_gossip_period"] = 80

                config["node_tracker"]["node_states_gossip_period"] = 80

                config["rpc_port"] = ports[cell_index][2 * master_index]
                config["monitoring_port"] = ports[cell_index][2 * master_index + 1]

                config["primary_master"] = {
                    "cell_id": self._primary_master_cell_id,
                    "addresses": addresses[0]
                }

                config["secondary_masters"] = []
                for index in xrange(secondary_master_cell_count):
                    config["secondary_masters"].append({})
                    config["secondary_masters"][index]["cell_id"] = self._secondary_masters_cell_ids[index]
                    config["secondary_masters"][index]["addresses"] = addresses[index + 1]

                config["timestamp_provider"]["addresses"] = addresses[0]
                config["changelogs"]["path"] = master_dirs[cell_index][master_index]
                config["snapshots"]["path"] = master_dirs[cell_index][master_index]
                config["logging"] = init_logging(config["logging"], master_dirs[cell_index][master_index],
                                                 "master-" + str(master_index), self.enable_debug_logging)

                config["tablet_manager"] = {
                    "cell_scan_period": 100
                }

                config["multicell_manager"] = {
                    "cell_statistics_gossip_period": 80
                }

                configs.append(config)

            cell_configs.append(configs)

            if cell_index == 0:
                self._master_addresses["primary"] = addresses[cell_index]
            else:
                self._master_addresses["secondary"].append(addresses[cell_index])

        return cell_configs, addresses

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        ports = [get_open_port() for _ in xrange(scheduler_count * 2)]
        scheduler_addresses = ["{0}:{1}".format(self.fqdn, ports[2 * i]) for i in xrange(scheduler_count)]

        configs = []

        for i in xrange(scheduler_count):
            config = default_configs.get_scheduler_config()

            config["cluster_connection"]["primary_master"] = {
                "addresses": self._master_addresses["primary"],
                "cell_id": self._primary_master_cell_id,
                "rpc_timeout": 5000
            }

            secondary_masters_info = zip(self._master_addresses["secondary"], self._secondary_masters_cell_ids)
            config["cluster_connection"]["secondary_masters"] = [{"addresses": addresses, "cell_id": cell_id}
                    for addresses, cell_id in secondary_masters_info]
            config["cluster_connection"]["timestamp_provider"]["addresses"] = self._master_addresses["primary"]
            config["cluster_connection"]["transaction_manager"]["default_ping_period"] = 500

            config["rpc_port"] = ports[2 * i]
            config["monitoring_port"] = ports[2 * i + 1]
            config["scheduler"]["snapshot_temp_path"] = os.path.join(scheduler_dirs[i], "snapshots")

            config["transaction_manager"]["default_ping_period"] = 500

            config["logging"] = init_logging(config["logging"], scheduler_dirs[i], "scheduler-" + str(i),
                                             self.enable_debug_logging)

            configs.append(config)

        self._scheduler_addresses = scheduler_addresses

        return configs, scheduler_addresses

    def get_proxy_config(self, proxy_dir, proxy_port=None):
        if proxy_port is not None and isinstance(proxy_port, int):
            ports = [proxy_port, get_open_port()]
        else:
            ports = [get_open_port(), get_open_port()]

        driver_config = default_configs.get_driver_config()
        driver_config["primary_master"] = {}
        driver_config["primary_master"]["addresses"] = self._master_addresses["primary"]
        driver_config["primary_master"]["cell_id"] = self._primary_master_cell_id
        secondary_masters_info = zip(self._master_addresses["secondary"], self._secondary_masters_cell_ids)
        driver_config["secondary_masters"] = [{"addresses": addresses, "cell_id": cell_id}
                for addresses, cell_id in secondary_masters_info]
        driver_config["timestamp_provider"]["addresses"] = self._master_addresses["primary"]

        proxy_config = default_configs.get_proxy_config()
        proxy_config["proxy"]["logging"] = init_logging(proxy_config["proxy"]["logging"], proxy_dir, "http_proxy",
                                                        self.enable_debug_logging)
        proxy_config["proxy"]["driver"] = driver_config
        proxy_config["port"] = ports[0]
        proxy_config["log_port"] = ports[1]
        proxy_config["fqdn"] = "localhost:{0}".format(ports[0])

        self._proxy_address = proxy_config["fqdn"]

        return proxy_config, self._proxy_address

    def get_node_configs(self, node_count, node_dirs):
        ports = [get_open_port() for _ in xrange(node_count * 2)]
        addresses = ["{0}:{1}".format(self.fqdn, ports[2 * i]) for i in xrange(node_count)]

        current_user = 10000

        configs = []

        for i in xrange(node_count):
            config = default_configs.get_node_config()

            config["addresses"] = {
                "default": self.fqdn,
                "interconnect": self.fqdn
            }

            config["rpc_port"] = ports[2 * i]
            config["monitoring_port"] = ports[2 * i + 1]

            config["cell_directory_synchronizer"] = {
                "sync_period": 1000
            }

            config["cluster_connection"]["primary_master"] = {
                "cell_id": self._primary_master_cell_id,
                "addresses": self._master_addresses["primary"],
                "rpc_timeout": 5000
            }

            config["cluster_connection"]["master_cache"] = {
                "cell_id": self._primary_master_cell_id,
                "addresses": self._master_addresses["primary"]
            }

            secondary_masters_info = zip(self._master_addresses["secondary"], self._secondary_masters_cell_ids)
            config["cluster_connection"]["secondary_masters"] = [{"addresses": addresses_, "cell_id": cell_id}
                    for addresses_, cell_id in secondary_masters_info]
            config["cluster_connection"]["timestamp_provider"]["addresses"] = self._master_addresses["primary"]
            config["cluster_connection"]["transaction_manager"]["default_ping_period"] = 500

            config["data_node"]["cache_locations"] = []
            config["data_node"]["cache_locations"].append({"path": os.path.join(node_dirs[i], "chunk_cache")})

            config["data_node"]["store_locations"].append({
                "path": os.path.join(node_dirs[i], "chunk_store"),
                "low_watermark": 0,
                "high_watermark": 0
            })
            config["exec_agent"]["slot_manager"]["start_uid"] = current_user
            config["exec_agent"]["slot_manager"]["paths"] = [os.path.join(node_dirs[i], "slots")]

            current_user += config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] + 1

            config["logging"] = init_logging(config["logging"], node_dirs[i], "node-{0}".format(i),
                                             self.enable_debug_logging)
            config["exec_agent"]["job_proxy_logging"] = init_logging(config["exec_agent"]["job_proxy_logging"],
                                                                     node_dirs[i], "job_proxy-{0}".format(i),
                                                                     self.enable_debug_logging)

            configs.append(config)

        self._node_addresses = addresses

        return configs, addresses

    def _get_cache_addresses(self):
        if self._node_addresses:
            return self._node_addresses
        else:
            return self._master_addresses["primary"]

    def get_driver_configs(self):
        configs = []

        for cell_index in xrange(self._secondary_master_cells_count + 1):
            config = default_configs.get_driver_config()

            config["primary_master"] = {
                "addresses": self._master_addresses["primary"],
                "cell_id": self._primary_master_cell_id,
                "rpc_timeout": 5000
            }

            # Main driver config requires secondary masters
            if cell_index == 0:
                secondary_masters_info = zip(self._master_addresses["secondary"], self._secondary_masters_cell_ids)

                config["secondary_masters"] = [{"addresses": addresses_, "cell_id": cell_id}
                    for addresses_, cell_id in secondary_masters_info]

            config["timestamp_provider"]["addresses"] = self._get_cache_addresses()
            config["transaction_manager"]["default_ping_period"] = 500

            configs.append(config)

        return configs
