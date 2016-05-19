import default_configs
from helpers import versions_cmp

from yt.wrapper.common import MB
from yt.common import YtError, unlist, update
from yt.yson import YsonString

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
    def create_for_version(version, ports, enable_debug_logging, fqdn):
        if versions_cmp(version, "0.17.4") >= 0 and versions_cmp(version, "0.18") < 0:
            return ConfigsProvider_17(ports, enable_debug_logging, fqdn)
        elif versions_cmp(version, "0.18") >= 0:
            return ConfigsProvider_18(ports, enable_debug_logging, fqdn)

        raise YtError("Cannot create configs provider for version: {0}".format(version))

class ConfigsProvider(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, ports, enable_debug_logging=True, fqdn=None):
        if fqdn is None:
            self.fqdn = socket.getfqdn()
        else:
            self.fqdn = fqdn

        self.ports = ports
        self.enable_debug_logging = enable_debug_logging
        # Generated addresses
        # _master_addresses["secondary"] is list of size secondary_master_cell_count
        self._master_addresses = {"primary": [], "secondary": []}
        self._node_addresses = []

    @abc.abstractmethod
    def get_master_configs(self, master_count, nonvoting_master_count, master_dirs,
                           tmpfs_master_dirs=None, secondary_master_cell_count=0, cell_tag=0):
        pass

    @abc.abstractmethod
    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        pass

    @abc.abstractmethod
    def get_node_configs(self, node_count, node_dirs, operations_memory_limit=None):
        pass

    @abc.abstractmethod
    def get_proxy_config(self, proxy_dir):
        pass

    @abc.abstractmethod
    def get_driver_configs(self):
        pass

    @abc.abstractmethod
    def get_ui_config(self, proxy_address):
        pass

def _set_bind_retry_options(config):
    if "bus_server" not in config:
        config["bus_server"] = {}
    config["bus_server"]["bind_retry_count"] = 10
    config["bus_server"]["bind_retry_backoff"] = 3000

def _generate_common_proxy_config(proxy_dir, proxy_port, enable_debug_logging, fqdn):
    proxy_config = default_configs.get_proxy_config()
    proxy_config["proxy"]["logging"] = init_logging(proxy_config["proxy"]["logging"], proxy_dir, "http_proxy",
                                                    enable_debug_logging)
    proxy_config["port"] = proxy_port
    proxy_config["fqdn"] = "{0}:{1}".format(fqdn, proxy_port)
    proxy_config["static"].append(["/ui", os.path.join(proxy_dir, "ui")])
    proxy_config["logging"]["filename"] = os.path.join(proxy_dir, "http_application.log")

    _set_bind_retry_options(proxy_config)

    return proxy_config

def _get_hydra_manager_config():
    return {"leader_lease_check_period": 100,
            "leader_lease_timeout": 500,
            "disable_leader_lease_grace_delay": True,
            "response_keeper": {
                "expiration_time": 25000,
                "warmup_time": 30000,
            }}

def _set_memory_limit_options(config, operations_memory_limit):
    if operations_memory_limit is None:
        return

    config["exec_agent"]["job_controller"]["resource_limits"]["memory"] = operations_memory_limit

    resource_limits = config.get("resource_limits", {})
    if "memory" in resource_limits:
        return

    resource_limits["memory"] = int(1.1 * 1024 * MB) + operations_memory_limit
    update(config, {"resource_limits": resource_limits})

class ConfigsProvider_17(ConfigsProvider):
    def __init__(self, ports, enable_debug_logging=True, fqdn=None):
        super(ConfigsProvider_17, self).__init__(ports, enable_debug_logging, fqdn)
        self._master_cell_tag = 0

    def get_master_configs(self, master_count, nonvoting_master_count, master_dirs,
                           tmpfs_master_dirs=None, secondary_master_cell_count=0, cell_tag=0):
        if secondary_master_cell_count > 0:
            raise YtError("Secondary master cells are not supported in YT version <= 0.18")

        master_dirs = unlist(master_dirs)
        tmpfs_master_dirs = unlist(tmpfs_master_dirs)
        ports = unlist(self.ports["master"])

        addresses = ["{0}:{1}".format(self.fqdn, ports[2 * i]) for i in xrange(master_count)]

        configs = []

        for i in xrange(master_count):
            config = default_configs.get_master_config()

            config["rpc_port"] = ports[2 * i]
            config["monitoring_port"] = ports[2 * i + 1]
            config["address_resolver"]["localhost_fqdn"] = self.fqdn

            config["master"] = {
                "cell_tag": cell_tag,
                "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
                "addresses": addresses
            }
            config["timestamp_provider"]["addresses"] = addresses

            if tmpfs_master_dirs is None:
                config["changelogs"]["path"] = os.path.join(master_dirs[i], "changelogs")
            else:
                config["changelogs"]["path"] = os.path.join(tmpfs_master_dirs[i], "changelogs")

            config["snapshots"]["path"] = os.path.join(master_dirs[i], "snapshots")
            config["logging"] = init_logging(config["logging"], master_dirs[i], "master-" + str(i),
                                             self.enable_debug_logging)

            config["node_tracker"]["online_node_timeout"] = 1000
            _set_bind_retry_options(config)

            config["hydra_manager"] = _get_hydra_manager_config()

            configs.append(config)

        self._master_addresses["primary"] = addresses
        self._master_cell_tag = cell_tag

        return [configs]

    def _get_cluster_connection_config(self, enable_master_cache=False,
                                       timestamp_provider_from_cache=False):
        cluster_connection = {}

        cluster_connection["master"] = {
            "addresses": self._master_addresses["primary"],
            "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
            "cell_tag": self._master_cell_tag,
            "rpc_timeout": 5000
        }
        if enable_master_cache:
            cluster_connection["master_cache"] = {
                "addresses": self._master_addresses["primary"],
                "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff",
                "cell_tag": self._master_cell_tag
            }

        if timestamp_provider_from_cache and self._node_addresses:
            cluster_connection["timestamp_provider"] = {"addresses": self._node_addresses}
        else:
            cluster_connection["timestamp_provider"] = {"addresses": self._master_addresses["primary"]}

        cluster_connection["transaction_manager"] = {"ping_period": 500}

        return cluster_connection

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs = []

        for i in xrange(scheduler_count):
            config = default_configs.get_scheduler_config()

            config["address_resolver"]["localhost_fqdn"] = self.fqdn
            update(config["cluster_connection"], self._get_cluster_connection_config())

            config["rpc_port"] = self.ports["scheduler"][2 * i]
            config["monitoring_port"] = self.ports["scheduler"][2 * i + 1]
            config["scheduler"]["snapshot_temp_path"] = os.path.join(scheduler_dirs[i], "snapshots")

            config["transaction_manager"]["ping_period"] = 500

            config["logging"] = init_logging(config["logging"], scheduler_dirs[i], "scheduler-" + str(i),
                                             self.enable_debug_logging)
            _set_bind_retry_options(config)

            configs.append(config)

        return configs

    def get_node_configs(self, node_count, node_dirs, operations_memory_limit=None):
        addresses = ["{0}:{1}".format(self.fqdn, self.ports["node"][2 * i]) for i in xrange(node_count)]

        current_user = 10000

        configs = []

        for i in xrange(node_count):
            config = default_configs.get_node_config(self.enable_debug_logging)

            config["address_resolver"]["localhost_fqdn"] = self.fqdn

            config["addresses"] = {
                "default": self.fqdn,
                "interconnect": self.fqdn
            }

            config["rpc_port"] = self.ports["node"][2 * i]
            config["monitoring_port"] = self.ports["node"][2 * i + 1]

            update(config["cluster_connection"],
                   self._get_cluster_connection_config(enable_master_cache=True))

            config["data_node"]["store_locations"].append({
                "path": os.path.join(node_dirs[i], "chunk_store"),
                "low_watermark": 0,
                "high_watermark": 0
            })

            config["data_node"]["cache_locations"] = [{
                "path": os.path.join(node_dirs[i], "chunk_cache")
            }]

            config["exec_agent"]["slot_manager"]["start_uid"] = current_user
            config["exec_agent"]["slot_manager"]["paths"] = [os.path.join(node_dirs[i], "slots")]

            current_user += config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] + 1

            config["logging"] = init_logging(config["logging"], node_dirs[i], "node-{0}".format(i),
                                             self.enable_debug_logging)

            config["exec_agent"]["job_proxy_logging"] = init_logging(config["exec_agent"]["job_proxy_logging"],
                                                                     node_dirs[i], "job_proxy-{0}".format(i),
                                                                     self.enable_debug_logging)
            _set_bind_retry_options(config)
            _set_memory_limit_options(config, operations_memory_limit)

            configs.append(config)

        self._node_addresses = addresses

        return configs

    def get_proxy_config(self, proxy_dir):
        driver_config = default_configs.get_driver_config()
        update(driver_config, self._get_cluster_connection_config())

        proxy_config = _generate_common_proxy_config(proxy_dir, self.ports["proxy"],
                                                     self.enable_debug_logging, self.fqdn)
        proxy_config["proxy"]["driver"] = driver_config

        return proxy_config

    def get_driver_configs(self):
        return [update(default_configs.get_driver_config(),
                       self._get_cluster_connection_config(timestamp_provider_from_cache=True))]

    def get_ui_config(self, proxy_address):
        return default_configs.get_ui_config()\
            .replace("%%proxy_address%%", "'{0}'".format(proxy_address))\
            .replace("%%masters%%",
                "masters: [ {0} ]".format(
                    ", ".join(["'{0}'".format(address) for address in self._master_addresses["primary"]])))

class ConfigsProvider_18(ConfigsProvider):
    def __init__(self, ports, enable_debug_logging=True, fqdn=None):
        super(ConfigsProvider_18, self).__init__(ports, enable_debug_logging, fqdn)
        self._primary_master_cell_id = 0
        self._secondary_masters_cell_ids = []

    def get_master_configs(self, master_count, nonvoting_master_count, master_dirs,
                           tmpfs_master_dirs=None, secondary_master_cell_count=0, cell_tag=0):
        addresses = []

        self._secondary_master_cell_count = secondary_master_cell_count
        self._primary_master_cell_tag = cell_tag
        self._primary_master_cell_id = "ffffffff-ffffffff-%x0259-ffffffff" % cell_tag
        self._secondary_masters_cell_tags = [cell_tag + index + 1
                                             for index in xrange(secondary_master_cell_count)]
        self._secondary_masters_cell_ids = ["ffffffff-ffffffff-%x0259-ffffffff" % tag
                                            for tag in self._secondary_masters_cell_tags]

        # Primary masters cell index is 0
        for cell_index in xrange(secondary_master_cell_count + 1):
            cell_addresses = []
            for i in xrange(master_count):
                voting = (i < master_count - nonvoting_master_count)
                address = YsonString("{0}:{1}".format(self.fqdn, self.ports["master"][cell_index][2 * i]))
                if not voting:
                    address.attributes["voting"] = False
                cell_addresses.append(address)
            addresses.append(cell_addresses)

        cell_configs = []

        for cell_index in xrange(secondary_master_cell_count + 1):
            configs = []

            for master_index in xrange(master_count):
                config = default_configs.get_master_config()

                config["address_resolver"]["localhost_fqdn"] = self.fqdn

                config["hydra_manager"] = _get_hydra_manager_config()

                config["security_manager"]["user_statistics_gossip_period"] = 80
                config["security_manager"]["account_statistics_gossip_period"] = 80

                config["node_tracker"]["node_states_gossip_period"] = 80

                config["rpc_port"] = self.ports["master"][cell_index][2 * master_index]
                config["monitoring_port"] = self.ports["master"][cell_index][2 * master_index + 1]

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
                config["snapshots"]["path"] = os.path.join(master_dirs[cell_index][master_index], "snapshots")

                if tmpfs_master_dirs is None:
                    config["changelogs"]["path"] = os.path.join(master_dirs[cell_index][master_index], "changelogs")
                else:
                    config["changelogs"]["path"] = os.path.join(tmpfs_master_dirs[cell_index][master_index], "changelogs")

                config["logging"] = init_logging(config["logging"], master_dirs[cell_index][master_index],
                                                 "master-" + str(master_index), self.enable_debug_logging)

                config["tablet_manager"] = {
                    "cell_scan_period": 100
                }

                config["multicell_manager"] = {
                    "cell_statistics_gossip_period": 80
                }

                _set_bind_retry_options(config)

                configs.append(config)

            cell_configs.append(configs)

            if cell_index == 0:
                self._master_addresses["primary"] = addresses[cell_index]
            else:
                self._master_addresses["secondary"].append(addresses[cell_index])

        return cell_configs

    def _get_cluster_connection_config(self, enable_master_cache=False, timestamp_provider_from_cache=False):
        cluster_connection = {}

        cluster_connection["primary_master"] = {
            "addresses": self._master_addresses["primary"],
            "cell_id": self._primary_master_cell_id,
            "rpc_timeout": 5000
        }

        if enable_master_cache:
            cluster_connection["master_cache"] = {
                "cell_id": self._primary_master_cell_id,
                "addresses": self._master_addresses["primary"]
            }

        secondary_masters_info = zip(self._master_addresses["secondary"], self._secondary_masters_cell_ids)
        cluster_connection["secondary_masters"] = []
        for addresses, cell_id in secondary_masters_info:
            cluster_connection["secondary_masters"].append({
                "addresses": addresses,
                "cell_id": cell_id
            })

        if timestamp_provider_from_cache and self._node_addresses:
            cluster_connection["timestamp_provider"] = {"addresses": self._node_addresses}
        else:
            cluster_connection["timestamp_provider"] = {"addresses": self._master_addresses["primary"]}

        cluster_connection["transaction_manager"] = {"default_ping_period": 500}

        return cluster_connection

    def get_scheduler_configs(self, scheduler_count, scheduler_dirs):
        configs = []

        for i in xrange(scheduler_count):
            config = default_configs.get_scheduler_config()

            config["address_resolver"]["localhost_fqdn"] = self.fqdn
            update(config["cluster_connection"],
                   self._get_cluster_connection_config())

            config["rpc_port"] = self.ports["scheduler"][2 * i]
            config["monitoring_port"] = self.ports["scheduler"][2 * i + 1]
            config["scheduler"]["snapshot_temp_path"] = os.path.join(scheduler_dirs[i], "snapshots")

            config["transaction_manager"]["default_ping_period"] = 500

            config["logging"] = init_logging(config["logging"], scheduler_dirs[i], "scheduler-" + str(i),
                                             self.enable_debug_logging)
            _set_bind_retry_options(config)

            configs.append(config)

        return configs

    def get_proxy_config(self, proxy_dir):
        driver_config = default_configs.get_driver_config()
        update(driver_config, self._get_cluster_connection_config())

        proxy_config = _generate_common_proxy_config(proxy_dir, self.ports["proxy"],
                                                     self.enable_debug_logging, self.fqdn)
        proxy_config["fqdn"] = self.fqdn
        proxy_config["proxy"]["driver"] = driver_config

        return proxy_config

    def get_node_configs(self, node_count, node_dirs, operations_memory_limit=None):
        addresses = ["{0}:{1}".format(self.fqdn, self.ports["node"][2 * i]) for i in xrange(node_count)]

        current_user = 10000

        configs = []

        for i in xrange(node_count):
            config = default_configs.get_node_config(self.enable_debug_logging)

            config["address_resolver"]["localhost_fqdn"] = self.fqdn

            config["addresses"] = {
                "default": self.fqdn,
                "interconnect": self.fqdn
            }

            config["rpc_port"] = self.ports["node"][2 * i]
            config["monitoring_port"] = self.ports["node"][2 * i + 1]

            config["cell_directory_synchronizer"] = {
                "sync_period": 1000
            }
            update(config["cluster_connection"],
                   self._get_cluster_connection_config(enable_master_cache=True))

            config["data_node"]["read_thread_count"] = 2
            config["data_node"]["write_thread_count"] = 2

            config["data_node"]["multiplexed_changelog"] = {}
            config["data_node"]["multiplexed_changelog"]["path"] = os.path.join(node_dirs[i], "multiplexed")

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
            config["tablet_node"]["hydra_manager"] = _get_hydra_manager_config()
            config["tablet_node"]["hydra_manager"]["restart_backoff_time"] = 100
            _set_bind_retry_options(config)
            _set_memory_limit_options(config, operations_memory_limit)

            configs.append(config)

        self._node_addresses = addresses

        return configs

    def get_driver_configs(self):
        configs = []

        for cell_index in xrange(self._secondary_master_cell_count + 1):
            config = default_configs.get_driver_config()
            update(config, self._get_cluster_connection_config(timestamp_provider_from_cache=True))

            # Only main driver config requires secondary masters
            if cell_index != 0 and "secondary_masters" in config:
                del config["secondary_masters"]

            configs.append(config)

        return configs

    def get_ui_config(self, proxy_address):
        address_blocks = []
        # Primary masters cell index is 0
        for cell_index in xrange(self._secondary_master_cell_count + 1):
            if cell_index == 0:
                cell_addresses = self._master_addresses["primary"]
                cell_tag = self._primary_master_cell_tag
            else:
                cell_addresses = self._master_addresses["secondary"][cell_index - 1]
                cell_tag = self._secondary_masters_cell_tags[cell_index - 1]
            block = "{{ addresses: [ '{0}' ], cellTag: {1} }}"\
                .format("', '".join(cell_addresses), cell_tag)
            address_blocks.append(block)
        masters = "primaryMaster: {0}".format(address_blocks[0])
        if self._secondary_master_cell_count:
            masters += ", secondaryMasters: [ '{0}' ]".format("', '".join(address_blocks[1:]))
        return default_configs.get_ui_config()\
            .replace("%%proxy_address%%", "'{0}'".format(proxy_address))\
            .replace("%%masters%%", masters)
