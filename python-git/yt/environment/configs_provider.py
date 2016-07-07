import default_configs

from yt.wrapper.common import MB
from yt.wrapper.mappings import VerifiedDict
from yt.common import YtError, unlist, update, get_value
from yt.yson import YsonString

import socket
import abc
import os
from copy import deepcopy

"""
TLDR: If you want to support new version of ytserver you should create your own ConfigsProvider
with overloaded necessary _build_*_configs methods and add it to dict VERSION_TO_CONFIGS_PROVIDER_CLASS
below. If you want to modify only local mode configs then modify_cluster_configuration function in
yt/local/cluster_configuration.py should be updated.

Configs for YT are generated the following way:
    1. For each version of ytserver ConfigsProvider_* is created
    2. ConfigsProvider takes provision (defines how many masters cells, nodes etc. to start and so on),
       directories where to store files for each service replica and ports generator in its build_configs()
       method and returns dict with cluster configuration.

       By default each config for each service replica is built from default config (see default_configs.py)
       by applying some patches and filling necessary (version specific) fields.

       Cluster configuration is formed by calling abstract _build_*_configs methods and then
       storing results in the dict with the following structure:
       {
           "master": {
               "primary_cell_tag": "123",
               "secondary_cell_tags": ["1", "2", "3"]  <-- only for ytserver 18.x (empty for older versions)
               "1": [
                   ...      <-- configs for master cell with tag "1"
               ],
               ...
           },
           "scheduler": [
                ...         <-- configs for each scheduler replica
           ],
           "node": [
                ...         <-- configs for each node replica
           ],
           "proxy": ...     <-- config for proxy
           "ui": ...        <-- config for ui
           "driver": {
                "1": ...    <-- connection config for cell with tag "1"
           }
       }

    3. Cluster configuration dict also can be modified before YT startup with modify_configs_func (see yt_env.py)
    3*. Local mode uses the same config providers but applies patches actual only for local mode by
        defining its own modify_configs_func.
"""

VERSION_TO_CONFIGS_PROVIDER_CLASS = {
    "0.17.4": "ConfigsProvider_17_4",
    "0.17.5": "ConfigsProvider_17_5",
    "18.3": "ConfigsProvider_18_3",
    "18.4": "ConfigsProvider_18_4",
    "18.5": "ConfigsProvider_18_5"
}

def create_configs_provider(version):
    if version.startswith("18."):
        # XXX(asaitgalin): Drops git depth from version.
        # Only major and minor version components are important.
        version = ".".join(version.split(".")[:2])

    configs_provider_class = VERSION_TO_CONFIGS_PROVIDER_CLASS.get(version)
    if configs_provider_class is None:
        raise YtError("Cannot create configs provider for version: {0}".format(version))
    return globals()[configs_provider_class]()

_default_provision = {
    "master": {
        "primary_cell_tag": 0,
        "secondary_cell_count": 0,
        "cell_size": 1,
        "cell_nonvoting_master_count": 0
    },
    "scheduler": {
        "count": 1
    },
    "node": {
        "count": 1,
        "operations_memory_limit": None
    },
    "proxy": {
        "enable": False,
        "http_port": None
    },
    "enable_debug_logging": True,
    "fqdn": socket.getfqdn()
}

def get_default_provision():
    return VerifiedDict([], None, deepcopy(_default_provision))

class ConfigsProvider(object):
    __metaclass__ = abc.ABCMeta

    def build_configs(self, ports_generator, master_dirs, master_tmpfs_dirs=None, scheduler_dirs=None,
                      node_dirs=None, proxy_dir=None, provision=None):
        provision = get_value(provision, get_default_provision())

        # XXX(asaitgalin): All services depend on master so it is useful to make
        # connection configs with addresses and other useful info about all master cells.
        master_configs, connection_configs = self._build_master_configs(
            provision,
            master_dirs,
            master_tmpfs_dirs,
            ports_generator)

        driver_configs = self._build_driver_configs(provision, deepcopy(connection_configs))
        scheduler_configs = self._build_scheduler_configs(provision, scheduler_dirs, deepcopy(connection_configs),
                                                          ports_generator)
        node_configs = self._build_node_configs(provision, node_dirs, deepcopy(connection_configs), ports_generator)
        proxy_config = self._build_proxy_config(provision, proxy_dir, deepcopy(connection_configs), ports_generator)
        ui_config = self._build_ui_config(provision, deepcopy(connection_configs),
                                          "{0}:{1}".format(provision["fqdn"], proxy_config["port"]))

        cluster_configuration = {
            "master": master_configs,
            "driver": driver_configs,
            "scheduler": scheduler_configs,
            "node": node_configs,
            "proxy": proxy_config,
            "ui": ui_config
        }

        return cluster_configuration

    @abc.abstractmethod
    def _build_master_configs(self, provision, master_dirs, master_tmpfs_dirs, ports_generator):
        pass

    @abc.abstractmethod
    def _build_scheduler_configs(self, provision, scheduler_dirs, master_connection_configs,
                                 ports_generator):
        pass

    @abc.abstractmethod
    def _build_node_configs(self, provision, node_dirs, master_connection_configs, ports_generator):
        pass

    @abc.abstractmethod
    def _build_proxy_config(self, provision, proxy_dir, master_connection_configs, ports_generator):
        pass

    @abc.abstractmethod
    def _build_driver_configs(self, provision, master_connection_configs):
        pass

    @abc.abstractmethod
    def _build_ui_config(self, provision, master_connection_configs, proxy_address):
        pass

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

DEFAULT_TRANSACTION_PING_PERIOD = 500

def _set_bind_retry_options(config):
    if "bus_server" not in config:
        config["bus_server"] = {}
    config["bus_server"]["bind_retry_count"] = 10
    config["bus_server"]["bind_retry_backoff"] = 3000

def _generate_common_proxy_config(proxy_dir, proxy_port, enable_debug_logging, fqdn, ports_generator):
    proxy_config = default_configs.get_proxy_config()
    proxy_config["proxy"]["logging"] = init_logging(proxy_config["proxy"]["logging"], proxy_dir, "http_proxy",
                                                    enable_debug_logging)
    proxy_config["port"] = proxy_port if proxy_port else next(ports_generator)
    proxy_config["fqdn"] = "{0}:{1}".format(fqdn, proxy_config["port"])
    proxy_config["static"].append(["/ui", os.path.join(proxy_dir, "ui")])
    proxy_config["logging"]["filename"] = os.path.join(proxy_dir, "http_application.log")

    _set_bind_retry_options(proxy_config)

    return proxy_config

def _get_hydra_manager_config():
    return {
        "leader_lease_check_period": 100,
        "leader_lease_timeout": 5000,
        "disable_leader_lease_grace_delay": True,
        "response_keeper": {
            "expiration_time": 25000,
            "warmup_time": 30000,
        }
    }

def _get_retrying_channel_config():
    return {
        "retry_backoff_time": 0,
        "retry_attempts": 5,
        "soft_backoff_time": 10,
        "hard_backoff_time": 10
    }

def _get_rpc_config():
    return {
        "rpc_timeout": 5000
    }

def _set_memory_limit_options(config, operations_memory_limit):
    if operations_memory_limit is None:
        return

    config["exec_agent"]["job_controller"]["resource_limits"]["memory"] = operations_memory_limit

    resource_limits = config.get("resource_limits", {})
    resource_limits.setdefault("memory", 0)

    resource_limits["memory"] = max(resource_limits["memory"],
                                    int(1.1 * 1024 * MB) + operations_memory_limit)

    update(config, {"resource_limits": resource_limits})

class ConfigsProvider_17(ConfigsProvider):
    def _build_master_configs(self, provision, master_dirs, master_tmpfs_dirs, ports_generator):
        if provision["master"]["secondary_cell_count"] > 0:
            raise YtError("Secondary master cells are not supported in YT version <= 0.18")

        master_dirs = unlist(master_dirs)
        master_tmpfs_dirs = unlist(master_tmpfs_dirs)

        master_cell_size = provision["master"]["cell_size"]

        ports = [next(ports_generator) for _ in xrange(2 * master_cell_size)]
        addresses = ["{0}:{1}".format(provision["fqdn"], ports[2 * i]) for i in xrange(master_cell_size)]

        cell_tag = str(provision["master"]["primary_cell_tag"])
        connection_configs = {
            cell_tag: {
                "addresses": addresses,
                "cell_tag": int(cell_tag),
                "cell_id": "ffffffff-ffffffff-ffffffff-ffffffff"
            },
            "primary_cell_tag": cell_tag,
            "secondary_cell_tags": []
        }

        configs = []
        for i in xrange(master_cell_size):
            config = default_configs.get_master_config()

            config["rpc_port"] = ports[2 * i]
            config["monitoring_port"] = ports[2 * i + 1]
            config["address_resolver"]["localhost_fqdn"] = provision["fqdn"]

            config["master"] = connection_configs[cell_tag]
            config["timestamp_provider"]["addresses"] = addresses

            if master_tmpfs_dirs is None:
                config["changelogs"]["path"] = os.path.join(master_dirs[i], "changelogs")
            else:
                config["changelogs"]["path"] = os.path.join(master_tmpfs_dirs[i], "changelogs")

            config["snapshots"]["path"] = os.path.join(master_dirs[i], "snapshots")
            config["logging"] = init_logging(config["logging"], master_dirs[i], "master-" + str(i),
                                             provision["enable_debug_logging"])

            config["node_tracker"]["online_node_timeout"] = 1000
            _set_bind_retry_options(config)

            config["hydra_manager"] = _get_hydra_manager_config()

            configs.append(config)

        cell_configs = {
            cell_tag: configs,
            "primary_cell_tag": cell_tag,
            "secondary_cell_tags": []
        }

        return cell_configs, connection_configs

    def _build_cluster_connection_config(self, master_connection_configs, enable_master_cache=False):
        primary_cell_tag = master_connection_configs["primary_cell_tag"]
        connection_config = master_connection_configs[primary_cell_tag]

        cluster_connection = {
            "master": connection_config,
            "timestamp_provider": {
                "addresses": connection_config["addresses"]
            },
            "transaction_manager": {
                "ping_period": DEFAULT_TRANSACTION_PING_PERIOD
            }
        }
        update(cluster_connection["master"], _get_retrying_channel_config())
        update(cluster_connection["master"], _get_rpc_config())

        if enable_master_cache:
            cluster_connection["master_cache"] = connection_config

        return cluster_connection

    def _build_scheduler_configs(self, provision, scheduler_dirs, master_connection_configs,
                                 ports_generator):
        configs = []

        for i in xrange(provision["scheduler"]["count"]):
            config = default_configs.get_scheduler_config()

            config["address_resolver"]["localhost_fqdn"] = provision["fqdn"]
            update(config["cluster_connection"],
                   self._build_cluster_connection_config(master_connection_configs))

            config["rpc_port"] = next(ports_generator)
            config["monitoring_port"] = next(ports_generator)
            config["scheduler"]["snapshot_temp_path"] = os.path.join(scheduler_dirs[i], "snapshots")

            config["logging"] = init_logging(config["logging"], scheduler_dirs[i], "scheduler-" + str(i),
                                             provision["enable_debug_logging"])
            _set_bind_retry_options(config)

            configs.append(config)

        return configs

    def _build_node_configs(self, provision, node_dirs, master_connection_configs, ports_generator):
        current_user = 10001

        configs = []

        for i in xrange(provision["node"]["count"]):
            config = default_configs.get_node_config(provision["enable_debug_logging"])

            config["address_resolver"]["localhost_fqdn"] = provision["fqdn"]

            config["addresses"] = {
                "default": provision["fqdn"],
                "interconnect": provision["fqdn"]
            }

            config["rpc_port"] = next(ports_generator)
            config["monitoring_port"] = next(ports_generator)

            update(config["cluster_connection"],
                   self._build_cluster_connection_config(master_connection_configs, enable_master_cache=True))

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
                                             provision["enable_debug_logging"])

            config["exec_agent"]["enable_cgroups"] = False
            config["exec_agent"]["environment_manager"] = {"environments": {"default": {"type": "unsafe"}}}

            config["exec_agent"]["job_proxy_logging"] = init_logging(config["exec_agent"]["job_proxy_logging"],
                                                                     node_dirs[i], "job_proxy-{0}".format(i),
                                                                     provision["enable_debug_logging"])
            _set_bind_retry_options(config)
            _set_memory_limit_options(config, provision["node"]["operations_memory_limit"])

            configs.append(config)

        return configs

    def _build_proxy_config(self, provision, proxy_dir, master_connection_configs, ports_generator):
        driver_config = default_configs.get_driver_config()
        update(driver_config, self._build_cluster_connection_config(master_connection_configs))

        proxy_config = _generate_common_proxy_config(proxy_dir, provision["proxy"]["http_port"],
                                                     provision["enable_debug_logging"], provision["fqdn"],
                                                     ports_generator)
        proxy_config["proxy"]["driver"] = driver_config

        return proxy_config

    def _build_driver_configs(self, provision, master_connection_configs):
        connection_config = self._build_cluster_connection_config(master_connection_configs)
        driver_config = update(default_configs.get_driver_config(), connection_config)
        return {str(provision["master"]["primary_cell_tag"]): driver_config}

    def _build_ui_config(self, provision, master_connection_configs, proxy_address):
        cell_tag = master_connection_configs["primary_cell_tag"]
        return default_configs.get_ui_config()\
            .replace("%%proxy_address%%", "'{0}'".format(proxy_address))\
            .replace("%%masters%%", "masters: [ {0} ]".format(
                     ", ".join(["'{0}'".format(address)
                               for address in master_connection_configs[cell_tag]["addresses"]])))

class ConfigsProvider_17_4(ConfigsProvider_17):
    pass

class ConfigsProvider_17_5(ConfigsProvider_17):
    pass

class ConfigsProvider_18(ConfigsProvider):
    def _build_master_configs(self, provision, master_dirs, master_tmpfs_dirs, ports_generator):
        ports = []

        cell_tags = [str(provision["master"]["primary_cell_tag"] + index)
                     for index in xrange(provision["master"]["secondary_cell_count"] + 1)]
        cell_ids = ["ffffffff-ffffffff-%x0259-ffffffff" % int(tag) for tag in cell_tags]

        nonvoting_master_count = provision["master"]["cell_nonvoting_master_count"]

        connection_configs = {}
        for cell_index in xrange(provision["master"]["secondary_cell_count"] + 1):
            cell_ports = []
            cell_addresses = []

            for i in xrange(provision["master"]["cell_size"]):
                rpc_port, monitoring_port = next(ports_generator), next(ports_generator)
                address = YsonString("{0}:{1}".format(provision["fqdn"], rpc_port))
                if i >= provision["master"]["cell_size"] - nonvoting_master_count:
                    address.attributes["voting"] = False
                cell_addresses.append(address)
                cell_ports.append((rpc_port, monitoring_port))

            ports.append(cell_ports)

            connection_config = {
                "addresses": cell_addresses,
                "cell_id": cell_ids[cell_index]
            }
            connection_configs[cell_tags[cell_index]] = connection_config

        connection_configs["primary_cell_tag"] = cell_tags[0]
        connection_configs["secondary_cell_tags"] = cell_tags[1:]

        configs = {}
        for cell_index in xrange(provision["master"]["secondary_cell_count"] + 1):
            cell_configs = []

            for master_index in xrange(provision["master"]["cell_size"]):
                config = default_configs.get_master_config()

                config["address_resolver"]["localhost_fqdn"] = provision["fqdn"]

                config["hydra_manager"] = _get_hydra_manager_config()

                config["security_manager"]["user_statistics_gossip_period"] = 80
                config["security_manager"]["account_statistics_gossip_period"] = 80

                config["node_tracker"]["node_states_gossip_period"] = 80

                config["rpc_port"], config["monitoring_port"] = ports[cell_index][master_index]

                config["primary_master"] = connection_configs[cell_tags[0]]
                config["secondary_masters"] = [connection_configs[tag]
                                               for tag in connection_configs["secondary_cell_tags"]]

                config["timestamp_provider"]["addresses"] = connection_configs[cell_tags[0]]["addresses"]
                config["snapshots"]["path"] = os.path.join(master_dirs[cell_index][master_index], "snapshots")

                if master_tmpfs_dirs is None:
                    config["changelogs"]["path"] = os.path.join(master_dirs[cell_index][master_index], "changelogs")
                else:
                    config["changelogs"]["path"] = os.path.join(master_tmpfs_dirs[cell_index][master_index], "changelogs")

                config["logging"] = init_logging(config["logging"], master_dirs[cell_index][master_index],
                                                 "master-" + str(master_index), provision["enable_debug_logging"])

                update(config, {
                    "tablet_manager": {
                        "cell_scan_period": 100
                    },
                    "multicell_manager": {
                        "cell_statistics_gossip_period": 80
                    }
                })

                _set_bind_retry_options(config)

                cell_configs.append(config)

            configs[cell_tags[cell_index]] = cell_configs

        configs["primary_cell_tag"] = cell_tags[0]
        configs["secondary_cell_tags"] = cell_tags[1:]

        return configs, connection_configs

    def _build_cluster_connection_config(self, master_connection_configs, enable_master_cache=False):
        cluster_connection = {}

        primary_cell_tag = master_connection_configs["primary_cell_tag"]
        secondary_cell_tags = master_connection_configs["secondary_cell_tags"]

        cluster_connection = {
            "primary_master": master_connection_configs[primary_cell_tag],
            "transaction_manager": {
                "default_ping_period": DEFAULT_TRANSACTION_PING_PERIOD
            },
            "timestamp_provider": {
                "addresses": master_connection_configs[primary_cell_tag]["addresses"]
            }
        }
        update(cluster_connection["primary_master"], _get_retrying_channel_config())
        update(cluster_connection["primary_master"], _get_rpc_config())

        if enable_master_cache:
            cluster_connection["master_cache"] = master_connection_configs[primary_cell_tag]

        cluster_connection["secondary_masters"] = []
        for tag in secondary_cell_tags:
            config = master_connection_configs[tag]
            update(config, _get_retrying_channel_config())
            update(config, _get_rpc_config())
            cluster_connection["secondary_masters"].append(config)

        return cluster_connection

    def _build_scheduler_configs(self, provision, scheduler_dirs, master_connection_configs,
                                 ports_generator):
        configs = []

        for i in xrange(provision["scheduler"]["count"]):
            config = default_configs.get_scheduler_config()

            config["address_resolver"]["localhost_fqdn"] = provision["fqdn"]
            update(config["cluster_connection"],
                   self._build_cluster_connection_config(master_connection_configs))

            config["rpc_port"] = next(ports_generator)
            config["monitoring_port"] = next(ports_generator)
            config["scheduler"]["snapshot_temp_path"] = os.path.join(scheduler_dirs[i], "snapshots")

            config["logging"] = init_logging(config["logging"], scheduler_dirs[i], "scheduler-" + str(i),
                                             provision["enable_debug_logging"])
            _set_bind_retry_options(config)

            configs.append(config)

        return configs

    def _build_proxy_config(self, provision, proxy_dir, master_connection_configs, ports_generator):
        driver_config = default_configs.get_driver_config()
        update(driver_config, self._build_cluster_connection_config(master_connection_configs))

        proxy_config = _generate_common_proxy_config(proxy_dir, provision["proxy"]["http_port"],
                                                     provision["enable_debug_logging"], provision["fqdn"],
                                                     ports_generator)
        proxy_config["proxy"]["driver"] = driver_config

        return proxy_config

    def _build_node_configs(self, provision, node_dirs, master_connection_configs, ports_generator):
        configs = []

        for i in xrange(provision["node"]["count"]):
            config = default_configs.get_node_config(provision["enable_debug_logging"])

            config["address_resolver"]["localhost_fqdn"] = provision["fqdn"]

            config["addresses"] = {
                "default": provision["fqdn"],
                "interconnect": provision["fqdn"]
            }

            config["rpc_port"] = next(ports_generator)
            config["monitoring_port"] = next(ports_generator)

            config["cell_directory_synchronizer"] = {
                "sync_period": 1000
            }
            update(config["cluster_connection"],
                   self._build_cluster_connection_config(master_connection_configs, enable_master_cache=True))

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

            config["logging"] = init_logging(config["logging"], node_dirs[i], "node-{0}".format(i),
                                             provision["enable_debug_logging"])
            config["exec_agent"]["job_proxy_logging"] = init_logging(config["exec_agent"]["job_proxy_logging"],
                                                                     node_dirs[i], "job_proxy-{0}".format(i),
                                                                     provision["enable_debug_logging"])
            config["tablet_node"]["hydra_manager"] = _get_hydra_manager_config()
            config["tablet_node"]["hydra_manager"]["restart_backoff_time"] = 100
            _set_bind_retry_options(config)
            _set_memory_limit_options(config, provision["node"]["operations_memory_limit"])

            configs.append(config)

        return configs

    def _build_driver_configs(self, provision, master_connection_configs):
        secondary_cell_tags = master_connection_configs["secondary_cell_tags"]
        primary_cell_tag = master_connection_configs["primary_cell_tag"]

        configs = {}
        for cell_index in xrange(provision["master"]["secondary_cell_count"] + 1):
            config = default_configs.get_driver_config()
            if cell_index == 0:
                tag = primary_cell_tag
                update(config, self._build_cluster_connection_config(master_connection_configs))
            else:
                tag = secondary_cell_tags[cell_index - 1]
                cell_connection_config = {
                    "primary_master": master_connection_configs[secondary_cell_tags[cell_index - 1]],
                    "timestamp_provider": {
                        "addresses": master_connection_configs[primary_cell_tag]["addresses"]
                    },
                    "transaction_manager": {
                        "default_ping_period": DEFAULT_TRANSACTION_PING_PERIOD
                    }
                }
                update(cell_connection_config["primary_master"], _get_retrying_channel_config())
                update(cell_connection_config["primary_master"], _get_rpc_config())

                update(config, cell_connection_config)

            configs[tag] = config

        return configs

    def _build_ui_config(self, provision, master_connection_configs, proxy_address):
        address_blocks = []
        # Primary masters cell index is 0
        for cell_index in xrange(provision["master"]["secondary_cell_count"] + 1):
            if cell_index == 0:
                cell_tag = master_connection_configs["primary_cell_tag"]
                cell_addresses = master_connection_configs[cell_tag]["addresses"]
            else:
                cell_tag = master_connection_configs["secondary_cell_tags"][cell_index - 1]
                cell_addresses = master_connection_configs[cell_tag]["addresses"]
            block = "{{ addresses: [ '{0}' ], cellTag: {1} }}"\
                .format("', '".join(cell_addresses), int(cell_tag))
            address_blocks.append(block)
        masters = "primaryMaster: {0}".format(address_blocks[0])
        if provision["master"]["secondary_cell_count"]:
            masters += ", secondaryMasters: [ '{0}' ]".format("', '".join(address_blocks[1:]))
        return default_configs.get_ui_config()\
            .replace("%%proxy_address%%", "'{0}'".format(proxy_address))\
            .replace("%%masters%%", masters)

class ConfigsProvider_18_3_18_4(ConfigsProvider_18):
    def _build_node_configs(self, provision, node_dirs, master_connection_configs, ports_generator):
        configs = super(ConfigsProvider_18_3_18_4, self)._build_node_configs(
                provision, node_dirs, master_connection_configs, ports_generator)

        current_user = 10000

        for i, config in enumerate(configs):
            config["exec_agent"]["slot_manager"]["start_uid"] = current_user
            config["exec_agent"]["slot_manager"]["paths"] = [os.path.join(node_dirs[i], "slots")]

            config["exec_agent"]["enable_cgroups"] = False
            config["exec_agent"]["environment_manager"] = {"environments": {"default": {"type": "unsafe"}}}

            current_user += config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] + 1

        return configs

class ConfigsProvider_18_3(ConfigsProvider_18_3_18_4):
    pass

class ConfigsProvider_18_4(ConfigsProvider_18_3_18_4):
    pass

class ConfigsProvider_18_5(ConfigsProvider_18):
    def _build_node_configs(self, provision, node_dirs, master_connection_configs, ports_generator):
        configs = super(ConfigsProvider_18_5, self)._build_node_configs(
                provision, node_dirs, master_connection_configs, ports_generator)

        current_user = 10000
        for i, config in enumerate(configs):
            config["addresses"] = [
                ("interconnect", provision["fqdn"]),
                ("default", provision["fqdn"])
            ]

            # TODO(psushin): This is a very dirty hack to ensure that different parallel
            # test and yt_node instances do not share same uids range for user jobs.
            start_uid = current_user + config["rpc_port"]

            config["exec_agent"]["slot_manager"]["job_environment"] = {
                "type": "simple",
                "start_uid" : start_uid,
            }
            config["exec_agent"]["slot_manager"]["locations"] = [{"path" : os.path.join(node_dirs[i], "slots")}]

        return configs
