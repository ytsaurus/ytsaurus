from . import default_config
from .helpers import canonize_uuid

from yt.wrapper.common import MB, GB
from yt.wrapper.mappings import VerifiedDict
from yt.common import YtError, get_value, update, update_inplace

from yt.yson import to_yson_type

from yt.packages.six import iteritems, add_metaclass
from yt.packages.six.moves import xrange

import random
import socket
import abc
import os
from copy import deepcopy


def _get_timestamp_provider_addresses(yt_config, master_connection_configs, clock_connection_configs):
    if yt_config.clock_count == 0:
        return master_connection_configs[master_connection_configs["primary_cell_tag"]]["addresses"]
    else:
        return clock_connection_configs[clock_connection_configs["cell_tag"]]["addresses"]


def build_configs(yt_config, ports_generator, dirs, logs_dir):
    clock_configs, clock_connection_configs = _build_clock_configs(
        yt_config,
        dirs["clock"],
        dirs["clock_tmpfs"],
        ports_generator,
        logs_dir)

    master_configs, master_connection_configs = _build_master_configs(
        yt_config,
        dirs["master"],
        dirs["master_tmpfs"],
        clock_connection_configs,
        ports_generator,
        logs_dir)

    scheduler_configs = _build_scheduler_configs(dirs["scheduler"], deepcopy(master_connection_configs), deepcopy(clock_connection_configs),
                                                 ports_generator, logs_dir, yt_config)

    controller_agent_configs = _build_controller_agent_configs(dirs["controller_agent"], deepcopy(master_connection_configs), deepcopy(clock_connection_configs),
                                                               ports_generator, logs_dir, yt_config)

    node_configs, node_addresses = _build_node_configs(
        dirs["node"],
        dirs["node_tmpfs"],
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_configs),
        ports_generator,
        logs_dir,
        yt_config)

    http_proxy_configs = _build_http_proxy_config(dirs["http_proxy"], deepcopy(master_connection_configs), deepcopy(clock_connection_configs),
                                                  ports_generator, logs_dir, master_cache_nodes=node_addresses, yt_config=yt_config)

    http_proxy_url = None
    if yt_config.http_proxy_count > 0:
        http_proxy_url = "{0}:{1}".format(yt_config.fqdn, http_proxy_configs[0]["port"])

    rpc_proxy_configs = _build_rpc_proxy_configs(logs_dir, deepcopy(master_connection_configs), deepcopy(clock_connection_configs),
                                                    node_addresses, ports_generator, yt_config)

    rpc_client_config = None
    rpc_proxy_addresses = None
    if yt_config.rpc_proxy_count > 0:
        rpc_proxy_addresses = [
            "{0}:{1}".format(yt_config.fqdn, rpc_proxy_config["rpc_port"])
            for rpc_proxy_config in rpc_proxy_configs
        ]
        rpc_client_config = {
            "connection_type": "rpc",
            "addresses": rpc_proxy_addresses
        }

    driver_configs = _build_native_driver_configs(
        deepcopy(master_connection_configs), deepcopy(clock_connection_configs),
        master_cache_nodes=node_addresses,
        yt_config=yt_config,
    )

    rpc_driver_config = _build_rpc_driver_config(
        deepcopy(master_connection_configs), deepcopy(clock_connection_configs),
        master_cache_nodes=node_addresses, rpc_proxy_addresses=rpc_proxy_addresses, http_proxy_url=http_proxy_url,
        yt_config=yt_config,
    )

    cluster_configuration = {
        "master": master_configs,
        "clock": clock_configs,
        "driver": driver_configs,
        "rpc_driver": rpc_driver_config,
        "scheduler": scheduler_configs,
        "controller_agent": controller_agent_configs,
        "node": node_configs,
        "http_proxy": http_proxy_configs,
        "rpc_proxy": rpc_proxy_configs,
        "rpc_client": rpc_client_config,
    }

    return cluster_configuration

def _build_master_configs(yt_config, master_dirs, master_tmpfs_dirs, clock_connection_configs,
                          ports_generator, logs_dir):
    ports = []

    cell_tags = [str(yt_config.primary_cell_tag + index)
                 for index in xrange(yt_config.secondary_cell_count + 1)]
    random_part = random.randint(0, 2 ** 32 - 1)
    cell_ids = [canonize_uuid("%x-ffffffff-%x0259-ffffffff" % (random_part, int(tag)))
                for tag in cell_tags]

    nonvoting_master_count = yt_config.nonvoting_master_count

    connection_configs = {}
    for cell_index in xrange(yt_config.secondary_cell_count + 1):
        cell_ports = []
        cell_addresses = []

        for i in xrange(yt_config.master_count):
            rpc_port, monitoring_port = next(ports_generator), next(ports_generator)
            address = to_yson_type("{0}:{1}".format(yt_config.fqdn, rpc_port))
            if i >= yt_config.master_count - nonvoting_master_count:
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
    for cell_index in xrange(yt_config.secondary_cell_count + 1):
        cell_configs = []

        for master_index in xrange(yt_config.master_count):
            config = default_config.get_master_config()

            init_singletons(config, yt_config.fqdn, "master", {
                "cell_role": "primary" if cell_index == 0 else "secondary",
                "master_index": str(master_index),
            })

            config["hydra_manager"] = _get_hydra_manager_config()

            config["rpc_port"], config["monitoring_port"] = ports[cell_index][master_index]

            config["primary_master"] = connection_configs[cell_tags[0]]
            config["secondary_masters"] = [connection_configs[tag]
                                           for tag in connection_configs["secondary_cell_tags"]]

            config["enable_timestamp_manager"] = (yt_config.clock_count == 0)

            set_at(config, "timestamp_provider/addresses",
                   _get_timestamp_provider_addresses(yt_config, connection_configs, clock_connection_configs))

            set_at(config, "snapshots/path",
                   os.path.join(master_dirs[cell_index][master_index], "snapshots"))

            if master_tmpfs_dirs is None:
                set_at(config, "changelogs/path",
                        os.path.join(master_dirs[cell_index][master_index], "changelogs"))
            else:
                set_at(config, "changelogs/path",
                        os.path.join(master_tmpfs_dirs[cell_index][master_index], "changelogs"))

            config["logging"] = _init_logging(logs_dir,
                                             "master-{0}-{1}".format(cell_index, master_index),
                                             yt_config,
                                             log_errors_to_stderr=True,
                                             has_structured_logs=True)

            cell_configs.append(config)

        configs[cell_tags[cell_index]] = cell_configs

    configs["primary_cell_tag"] = cell_tags[0]
    configs["secondary_cell_tags"] = cell_tags[1:]

    return configs, connection_configs

def _build_clock_configs(yt_config, clock_dirs, clock_tmpfs_dirs, ports_generator, logs_dir):
    cell_tag = 1000
    random_part = random.randint(0, 2 ** 32 - 1)
    cell_id = canonize_uuid("%x-ffffffff-%x0259-ffffffff" % (random_part, int(cell_tag)))

    ports = []
    cell_addresses = []

    for i in xrange(yt_config.clock_count):
        rpc_port, monitoring_port = next(ports_generator), next(ports_generator)
        address = to_yson_type("{0}:{1}".format(yt_config.fqdn, rpc_port))
        cell_addresses.append(address)
        ports.append((rpc_port, monitoring_port))

    connection_config = {
        "addresses": cell_addresses,
        "cell_id": cell_id
    }

    connection_configs = {}
    connection_configs[cell_tag] = connection_config

    configs = {}

    instance_configs = []

    for clock_index in xrange(yt_config.clock_count):
        config = default_config.get_clock_config()

        init_singletons(config, yt_config.fqdn, "clock", {"clock_index": str(clock_index)})

        config["hydra_manager"] = _get_hydra_manager_config()

        config["rpc_port"], config["monitoring_port"] = ports[clock_index]

        config["clock_cell"] = connection_config

        set_at(config, "timestamp_provider/addresses", connection_config["addresses"])
        set_at(config, "snapshots/path",
                os.path.join(clock_dirs[clock_index], "snapshots"))

        if clock_tmpfs_dirs is None:
            set_at(config, "changelogs/path",
                    os.path.join(clock_dirs[clock_index], "changelogs"))
        else:
            set_at(config, "changelogs/path",
                    os.path.join(clock_tmpfs_dirs[clock_index], "changelogs"))

        config["logging"] = _init_logging(logs_dir,
                                         "clock-{0}".format(clock_index),
                                         yt_config,
                                         log_errors_to_stderr=True)

        instance_configs.append(config)

    configs[cell_tag] = instance_configs

    configs["cell_tag"] = cell_tag
    connection_configs["cell_tag"] = cell_tag

    return configs, connection_configs

def _build_scheduler_configs(scheduler_dirs, master_connection_configs, clock_connection_configs,
                             ports_generator, logs_dir, yt_config):
    configs = []

    for index in xrange(yt_config.scheduler_count):
        config = default_config.get_scheduler_config()

        init_singletons(config, yt_config.fqdn, "scheduler", {"scheduler_index": str(index)})
        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_configs,
                config_template=config["cluster_connection"])

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)
        config["logging"] = _init_logging(logs_dir,
                                         "scheduler-" + str(index),
                                         yt_config,
                                         has_structured_logs=True)

        configs.append(config)

    return configs

def _build_controller_agent_configs(controller_agent_dirs, master_connection_configs, clock_connection_configs,
                                    ports_generator, logs_dir, yt_config):
    configs = []

    for index in xrange(yt_config.controller_agent_count):
        config = default_config.get_controller_agent_config()

        init_singletons(config, yt_config.fqdn, "controller_agent", {"controller_agent_index": str(index)})
        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_configs,
                config_template=config["cluster_connection"])

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)
        config["logging"] = _init_logging(logs_dir,
                                         "controller-agent-" + str(index),
                                         yt_config,
                                         has_structured_logs=True)

        configs.append(config)

    return configs

def _build_node_configs(node_dirs, node_tmpfs_dirs, master_connection_configs, clock_connection_configs,
                        ports_generator, logs_dir, yt_config):
    configs = []
    addresses = []

    current_user = 10000

    for index in xrange(yt_config.node_count):
        config = default_config.get_node_config()

        init_singletons(config, yt_config.fqdn, "node", {"node_index": str(index)})

        config["addresses"] = [
            ("interconnect", yt_config.fqdn),
            ("default", yt_config.fqdn)
        ]

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)
        config["skynet_http_port"] = next(ports_generator)

        addresses.append("{0}:{1}".format(yt_config.fqdn, config["rpc_port"]))

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_configs,
                config_template=config["cluster_connection"])

        set_at(config, "data_node/multiplexed_changelog/path", os.path.join(node_dirs[index], "multiplexed"))

        cache_location_config = {
            "quota": 256 * MB,
            "io_config": {
                "enable_sync": False,
            },
        }

        if node_tmpfs_dirs is not None and yt_config.allow_chunk_storage_in_tmpfs:
            cache_location_config["path"] = os.path.join(node_tmpfs_dirs[index], "chunk_cache")
        else:
            cache_location_config["path"] = os.path.join(node_dirs[index], "chunk_cache")

        set_at(config, "data_node/cache_locations", [cache_location_config])

        start_uid = current_user + config["rpc_port"]
        set_at(config, "exec_agent/slot_manager/job_environment/start_uid", start_uid)
        set_at(config, "exec_agent/slot_manager/locations", [
            {"path": os.path.join(node_dirs[index], "slots"), "disk_usage_watermark": 0}
        ])
        set_at(config, "exec_agent/root_fs_binds", [
            {"external_path": node_dirs[index], "internal_path": node_dirs[index]}
        ])

        store_location_config = {
            "low_watermark": 0,
            "high_watermark": 0,
            "disable_writes_watermark": 0,
            "io_config": {
                "enable_sync": False,
            },
        }

        layer_location_config = {
            "low_watermark": 1,
            "location_is_absolute": False,
        }

        if yt_config.node_chunk_store_quota is not None:
            store_location_config["quota"] = yt_config.node_chunk_store_quota

        if node_tmpfs_dirs is not None and yt_config.allow_chunk_storage_in_tmpfs:
            store_location_config["path"] = os.path.join(node_tmpfs_dirs[index], "chunk_store")
            layer_location_config["path"] = os.path.join(node_tmpfs_dirs[index], "layers")
        else:
            store_location_config["path"] = os.path.join(node_dirs[index], "chunk_store")
            layer_location_config["path"] = os.path.join(node_dirs[index], "layers")

        set_at(config, "data_node/store_locations", [store_location_config])
        set_at(config, "data_node/volume_manager/layer_locations", [layer_location_config])

        config["logging"] = _init_logging(logs_dir, "node-{0}".format(index), yt_config)

        job_proxy_logging = get_at(config, "exec_agent/job_proxy_logging")
        log_name = "job_proxy-{0}-slot-%slot_index%".format(index)
        set_at(
            config,
            "exec_agent/job_proxy_logging",
            _init_logging(logs_dir, log_name, yt_config)
        )
        set_at(
            config,
            "exec_agent/job_proxy_stderr_path",
            os.path.join(logs_dir, "job_proxy-{0}-stderr-slot-%slot_index%".format(index)),
        )

        set_at(config, "tablet_node/hydra_manager", _get_hydra_manager_config(), merge=True)
        set_at(config, "tablet_node/hydra_manager/restart_backoff_time", 100)
        set_at(config, "exec_agent/job_controller/resource_limits", yt_config.jobs_resource_limits, merge=True)
        set_at(config, "resource_limits", _get_node_resource_limits_config(yt_config), merge=True)

        configs.append(config)

    if hasattr(ports_generator, "local_port_range"):
        USER_PORT_START = ports_generator.local_port_range[1]
        USER_PORT_END = min(USER_PORT_START + 10000, 2 ** 16)
        assert USER_PORT_START < USER_PORT_END
    else:
        # Legacy constants.
        USER_PORT_START = 20000
        USER_PORT_END = 30000

    node_count = len(configs)
    for index, config in enumerate(configs):
        port_start = USER_PORT_START + (index * (USER_PORT_END - USER_PORT_START)) // node_count
        port_end = USER_PORT_START + ((index + 1) * (USER_PORT_END - USER_PORT_START)) // node_count

        if yt_config.node_port_set_size is None:
            set_at(config, "exec_agent/job_controller/start_port", port_start)
            set_at(config, "exec_agent/job_controller/port_count", port_end - port_start)
        else:
            ports = [next(ports_generator) for _ in xrange(yt_config.node_port_set_size)]
            set_at(config, "exec_agent/job_controller/port_set", ports)

    return configs, addresses

def _build_http_proxy_config(proxy_dir, master_connection_configs, clock_connection_configs, ports_generator, logs_dir, master_cache_nodes, yt_config):
    driver_config = default_config.get_driver_config()
    update_inplace(driver_config, _build_cluster_connection_config(
        yt_config,
        master_connection_configs,
        clock_connection_configs,
        master_cache_nodes=master_cache_nodes))

    proxy_configs = []

    for index in xrange(yt_config.http_proxy_count):
        proxy_config = default_config.get_proxy_config()
        proxy_config["port"] = yt_config.http_proxy_ports[index] if yt_config.http_proxy_ports else next(ports_generator)
        proxy_config["monitoring_port"] = next(ports_generator)
        proxy_config["rpc_port"] = next(ports_generator)

        fqdn = "{0}:{1}".format(yt_config.fqdn, proxy_config["port"])
        set_at(proxy_config, "coordinator/public_fqdn", fqdn)
        init_singletons(proxy_config, yt_config.fqdn, "http_proxy", {"http_proxy_index": str(index)})

        proxy_config["logging"] = _init_logging(logs_dir, "http-proxy-{}".format(index), yt_config,
                                               has_structured_logs=True)

        proxy_config["driver"] = driver_config

        proxy_configs.append(proxy_config)

    return proxy_configs

def _build_native_driver_configs(master_connection_configs, clock_connection_configs, master_cache_nodes, yt_config):
    secondary_cell_tags = master_connection_configs["secondary_cell_tags"]
    primary_cell_tag = master_connection_configs["primary_cell_tag"]

    configs = {}
    for cell_index in xrange(yt_config.secondary_cell_count + 1):
        config = default_config.get_driver_config()

        if cell_index == 0:
            tag = primary_cell_tag
            update_inplace(config, _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_configs,
                master_cache_nodes=master_cache_nodes))
        else:
            tag = secondary_cell_tags[cell_index - 1]
            cell_connection_config = {
                "primary_master": master_connection_configs[secondary_cell_tags[cell_index - 1]],
                "master_cell_directory_synchronizer": {"sync_period": None},
                "timestamp_provider": {
                    "addresses": _get_timestamp_provider_addresses(
                        yt_config,
                        master_connection_configs,
                        clock_connection_configs
                    ),
                },
                "transaction_manager": {
                    "default_ping_period": DEFAULT_TRANSACTION_PING_PERIOD
                }
            }
            update_inplace(cell_connection_config["primary_master"], _get_retrying_channel_config())
            update_inplace(cell_connection_config["primary_master"], _get_rpc_config())

            update_inplace(config, cell_connection_config)

        configs[tag] = config

    if yt_config.clock_count > 0:
        tag = clock_connection_configs["cell_tag"]
        config = deepcopy(configs[primary_cell_tag])
        update_inplace(config["timestamp_provider"], clock_connection_configs[tag])
        configs[tag] = config

    return configs

def _build_rpc_driver_config(master_connection_configs, clock_connection_configs, master_cache_nodes,
                             rpc_proxy_addresses, http_proxy_url, yt_config):
    config = default_config.get_driver_config()
    config["connection_type"] = "rpc"

    if http_proxy_url is not None:
        config["cluster_url"] = http_proxy_url
    else:
        config["addresses"] = rpc_proxy_addresses

    return config

def _build_rpc_proxy_configs(logs_dir, master_connection_configs, clock_connection_configs, master_cache_nodes, ports_generator, yt_config):
    configs = []

    for rpc_proxy_index in xrange(yt_config.rpc_proxy_count):
        grpc_server_config = {
            "addresses": [
                {
                    "address": "{}:{}".format(yt_config.fqdn, next(ports_generator))
                }
            ]
        }

        config = {
            "cluster_connection": master_connection_configs,
            "discovery_service": {
                "liveness_update_period": 500,
                "proxy_update_period": 500
            },
            "grpc_server": grpc_server_config,
            "monitoring_port": next(ports_generator),
            "yt_alloc_dump_period": 15000,
            "ref_counted_tracker_dump_period": 15000,
            "enable_authentication": False,
            "api_service": {
                "security_manager": {
                    "user_cache": {
                        "expire_after_successful_update_time": 0,
                        "refresh_time": 0,
                        "expire_after_failed_update_time": 0,
                        "expire_after_access_time": 0,
                    }
                }
            }
        }
        init_singletons(config, yt_config.fqdn, "rpc_proxy", {"rpc_proxy_index": str(rpc_proxy_index)})
        config["cluster_connection"] = _build_cluster_connection_config(
            yt_config,
            master_connection_configs,
            clock_connection_configs,
            master_cache_nodes=master_cache_nodes)
        config["logging"] = _init_logging(logs_dir, "rpc-proxy-{}".format(rpc_proxy_index), yt_config)

        config["rpc_port"] = yt_config.rpc_proxy_ports[rpc_proxy_index] if yt_config.rpc_proxy_ports else next(ports_generator)

        configs.append(config)

    return configs

def _build_cluster_connection_config(yt_config,
                                     master_connection_configs,
                                     clock_connection_configs,
                                     master_cache_nodes=None,
                                     config_template=None):
    primary_cell_tag = master_connection_configs["primary_cell_tag"]
    secondary_cell_tags = master_connection_configs["secondary_cell_tags"]

    cluster_connection = {
        "cell_directory": _get_balancing_channel_config(),
        "primary_master": master_connection_configs[primary_cell_tag],
        "transaction_manager": {
            "default_ping_period": DEFAULT_TRANSACTION_PING_PERIOD
        },
        "timestamp_provider": {
            "addresses": _get_timestamp_provider_addresses(yt_config, master_connection_configs, clock_connection_configs),
            "update_period": 500,
            "soft_backoff_time": 100,
            "hard_backoff_time": 100
        },
        "cell_directory_synchronizer": {
            "sync_period": 500
        },
        "cluster_directory_synchronizer": {
            "sync_period": 500,
            "success_expiration_time": 500,
            "failure_expiration_time": 500
        },
        "table_mount_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0
        },
        "permission_cache": {
        },
        "master_cell_directory_synchronizer": {
            "sync_period": 500,
            "success_expiration_time": 500,
            "failure_expiration_time": 500
        },
        "job_node_descriptor_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0
        }
    }

    update_inplace(cluster_connection["primary_master"], _get_retrying_channel_config())
    update_inplace(cluster_connection["primary_master"], _get_balancing_channel_config())
    update_inplace(cluster_connection["primary_master"], _get_rpc_config())

    cluster_connection["secondary_masters"] = []
    for tag in secondary_cell_tags:
        config = master_connection_configs[tag]
        update_inplace(config, _get_retrying_channel_config())
        update_inplace(config, _get_balancing_channel_config())
        update_inplace(config, _get_rpc_config())
        cluster_connection["secondary_masters"].append(config)

    if config_template is not None:
        cluster_connection = update_inplace(config_template, cluster_connection)

    if yt_config.enable_master_cache and master_cache_nodes:
        cluster_connection["master_cache"] = {
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
            "rpc_timeout": 25000,
            "addresses": master_cache_nodes,
            "cell_id": master_connection_configs[primary_cell_tag]["cell_id"],
        }

    if not yt_config.enable_permission_cache:
        cluster_connection["permission_cache"] = {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0
        }

    return cluster_connection

def _init_logging(path, name, yt_config, log_errors_to_stderr=False, has_structured_logs=False):
    return init_logging(
        path,
        name,
        enable_debug_logging=yt_config.enable_debug_logging,
        enable_log_compression=yt_config.enable_log_compression,
        log_compression_method=yt_config.log_compression_method,
        enable_structured_logging=yt_config.enable_structured_logging and has_structured_logs,
        log_errors_to_stderr=log_errors_to_stderr)

def init_logging(path, name,
                 enable_debug_logging=False,
                 enable_log_compression=False,
                 log_compression_method="gzip",
                 enable_structured_logging=False,
                 log_errors_to_stderr=False):
    if enable_log_compression and log_compression_method == "zstd":
        suffix = ".zst"
        compression_options = {
            "enable_compression": True,
            "compression_method": "zstd",
            "compression_level": 1,
        }
    elif enable_log_compression:
        suffix = ".gz"
        compression_options = {
            "enable_compression": True,
        }
    else:
        suffix = ""
        compression_options = {}

    config = {
        "abort_on_alert": True,
        "rules": [
            {"min_level": "info", "writers": ["info"]},
        ],
        "writers": {
            "info": {
                "type": "file",
                "file_name": "{path}/{name}.log".format(path=path, name=name) + suffix,
            }
        }
    }
    config["writers"]["info"].update(compression_options)

    if log_errors_to_stderr:
        config["rules"].append(
            {"min_level": "error", "writers": ["stderr"]}
        )
        config["writers"]["stderr"] = {
            "type": "stderr",
        }

    if enable_debug_logging:
        config["rules"].append({
            "min_level": "debug",
            "exclude_categories": ["Bus"],
            "writers": ["debug"],
        })
        config["writers"]["debug"] = {
            "type": "file",
            "file_name": "{path}/{name}.debug.log".format(path=path, name=name)  + suffix,
        }
        config["writers"]["debug"].update(compression_options)

    if enable_structured_logging:
        config["rules"].append({
            "min_level": "debug",
            "writers": ["json"],
            "message_format": "structured",
        })
        config["writers"]["json"] = {
            "type": "file",
            "file_name": "{path}/{name}.json.log".format(path=path, name=name),
            "accepted_message_format": "structured",
        }

    return config

DEFAULT_TRANSACTION_PING_PERIOD = 500

def set_at(config, path, value, merge=False):
    """Sets value in config by path creating intermediate dict nodes."""
    parts = path.split("/")
    for index, part in enumerate(parts):
        if index != len(parts) - 1:
            config = config.setdefault(part, {})
        else:
            if merge:
                config[part] = update(config.get(part, {}), value)
            else:
                config[part] = value

def init_singletons(config, fqdn, name, process_tags={}):
    set_at(config, "address_resolver/localhost_fqdn", fqdn)
    set_at(config, "solomon_exporter/enable_core_profiling_compatibility", True)

    if "JAEGER_COLLECTOR" in os.environ:
        set_at(config, "jaeger", {
            "service_name": name,
            "flush_period": 100,
            "collector_channel_config": {"address": os.environ["JAEGER_COLLECTOR"]},
            "enable_pid_tag": True,
            "process_tags": process_tags,
        })

def get_at(config, path, default_value=None):
    for part in path.split("/"):
        if not isinstance(config, dict):
            raise ValueError("Path should not contain non-dict intermediate values")
        if part not in config:
            return default_value
        config = config[part]
    return config

def _get_hydra_manager_config():
    return {
        "leader_lease_check_period": 100,
        "leader_lease_timeout": 20000,
        "disable_leader_lease_grace_delay": True,
        "response_keeper": {
            "enable_warmup": False,
            "expiration_time": 25000,
            "warmup_time": 30000,
        }
    }

def _get_balancing_channel_config():
    return {
        "soft_backoff_time": 100,
        "hard_backoff_time": 100
    }

def _get_retrying_channel_config():
    return {
        "retry_backoff_time": 100,
        "retry_attempts": 100
    }

def _get_rpc_config():
    return {
        "rpc_timeout": 25000
    }

def _get_node_resource_limits_config(yt_config):
    FOOTPRINT_MEMORY = 1 * GB
    CHUNK_META_CACHE_MEMORY = 1 * GB
    BLOB_SESSIONS_MEMORY = 2 * GB

    memory = 0
    memory += yt_config.jobs_resource_limits.get("memory", 0)
    if yt_config.node_memory_limit_addition is not None:
        memory += yt_config.node_memory_limit_addition

    memory += FOOTPRINT_MEMORY
    memory += CHUNK_META_CACHE_MEMORY
    memory += BLOB_SESSIONS_MEMORY

    return {"memory": memory}
