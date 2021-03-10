from __future__ import print_function

from .configs_provider import _init_logging, build_configs
from .default_config import get_dynamic_master_config
from .helpers import (
    read_config, write_config, is_dead, OpenPortIterator,
    wait_for_removing_file_lock, get_value_from_config, WaitFailed,
    is_port_opened)
from .porto_helpers import PortoSubprocess, porto_avaliable
from .watcher import ProcessWatcher
from .init_cluster import _initialize_world
from .local_cypress import _synchronize_cypress_with_local_dir
from .local_cluster_configuration import modify_cluster_configuration

from yt.common import YtError, remove_file, makedirp, update, get_value, which
from yt.wrapper.common import flatten
from yt.wrapper.errors import YtResponseError
from yt.wrapper import YtClient

from yt.test_helpers import wait

import yt.yson as yson
import yt.subprocess_wrapper as subprocess

from yt.packages.six import itervalues, iteritems
from yt.packages.six.moves import xrange, map as imap
import yt.packages.requests as requests

import logging
import os
import copy
import errno
import time
import signal
import socket
import shutil
import sys
import traceback
from collections import defaultdict, namedtuple, OrderedDict
from threading import RLock
from itertools import count

logger = logging.getLogger("Yt.local")

# YT-12960: disable debug logging about file locks.
logging.getLogger("library.python.filelock").setLevel(logging.INFO)

BinaryVersion = namedtuple("BinaryVersion", ["abi", "literal"])

_environment_driver_logging_config = None

def set_environment_driver_logging_config(config):
    global _environment_driver_logging_config
    _environment_driver_logging_config = config

class YtEnvRetriableError(YtError):
    pass

def _parse_version(s):
    if "version:" in s:
        # "ytserver  version: 0.17.3-unknown~debug~0+local"
        # "ytserver  version: 18.5.0-local~local"
        literal = s.split(":", 1)[1].strip()
    else:
        # "19.0.0-local~debug~local"
        literal = s.strip()
    parts = list(imap(int, literal.split("-")[0].split(".")[:3]))
    abi = tuple(parts[:2])
    return BinaryVersion(abi, literal)

def _get_yt_binary_path(binary, custom_paths):
    paths = which(binary, custom_paths=custom_paths)
    if paths:
        return paths[0]
    return None

def _get_yt_versions(custom_paths):
    result = OrderedDict()
    binaries = ["ytserver-master", "ytserver-node", "ytserver-scheduler", "ytserver-controller-agent",
                "ytserver-http-proxy", "ytserver-proxy", "ytserver-job-proxy", "ytserver-clock",
                "ytserver-exec", "ytserver-tools", "ytserver-timestamp-provider", "ytserver-master-cache"]
    for binary in binaries:
        binary_path = _get_yt_binary_path(binary, custom_paths=custom_paths)
        if binary_path is not None:
            version_string = subprocess.check_output([binary_path, "--version"], stderr=subprocess.STDOUT)
            result[binary] = _parse_version(version_string)
    return result


def _configure_logger():
    logger.propagate = False
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())

    if os.environ.get("YT_ENABLE_VERBOSE_LOGGING"):
        logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    else:
        logger.handlers[0].setFormatter(logging.Formatter("%(message)s"))


def _get_ports_generator(yt_config):
    if yt_config.port_range_start:
        return count(yt_config.port_range_start)
    elif yt_config.listen_port_pool is not None:
        return iter(yt_config.listen_port_pool)
    else:
        return OpenPortIterator(
            port_locks_path=os.environ.get("YT_LOCAL_PORT_LOCKS_PATH", None),
            local_port_range=yt_config.local_port_range)


class YTInstance(object):
    def __init__(self, path, yt_config,
                 modify_configs_func=None,
                 kill_child_processes=False,
                 watcher_config=None,
                 run_watcher=True,
                 ytserver_all_path=None,
                 watcher_binary=None,
                 stderrs_path=None,
                 preserve_working_dir=False,
                 tmpfs_path=None):
        _configure_logger()

        self.yt_config = yt_config

        if yt_config.master_count == 0:
            raise YtError("Can't start local YT without master")
        if yt_config.scheduler_count == 0 and yt_config.controller_agent_count != 0:
            raise YtError("Can't start controller agent without scheduler")

        if yt_config.use_porto_for_servers and not porto_avaliable():
            raise YtError("Option use_porto_for_servers is specified but porto is not available")
        self._subprocess_module = PortoSubprocess if yt_config.use_porto_for_servers else subprocess

        self._watcher_binary = watcher_binary

        self.path = os.path.realpath(os.path.abspath(path))
        self.bin_path = os.path.abspath(os.path.join(self.path, "bin"))
        self.logs_path = os.path.abspath(os.path.join(self.path, "logs"))
        self.configs_path = os.path.abspath(os.path.join(self.path, "configs"))
        self.runtime_data_path = os.path.abspath(os.path.join(self.path, "runtime_data"))
        self.pids_filename = os.path.join(self.path, "pids.txt")

        self._load_existing_environment = False
        if os.path.exists(self.path):
            if not preserve_working_dir:
                shutil.rmtree(self.path, ignore_errors=True)
            else:
                self._load_existing_environment = True

        if not self._load_existing_environment:
            if ytserver_all_path is None:
                ytserver_all_path = os.environ.get("YTSERVER_ALL_PATH")
            if ytserver_all_path is not None:
                if not os.path.exists(ytserver_all_path):
                    raise YtError("ytserver-all binary is missing at path " + ytserver_all_path)
                makedirp(self.bin_path)
                programs = ["master", "clock", "node", "job-proxy", "exec",
                            "proxy", "http-proxy", "tools", "scheduler",
                            "controller-agent", "timestamp-provider", "master-cache"]
                for program in programs:
                    os.symlink(os.path.abspath(ytserver_all_path), os.path.join(self.bin_path, "ytserver-" + program))
                os.environ["PATH"] = self.bin_path + ":" + os.environ["PATH"]

        if os.path.exists(self.bin_path):
            self.custom_paths = [self.bin_path]
        else:
            self.custom_paths = None

        self._binaries = _get_yt_versions(custom_paths=self.custom_paths)
        abi_versions = set(imap(lambda v: v.abi, self._binaries.values()))
        self.abi_version = abi_versions.pop()

        if "ytserver-master" not in self._binaries:
            raise YtError("Failed to find YT binaries (ytserver-*) in $PATH. Make sure that YT is installed.")

        self._lock = RLock()

        self.configs = defaultdict(list)
        self.config_paths = defaultdict(list)

        makedirp(self.path)
        makedirp(self.logs_path)
        makedirp(self.configs_path)
        makedirp(self.runtime_data_path)

        self.stderrs_path = stderrs_path or os.path.join(self.path, "stderrs")
        makedirp(self.stderrs_path)
        self._stderr_paths = defaultdict(list)

        self._tmpfs_path = tmpfs_path
        if self._tmpfs_path is not None:
            self._tmpfs_path = os.path.abspath(self._tmpfs_path)

        # Dictionary from service name to the list of processes.
        self._service_processes = defaultdict(list)
        # Dictionary from pid to tuple with Process and arguments
        self._pid_to_process = {}

        self._kill_child_processes = kill_child_processes
        self._started = False
        self._wait_functions = []

        self._run_watcher = run_watcher
        self.watcher_config = watcher_config
        if self.watcher_config is None:
            self.watcher_config = {}

        if not yt_config.enable_log_compression:
            self.watcher_config["logs_rotate_compress"] = True
        else:
            self.watcher_config["disable_logrotate"] = True

        self._default_client_config = {"enable_token": False}

        self._open_port_iterator = _get_ports_generator(yt_config)
        self._prepare_environment(self._open_port_iterator, modify_configs_func)

    def _prepare_directories(self):
        master_dirs = []
        master_tmpfs_dirs = [] if self._tmpfs_path else None

        for cell_index in xrange(self.yt_config.secondary_cell_count + 1):
            name = self._get_master_name("master", cell_index)
            master_dirs.append([os.path.join(self.runtime_data_path, name, str(i)) for i in xrange(self.yt_config.master_count)])
            for dir_ in master_dirs[cell_index]:
                makedirp(dir_)

            if self._tmpfs_path is not None:
                master_tmpfs_dirs.append([os.path.join(self._tmpfs_path, name, str(i)) for i in xrange(self.yt_config.master_count)])

                for dir_ in master_tmpfs_dirs[cell_index]:
                    makedirp(dir_)

        clock_dirs = [os.path.join(self.runtime_data_path, "clock", str(i)) for i in xrange(self.yt_config.clock_count)]
        for dir_ in clock_dirs:
            makedirp(dir_)

        clock_tmpfs_dirs = None
        if self._tmpfs_path is not None:
            clock_tmpfs_dirs = [os.path.join(self._tmpfs_path, name, str(i)) for i in xrange(self.yt_config.clock_count)]
            for dir_ in clock_tmpfs_dirs:
                makedirp(dir_)

        timestamp_provider_dirs = [os.path.join(self.runtime_data_path, "timestamp_provider", str(i)) for i in xrange(self.yt_config.timestamp_provider_count)]
        for dir_ in timestamp_provider_dirs:
            makedirp(dir_)

        scheduler_dirs = [os.path.join(self.runtime_data_path, "scheduler", str(i)) for i in xrange(self.yt_config.scheduler_count)]
        for dir_ in scheduler_dirs:
            makedirp(dir_)

        controller_agent_dirs = [os.path.join(self.runtime_data_path, "controller_agent", str(i)) for i in xrange(self.yt_config.controller_agent_count)]
        for dir_ in controller_agent_dirs:
            makedirp(dir_)

        node_dirs = [os.path.join(self.runtime_data_path, "node", str(i)) for i in xrange(self.yt_config.node_count)]
        for dir_ in node_dirs:
            makedirp(dir_)

        node_tmpfs_dirs = None
        if self._tmpfs_path is not None:
            node_tmpfs_dirs = [os.path.join(self._tmpfs_path, "node", str(i)) for i in xrange(self.yt_config.node_count)]
            for dir_ in node_tmpfs_dirs:
                makedirp(dir_)

        master_cache_dirs = [os.path.join(self.runtime_data_path, "master_cache", str(i)) for i in xrange(self.yt_config.master_cache_count)]
        for dir_ in master_cache_dirs:
            makedirp(dir_)

        http_proxy_dirs = [os.path.join(self.runtime_data_path, "http_proxy", str(i)) for i in xrange(self.yt_config.http_proxy_count)]
        for dir_ in http_proxy_dirs:
            makedirp(dir_)

        rpc_proxy_dirs = [os.path.join(self.runtime_data_path, "rpc_proxy", str(i)) for i in xrange(self.yt_config.rpc_proxy_count)]
        for dir_ in rpc_proxy_dirs:
            makedirp(dir_)

        return {"master": master_dirs,
                "master_tmpfs": master_tmpfs_dirs,
                "clock": clock_dirs,
                "clock_tmpfs": clock_tmpfs_dirs,
                "timestamp_provider": timestamp_provider_dirs,
                "scheduler": scheduler_dirs,
                "controller_agent": controller_agent_dirs,
                "node": node_dirs,
                "node_tmpfs": node_tmpfs_dirs,
                "master_cache_dirs": master_cache_dirs,
                "http_proxy": http_proxy_dirs,
                "rpc_proxy": rpc_proxy_dirs}

    def _prepare_environment(self, ports_generator, modify_configs_func):
        logger.info("Preparing cluster instance as follows:")
        logger.info("  clocks                %d", self.yt_config.clock_count)
        logger.info("  masters               %d (%d nonvoting)", self.yt_config.master_count, self.yt_config.nonvoting_master_count)
        logger.info("  timestamp providers   %d", self.yt_config.timestamp_provider_count)
        logger.info("  nodes                 %d", self.yt_config.node_count)
        logger.info("  master caches         %d", self.yt_config.master_cache_count)
        logger.info("  schedulers            %d", self.yt_config.scheduler_count)
        logger.info("  controller agents     %d", self.yt_config.controller_agent_count)

        if self.yt_config.secondary_cell_count > 0:
            logger.info("  secondary cells       %d", self.yt_config.secondary_cell_count)

        logger.info("  HTTP proxies          %d", self.yt_config.http_proxy_count)
        logger.info("  RPC proxies           %d", self.yt_config.rpc_proxy_count)
        logger.info("  working dir           %s", self.yt_config.path)

        if self.yt_config.master_count == 0:
            logger.warning("Master count is zero. Instance is not prepared.")
            return

        dirs = self._prepare_directories()

        cluster_configuration = build_configs(self.yt_config, ports_generator, dirs, self.logs_path)

        modify_cluster_configuration(self.yt_config, cluster_configuration)

        if modify_configs_func:
            modify_configs_func(cluster_configuration, self.abi_version)

        self._cluster_configuration = cluster_configuration

        if self.yt_config.master_count + self.yt_config.secondary_cell_count > 0:
            self._prepare_masters(cluster_configuration["master"])
        if self.yt_config.clock_count > 0:
            self._prepare_clocks(cluster_configuration["clock"])
        if self.yt_config.timestamp_provider_count > 0:
            self._prepare_timestamp_providers(cluster_configuration["timestamp_provider"])
        if self.yt_config.node_count > 0:
            self._prepare_nodes(cluster_configuration["node"])
        if self.yt_config.master_cache_count > 0:
            self._prepare_master_caches(cluster_configuration["master_cache"])
        if self.yt_config.scheduler_count > 0:
            self._prepare_schedulers(cluster_configuration["scheduler"])
        if self.yt_config.controller_agent_count > 0:
            self._prepare_controller_agents(cluster_configuration["controller_agent"])
        if self.yt_config.http_proxy_count > 0:
            self._prepare_http_proxies(cluster_configuration["http_proxy"])
        if self.yt_config.rpc_proxy_count > 0:
            self._prepare_rpc_proxies(cluster_configuration["rpc_proxy"], cluster_configuration["rpc_client"])

        self._prepare_drivers(
            cluster_configuration["driver"],
            cluster_configuration["rpc_driver"],
            cluster_configuration["master"],
            cluster_configuration["clock"])

    def _wait_or_skip(self, function, sync):
        if sync:
            function()
        else:
            self._wait_functions.append(function)

    def remove_runtime_data(self):
        if os.path.exists(self.runtime_data_path):
            shutil.rmtree(self.runtime_data_path, ignore_errors=True)

    def start(self, on_masters_started_func=None):
        for name, processes in iteritems(self._service_processes):
            for index in xrange(len(processes)):
                processes[index] = None
        self._pid_to_process.clear()

        if self.yt_config.master_count == 0:
            logger.warning("Cannot start YT instance without masters")
            return

        self.pids_file = open(self.pids_filename, "wt")
        try:
            self._configure_driver_logging()
            self._configure_yt_tracing()

            if self.yt_config.http_proxy_count > 0:
                self.start_http_proxy(sync=False)

            self.start_master_cell(sync=False)

            if self.yt_config.clock_count > 0:
                self.start_clock(sync=False)

            if self.yt_config.timestamp_provider_count > 0:
                self.start_timestamp_providers(sync=False)

            if self.yt_config.master_cache_count > 0:
                self.start_master_caches(sync=False)

            if self.yt_config.rpc_proxy_count > 0:
                self.start_rpc_proxy(sync=False)

            self.synchronize()

            if not self.yt_config.defer_secondary_cell_start:
                self.start_secondary_master_cells(sync=False)
                self.synchronize()

            client = self._create_cluster_client()

            if self.yt_config.http_proxy_count > 0:
                # NB: it is used to determine proper operation URL in local mode.
                client.set("//sys/@local_mode_proxy_address", self.get_http_proxy_address())

            if on_masters_started_func is not None:
                on_masters_started_func()
            if self.yt_config.node_count > 0 and not self.yt_config.defer_node_start:
                self.start_nodes(sync=False)
            if self.yt_config.scheduler_count > 0 and not self.yt_config.defer_scheduler_start:
                self.start_schedulers(sync=False)
            if self.yt_config.controller_agent_count > 0 and not self.yt_config.defer_controller_agent_start:
                self.start_controller_agents(sync=False)

            self.synchronize()

            if self._run_watcher:
                self._start_watcher()
                self._started = True

            if self.yt_config.initialize_world:
                if not self._load_existing_environment:
                    client = self.create_client()

                    # This hack is necessary to correctly run inside docker container.
                    # In this case public proxy port differs from proxy port inside container and
                    # we should use latter.
                    client.config["proxy"]["enable_proxy_discovery"] = False

                    _initialize_world(
                        client,
                        self,
                        self.yt_config)

                    if self.yt_config.local_cypress_dir is not None:
                        _synchronize_cypress_with_local_dir(
                            self.yt_config.local_cypress_dir,
                            self.yt_config.meta_files_suffix,
                            client)

            self._write_environment_info_to_file()
        except (YtError, KeyboardInterrupt) as err:
            self.stop(force=True)
            raise YtError("Failed to start environment", inner_errors=[err])

    def stop(self, force=False):
        if not self._started and not force:
            return

        with self._lock:
            self.stop_impl()

        self._started = False

    def stop_impl(self):
        killed_services = set()

        if self._run_watcher:
            self.kill_service("watcher")
            killed_services.add("watcher")

        for name in ["http_proxy", "node", "scheduler", "controller_agent", "master",
                     "rpc_proxy", "timestamp_provider", "master_caches"]:
            if name in self.configs:
                self.kill_service(name)
                killed_services.add(name)

        for name in self.configs:
            if name not in killed_services:
                self.kill_service(name)

        self.pids_file.close()
        remove_file(self.pids_filename, force=True)

        if self._open_port_iterator is not None:
            self._open_port_iterator.release()
            self._open_port_iterator = None

        wait_for_removing_file_lock(os.path.join(self.path, "lock_file"))

    def synchronize(self):
        for func in self._wait_functions:
            func()
        self._wait_functions = []

    def rewrite_master_configs(self):
        self._prepare_masters(self._cluster_configuration["master"], force_overwrite=True)

    def rewrite_node_configs(self):
        self._prepare_nodes(self._cluster_configuration["node"], force_overwrite=True)

    def rewrite_scheduler_configs(self):
        self._prepare_schedulers(self._cluster_configuration["scheduler"], force_overwrite=True)

    def rewrite_controller_agent_configs(self):
        self._prepare_controller_agents(self._cluster_configuration["controller_agent"], force_overwrite=True)

    def get_node_address(self, index, with_port=True):
        node_config = self.configs["node"][index]
        node_address = node_config["address_resolver"]["localhost_fqdn"]
        if with_port:
            node_address = "{}:{}".format(node_address, node_config["rpc_port"])
        return node_address

    # TODO(max42): remove this method and rename all its usages to get_http_proxy_address.
    def get_proxy_address(self):
        return self.get_http_proxy_address()

    def get_http_proxy_address(self):
        return self.get_http_proxy_addresses()[0]

    def get_http_proxy_addresses(self):
        if self.yt_config.http_proxy_count == 0:
            raise YtError("Http proxies are not started")
        return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, "port", "http_proxy")) for config in self.configs["http_proxy"]]

    def get_grpc_proxy_address(self):
        if self.yt_config.rpc_proxy_count == 0:
            raise YtError("Rpc proxies are not started")
        addresses = get_value_from_config(self.configs["rpc_proxy"][0], "grpc_server/addresses", "rpc_proxy")
        return addresses[0]["address"]

    def get_timestamp_provider_monitoring_addresses(self):
        if self.yt_config.timestamp_provider_count == 0:
            raise YtError("Timestamp providers are not started")
        return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, "monitoring_port", "timestamp_provider")) for config in self.configs["timestamp_provider"]]

    def get_master_cache_monitoring_addresses(self):
        if self.yt_config.master_cache_count == 0:
            raise YtError("Master caches are not started")
        return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, "monitoring_port", "master_cache")) for config in self.configs["master_cacje"]]

    def kill_schedulers(self, indexes=None):
        self.kill_service("scheduler", indexes=indexes)

    def kill_controller_agents(self, indexes=None):
        self.kill_service("controller_agent", indexes=indexes)

    def kill_nodes(self, indexes=None, wait_offline=True):
        self.kill_service("node", indexes=indexes)

        addresses = None
        if indexes is None:
            indexes = list(xrange(self.yt_config.node_count))
        addresses = [self.get_node_address(index) for index in indexes]

        client = self._create_cluster_client()
        for node in client.list("//sys/cluster_nodes", attributes=["lease_transaction_id"]):
            if str(node) not in addresses:
                continue
            if "lease_transaction_id" in node.attributes:
                client.abort_transaction(node.attributes["lease_transaction_id"])

        if wait_offline:
            wait(lambda:
                all([
                    node.attributes["state"] == "offline"
                    for node in client.list("//sys/cluster_nodes", attributes=["state"])
                    if str(node) in addresses
                ])
            )

    def kill_http_proxies(self, indexes=None):
        self.kill_service("proxy", indexes=indexes)

    def kill_masters_at_cells(self, indexes=None, cell_indexes=None):
        if cell_indexes is None:
            cell_indexes = [0]
        for cell_index in cell_indexes:
            name = self._get_master_name("master", cell_index)
            self.kill_service(name, indexes=indexes)

    def kill_all_masters(self):
        self.kill_masters_at_cells(indexes=None, cell_indexes=xrange(self.yt_config.secondary_cell_count + 1))

    def kill_service(self, name, indexes=None):
        with self._lock:
            logger.info("Killing %s", name)
            processes = self._service_processes[name]
            for index, process in enumerate(processes):
                if process is None or (indexes is not None and index not in indexes):
                    continue
                self._kill_process(process, name)
                if isinstance(process, PortoSubprocess):
                    process.destroy()
                del self._pid_to_process[process.pid]
                processes[index] = None

    def get_service_pids(self, name, indexes=None):
        result = []
        with self._lock:
            logger.info("Obtaining %s pids", name)
            processes = self._service_processes[name]
            for index, process in enumerate(processes):
                if process is None or (indexes is not None and index not in indexes):
                    continue
                result.append(process.pid)
        return result

    def list_node_subcontainers(self, index):
        with self._lock:
            processes = self._service_processes["node"]
            process = processes[index]
            if not isinstance(process, PortoSubprocess):
                raise YtError("Cannot list subcontainers for non-porto environment")

            return process.list_subcontainers()

    def set_nodes_cpu_limit(self, cpu_limit):
        with self._lock:
            logger.info("Setting cpu limit {0} for nodes".format(cpu_limit))
            processes = self._service_processes["node"]
            for process in processes:
                if not isinstance(process, PortoSubprocess):
                    raise YtError("Cpu limits are not supported for non-porto environment")
                process.set_cpu_limit(cpu_limit)

    def set_nodes_memory_limit(self, memory_limit):
        with self._lock:
            logger.info("Setting memory limit {0} for nodes".format(memory_limit))
            processes = self._service_processes["node"]
            for process in processes:
                if not isinstance(process, PortoSubprocess):
                    raise YtError("Memory limits are not supported for non-porto environment")
                process.set_memory_limit(memory_limit)

    def check_liveness(self, callback_func):
        with self._lock:
            for info in itervalues(self._pid_to_process):
                proc, args = info
                proc.poll()
                if proc.returncode is not None:
                    callback_func(self, proc, args)
                    break

    def get_component_version(self, component):
        """
        Return structure identifying component version.
        Returned object has fields "abi" and "literal", e.g. (20, 3) and "20.3.1234-...".

        :param component: should be binary name, e.g. "ytserver-master", "ytserver-node", etc.
        :type component str
        :return: component version.
        :rtype BinaryVersion
        """
        if component not in self._binaries:
            raise YtError("Component {} not found; available components are {}".format(
                component, self._binaries.keys()))
        return self._binaries[component]

    def _configure_driver_logging(self):
        try:
            import yt_driver_bindings
            yt_driver_bindings.configure_logging(
                _environment_driver_logging_config or self.configs["driver_logging"]
            )
        except ImportError:
            pass

        import yt.wrapper.native_driver as native_driver
        native_driver.logging_configured = True

    def _configure_yt_tracing(self):
        try:
            import yt_tracing
        except ImportError:
            return

        if "JAEGER_COLLECTOR" in os.environ:
            yt_tracing.initialize_tracer({
                "service_name": "python",
                "flush_period": 100,
                "collector_channel_config": {"address": os.environ["JAEGER_COLLECTOR"]},
                "enable_pid_tag": True,
            })

    def _write_environment_info_to_file(self):
        info = {}
        if self.yt_config.http_proxy_count > 0:
            info["http_proxies"] = [{"address": address} for address in self.get_http_proxy_addresses()]
            if len(self.get_http_proxy_addresses()) == 1:
                info["proxy"] = {"address": self.get_http_proxy_addresses()[0]}
        with open(os.path.join(self.path, "info.yson"), "wb") as fout:
            yson.dump(info, fout, yson_format="pretty")

    def _kill_process(self, proc, name):
        proc.poll()
        if proc.returncode is not None:
            if self._started:
                logger.warning("{0} (pid: {1}, working directory: {2}) is already terminated with exit code {3}".format(
                               name, proc.pid, os.path.join(self.path, name), proc.returncode))
            return

        logger.info("Sending SIGKILL (pid: {}, current_process_pid: {})".format(proc.pid, os.getpid()))
        os.kill(proc.pid, signal.SIGKILL)
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError as e:
            logger.error("killpg({}) failed: {}({})".format(proc.pid, e.errno, errno.errorcode[e.errno]))
            if e.errno != errno.ESRCH:
                raise
        try:
            wait(lambda: is_dead(proc.pid))
        except WaitFailed:
            if not is_dead(proc.pid):
                try:
                    with open("/proc/{0}/status".format(proc.pid), "r") as fin:
                        logger.error("Process status: %s", fin.read().replace("\n", "\\n"))
                    stack = subprocess.check_output(["sudo", "cat", "/proc/{0}/stack".format(proc.pid)])
                    logger.error("Process stack: %s", stack.replace("\n", "\\n"))
                except (IOError, subprocess.CalledProcessError):
                    pass
                raise

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + "\n")
        self.pids_file.flush()

    def _process_stderrs(self, name, number=None):
        has_some_bind_failure = False

        def process_stderr(path, num=None):
            number_suffix = ""
            if num is not None:
                number_suffix = "-" + str(num)

            stderr = open(path).read()
            if stderr:
                sys.stderr.write("{0}{1} stderr:\n{2}"
                                 .format(name.capitalize(), number_suffix, stderr))
                if "Address already in use" in stderr:
                    return True
            return False

        if number is not None:
            has_bind_failure = process_stderr(self._stderr_paths[name][number], number)
            has_some_bind_failure = has_some_bind_failure or has_bind_failure
        else:
            for i, stderr_path in enumerate(self._stderr_paths[name]):
                has_bind_failure = process_stderr(stderr_path, i)
                has_some_bind_failure = has_some_bind_failure or has_bind_failure

        if has_some_bind_failure:
            raise YtEnvRetriableError("Process failed to bind on some of ports")

    def _run(self, args, name, env=None, number=None):
        with self._lock:
            index = number if number is not None else 0
            name_with_number = name
            if number is not None:
                name_with_number = "{0}-{1}".format(name, number)

            if self._service_processes[name][index] is not None:
                logger.debug("Process %s already running", name_with_number, self._service_processes[name][index].pid)
                return

            stderr_path = os.path.join(self.stderrs_path, "stderr.{0}".format(name_with_number))
            self._stderr_paths[name].append(stderr_path)

            stdout = open(os.devnull, "w")
            stderr = open(stderr_path, "w")

            if self._kill_child_processes:
                args += ["--pdeathsig", str(int(signal.SIGTERM))]
            args += ["--setsid"]

            if env is None:
                env = copy.copy(os.environ)
                if "YT_LOG_LEVEL" in env:
                    del env["YT_LOG_LEVEL"]
            env = update(env, {"YT_ALLOC_CONFIG": "{enable_eager_memory_release=%true}"})

            p = self._subprocess_module.Popen(args, shell=False, close_fds=True, cwd=self.runtime_data_path,
                                              env=env,
                                              stdout=stdout, stderr=stderr)

            self._validate_process_is_running(p, name, number)

            self._service_processes[name][index] = p
            self._pid_to_process[p.pid] = (p, args)
            self._append_pid(p.pid)

            tags = []
            if self.yt_config.use_porto_for_servers:
                tags.append("name: {}".format(p._portoName))
            tags.append("pid: {}".format(p.pid))
            tags.append("current_process_pid: {}".format(os.getpid()))

            logger.debug("Process %s started (%s)", name, ", ".join(tags))

            return p

    def _run_yt_component(self, component, name=None, config_option=None):
        if name is None:
            name = component
        if config_option is None:
            config_option = "--config"

        logger.info("Starting %s", name)

        for index in xrange(len(self.configs[name])):
            args = [_get_yt_binary_path("ytserver-" + component, custom_paths=self.custom_paths)]
            if self._kill_child_processes:
                args.extend(["--pdeathsig", str(int(signal.SIGKILL))])
            args.extend([config_option, self.config_paths[name][index]])

            number = None if len(self.configs[name]) == 1 else index
            self._run(args, name, number=number)

    def _get_master_name(self, master_name, cell_index):
        if cell_index == 0:
            return master_name
        else:
            return master_name + "_secondary_" + str(cell_index - 1)

    def _prepare_masters(self, master_configs, force_overwrite=False):
        for cell_index in xrange(self.yt_config.secondary_cell_count + 1):
            master_name = self._get_master_name("master", cell_index)
            if cell_index == 0:
                cell_tag = master_configs["primary_cell_tag"]
            else:
                cell_tag = master_configs["secondary_cell_tags"][cell_index - 1]

            if force_overwrite:
                self.configs[master_name] = []
                self.config_paths[master_name] = []
                self._service_processes[master_name] = []

            for master_index in xrange(self.yt_config.master_count):
                master_config_name = "master-{0}-{1}.yson".format(cell_index, master_index)
                config_path = os.path.join(self.configs_path, master_config_name)
                if self._load_existing_environment and not force_overwrite:
                    if not os.path.isfile(config_path):
                        raise YtError("Master config {0} not found. It is possible that you requested "
                                      "more masters than configs exist".format(config_path))
                    config = read_config(config_path)
                else:
                    config = master_configs[cell_tag][master_index]
                    write_config(config, config_path)

                self.configs[master_name].append(config)
                self.config_paths[master_name].append(config_path)
                self._service_processes[master_name].append(None)

    def start_master_cell(self, cell_index=0, sync=True):
        master_name = self._get_master_name("master", cell_index)
        secondary = cell_index > 0

        self._run_yt_component("master", name=master_name)

        def quorum_ready():
            self._validate_processes_are_running(master_name)

            logger = logging.getLogger("Yt")
            old_level = logger.level
            logger.setLevel(logging.ERROR)
            try:
                # XXX(asaitgalin): Once we're able to execute set("//sys/@config") then quorum is ready
                # and the world is initialized. Secondary masters can only be checked with native
                # driver so it is requirement to have driver bindings if secondary cells are started.
                client = None
                if not secondary:
                    client = self._create_cluster_client()
                else:
                    client = self.create_native_client(master_name.replace("master", "driver"))
                client.config["proxy"]["retries"]["enable"] = False
                client.set("//sys/@config", get_dynamic_master_config())
                return True
            except (requests.RequestException, YtError) as err:
                return False, err
            finally:
                logger.setLevel(old_level)

        cell_ready = quorum_ready

        if secondary:
            primary_cell_client = self.create_native_client()
            cell_tag = int(
                self._cluster_configuration["master"]["secondary_cell_tags"][cell_index - 1])

            def quorum_ready_and_cell_registered():
                result = quorum_ready()
                if isinstance(result, tuple) and not result[0]:
                    return result

                return cell_tag in primary_cell_client.get("//sys/@registered_master_cell_tags")

            cell_ready = quorum_ready_and_cell_registered

        self._wait_or_skip(lambda: self._wait_for(cell_ready, master_name, max_wait_time=30), sync)

    def start_all_masters(self, start_secondary_master_cells, sync=True):
        self.start_master_cell(sync=sync)

        if start_secondary_master_cells:
            self.start_secondary_master_cells(sync=sync)

    def start_secondary_master_cells(self, sync=True):
        for i in xrange(self.yt_config.secondary_cell_count):
            self.start_master_cell(i + 1, sync=sync)

    def _prepare_clocks(self, clock_configs):
        for clock_index in xrange(self.yt_config.clock_count):
            clock_config_name = "clock-{0}.yson".format(clock_index)
            config_path = os.path.join(self.configs_path, clock_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Clock config {0} not found. It is possible that you requested "
                                  "more clocks than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = clock_configs[clock_configs["cell_tag"]][clock_index]
                write_config(config, config_path)

            self.configs["clock"].append(config)
            self.config_paths["clock"].append(config_path)
            self._service_processes["clock"].append(None)

    def start_clock(self, sync=True):
        self._run_yt_component("clock")

        def quorum_ready():
            self._validate_processes_are_running("clock")

            logger = logging.getLogger("Yt")
            old_level = logger.level
            logger.setLevel(logging.ERROR)
            try:
                client = self.create_native_client("clock_driver")
                client.generate_timestamp()
                return True
            except (requests.RequestException, YtError) as err:
                return False, err
            finally:
                logger.setLevel(old_level)

        self._wait_or_skip(lambda: self._wait_for(quorum_ready, "clock", max_wait_time=30), sync)

    def _prepare_timestamp_providers(self, timestamp_provider_configs):
        for timestamp_provider_index in xrange(self.yt_config.timestamp_provider_count):
            timestamp_provider_config_name = "timestamp_provider-{0}.yson".format(timestamp_provider_index)
            config_path = os.path.join(self.configs_path, timestamp_provider_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Timestamp provider config {0} not found. It is possible that you requested "
                                  "more timestamp providers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = timestamp_provider_configs[timestamp_provider_index]
                write_config(config, config_path)

            self.configs["timestamp_provider"].append(config)
            self.config_paths["timestamp_provider"].append(config_path)
            self._service_processes["timestamp_provider"].append(None)

    def start_timestamp_providers(self, sync=True):
        self._run_yt_component("timestamp-provider", name="timestamp_provider")

        def timestamp_providers_ready():
            self._validate_processes_are_running("timestamp_provider")

            addresses = self.get_timestamp_provider_monitoring_addresses()
            try:
                for address in addresses:
                    resp = requests.get("http://{0}/orchid".format(address))
                    resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False, traceback.format_exc()

            return True

        self._wait_or_skip(lambda: self._wait_for(timestamp_providers_ready, "timestamp_provider", max_wait_time=20), sync)

    def _prepare_nodes(self, node_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["node"] = []
            self.config_paths["node"] = []
            self._service_processes["node"] = []

        for node_index in xrange(self.yt_config.node_count):
            node_config_name = "node-" + str(node_index) + ".yson"
            config_path = os.path.join(self.configs_path, node_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Node config {0} not found. It is possible that you requested "
                                  "more nodes than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = node_configs[node_index]
                write_config(config, config_path)

            self.configs["node"].append(config)
            self.config_paths["node"].append(config_path)
            self._service_processes["node"].append(None)

    def start_nodes(self, sync=True):
        self._run_yt_component("node")

        client = self._create_cluster_client()

        def nodes_ready():
            self._validate_processes_are_running("node")

            nodes = client.list("//sys/cluster_nodes", attributes=["state"])
            return len(nodes) == self.yt_config.node_count and all(node.attributes["state"] == "online" for node in nodes)

        wait_function = lambda: self._wait_for(nodes_ready, "node", max_wait_time=max(self.yt_config.node_count * 6.0, 60))
        self._wait_or_skip(wait_function, sync)

    def _prepare_master_caches(self, master_cache_configs):
        for master_cache_index in xrange(self.yt_config.master_cache_count):
            master_cache_config_name = "master_cache-{0}.yson".format(master_cache_index)
            config_path = os.path.join(self.configs_path, master_cache_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Master cache config {0} not found. It is possible that you requested "
                                  "more timestamp providers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = master_cache_configs[master_cache_index]
                write_config(config, config_path)

            self.configs["master_cache"].append(config)
            self.config_paths["master_cache"].append(config_path)
            self._service_processes["master_cache"].append(None)

    def start_master_caches(self, sync=True):
        self._run_yt_component("master-cache", name="master_cache")

        def master_caches_ready():
            self._validate_processes_are_running("master_cache")

            addresses = self.get_master_cache_monitoring_addresses()
            try:
                for address in addresses:
                    resp = requests.get("http://{0}/orchid".format(address))
                    resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False, traceback.format_exc()

            return True

        self._wait_or_skip(lambda: self._wait_for(master_caches_ready, "master_cache", max_wait_time=20), sync)

    def _prepare_schedulers(self, scheduler_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["scheduler"] = []
            self.config_paths["scheduler"] = []
            self._service_processes["scheduler"] = []

        for scheduler_index in xrange(self.yt_config.scheduler_count):
            scheduler_config_name = "scheduler-" + str(scheduler_index) + ".yson"
            config_path = os.path.join(self.configs_path, scheduler_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Scheduler config {0} not found. It is possible that you requested "
                                  "more schedulers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = scheduler_configs[scheduler_index]
                write_config(config, config_path)

            self.configs["scheduler"].append(config)
            self.config_paths["scheduler"].append(config_path)
            self._service_processes["scheduler"].append(None)

    def _prepare_controller_agents(self, controller_agent_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["controller_agent"] = []
            self.config_paths["controller_agent"] = []
            self._service_processes["controller_agent"] = []

        for controller_agent_index in xrange(self.yt_config.controller_agent_count):
            controller_agent_config_name = "controller_agent-" + str(controller_agent_index) + ".yson"
            config_path = os.path.join(self.configs_path, controller_agent_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Controller agent config {0} not found. It is possible that you requested "
                                  "more controller agents than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = controller_agent_configs[controller_agent_index]
                write_config(config, config_path)

            self.configs["controller_agent"].append(config)
            self.config_paths["controller_agent"].append(config_path)
            self._service_processes["controller_agent"].append(None)

    def start_schedulers(self, sync=True):
        self._remove_scheduler_lock()

        client = self._create_cluster_client()

        # COMPAT(max42): drop this when 20.2 is deprecated.
        is_pool_tree_config_present = self.get_component_version("ytserver-master").abi >= (20, 3)
        if client.get("//sys/pool_trees/@type") == "map_node":
            if is_pool_tree_config_present:
                client.create("map_node", "//sys/pool_trees/default", attributes={"config": {}},
                    ignore_existing=True, recursive=True)
            else:
                client.create("map_node", "//sys/pool_trees/default", ignore_existing=True, recursive=True)
        else:
            client.create("scheduler_pool_tree", attributes={"name": "default"}, ignore_existing=True)
        client.set("//sys/pool_trees/@default_tree", "default")

        if is_pool_tree_config_present:
            client.set("//sys/pool_trees/default/@config/max_ephemeral_pools_per_user", 5)
        else:
            client.set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 5)

        if not client.exists("//sys/pools"):
            client.link("//sys/pool_trees/default", "//sys/pools", ignore_existing=True)

        self._run_yt_component("scheduler")

        def schedulers_ready():
            def check_node_state(node):
                if "state" in node:
                    return node["state"] == "online"
                return node["scheduler_state"] == "online" and node["master_state"] == "online"

            self._validate_processes_are_running("scheduler")

            instances = client.list("//sys/scheduler/instances")
            if len(instances) != self.yt_config.scheduler_count:
                return False

            try:
                active_scheduler_orchid_path = None
                for instance in instances:
                    orchid_path = "//sys/scheduler/instances/{0}/orchid".format(instance)
                    try:
                        client.set(orchid_path + "/@retry_backoff_time", 100)
                        if client.get(orchid_path + "/scheduler/connected"):
                            active_scheduler_orchid_path = orchid_path
                    except YtResponseError as error:
                        if not error.is_resolve_error():
                            raise

                if active_scheduler_orchid_path is None:
                    return False, "No active scheduler found"

                try:
                    master_cell_id = client.get("//sys/@cell_id")
                    scheduler_cell_id = client.get(active_scheduler_orchid_path + "/config/cluster_connection/primary_master/cell_id")
                    if master_cell_id != scheduler_cell_id:
                        return False, "Incorrect scheduler connected, its cell_id {0} does not match master cell {1}".format(scheduler_cell_id, master_cell_id)
                except YtResponseError as error:
                    if error.is_resolve_error():
                        return False, "Failed to request primary master cell id from master and scheduler" + str(error)
                    else:
                        raise

                nodes = client.list("//sys/cluster_nodes", attributes=["flavors"])
                def check_node(node):
                    # COMPAT(gritukan)
                    if "flavors" not in node.attributes:
                        return True
                    return "exec" in node.attributes["flavors"]

                exec_nodes = list(filter(check_node, nodes))

                if not self.yt_config.defer_node_start:
                    nodes = list(itervalues(client.get(active_scheduler_orchid_path + "/scheduler/nodes")))
                    return len(nodes) == len(exec_nodes) and all(check_node_state(node) for node in nodes)

                return True

            except YtResponseError as err:
                # Orchid connection refused
                if not err.contains_code(105) and not err.contains_code(100):
                    raise
                return False, err

        self._wait_or_skip(lambda: self._wait_for(schedulers_ready, "scheduler"), sync)

    def start_controller_agents(self, sync=True):
        self._run_yt_component("controller-agent", name="controller_agent")

        def controller_agents_ready():
            self._validate_processes_are_running("controller_agent")

            client = self._create_cluster_client()
            instances = client.list("//sys/controller_agents/instances")
            if len(instances) != self.yt_config.controller_agent_count:
                return False, "Only {0} agents are registered in cypress".format(len(instances))

            try:
                active_agents_count = 0
                for instance in instances:
                    orchid_path = "//sys/controller_agents/instances/{0}/orchid".format(instance)
                    path = orchid_path + "/controller_agent"
                    try:
                        client.set(orchid_path + "/@retry_backoff_time", 100)
                        active_agents_count += client.get(path + "/connected")
                    except YtResponseError as error:
                        if not error.is_resolve_error():
                            raise

                if active_agents_count < self.yt_config.controller_agent_count:
                    return False, "Only {0} agents are active".format(active_agents_count)

                return True
            except YtResponseError as err:
                # Orchid connection refused
                if not err.contains_code(105) and not err.contains_code(100):
                    raise
                return False, err

        self._wait_or_skip(lambda: self._wait_for(controller_agents_ready, "controller_agent", max_wait_time=20), sync)

    def create_client(self):
        if self.yt_config.http_proxy_count > 0:
            return YtClient(proxy=self.get_http_proxy_address(), config=self._default_client_config)
        return self.create_native_client()

    def create_native_client(self, driver_name="driver"):
        driver_config_path = self.config_paths[driver_name]

        with open(driver_config_path, "rb") as f:
            driver_config = yson.load(f)

        driver_config["connection_type"] = "native"

        config = update(
            self._default_client_config,
            {
                "backend": "native",
                "driver_config": driver_config
            }
        )

        return YtClient(config=config)

    def _create_cluster_client(self):
        if self.yt_config.use_native_client:
            return self.create_native_client()
        else:
            return self.create_client()

    def _remove_scheduler_lock(self):
        client = self._create_cluster_client()
        try:
            tx_id = client.get("//sys/scheduler/lock/@locks/0/transaction_id")
            if tx_id:
                client.abort_transaction(tx_id)
                logger.info("Previous scheduler transaction was aborted")
        except YtResponseError as error:
            if not error.is_resolve_error():
                raise

    def _prepare_drivers(self, driver_configs, rpc_driver_config, master_configs, clock_configs):
        for cell_index in xrange(self.yt_config.secondary_cell_count + 1):
            if cell_index == 0:
                tag = master_configs["primary_cell_tag"]
                name = "driver"
            else:
                tag = master_configs["secondary_cell_tags"][cell_index - 1]
                name = "driver_secondary_" + str(cell_index - 1)

            config_path = os.path.join(self.configs_path, "driver-{}.yson".format(cell_index))

            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = driver_configs[tag]
                write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        if self.yt_config.clock_count > 0:
            name = "clock_driver"
            config_path = os.path.join(self.configs_path, "clock-driver.yson")

            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = driver_configs[tag]
                write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        if self.yt_config.rpc_proxy_count > 0:
            name = "rpc_driver"
            config_path = os.path.join(self.configs_path, "rpc-driver.yson")

            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = rpc_driver_config
                write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        config_path = os.path.join(self.configs_path, "driver-logging.yson")

        config = _init_logging(self.logs_path, "driver", self.yt_config)
        write_config(config, config_path)

        self.configs["driver_logging"] = config
        self.config_paths["driver_logging"] = config_path

    def _prepare_http_proxies(self, proxy_configs, force_overwrite=False):
        self.configs["http_proxy"] = []
        self.config_paths["http_proxy"] = []

        for i in xrange(self.yt_config.http_proxy_count):
            config_path = os.path.join(self.configs_path, "http-proxy-{}.yson".format(i))
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Http proxy config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = proxy_configs[i]
                write_config(config, config_path)

            self.configs["http_proxy"].append(config)
            self.config_paths["http_proxy"].append(config_path)
            self._service_processes["http_proxy"].append(None)

    def _prepare_rpc_proxies(self, rpc_proxy_configs, rpc_client_config):
        self.configs["rpc_proxy"] = []
        self.config_paths["rpc_proxy"] = []

        for i in xrange(self.yt_config.rpc_proxy_count):
            config_path = os.path.join(self.configs_path, "rpc-proxy-{}.yson".format(i))
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Rpc proxy config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = rpc_proxy_configs[i]
                write_config(config, config_path)

            self.configs["rpc_proxy"].append(config)
            self.config_paths["rpc_proxy"].append(config_path)
            self._service_processes["rpc_proxy"].append(None)

            rpc_client_config_path = os.path.join(self.configs_path, "rpc-client.yson")
            if self._load_existing_environment:
                if not os.path.isfile(rpc_client_config_path):
                    raise YtError("Rpc client config {0} not found".format(rpc_client_config_path))
            else:
                write_config(rpc_client_config, rpc_client_config_path)

    def start_http_proxy(self, sync=True):
        self._run_yt_component("http-proxy", name="http_proxy")

        def proxy_ready():
            self._validate_processes_are_running("http_proxy")

            try:
                for http_proxy_address in self.get_http_proxy_addresses():
                    address = "127.0.0.1:{0}".format(http_proxy_address.split(":")[1])
                    resp = requests.get("http://{0}/api".format(address))
                    resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False, traceback.format_exc()

            return True

        self._wait_or_skip(lambda: self._wait_for(proxy_ready, "http_proxy", max_wait_time=20), sync)

    def start_rpc_proxy(self, sync=True):
        self._run_yt_component("proxy", name="rpc_proxy")

        client = self._create_cluster_client()

        proxies_with_discovery_count = 0
        proxies_ports = []
        for proxy in self.configs["rpc_proxy"]:
            proxies_ports.append(proxy["rpc_port"])

            discovery_service_config = get_value(proxy.get("discovery_service"), {})
            discovery_service_enabled = get_value(discovery_service_config.get("enable"), True)
            if discovery_service_enabled:
                proxies_with_discovery_count += 1

        def rpc_proxy_ready():
            self._validate_processes_are_running("rpc_proxy")

            proxies_from_discovery = client.get("//sys/rpc_proxies")
            proxies_discovery_ready = len(proxies_from_discovery) == proxies_with_discovery_count and \
                all("alive" in proxy for proxy in proxies_from_discovery.values())

            proxies_ports_ready = all(is_port_opened(port) for port in proxies_ports)

            return proxies_discovery_ready and proxies_ports_ready

        self._wait_or_skip(lambda: self._wait_for(rpc_proxy_ready, "rpc_proxy", max_wait_time=20), sync)

    def _validate_process_is_running(self, process, name, number=None):
        if number is not None:
            name_with_number = "{0}-{1}".format(name, number)
        else:
            name_with_number = name

        if process.poll() is not None:
            self._process_stderrs(name, number)
            raise YtError("Process {0} unexpectedly terminated with error code {1}. "
                          "If the problem is reproducible please report to yt@yandex-team.ru mailing list."
                          .format(name_with_number, process.returncode))

    def _validate_processes_are_running(self, name):
        for index, process in enumerate(self._service_processes[name]):
            if process is None:
                continue
            self._validate_process_is_running(process, name, index)

    def _wait_for(self, condition, name, max_wait_time=40, sleep_quantum=0.1):
        condition_error = None
        current_wait_time = 0
        logger.info("Waiting for %s...", name)
        while current_wait_time < max_wait_time:
            result = condition()
            if isinstance(result, tuple):
                ok, condition_error = result
            else:
                ok = result

            if ok:
                logger.info("%s ready", name.capitalize())
                return
            time.sleep(sleep_quantum)
            current_wait_time += sleep_quantum

        self._process_stderrs(name)

        error = YtError("{0} still not ready after {1} seconds. See logs in working dir for details."
                        .format(name.capitalize(), max_wait_time))
        if condition_error is not None:
            if isinstance(condition_error, str):
                condition_error = YtError(condition_error)
            error.inner_errors = [condition_error]

        raise error

    def _start_watcher(self):
        logger.info("Starting watcher")

        log_paths = []
        if self.yt_config.enable_debug_logging:
            def extract_debug_log_paths(service, config, result):
                log_config_path = "logging/writers/debug/file_name"
                result.append(get_value_from_config(config, log_config_path, service))
                if service == "node":
                    job_proxy_log_config_path = "exec_agent/job_proxy_logging/writers/debug/file_name"
                    result.append(get_value_from_config(config, job_proxy_log_config_path, service))

            extract_debug_log_paths("driver", {"logging": self.configs["driver_logging"]}, log_paths)

            for service, configs in list(iteritems(self.configs)):
                for config in flatten(configs):
                    if service.startswith("driver") or service.startswith("rpc_driver") or service.startswith("clock_driver"):
                        continue

                    extract_debug_log_paths(service, config, log_paths)

        self._service_processes["watcher"].append(None)

        self._watcher = ProcessWatcher(
            self._watcher_binary,
            self._pid_to_process.keys(),
            log_paths,
            lock_path=os.path.join(self.path, "lock_file"),
            config_dir=self.configs_path,
            logs_dir=self.logs_path,
            runtime_dir=self.runtime_data_path,
            process_runner=lambda args, env=None: self._run(args, "watcher", env=env),
            config=self.watcher_config)

        try:
            self._watcher.start()
        except YtError:
            logger.warning("Watcher stderr: %s", open(self._stderr_paths["watcher"][0]).read())
            raise

        logger.info("Watcher started")
