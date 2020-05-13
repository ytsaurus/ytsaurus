from __future__ import print_function

from .configs_provider import init_logging, get_default_provision, create_configs_provider
from .default_configs import get_dynamic_master_config
from .helpers import (
    read_config, write_config, is_dead_or_zombie, OpenPortIterator,
    wait_for_removing_file_lock, get_value_from_config, WaitFailed)
from .porto_helpers import PortoSubprocess, porto_avaliable
from .watcher import ProcessWatcher
try:
    from .arcadia_interop import get_gdb_path
except:
    get_gdb_path = None

from yt.common import YtError, remove_file, makedirp, update, which
from yt.wrapper.common import generate_uuid, flatten
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
import getpass
import random
import traceback
from collections import defaultdict, namedtuple
from threading import RLock
from itertools import count

logger = logging.getLogger("Yt.local")

CGROUP_TYPES = frozenset(["cpuacct", "cpu", "blkio", "freezer"])

BinaryVersion = namedtuple("BinaryVersion", ["abi", "literal"])

# Used to configure driver logging exactly once per environment (as a set of YT instances).
_environment_driver_logging_config_per_type = {}

def get_environment_driver_logging_config(default_config, driver_type):
    if driver_type not in _environment_driver_logging_config_per_type:
        return default_config
    # COMPAT
    if None in _environment_driver_logging_config_per_type:
        return _environment_driver_logging_config_per_type[None]
    return _environment_driver_logging_config_per_type[driver_type]

# COMPAT: default None value is for compatibility only.
def set_environment_driver_logging_config(config, driver_type=None):
    if config is None:
        raise YtError("Could not set environment driver logging config to None")
    global _environment_driver_logging_config_per_type
    _environment_driver_logging_config_per_type[driver_type] = config

class YtEnvRetriableError(YtError):
    pass

# TODO(ignat): replace with 'which' from yt.common after next major release.
def _which(name, flags=os.X_OK, custom_paths=None):
    """Return list of files in system paths with given name."""
    # TODO: check behavior when dealing with symlinks
    result = []
    paths = os.environ.get("PATH", "").split(os.pathsep)
    if custom_paths is not None:
        paths = custom_paths + paths
    for dir in paths:
        path = os.path.join(dir, name)
        if os.access(path, flags):
            result.append(path)
    return result

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
    paths = _which(binary, custom_paths=custom_paths)
    if paths:
        return paths[0]
    return None

def _get_yt_versions(custom_paths):
    result = {}
    binaries = ["ytserver-master", "ytserver-node", "ytserver-scheduler"]
    for binary in binaries:
        binary_path = _get_yt_binary_path(binary, custom_paths=custom_paths)
        if binary_path is not None:
            version_string = subprocess.check_output([binary_path, "--version"], stderr=subprocess.STDOUT)
            result[binary] = _parse_version(version_string)
    return result

def _is_ya_package(proxy_binary_path):
    proxy_binary_path = os.path.realpath(proxy_binary_path)
    ytnode_node = os.path.join(
        os.path.dirname(os.path.dirname(proxy_binary_path)),
        "built_by_ya.txt")
    return os.path.exists(ytnode_node)

def _configure_logger():
    logger.propagate = False

    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())

    if os.environ.get("YT_ENABLE_VERBOSE_LOGGING"):
        logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    else:
        logger.handlers[0].setFormatter(logging.Formatter("%(message)s"))


def _has_cgroups():
    if not sys.platform.startswith("linux"):
        return False
    if not os.path.exists("/sys/fs/cgroup"):
        return False
    for cgroup_type in CGROUP_TYPES:
        checked_path = "/sys/fs/cgroup/{0}/{1}".format(cgroup_type, getpass.getuser())
        if not os.path.exists(checked_path):
            checked_path = "/sys/fs/cgroup/{0}".format(cgroup_type)
        if not os.access(checked_path, os.R_OK | os.W_OK):
            return False
    return True


def _get_cgroup_path(cgroup_type, *args):
    return "/sys/fs/cgroup/{0}/{1}/{2}".format(cgroup_type, getpass.getuser(), "/".join(imap(str, args)))


def _list_process_children(pid):
    with open("/proc/{pid}/task/{pid}/children".format(pid=pid)) as f:
        return f.read().strip().split()


class YTInstance(object):
    # TODO(renadeen): remove extended_master_config when stable will get test_structured_security_logs
    def __init__(self, path, master_count=1, nonvoting_master_count=0, secondary_master_cell_count=0, clock_count=0,
                 node_count=1, defer_node_start=False,
                 scheduler_count=1, defer_scheduler_start=False,
                 controller_agent_count=None, defer_controller_agent_start=False,
                 http_proxy_count=0, http_proxy_ports=None, rpc_proxy_count=None, rpc_proxy_ports=None, cell_tag=0,
                 enable_debug_logging=True, enable_logging_compression=True, preserve_working_dir=False, tmpfs_path=None,
                 port_locks_path=None, local_port_range=None, port_range_start=None, node_port_set_size=None,
                 fqdn=None, jobs_resource_limits=None, jobs_memory_limit=None,
                 jobs_cpu_limit=None, jobs_user_slot_count=None, node_memory_limit_addition=None,
                 node_chunk_store_quota=None, allow_chunk_storage_in_tmpfs=False, modify_configs_func=None,
                 kill_child_processes=False, use_porto_for_servers=False, watcher_config=None,
                 add_binaries_to_path=True, enable_master_cache=None, enable_permission_cache=None,
                 driver_backend="native",
                 enable_structured_master_logging=False, enable_structured_scheduler_logging=False,
                 use_native_client=False, run_watcher=True, capture_stderr_to_file=None,
                 ytserver_all_path=None):
        _configure_logger()

        if use_porto_for_servers and not porto_avaliable():
            raise YtError("Option use_porto_for_servers is specified but porto is not available")

        self._use_porto_for_servers = use_porto_for_servers
        self._use_cgroup_for_servers = not use_porto_for_servers and _has_cgroups()

        self._subprocess_module = PortoSubprocess if use_porto_for_servers else subprocess

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
                            "proxy", "http-proxy", "tools", "scheduler", "controller-agent"]
                for program in programs:
                    os.symlink(os.path.abspath(ytserver_all_path), os.path.join(self.bin_path, "ytserver-" + program))
                os.environ["PATH"] = self.bin_path + ":" + os.environ["PATH"]

        if os.path.exists(self.bin_path):
            self.custom_paths = [self.bin_path]
        else:
            self.custom_paths = None

        self._binaries = _get_yt_versions(custom_paths=self.custom_paths)
        if ("ytserver-master" in self._binaries and
            "ytserver-node" in self._binaries and
            "ytserver-scheduler" in self._binaries):
            logger.info("Using multiple YT binaries with the following versions:")
            logger.info("  ytserver-master     %s", self._binaries["ytserver-master"].literal)
            logger.info("  ytserver-node       %s", self._binaries["ytserver-node"].literal)
            logger.info("  ytserver-scheduler  %s", self._binaries["ytserver-scheduler"].literal)

            abi_versions = set(imap(lambda v: v.abi, self._binaries.values()))
            if len(abi_versions) > 1:
                raise YtError("Mismatching YT versions. Make sure that all binaries are of compatible versions.")
            self.abi_version = abi_versions.pop()
        else:
            raise YtError("Failed to find YT binaries (ytserver-*) in $PATH. Make sure that YT is installed.")

        valid_driver_backends = ("native", "rpc")
        if driver_backend not in valid_driver_backends:
            raise YtError('Unrecognized driver backend: expected one of {0}, got "{1}"'.format(valid_driver_backends, driver_backend))

        if driver_backend == "rpc" and rpc_proxy_count is None:
            rpc_proxy_count = 1

        if driver_backend == "rpc" and rpc_proxy_count == 0:
            raise YtError("Driver with RPC backend is requested but RPC proxies aren't enabled.")

        if rpc_proxy_count is None:
            rpc_proxy_count = 0

        self._use_native_client = use_native_client

        self._random_generator = random.Random(random.SystemRandom().random())
        self._uuid = generate_uuid(self._random_generator)
        self._lock = RLock()

        self.configs = defaultdict(list)
        self.config_paths = defaultdict(list)

        makedirp(self.path)
        makedirp(self.logs_path)
        makedirp(self.configs_path)
        makedirp(self.runtime_data_path)

        self.stderrs_path = os.path.join(self.path, "stderrs")
        self.backtraces_path = os.path.join(self.path, "backtraces")
        makedirp(self.stderrs_path)
        makedirp(self.backtraces_path)
        self._stderr_paths = defaultdict(list)

        self._tmpfs_path = tmpfs_path
        if self._tmpfs_path is not None:
            self._tmpfs_path = os.path.abspath(self._tmpfs_path)

        self.port_locks_path = port_locks_path
        if self.port_locks_path is not None:
            makedirp(self.port_locks_path)
        self.local_port_range = local_port_range
        self._open_port_iterator = None

        if fqdn is None:
            self._hostname = socket.getfqdn()
        else:
            self._hostname = fqdn

        if capture_stderr_to_file is None:
            capture_stderr_to_file = bool(int(os.environ.get("YT_CAPTURE_STDERR_TO_FILE", "0")))
        self._capture_stderr_to_file = capture_stderr_to_file

        # Dictionary from service name to the list of processes.
        self._service_processes = defaultdict(list)
        # Dictionary from pid to tuple with Process and arguments
        self._pid_to_process = {}
        # List of all used cgroup paths.
        self._process_cgroup_paths = []

        self.master_count = master_count
        self.nonvoting_master_count = nonvoting_master_count
        self.secondary_master_cell_count = secondary_master_cell_count
        self.clock_count = clock_count
        self.node_count = node_count
        self.defer_node_start = defer_node_start
        self.scheduler_count = scheduler_count
        self.defer_scheduler_start = defer_scheduler_start
        if controller_agent_count is None:
            if scheduler_count > 0:
                controller_agent_count = 1
            else:
                controller_agent_count = 0
        self.defer_controller_agent_start = defer_controller_agent_start
        self.controller_agent_count = controller_agent_count
        self.has_http_proxy = http_proxy_count > 0
        self.http_proxy_count = http_proxy_count
        self.http_proxy_ports = http_proxy_ports
        self.has_rpc_proxy = rpc_proxy_count > 0
        self.rpc_proxy_count = rpc_proxy_count
        self.rpc_proxy_ports = rpc_proxy_ports
        self._enable_debug_logging = enable_debug_logging
        self._enable_logging_compression = enable_logging_compression
        self._cell_tag = cell_tag
        self._kill_child_processes = kill_child_processes
        self._started = False
        self._wait_functions = []
        self.driver_backend = driver_backend

        self._run_watcher = run_watcher
        self.watcher_config = watcher_config
        if self.watcher_config is None:
            self.watcher_config = {}

        # Enable logrotate compression if logs are not compressed by default.
        if not self._enable_logging_compression:
            self.watcher_config["logs_rotate_compress"] = True
        else:
            self.watcher_config["disable_logrotate"] = True

        self._default_client_config = {"enable_token": False}

        self._prepare_environment(jobs_resource_limits, jobs_memory_limit, jobs_cpu_limit, jobs_user_slot_count, node_chunk_store_quota,
                                  node_memory_limit_addition, allow_chunk_storage_in_tmpfs, port_range_start, node_port_set_size,
                                  enable_master_cache, enable_permission_cache, modify_configs_func,
                                  enable_structured_master_logging, enable_structured_scheduler_logging)

    def _get_ports_generator(self, port_range_start):
        if port_range_start and isinstance(port_range_start, int):
            return count(port_range_start)
        else:
            self._open_port_iterator = OpenPortIterator(
                port_locks_path=self.port_locks_path,
                local_port_range=self.local_port_range)
            return self._open_port_iterator

    def _get_cgroup_path(self, cgroup_type, *args):
        return _get_cgroup_path(cgroup_type, "yt", self._uuid, *args)

    def _prepare_cgroups(self):
        if self._use_cgroup_for_servers:
            for cgroup_type in CGROUP_TYPES:
                cgroup_path = self._get_cgroup_path(cgroup_type)
                makedirp(cgroup_path)
                self._process_cgroup_paths.append(cgroup_path)
                logger.info("Registered cgroup {0}".format(cgroup_path))

    def _prepare_directories(self):
        master_dirs = []
        master_tmpfs_dirs = [] if self._tmpfs_path else None

        for cell_index in xrange(self.secondary_master_cell_count + 1):
            name = self._get_master_name("master", cell_index)
            master_dirs.append([os.path.join(self.runtime_data_path, name, str(i)) for i in xrange(self.master_count)])
            for dir_ in master_dirs[cell_index]:
                makedirp(dir_)

            if self._tmpfs_path is not None:
                master_tmpfs_dirs.append([os.path.join(self._tmpfs_path, name, str(i)) for i in xrange(self.master_count)])

                for dir_ in master_tmpfs_dirs[cell_index]:
                    makedirp(dir_)

        clock_dirs = [os.path.join(self.runtime_data_path, "clock", str(i)) for i in xrange(self.clock_count)]
        for dir_ in clock_dirs:
            makedirp(dir_)

        clock_tmpfs_dirs = None
        if self._tmpfs_path is not None:
            clock_tmpfs_dirs = [os.path.join(self._tmpfs_path, name, str(i)) for i in xrange(self.clock_count)]
            for dir_ in clock_tmpfs_dirs:
                makedirp(dir_)

        scheduler_dirs = [os.path.join(self.runtime_data_path, "scheduler", str(i)) for i in xrange(self.scheduler_count)]
        for dir_ in scheduler_dirs:
            makedirp(dir_)

        controller_agent_dirs = [os.path.join(self.runtime_data_path, "controller_agent", str(i)) for i in xrange(self.controller_agent_count)]
        for dir_ in controller_agent_dirs:
            makedirp(dir_)

        node_dirs = [os.path.join(self.runtime_data_path, "node", str(i)) for i in xrange(self.node_count)]
        for dir_ in node_dirs:
            makedirp(dir_)

        node_tmpfs_dirs = None
        if self._tmpfs_path is not None:
            node_tmpfs_dirs = [os.path.join(self._tmpfs_path, "node", str(i)) for i in xrange(self.node_count)]
            for dir_ in node_tmpfs_dirs:
                makedirp(dir_)

        http_proxy_dirs = [os.path.join(self.runtime_data_path, "http_proxy", str(i)) for i in xrange(self.http_proxy_count)]
        for dir_ in http_proxy_dirs:
            makedirp(dir_)

        rpc_proxy_dirs = [os.path.join(self.runtime_data_path, "rpc_proxy", str(i)) for i in xrange(self.rpc_proxy_count)]
        for dir_ in rpc_proxy_dirs:
            makedirp(dir_)

        return {"master": master_dirs,
                "master_tmpfs": master_tmpfs_dirs,
                "clock": clock_dirs,
                "clock_tmpfs": clock_tmpfs_dirs,
                "scheduler": scheduler_dirs,
                "controller_agent": controller_agent_dirs,
                "node": node_dirs,
                "node_tmpfs": node_tmpfs_dirs,
                "http_proxy": http_proxy_dirs,
                "rpc_proxy": rpc_proxy_dirs}

    def _prepare_environment(self, jobs_resource_limits, jobs_memory_limit, jobs_cpu_limit, jobs_user_slot_count, node_chunk_store_quota,
                             node_memory_limit_addition, allow_chunk_storage_in_tmpfs, port_range_start, node_port_set_size,
                             enable_master_cache, enable_permission_cache, modify_configs_func,
                             enable_structured_master_logging, enable_structured_scheduler_logging):
        logger.info("Preparing cluster instance as follows:")
        logger.info("  uuid               %s", self._uuid)
        logger.info("  clocks             %d", self.clock_count)
        logger.info("  masters            %d (%d nonvoting)", self.master_count, self.nonvoting_master_count)
        logger.info("  nodes              %d", self.node_count)
        logger.info("  schedulers         %d", self.scheduler_count)
        logger.info("  controller agents  %d", self.controller_agent_count)

        if self.secondary_master_cell_count > 0:
            logger.info("  secondary cells    %d", self.secondary_master_cell_count)

        logger.info("  HTTP proxies       %d", self.http_proxy_count)
        logger.info("  RPC proxies        %d", self.rpc_proxy_count)
        logger.info("  working dir        %s", self.path)

        if self.master_count == 0:
            logger.warning("Master count is zero. Instance is not prepared.")
            return

        configs_provider = create_configs_provider(self.abi_version)

        provision = get_default_provision()
        provision["master"]["cell_size"] = self.master_count
        provision["master"]["secondary_cell_count"] = self.secondary_master_cell_count
        provision["master"]["primary_cell_tag"] = self._cell_tag
        provision["master"]["cell_nonvoting_master_count"] = self.nonvoting_master_count
        provision["clock"]["cell_size"] = self.clock_count
        provision["scheduler"]["count"] = self.scheduler_count
        provision["controller_agent"]["count"] = self.controller_agent_count
        provision["node"]["count"] = self.node_count
        if jobs_resource_limits is not None:
            provision["node"]["jobs_resource_limits"] = jobs_resource_limits
        if jobs_memory_limit is not None:
            provision["node"]["jobs_resource_limits"]["memory"] = jobs_memory_limit
        if jobs_cpu_limit is not None:
            provision["node"]["jobs_resource_limits"]["cpu"] = jobs_cpu_limit
        if jobs_user_slot_count is not None:
            provision["node"]["jobs_resource_limits"]["user_slots"] = jobs_user_slot_count
        provision["node"]["memory_limit_addition"] = node_memory_limit_addition
        provision["node"]["chunk_store_quota"] = node_chunk_store_quota
        provision["node"]["allow_chunk_storage_in_tmpfs"] = allow_chunk_storage_in_tmpfs
        provision["node"]["port_set_size"] = node_port_set_size
        provision["http_proxy"]["count"] = self.http_proxy_count
        provision["http_proxy"]["http_ports"] = self.http_proxy_ports
        provision["rpc_proxy"]["count"] = self.rpc_proxy_count
        provision["rpc_proxy"]["rpc_ports"] = self.rpc_proxy_ports
        provision["driver"]["backend"] = self.driver_backend
        provision["fqdn"] = self._hostname
        provision["enable_debug_logging"] = self._enable_debug_logging
        provision["enable_logging_compression"] = self._enable_logging_compression
        if enable_master_cache is not None:
            provision["enable_master_cache"] = enable_master_cache
        if enable_permission_cache is not None:
            provision["enable_permission_cache"] = enable_permission_cache
        provision["enable_structured_master_logging"] = enable_structured_master_logging
        provision["enable_structured_scheduler_logging"] = enable_structured_scheduler_logging

        dirs = self._prepare_directories()

        cluster_configuration = configs_provider.build_configs(self._get_ports_generator(port_range_start), dirs, self.logs_path, provision)

        if modify_configs_func:
            modify_configs_func(cluster_configuration, self.abi_version)

        self._cluster_configuration = cluster_configuration

        self._prepare_cgroups()
        if self.master_count + self.secondary_master_cell_count > 0:
            self._prepare_masters(cluster_configuration["master"])
        if self.clock_count > 0:
            self._prepare_clocks(cluster_configuration["clock"])
        if self.node_count > 0:
            self._prepare_nodes(cluster_configuration["node"])
        if self.scheduler_count > 0:
            self._prepare_schedulers(cluster_configuration["scheduler"])
        if self.controller_agent_count > 0:
            self._prepare_controller_agents(cluster_configuration["controller_agent"])
        if self.has_http_proxy:
            self._prepare_http_proxies(cluster_configuration["http_proxy"])
        if self.has_rpc_proxy:
            self._prepare_rpc_proxies(cluster_configuration["rpc_proxy"], cluster_configuration["rpc_client"])

        http_proxy_url = None
        if self.has_http_proxy:
            http_proxy_url = "localhost:" + self.get_proxy_address().split(":", 1)[1]
        self._prepare_driver(
            cluster_configuration["driver"],
            cluster_configuration["rpc_driver"],
            cluster_configuration["master"],
            cluster_configuration["clock"],
            http_proxy_url)
        # COMPAT. Will be removed eventually.
        self._prepare_console_driver()

    def _wait_or_skip(self, function, sync):
        if sync:
            function()
        else:
            self._wait_functions.append(function)

    def remove_runtime_data(self):
        if os.path.exists(self.runtime_data_path):
            shutil.rmtree(self.runtime_data_path, ignore_errors=True)

    def start(self, start_secondary_master_cells=False, on_masters_started_func=None):
        for name, processes in iteritems(self._service_processes):
            for index in xrange(len(processes)):
                processes[index] = None
        self._pid_to_process.clear()

        if self.master_count == 0:
            logger.warning("Cannot start YT instance without masters")
            return

        self.pids_file = open(self.pids_filename, "wt")
        try:
            self._configure_driver_logging()

            if self.has_http_proxy:
                self.start_http_proxy(sync=False)

            self.start_master_cell(sync=False)

            if self.clock_count > 0:
                self.start_clock(sync=False)

            if self.has_rpc_proxy:
                self.start_rpc_proxy(sync=False)

            self.synchronize()

            if start_secondary_master_cells:
                self.start_secondary_master_cells(sync=False)
                self.synchronize()

            # TODO(asaitgalin): Create this user inside master.
            client = self._create_cluster_client()
            user_id = client.create("user", attributes={"name": "application_operations"})
            # Tests suites that delay secondary master start probably won't need this user anyway.
            if start_secondary_master_cells:
                wait(lambda: client.get("#" + user_id + "/@life_stage") == "creation_committed")
                client.add_member("application_operations", "superusers")

            if self.has_http_proxy:
                # NB: it is used to determine proper operation URL in local mode.
                client.set("//sys/@local_mode_proxy_address", self.get_http_proxy_address())

            if on_masters_started_func is not None:
                on_masters_started_func()
            if self.node_count > 0 and not self.defer_node_start:
                self.start_nodes(sync=False)
            if self.scheduler_count > 0 and not self.defer_scheduler_start:
                self.start_schedulers(sync=False)
            if self.controller_agent_count > 0 and not self.defer_controller_agent_start:
                self.start_controller_agents(sync=False)

            self.synchronize()

            if self._run_watcher:
                self._start_watcher()
                self._started = True

            self._write_environment_info_to_file()
        except (YtError, KeyboardInterrupt) as err:
            logger.exception("Failed to start environment, dumping GDB backtraces")
            try:
                children = _list_process_children(os.getpid())
                logger.info("Process children: %s", children)
                for child in children:
                    backtrace_path = os.path.join(self.backtraces_path, "gdb.{}".format(child))
                    self._dump_backtrace(int(child), backtrace_path)
            except Exception as e:
                logger.warning("Failed to list children of current process: %s", e)
                logger.exception(e)
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

        for name in ["http_proxy", "node", "scheduler", "controller_agent", "master", "rpc_proxy"]:
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

    def rewrite_http_proxy_configs(self):
        self._prepare_http_proxies(self._cluster_configuration["http_proxy"], force_overwrite=True)

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
        if not self.has_http_proxy:
            raise YtError("Http proxies are not started")
        return ["{0}:{1}".format(self._hostname, get_value_from_config(config, "port", "http_proxy")) for config in self.configs["http_proxy"]]

    # XXX(kiselyovp) Only returns one GRPC proxy address even if multiple RPC proxy servers are launched.
    def get_grpc_proxy_address(self):
        if not self.has_rpc_proxy:
            raise YtError("Rpc proxies are not started")
        addresses = get_value_from_config(self.configs["rpc_proxy"][0], "grpc_server/addresses", "rpc_proxy")
        return addresses[0]["address"]

    def kill_cgroups(self):
        if self._use_cgroup_for_servers:
            with self._lock:
                self.kill_cgroups_impl()

    def kill_cgroups_impl(self):
        freezer_cgroups = []
        for cgroup_path in self._process_cgroup_paths:
            if "cgroup/freezer" in cgroup_path:
                freezer_cgroups.append(cgroup_path)

        for freezer_path in freezer_cgroups:
            logger.info("Checking tasks in {}".format(freezer_path))
            with open(os.path.join(freezer_path, "tasks")) as f:
                for line in f:
                    pid = int(line)
                    # Stopping process activity. This prevents
                    # forking of new processes, for example.
                    logger.info("Sending SIGSTOP (pid: {})".format(pid))
                    os.kill(pid, signal.SIGSTOP)

        for freezer_path in freezer_cgroups:
            with open(os.path.join(freezer_path, "tasks")) as f:
                for line in f:
                    pid = int(line)
                    # Stopping process activity. This prevents
                    # forking of new processes, for example.
                    logger.info("Sending SIGKILL (pid: {})".format(pid))
                    os.kill(pid, signal.SIGKILL)

        for cgroup_path in self._process_cgroup_paths:
            for dirpath, dirnames, _ in os.walk(cgroup_path, topdown=False):
                for dirname in dirnames:
                    inner_cgroup_path = os.path.join(dirpath, dirname)
                    for iter in xrange(5):
                        try:
                            logger.info("Removing {}".format(inner_cgroup_path))
                            os.rmdir(inner_cgroup_path)
                            break
                        except OSError:
                            try:
                                with open(os.path.join(inner_cgroup_path, "tasks")) as f:
                                    logger.exception("Failed to remove cgroup dir, tasks {0}".format(f.readlines()))
                            except:
                                logger.exception("Failed to remove cgroup dir")

                            time.sleep(0.5)

            try:
                # NB(psushin): sometimes cgroups still remain busy.
                # We don't want to fail tests in this case.
                logger.info("Removing {}".format(cgroup_path))
                os.rmdir(cgroup_path)
            except OSError:
                logger.exception("Failed to remove cgroup dir {0}".format(cgroup_path))

        self._process_cgroup_paths = []

    def kill_schedulers(self, indexes=None):
        self.kill_service("scheduler", indexes=indexes)

    def kill_controller_agents(self, indexes=None):
        self.kill_service("controller_agent", indexes=indexes)

    def kill_nodes(self, indexes=None, wait_offline=True):
        self.kill_service("node", indexes=indexes)

        addresses = None
        if indexes is None:
            indexes = list(xrange(self.node_count))
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
        self.kill_masters_at_cells(indexes=None, cell_indexes=xrange(self.secondary_master_cell_count + 1))

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

    def _configure_driver_logging(self):
        try:
            import yt_driver_bindings
            yt_driver_bindings.configure_logging(
                get_environment_driver_logging_config(self.driver_logging_config, "native")
            )
        except ImportError:
            pass

        try:
            import yt_driver_rpc_bindings
            yt_driver_rpc_bindings.configure_logging(
                get_environment_driver_logging_config(self.rpc_driver_logging_config, "rpc")
            )
        except ImportError:
            pass

        import yt.wrapper.native_driver as native_driver
        native_driver.logging_configured = True

    def _write_environment_info_to_file(self):
        info = {}
        if self.has_http_proxy:
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

        logger.info("Sending SIGKILL (pid: {})".format(proc.pid))
        os.kill(proc.pid, signal.SIGKILL)
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError as e:
            logger.error("killpg({}) failed: {}({})".format(proc.pid, e.errno, errno.errorcode[e.errno]))
            if e.errno != errno.ESRCH:
                raise
        try:
            wait(lambda: is_dead_or_zombie(proc.pid))
        except WaitFailed:
            try:
                with open("/proc/{0}/status".format(proc.pid), "r") as fin:
                    logger.error("Process status: %s", fin.read().replace("\n", "\\n"))
            except IOError:
                pass
            raise

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + "\n")
        self.pids_file.flush()

    def _process_stderrs(self, name, number=None):
        if not self._capture_stderr_to_file:
            return

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

    def _run(self, args, name, number=None, cgroup_paths=None, timeout=0.1):
        if cgroup_paths is None:
            cgroup_paths = []

        with self._lock:
            index = number if number is not None else 0
            name_with_number = name
            if number:
                name_with_number = "{0}-{1}".format(name, number)

            if self._service_processes[name][index] is not None:
                logger.debug("Process %s already running", name_with_number, self._service_processes[name][index].pid)
                return

            stderr_path = os.path.join(self.stderrs_path, "stderr.{0}".format(name_with_number))
            self._stderr_paths[name].append(stderr_path)

            for cgroup_path in cgroup_paths:
                makedirp(cgroup_path)

            stdout = open(os.devnull, "w")
            stderr = None
            if self._capture_stderr_to_file or isinstance(self._subprocess_module, PortoSubprocess):
                stderr = open(stderr_path, "w")

            if self._kill_child_processes:
                args += ["--pdeathsig", str(int(signal.SIGTERM))]
            for cgroup_path in cgroup_paths:
                args += ["--cgroup", cgroup_path]
            args += ["--setsid"]

            env = copy.copy(os.environ)
            env = update(env, {"YT_ALLOC_CONFIG": "{enable_eager_memory_release=%true}"})

            p = self._subprocess_module.Popen(args, shell=False, close_fds=True, cwd=self.runtime_data_path,
                                              env=env,
                                              stdout=stdout, stderr=stderr)

            time.sleep(timeout)
            self._validate_process_is_running(p, name, number)

            self._service_processes[name][index] = p
            self._pid_to_process[p.pid] = (p, args)
            self._append_pid(p.pid)

            logger.debug("Process %s started (pid: %d)", name, p.pid)

            return p

    def _run_yt_component(self, component, name=None, config_option=None):
        if name is None:
            name = component
        if config_option is None:
            config_option = "--config"

        logger.info("Starting %s", name)

        for i in xrange(len(self.configs[name])):
            args = None
            cgroup_paths = None
            if self.abi_version[0] >= 19:
                args = [_get_yt_binary_path("ytserver-" + component, custom_paths=self.custom_paths)]
                if self._kill_child_processes:
                    args.extend(["--pdeathsig", str(int(signal.SIGKILL))])
            else:
                raise YtError("Unsupported YT ABI version {0}".format(self.abi_version))
            args.extend([config_option, self.config_paths[name][i]])
            cgroup_paths = None
            if self._use_cgroup_for_servers:
                cgroup_paths = []
                for cgroup_type in CGROUP_TYPES:
                    cgroup_path = self._get_cgroup_path(cgroup_type, "{0}-{1}".format(name, i))
                    cgroup_paths.append(cgroup_path)
            self._run(args, name, number=i, cgroup_paths=cgroup_paths)

    def _get_master_name(self, master_name, cell_index):
        if cell_index == 0:
            return master_name # + "_primary"
        else:
            return master_name + "_secondary_" + str(cell_index - 1)

    def _prepare_masters(self, master_configs, force_overwrite=False):
        for cell_index in xrange(self.secondary_master_cell_count + 1):
            master_name = self._get_master_name("master", cell_index)
            if cell_index == 0:
                cell_tag = master_configs["primary_cell_tag"]
            else:
                cell_tag = master_configs["secondary_cell_tags"][cell_index - 1]

            if force_overwrite:
                self.configs[master_name] = []
                self.config_paths[master_name] = []
                self._service_processes[master_name] = []

            for master_index in xrange(self.master_count):
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
        for i in xrange(self.secondary_master_cell_count):
            self.start_master_cell(i + 1, sync=sync)

    def _prepare_clocks(self, clock_configs):
        for clock_index in xrange(self.clock_count):
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

    def _prepare_nodes(self, node_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["node"] = []
            self.config_paths["node"] = []
            self._service_processes["node"] = []

        for node_index in xrange(self.node_count):
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
            return len(nodes) == self.node_count and all(node.attributes["state"] == "online" for node in nodes)

        wait_function = lambda: self._wait_for(nodes_ready, "node", max_wait_time=max(self.node_count * 6.0, 20))
        self._wait_or_skip(wait_function, sync)

    def _prepare_schedulers(self, scheduler_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["scheduler"] = []
            self.config_paths["scheduler"] = []
            self._service_processes["scheduler"] = []

        for scheduler_index in xrange(self.scheduler_count):
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

        for controller_agent_index in xrange(self.controller_agent_count):
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
        # COMPAT(renadeen): add yt/yt master branch commit hash with user managed pools
        if client.get("//sys/pool_trees/@type") == "map_node":
            client.create("map_node", "//sys/pool_trees/default", ignore_existing=True, recursive=True)
        else:
            client.create("scheduler_pool_tree", attributes={"name": "default"}, ignore_existing=True)
        client.set("//sys/pool_trees/@default_tree", "default")
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
            if len(instances) != self.scheduler_count:
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

                if not self.defer_node_start:
                    nodes = list(itervalues(client.get(active_scheduler_orchid_path + "/scheduler/nodes")))
                    return len(nodes) == self.node_count and all(check_node_state(node) for node in nodes)

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
            if len(instances) != self.controller_agent_count:
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

                if active_agents_count < self.controller_agent_count:
                    return False, "Only {0} agents are active".format(active_agents_count)

                return True
            except YtResponseError as err:
                # Orchid connection refused
                if not err.contains_code(105) and not err.contains_code(100):
                    raise
                return False, err

        self._wait_or_skip(lambda: self._wait_for(controller_agents_ready, "controller_agent", max_wait_time=20), sync)

    def create_client(self):
        if self.has_http_proxy:
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
        if self._use_native_client:
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

    def _prepare_driver(self, driver_configs, rpc_driver_configs, master_configs, clock_configs, http_proxy_url):
        driver_types = ["native"]
        if self.has_rpc_proxy:
            driver_types.append("rpc")

        for driver_type in driver_types:
            for cell_index in xrange(self.secondary_master_cell_count + 1):
                if cell_index == 0:
                    tag = master_configs["primary_cell_tag"]
                    name = "driver"
                else:
                    tag = master_configs["secondary_cell_tags"][cell_index - 1]
                    name = "driver_secondary_" + str(cell_index - 1)

                if driver_type == "rpc":
                    prefix = "rpc_"
                else:
                    prefix = ""

                config_path = os.path.join(self.configs_path, "{0}driver-{1}.yson".format(prefix, cell_index))

                if self._load_existing_environment:
                    if not os.path.isfile(config_path):
                        raise YtError("Driver config {0} not found".format(config_path))
                    config = read_config(config_path)
                else:
                    if driver_type == "rpc":
                        config = rpc_driver_configs[tag]
                        # TODO(ignat): Fix this hack.
                        if "addresses" in config and http_proxy_url is not None:
                            del config["addresses"]
                            config["cluster_url"] = http_proxy_url
                            config["discover_proxies_from_cypress"] = False
                    else:
                        config = driver_configs[tag]
                    write_config(config, config_path)

                # COMPAT
                if cell_index == 0:
                    link_path = os.path.join(self.path, "driver.yson")
                    if os.path.exists(link_path):
                        os.remove(link_path)
                    os.symlink(config_path, link_path)

                self.configs[prefix + name] = config
                self.config_paths[prefix + name] = config_path

        if self.clock_count > 0:
            name = "clock_driver"
            config_path = os.path.join(self.configs_path, "clock-driver.yson")

            config = driver_configs[clock_configs["cell_tag"]]
            write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        self.driver_logging_config = init_logging(
            None, self.logs_path, "driver",
            log_errors_to_stderr=False,
            enable_debug_logging=self._enable_debug_logging,
            enable_compression=self._enable_logging_compression)
        self.rpc_driver_logging_config = init_logging(
            None, self.logs_path, "rpc_driver",
            log_errors_to_stderr=False,
            enable_debug_logging=self._enable_debug_logging,
            enable_compression=self._enable_logging_compression)

    def _prepare_console_driver(self):
        config = {}
        config["driver"] = self.configs["driver"]
        config["logging"] = init_logging(
            None, self.path, "console_driver",
            log_errors_to_stderr=False,
            enable_debug_logging=self._enable_debug_logging,
            enable_compression=self._enable_logging_compression)

        config_path = os.path.join(self.path, "console_driver_config.yson")

        write_config(config, config_path)

        self.configs["console_driver"].append(config)
        self.config_paths["console_driver"].append(config_path)

    def _prepare_http_proxies(self, proxy_configs, force_overwrite=False):
        self.configs["http_proxy"] = []
        self.config_paths["http_proxy"] = []

        for i in xrange(self.http_proxy_count):
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

        for i in xrange(self.rpc_proxy_count):
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

        def rpc_proxy_ready():
            self._validate_processes_are_running("rpc_proxy")

            proxies = client.get("//sys/rpc_proxies")
            return len(proxies) == self.rpc_proxy_count and all("alive" in proxy for proxy in proxies.values())

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

        self._dump_backtraces(name)

        error = YtError("{0} still not ready after {1} seconds. See logs in working dir for details."
                        .format(name.capitalize(), max_wait_time))
        if condition_error is not None:
            if isinstance(condition_error, str):
                condition_error = YtError(condition_error)
            error.inner_errors = [condition_error]

        raise error

    def _dump_backtrace(self, pid, backtrace_path):
        logger.info("Dumping backtrace for process {} to {}".format(pid, backtrace_path))
        subprocess.check_call(
            "{gdb} -p {pid} -ex 'set confirm off' -ex 'set pagination off' -ex 'thread apply all bt' -ex 'quit'".format(
                gdb=get_gdb_path(),
                pid=pid,
            ),
            stdout=open(backtrace_path, "w"),
            stderr=sys.stderr,
            shell=True,
        )

    def _dump_backtraces(self, name):
        for index, process in enumerate(self._service_processes[name]):
            if process is None:
                continue
            if get_gdb_path is not None and process.poll() is None:
                backtrace_path = os.path.join(self.backtraces_path, "gdb.{}-{}".format(name, index))
                self._dump_backtrace(process.pid, backtrace_path)

    def _start_watcher(self):
        log_paths = []
        if self._enable_debug_logging:
            def extract_debug_log_paths(service, config, result):
                log_config_path = "logging/writers/debug/file_name"
                result.append(get_value_from_config(config, log_config_path, service))
                if service == "node":
                    job_proxy_log_config_path = "exec_agent/job_proxy_logging/writers/debug/file_name"
                    result.append(get_value_from_config(config, job_proxy_log_config_path, service))

            extract_debug_log_paths("driver", {"logging": self.driver_logging_config}, log_paths)
            for service, configs in list(iteritems(self.configs)):
                for config in flatten(configs):
                    if service.startswith("driver") or service.startswith("rpc_driver") or service.startswith("clock_driver"):
                        continue

                    extract_debug_log_paths(service, config, log_paths)

        self._service_processes["watcher"].append(None)

        self._watcher = ProcessWatcher(
            self._pid_to_process.keys(),
            log_paths,
            lock_path=os.path.join(self.path, "lock_file"),
            config_dir=self.configs_path,
            logs_dir=self.logs_path,
            runtime_dir=self.runtime_data_path,
            process_runner=lambda args: self._run(args, "watcher"),
            config=self.watcher_config)

        self._watcher.start()
