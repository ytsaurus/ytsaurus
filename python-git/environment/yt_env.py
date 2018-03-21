from __future__ import print_function

from .configs_provider import init_logging, get_default_provision, create_configs_provider
from .default_configs import get_watcher_config
from .helpers import read_config, write_config, is_dead_or_zombie, OpenPortIterator, wait_for_removing_file_lock, WEB_INTERFACE_RESOURCES_PATH
from .porto_helpers import PortoSubprocess, porto_avaliable

from yt.common import YtError, remove_file, makedirp, set_pdeathsig, which, to_native_str
from yt.wrapper.common import generate_uuid, flatten
from yt.wrapper.errors import YtResponseError
from yt.wrapper import YtClient
import yt.yson as yson
import yt.subprocess_wrapper as subprocess

from yt.packages.six import itervalues, iteritems
from yt.packages.six.moves import xrange, map as imap, filter as ifilter
import yt.packages.requests as requests

import logging
import os
import time
import signal
import socket
import shutil
import sys
import getpass
from collections import defaultdict, namedtuple
from threading import RLock
from itertools import count

logger = logging.getLogger("Yt.local")

CGROUP_TYPES = frozenset(["cpuacct", "cpu", "blkio", "memory", "freezer"])

BinaryVersion = namedtuple("BinaryVersion", ["abi", "literal"])

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

def _which_yt_binaries():
    result = {}
    binaries = ["ytserver", "ytserver-master", "ytserver-node", "ytserver-scheduler"]
    for binary in binaries:
        if which(binary):
            version_string = subprocess.check_output([binary, "--version"], stderr=subprocess.STDOUT)
            result[binary] = _parse_version(version_string)
    return result

def _find_nodejs():
    nodejs_binary = None
    for name in ["nodejs", "node"]:
        if which(name):
            nodejs_binary = name
            break

    if nodejs_binary is None:
        raise YtError("Failed to find nodejs binary. "
                      "Make sure you added nodejs directory to PATH variable")

    version = subprocess.check_output([nodejs_binary, "-v"])

    if not version.startswith("v0.8"):
        raise YtError("Failed to find appropriate nodejs version (should start with 0.8)")

    return nodejs_binary

def _get_proxy_version(node_binary_path, proxy_binary_path):
    # Output example: "*** YT HTTP Proxy ***\nVersion 0.17.3\nDepends..."
    process = subprocess.Popen([node_binary_path, proxy_binary_path, "-v"], stderr=subprocess.PIPE)
    _, err = process.communicate()
    version_str = to_native_str(err).split("\n")[1].split()[1]

    try:
        version = tuple(imap(int, version_str.split(".")))
    except ValueError:
        return None

    return version

def _config_safe_get(config, service_name, key):
    d = config
    parts = key.split("/")
    for k in parts:
        d = d.get(k)
        if d is None:
            raise YtError('Failed to get required key "{0}" from {1} config'.format(key, service_name))
    return d

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


class YTInstance(object):
    def __init__(self, path, master_count=1, nonvoting_master_count=0, secondary_master_cell_count=0,
                 node_count=1, scheduler_count=1, controller_agent_count=None,
                 has_proxy=False, proxy_port=None, has_rpc_proxy=False,
                 rpc_proxy_port=None, cell_tag=0, skynet_manager_count=0,
                 enable_debug_logging=True, preserve_working_dir=False, tmpfs_path=None,
                 port_locks_path=None, port_range_start=None, fqdn=None, jobs_memory_limit=None,
                 jobs_cpu_limit=None, jobs_user_slot_count=None, node_memory_limit_addition=None,
                 node_chunk_store_quota=None, allow_chunk_storage_in_tmpfs=False, modify_configs_func=None,
                 kill_child_processes=False, use_porto_for_servers=False, watcher_config=None):
        _configure_logger()

        self._subprocess_module = PortoSubprocess if use_porto_for_servers and porto_avaliable() else subprocess
        self._use_porto_for_servers = use_porto_for_servers

        self._binaries = _which_yt_binaries()
        if "ytserver" in self._binaries:
            # Pre-19 era.
            logger.info('Using single binary "ytserver"')
            logger.info("  ytserver  %s", self._binaries["ytserver"].literal)

            self.abi_version = self._binaries["ytserver"].abi
        elif (
            "ytserver-master" in self._binaries and
            "ytserver-node" in self._binaries and
            "ytserver-scheduler" in self._binaries):
            # Post-19 era.
            logger.info("Using multiple YT binaries with the following versions:")
            logger.info("  ytserver-master     %s", self._binaries["ytserver-master"].literal)
            logger.info("  ytserver-node       %s", self._binaries["ytserver-node"].literal)
            logger.info("  ytserver-scheduler  %s", self._binaries["ytserver-scheduler"].literal)

            abi_versions = set(imap(lambda v: v.abi, self._binaries.values()))
            if len(abi_versions) > 1:
                raise YtError("Mismatching YT versions. Make sure that all binaries are of compatible versions.")
            self.abi_version = abi_versions.pop()
        else:
            raise YtError("Failed to find YT binaries (ytserver, ytserver-*) in $PATH. Make sure that YT is installed.")

        self._uuid = generate_uuid()
        self._lock = RLock()

        self.path = os.path.realpath(os.path.abspath(path))
        self.logs_path = os.path.abspath(os.path.join(self.path, "logs"))
        self.configs_path = os.path.abspath(os.path.join(self.path, "configs"))
        self.runtime_data_path = os.path.abspath(os.path.join(self.path, "runtime_data"))
        self.pids_filename = os.path.join(self.path, "pids.txt")

        self.configs = defaultdict(list)
        self.config_paths = defaultdict(list)

        self._load_existing_environment = False
        if os.path.exists(self.path):
            if not preserve_working_dir:
                shutil.rmtree(self.path, ignore_errors=True)
            else:
                self._load_existing_environment = True

        makedirp(self.path)
        makedirp(self.logs_path)
        makedirp(self.configs_path)
        makedirp(self.runtime_data_path)

        self.stderrs_path = os.path.join(self.path, "stderrs")
        makedirp(self.stderrs_path)
        self._stderr_paths = defaultdict(list)

        self._tmpfs_path = tmpfs_path
        if self._tmpfs_path is not None:
            self._tmpfs_path = os.path.abspath(self._tmpfs_path)

        self.port_locks_path = port_locks_path
        if self.port_locks_path is not None:
            makedirp(self.port_locks_path)
        self._open_port_iterator = None

        if fqdn is None:
            self._hostname = socket.getfqdn()
        else:
            self._hostname = fqdn

        self._capture_stderr_to_file = bool(int(os.environ.get("YT_CAPTURE_STDERR_TO_FILE", "0")))

        self._process_to_kill = defaultdict(list)
        self._all_processes = {}
        self._all_cgroups = []

        self.master_count = master_count
        self.nonvoting_master_count = nonvoting_master_count
        self.secondary_master_cell_count = secondary_master_cell_count
        self.node_count = node_count
        self.scheduler_count = scheduler_count
        if controller_agent_count is None:
            if self.abi_version >= (19, 3) and scheduler_count > 0:
                controller_agent_count = 1
            else:
                controller_agent_count = 0
        self.controller_agent_count = controller_agent_count
        self.has_proxy = has_proxy
        self.has_rpc_proxy = has_rpc_proxy
        self.skynet_manager_count = skynet_manager_count
        self._enable_debug_logging = enable_debug_logging
        self._cell_tag = cell_tag
        self._kill_child_processes = kill_child_processes
        self._started = False
        self._wait_functions = []

        if watcher_config is None:
            watcher_config = get_watcher_config()
        self.watcher_config = watcher_config

        self._prepare_environment(jobs_memory_limit, jobs_cpu_limit, jobs_user_slot_count, node_chunk_store_quota,
                                  node_memory_limit_addition, allow_chunk_storage_in_tmpfs, port_range_start,
                                  proxy_port, rpc_proxy_port, modify_configs_func)

    def _get_ports_generator(self, port_range_start):
        if port_range_start and isinstance(port_range_start, int):
            return count(port_range_start)
        else:
            self._open_port_iterator = OpenPortIterator(self.port_locks_path)
            return self._open_port_iterator

    def _get_cgroup_path(self, cgroup_type, *args):
        return _get_cgroup_path(cgroup_type, "yt", self._uuid, *args)

    def _prepare_cgroups(self):
        if not self._use_porto_for_servers and _has_cgroups():
            for cgroup_type in CGROUP_TYPES:
                cgroup_path = self._get_cgroup_path(cgroup_type)
                makedirp(cgroup_path)
                self._all_cgroups.append(cgroup_path)

    def _prepare_directories(self):
        master_dirs = []
        master_tmpfs_dirs = [] if self._tmpfs_path else None

        for cell_index in xrange(self.secondary_master_cell_count + 1):
            name = self._get_master_name("master", cell_index)
            master_dirs.append([os.path.join(self.runtime_data_path, name, str(i)) for i in xrange(self.master_count)])
            for dir_ in master_dirs[cell_index]:
                makedirp(dir_)

            if self._tmpfs_path is not None and not self._load_existing_environment:
                master_tmpfs_dirs.append([os.path.join(self._tmpfs_path, name, str(i)) for i in xrange(self.master_count)])
                for dir_ in master_tmpfs_dirs[cell_index]:
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
        if self._tmpfs_path is not None and not self._load_existing_environment:
            node_tmpfs_dirs = [os.path.join(self._tmpfs_path, "node", str(i)) for i in xrange(self.node_count)]
            for dir_ in node_tmpfs_dirs:
                makedirp(dir_)

        proxy_dir = os.path.join(self.runtime_data_path, "proxy")
        makedirp(proxy_dir)

        rpc_proxy_dir = os.path.join(self.runtime_data_path, "rpc_proxy")
        makedirp(rpc_proxy_dir)

        skynet_manager_dirs = [os.path.join(self.runtime_data_path, "skynet_manager", str(i)) for i in xrange(self.skynet_manager_count)]
        for dir_ in skynet_manager_dirs:
            makedirp(dir_)

        return master_dirs, master_tmpfs_dirs, scheduler_dirs, controller_agent_dirs, node_dirs, node_tmpfs_dirs, proxy_dir, rpc_proxy_dir, skynet_manager_dirs

    def _prepare_environment(self, jobs_memory_limit, jobs_cpu_limit, jobs_user_slot_count, node_chunk_store_quota,
                             node_memory_limit_addition, allow_chunk_storage_in_tmpfs, port_range_start, proxy_port, rpc_proxy_port,
                             modify_configs_func):
        logger.info("Preparing cluster instance as follows:")
        logger.info("  masters            %d (%d nonvoting)", self.master_count, self.nonvoting_master_count)
        logger.info("  nodes              %d", self.node_count)
        logger.info("  schedulers         %d", self.scheduler_count)
        logger.info("  controller agents  %d", self.controller_agent_count)

        if self.secondary_master_cell_count > 0:
            logger.info("  secondary cells  %d", self.secondary_master_cell_count)

        logger.info("  HTTP proxies       %d", int(self.has_proxy))
        logger.info("  RPC proxies        %d", int(self.has_rpc_proxy))
        logger.info("  skynet managers    %d", self.skynet_manager_count)
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
        provision["scheduler"]["count"] = self.scheduler_count
        provision["controller_agent"]["count"] = self.controller_agent_count
        provision["node"]["count"] = self.node_count
        if jobs_memory_limit is not None:
            provision["node"]["jobs_resource_limits"]["memory"] = jobs_memory_limit
        if jobs_cpu_limit is not None:
            provision["node"]["jobs_resource_limits"]["cpu"] = jobs_cpu_limit
        if jobs_user_slot_count is not None:
            provision["node"]["jobs_resource_limits"]["user_slots"] = jobs_user_slot_count
        provision["node"]["memory_limit_addition"] = node_memory_limit_addition
        provision["node"]["chunk_store_quota"] = node_chunk_store_quota
        provision["node"]["allow_chunk_storage_in_tmpfs"] = allow_chunk_storage_in_tmpfs
        provision["proxy"]["enable"] = self.has_proxy
        provision["proxy"]["http_port"] = proxy_port
        provision["rpc_proxy"]["enable"] = self.has_rpc_proxy
        provision["rpc_proxy"]["rpc_port"] = rpc_proxy_port
        provision["skynet_manager"]["count"] = self.skynet_manager_count
        provision["fqdn"] = self._hostname
        provision["enable_debug_logging"] = self._enable_debug_logging

        master_dirs, master_tmpfs_dirs, scheduler_dirs, controller_agent_dirs, node_dirs, node_tmpfs_dirs, proxy_dir, rpc_proxy_dir, skynet_manager_dirs = self._prepare_directories()
        cluster_configuration = configs_provider.build_configs(
            self._get_ports_generator(port_range_start),
            master_dirs,
            master_tmpfs_dirs,
            scheduler_dirs,
            controller_agent_dirs,
            node_dirs,
            node_tmpfs_dirs,
            proxy_dir,
            rpc_proxy_dir,
            skynet_manager_dirs,
            self.logs_path,
            provision)

        if modify_configs_func:
            modify_configs_func(cluster_configuration, self.abi_version)

        self._cluster_configuration = cluster_configuration

        self._prepare_cgroups()
        self._prepare_masters(cluster_configuration["master"], master_dirs)
        if self.node_count > 0:
            self._prepare_nodes(cluster_configuration["node"], node_dirs)
        if self.scheduler_count > 0:
            self._prepare_schedulers(cluster_configuration["scheduler"], scheduler_dirs)
        if self.controller_agent_count > 0:
            self._prepare_controller_agents(cluster_configuration["controller_agent"], controller_agent_dirs)
        if self.has_proxy:
            self._prepare_proxy(cluster_configuration["proxy"], cluster_configuration["ui"], proxy_dir)
        if self.has_rpc_proxy:
            self._prepare_rpc_proxy(cluster_configuration["rpc_proxy"], cluster_configuration["rpc_client"], rpc_proxy_dir)
        if self.skynet_manager_count > 0:
            self._prepare_skynet_managers(cluster_configuration["skynet_manager"], skynet_manager_dirs)
        self._prepare_driver(cluster_configuration["driver"], cluster_configuration["master"])
        # COMPAT. Will be removed eventually.
        self._prepare_console_driver()

    def _wait_or_skip(self, function, sync):
        if sync:
            function()
        else:
            self._wait_functions.append(function)

    def start(self, use_proxy_from_package=True, start_secondary_master_cells=False, on_masters_started_func=None):
        self._process_to_kill.clear()
        self._all_processes.clear()

        if self.master_count == 0:
            logger.warning("Cannot start YT instance without masters")
            return

        self.pids_file = open(self.pids_filename, "wt")
        try:
            if self.has_proxy:
                self.start_proxy(use_proxy_from_package=use_proxy_from_package, sync=False)

            self.start_master_cell(sync=False)

            for func in self._wait_functions:
                func()

            if start_secondary_master_cells:
                self._wait_functions = []
                self.start_secondary_master_cells(sync=False)
                for func in self._wait_functions:
                    func()

            # TODO(asaitgalin): Create this user inside master.
            client = self.create_client()
            if not client.exists("//sys/users/application_operations"):
                client.create("user", attributes={"name": "application_operations"})
                client.add_member("application_operations", "superusers")

            self._wait_functions = []
            if on_masters_started_func is not None:
                on_masters_started_func()
            if self.node_count > 0:
                self.start_nodes(sync=False)
            if self.scheduler_count > 0:
                self.start_schedulers(sync=False)
            if self.controller_agent_count > 0:
                self.start_controller_agents(sync=False)
            if self.has_rpc_proxy:
                self.start_rpc_proxy(sync=False)
            if self.skynet_manager_count > 0:
                self.start_skynet_managers(sync=False)

            for func in self._wait_functions:
                func()

            self._start_watcher()
            self._started = True

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

        self.kill_service("watcher")
        killed_services.add("watcher")

        for name in ["proxy", "node", "scheduler", "controller_agent", "master", "rpc_proxy", "skynet_manager"]:
            if name in self.configs:
                self.kill_service(name)
                killed_services.add(name)

        for name in self.configs:
            if name not in killed_services:
                self.kill_service(name)

        self.pids_file.close()
        remove_file(self.pids_filename, force=True)

        if self._open_port_iterator is not None:
            self._open_port_iterator.release_locks()
            self._open_port_iterator = None

        wait_for_removing_file_lock(os.path.join(self.path, "lock_file"))

    def get_proxy_address(self):
        if not self.has_proxy:
            raise YtError("Proxy is not started")
        return "{0}:{1}".format(self._hostname, _config_safe_get(self.configs["proxy"], "proxy", "port"))

    def kill_cgroups(self):
        if self._use_porto_for_servers or not _has_cgroups():
            return

        with self._lock:
            self.kill_cgroups_impl()

    def kill_cgroups_impl(self):
        freezer_cgroups = []
        for cgroup_path in self._all_cgroups:
            if "cgroup/freezer" in cgroup_path:
                freezer_cgroups.append(cgroup_path)

        for freezer_path in freezer_cgroups:
            with open(os.path.join(cgroup_path, "tasks")) as f:
                for line in f:
                    pid = int(line)
                    # Stopping process activity. This prevents
                    # forking of new processes, for example.
                    os.kill(pid, signal.SIGSTOP)

        for freezer_path in freezer_cgroups:
            with open(os.path.join(cgroup_path, "tasks")) as f:
                for line in f:
                    pid = int(line)
                    # Stopping process activity. This prevents
                    # forking of new processes, for example.
                    os.kill(pid, signal.SIGKILL)

        for cgroup_path in self._all_cgroups:
            for dirpath, dirnames, _ in os.walk(cgroup_path, topdown=False):
                for dirname in dirnames:
                    for iter in xrange(5):
                        try:
                            os.rmdir(os.path.join(dirpath, dirname))
                            break
                        except OSError:
                            logger.exception("Failed to remove cgroup dir")
                            time.sleep(0.5)
            os.rmdir(cgroup_path)

        self._all_cgroups = []

    def kill_schedulers(self):
        self.kill_service("scheduler")

    def kill_controller_agents(self):
        self.kill_service("controller_agent")

    def kill_nodes(self):
        self.kill_service("node")

    def kill_proxy(self):
        self.kill_service("proxy")

    def kill_master_cell(self, cell_index=0):
        name = self._get_master_name("master", cell_index)
        self.kill_service(name)

    def kill_service(self, name):
        with self._lock:
            logger.info("Killing %s", name)
            for p in self._process_to_kill[name]:
                self._kill_process(p, name)
                if isinstance(p, PortoSubprocess):
                    p.destroy()
                del self._all_processes[p.pid]
            del self._process_to_kill[name]

    def check_liveness(self, callback_func):
        with self._lock:
            for info in itervalues(self._all_processes):
                proc, args = info
                proc.poll()
                if proc.returncode is not None:
                    callback_func(self, args)
                    break

    def _write_environment_info_to_file(self):
        info = {}
        if self.has_proxy:
            info["proxy"] = {"address": self.get_proxy_address()}
        with open(os.path.join(self.path, "info.yson"), "wb") as fout:
            yson.dump(info, fout, yson_format="pretty")

    def _kill_process(self, proc, name):
        proc.poll()
        if proc.returncode is not None:
            if self._started:
                logger.warning("{0} (pid: {1}, working directory: {2}) is already terminated with exit code {3}".format(
                               name, proc.pid, os.path.join(self.path, name), proc.returncode))
            return

        os.killpg(proc.pid, signal.SIGKILL)
        time.sleep(0.2)

        if not is_dead_or_zombie(proc.pid):
            logger.error("Failed to kill process %s (pid %d) ", name, proc.pid)

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + "\n")
        self.pids_file.flush()

    def _print_stderrs(self, name, number=None):
        if not self._capture_stderr_to_file:
            return

        def print_stderr(path, num=None):
            number_suffix = ""
            if num is not None:
                number_suffix = "-" + str(num)

            process_stderr = open(path).read()
            if process_stderr:
                sys.stderr.write("{0}{1} stderr:\n{2}"
                                 .format(name.capitalize(), number_suffix, process_stderr))

        if number is not None:
            print_stderr(self._stderr_paths[name][number], number)
        else:
            for i, stderr_path in enumerate(self._stderr_paths[name]):
                print_stderr(stderr_path, i)

    def _run(self, args, name, number=None, cgroup_paths=None, timeout=0.1):
        if cgroup_paths is None:
            cgroup_paths = []

        with self._lock:
            name_with_number = name
            if number:
                name_with_number = "{0}-{1}".format(name, number)
            stderr_path = os.path.join(self.stderrs_path, "stderr.{0}".format(name_with_number))
            self._stderr_paths[name].append(stderr_path)

            for cgroup_path in cgroup_paths:
                makedirp(cgroup_path)

            stdout = open(os.devnull, "w")
            stderr = None
            if self._capture_stderr_to_file:
                stderr = open(stderr_path, "w")

            def preexec():
                os.setsid()
                if self._kill_child_processes:
                    set_pdeathsig()
                for cgroup_path in cgroup_paths:
                    with open(os.path.join(cgroup_path, "tasks"), "at") as handle:
                        handle.write(str(os.getpid()))
                        handle.write("\n")

            p = self._subprocess_module.Popen(args, shell=False, close_fds=True, preexec_fn=preexec, cwd=self.runtime_data_path,
                                              stdout=stdout, stderr=stderr)

            time.sleep(timeout)
            self._validate_process_is_running(p, name, number)

            self._process_to_kill[name].append(p)
            self._all_processes[p.pid] = (p, args)
            self._append_pid(p.pid)

    def _supports_pdeath_signal(self):
        if hasattr(self, "_supports_pdeath_signal_result"):
            return self._supports_pdeath_signal_result
        help_output = subprocess.check_output(["ytserver", "--help"])
        self._supports_pdeath_signal_result = "--pdeath-signal" in help_output
        return self._supports_pdeath_signal_result

    def _run_yt_component(self, component, name=None):
        if name is None:
            name = component

        logger.info("Starting %s", name)

        for i in xrange(len(self.configs[name])):
            args = None
            cgroup_paths = None
            if self.abi_version[0] == 18:
                args = ["ytserver", "--" + component]
                if self._kill_child_processes and self._supports_pdeath_signal():
                    args.extend(["--pdeath-signal", str(signal.SIGTERM)])
            elif self.abi_version[0] == 19:
                args = ["ytserver-" + component]
                if self._kill_child_processes:
                    args.extend(["--pdeathsig", str(signal.SIGKILL)])
            else:
                raise YtError("Unsupported YT ABI version {0}".format(self.abi_version))
            args.extend(["--config", self.config_paths[name][i]])
            cgroup_paths = None
            if not self._use_porto_for_servers and _has_cgroups():
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

    def _prepare_masters(self, master_configs, master_dirs):
        for cell_index in xrange(self.secondary_master_cell_count + 1):
            master_name = self._get_master_name("master", cell_index)
            if cell_index == 0:
                cell_tag = master_configs["primary_cell_tag"]
            else:
                cell_tag = master_configs["secondary_cell_tags"][cell_index - 1]

            for master_index in xrange(self.master_count):
                master_config_name = "master-{0}-{1}.yson".format(cell_index, master_index)
                config_path = os.path.join(self.configs_path, master_config_name)
                if self._load_existing_environment:
                    if not os.path.isfile(config_path):
                        raise YtError("Master config {0} not found. It is possible that you requested "
                                      "more masters than configs exist".format(config_path))
                    config = read_config(config_path)
                else:
                    config = master_configs[cell_tag][master_index]
                    write_config(config, config_path)

                self.configs[master_name].append(config)
                self.config_paths[master_name].append(config_path)

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
                # XXX(asaitgalin): If get("/") request is successful then quorum is ready
                # and world is initialized. Secondary masters can only be checked with native
                # driver so it is requirement to have driver bindings if secondary cells are started.
                client = None
                if not secondary:
                    client = self.create_client()
                else:
                    client = self.create_native_client(master_name.replace("master", "driver"))
                client.config["proxy"]["retries"]["enable"] = False
                client.get("/")
                return True
            except (requests.RequestException, YtError) as err:
                return False, err
            finally:
                logger.setLevel(old_level)

        cell_ready = quorum_ready

        if secondary:
            primary_cell_client = self.create_client()
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

    def _prepare_nodes(self, node_configs, node_dirs):
        for node_index in xrange(self.node_count):
            node_config_name = "node-" + str(node_index) + ".yson"
            config_path = os.path.join(self.configs_path, node_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Node config {0} not found. It is possible that you requested "
                                  "more nodes than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = node_configs[node_index]
                write_config(config, config_path)

            self.configs["node"].append(config)
            self.config_paths["node"].append(config_path)

    def start_nodes(self, sync=True):
        self._run_yt_component("node")

        native_client = self.create_client()

        def nodes_ready():
            self._validate_processes_are_running("node")

            nodes = native_client.list("//sys/nodes", attributes=["state"])
            return len(nodes) == self.node_count and all(node.attributes["state"] == "online" for node in nodes)

        wait_function = lambda: self._wait_for(nodes_ready, "node", max_wait_time=max(self.node_count * 6.0, 20))
        self._wait_or_skip(wait_function, sync)

    def _prepare_schedulers(self, scheduler_configs, scheduler_dirs):
        for scheduler_index in xrange(self.scheduler_count):
            scheduler_config_name = "scheduler-" + str(scheduler_index) + ".yson"
            config_path = os.path.join(self.configs_path, scheduler_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Scheduler config {0} not found. It is possible that you requested "
                                  "more schedulers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = scheduler_configs[scheduler_index]
                write_config(config, config_path)

            self.configs["scheduler"].append(config)
            self.config_paths["scheduler"].append(config_path)

    def _prepare_controller_agents(self, controller_agent_configs, controller_agent_dirs):
        for controller_agent_index in xrange(self.controller_agent_count):
            controller_agent_config_name = "controller_agent-" + str(controller_agent_index) + ".yson"
            config_path = os.path.join(self.configs_path, controller_agent_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Controller agent config {0} not found. It is possible that you requested "
                                  "more controller agents than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = controller_agent_configs[controller_agent_index]
                write_config(config, config_path)

            self.configs["controller_agent"].append(config)
            self.config_paths["controller_agent"].append(config_path)

    def start_schedulers(self, sync=True):
        self._remove_scheduler_lock()

        client = self.create_client()
        client.create("map_node", "//sys/pool_trees/default", ignore_existing=True, recursive=True)
        client.set("//sys/pool_trees/@default_tree", "default")
        if not client.exists("//sys/pools"):
            client.link("//sys/pool_trees/default", "//sys/pools", ignore_existing=True)

        self._run_yt_component("scheduler")

        def schedulers_ready():
            self._validate_processes_are_running("scheduler")

            instances = client.list("//sys/scheduler/instances")
            if len(instances) != self.scheduler_count:
                return False

            try:
                active_scheduler_orchid_path = None
                for instance in instances:
                    orchid_path = "//sys/scheduler/instances/{0}/orchid".format(instance)
                    path = orchid_path + "/scheduler"
                    try:
                        client.set(orchid_path + "/@retry_backoff_time", 100)
                        if client.get(path + "/connected"):
                            active_scheduler_orchid_path = path
                    except YtError as err:
                        if not err.is_resolve_error():
                            raise

                if active_scheduler_orchid_path is None:
                    return False, "No active scheduler found"

                # TODO(ignat): /config/environment/primary_master_cell_id is temporary solution.
                try:
                    master_cell_id = client.get("//sys/@cell_id")
                    scheduler_cell_id = client.get(active_scheduler_orchid_path + "/config/environment/primary_master_cell_id")
                    if master_cell_id != scheduler_cell_id:
                        return False, "Incorrect scheduler connected, its cell_id {0} does not match master cell {1}".format(scheduler_cell_id, master_cell_id)
                except YtResponseError as err:
                    if err.is_resolve_error():
                        return False, "Failed to request primary_master_cell_id from master and scheduler" + str(err)
                    else:
                        raise

                nodes = list(itervalues(client.get(active_scheduler_orchid_path + "/nodes")))
                return len(nodes) == self.node_count and all(node["state"] == "online" for node in nodes)
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

            client = self.create_client()
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
                    except YtError as err:
                        if not err.is_resolve_error():
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
        if self.has_proxy:
            return YtClient(proxy=self.get_proxy_address())
        return self.create_native_client()

    def create_native_client(self, driver_name="driver"):
        driver_config_path = self.config_paths[driver_name]

        with open(driver_config_path, "rb") as f:
            driver_config = yson.load(f)

        config = {
            "backend": "native",
            "driver_config": driver_config
        }

        try:
            import yt_driver_bindings
        except ImportError:
            guessed_version = list(self._binaries.items())[0][1].literal
            raise YtError("YT driver bindings not found. Make sure you have installed "
                          "yandex-yt-python-driver package (appropriate version: {0})"
                          .format(guessed_version))

        yt_driver_bindings.configure_logging(self.driver_logging_config)

        return YtClient(config=config)

    def _remove_scheduler_lock(self):
        client = self.create_client()
        try:
            tx_id = client.get("//sys/scheduler/lock/@locks/0/transaction_id")
            if tx_id:
                client.abort_transaction(tx_id)
                logger.info("Previous scheduler transaction was aborted")
        except YtError as err:
            if not err.is_resolve_error():
                raise

    def _prepare_driver(self, driver_configs, master_configs):
        for cell_index in xrange(self.secondary_master_cell_count + 1):
            if cell_index == 0:
                tag = master_configs["primary_cell_tag"]
                name = "driver"
            else:
                tag = master_configs["secondary_cell_tags"][cell_index - 1]
                name = "driver_secondary_" + str(cell_index - 1)

            config_path = os.path.join(self.configs_path, "driver-{0}.yson".format(cell_index))

            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = driver_configs[tag]
                write_config(config, config_path)

            # COMPAT
            if cell_index == 0:
                link_path = os.path.join(self.path, "driver.yson")
                if os.path.exists(link_path):
                    os.remove(link_path)
                os.symlink(config_path, link_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        self.driver_logging_config = init_logging(None, self.logs_path, "driver", self._enable_debug_logging)

    def _prepare_console_driver(self):
        config = {}
        config["driver"] = self.configs["driver"]
        config["logging"] = init_logging(None, self.path, "console_driver", self._enable_debug_logging)

        config_path = os.path.join(self.path, "console_driver_config.yson")

        write_config(config, config_path)

        self.configs["console_driver"].append(config)
        self.config_paths["console_driver"].append(config_path)

    def _prepare_proxy(self, proxy_config, ui_config, proxy_dir):
        config_path = os.path.join(self.configs_path, "proxy.json")
        if self._load_existing_environment:
            if not os.path.isfile(config_path):
                raise YtError("Proxy config {0} not found".format(config_path))
            config = read_config(config_path, format="json")
        else:
            config = proxy_config
            write_config(config, config_path, format="json")

        self.configs["proxy"] = config
        self.config_paths["proxy"] = config_path

        # UI configuration
        if not os.path.exists(WEB_INTERFACE_RESOURCES_PATH):
            logger.warning("Failed to configure UI, web interface resources are not installed. "
                           "Try to install yandex-yt-web-interface or set YT_LOCAL_THOR_PATH.")
            return

        ui_config_path = os.path.join(proxy_dir, "ui", "config.js")
        if not self._load_existing_environment:
            shutil.copytree(WEB_INTERFACE_RESOURCES_PATH, os.path.join(proxy_dir, "ui"))
            write_config(ui_config, ui_config_path, format=None)
        else:
            if not os.path.isfile(ui_config_path):
                logger.warning("Failed to configure UI, file {0} not found".format(ui_config_path))

    def _prepare_rpc_proxy(self, rpc_proxy_config, rpc_client_config, rpc_proxy_dir):
        config_path = os.path.join(self.configs_path, "rpc-proxy.yson")
        write_config(rpc_proxy_config, config_path)
        self.configs["rpc_proxy"].append(rpc_proxy_config)
        self.config_paths["rpc_proxy"].append(config_path)

        rpc_client_config_path = os.path.join(self.configs_path, "rpc-client.yson")
        write_config(rpc_client_config, rpc_client_config_path)

    def _prepare_skynet_managers(self, skynet_manager_configs, skynet_manager_dirs):
        self.configs["skynet_manager"] = []
        self.config_paths["skynet_manager"] = []

        for i in xrange(len(skynet_manager_dirs)):
            config_path = os.path.join(self.configs_path, "skynet-manager-{}.yson".format(i))
            write_config(skynet_manager_configs[i], config_path)
            self.configs["skynet_manager"].append(skynet_manager_configs[i])
            self.config_paths["skynet_manager"].append(config_path)

    def _start_proxy_from_package(self):
        node_path = list(ifilter(lambda x: x != "", os.environ.get("NODE_PATH", "").split(":")))
        for path in node_path + ["/usr/lib/node_modules"]:
            proxy_binary_path = os.path.join(path, "yt", "bin", "yt_http_proxy")
            if os.path.exists(proxy_binary_path):
                break
        else:
            raise YtError("Failed to find YT http proxy binary. Make sure you installed "
                          "yandex-yt-http-proxy package or specify NODE_PATH manually")

        nodejs_binary_path = _find_nodejs()

        proxy_version = _get_proxy_version(nodejs_binary_path, proxy_binary_path)
        if proxy_version and proxy_version[:2] != self.abi_version:
            raise YtError("Proxy version (which is {0}.{1}) is incompatible with ytserver* ABI "
                          "(which is {2}.{3})".format(*(proxy_version[:2] + self.abi_version)))

        self._run([nodejs_binary_path,
                   proxy_binary_path,
                   "-c", self.config_paths["proxy"]],
                   "proxy")

    def start_proxy(self, use_proxy_from_package, sync=True):
        logger.info("Starting proxy")
        if use_proxy_from_package:
            self._start_proxy_from_package()
        else:
            if not which("run_proxy.sh"):
                raise YtError("Failed to start proxy from source tree. "
                              "Make sure you added directory with run_proxy.sh to PATH")
            self._run(["run_proxy.sh",
                       "-c", self.config_paths["proxy"]],
                       "proxy")

        def proxy_ready():
            self._validate_processes_are_running("proxy")

            try:
                address = "127.0.0.1:{0}".format(self.get_proxy_address().split(":")[1])
                resp = requests.get("http://{0}/api".format(address))
                resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False

            return True

        self._wait_or_skip(lambda: self._wait_for(proxy_ready, "proxy", max_wait_time=20), sync)

    def start_rpc_proxy(self, sync=True):
        self._run_yt_component("proxy", name="rpc_proxy")

        native_client = self.create_client()

        def rpc_proxy_ready():
            self._validate_processes_are_running("rpc_proxy")

            return len(native_client.list("//sys/rpc_proxies")) == 1

        self._wait_or_skip(lambda: self._wait_for(rpc_proxy_ready, "rpc_proxy", max_wait_time=20), sync)

    def start_skynet_managers(self, sync=True):
        self._run_yt_component("skynet-manager", name="skynet_manager")

        native_client = self.create_client()

        def skynet_manager_ready():
            self._validate_processes_are_running("skynet_manager")

            return ("skynet_manager" in native_client.list("//sys") and
                len(native_client.list("//sys/skynet_manager/managers")) == self.skynet_manager_count)

        self._wait_or_skip(lambda: self._wait_for(skynet_manager_ready, "skynet_manager", max_wait_time=20), sync)

    def _validate_process_is_running(self, process, name, number=None):
        if number is not None:
            name_with_number = "{0}-{1}".format(name, number)
        else:
            name_with_number = name

        if process.poll() is not None:
            self._print_stderrs(name, number)
            raise YtError("Process {0} unexpectedly terminated with error code {1}. "
                          "If the problem is reproducible please report to yt@yandex-team.ru mailing list."
                          .format(name_with_number, process.returncode))

    def _validate_processes_are_running(self, name):
        if name == "proxy":
            self._validate_process_is_running(self._process_to_kill[name][-1], name)
        else:
            for index, process in enumerate(self._process_to_kill[name]):
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

        self._print_stderrs(name)

        error = YtError("{0} still not ready after {1} seconds. See logs in working dir for details."
                        .format(name.capitalize(), max_wait_time))
        if condition_error is not None:
            if isinstance(condition_error, str):
                condition_error = YtError(condition_error)
            error.inner_errors = [condition_error]

        raise error

    def _get_watcher_path(self):
        watcher_path = which("yt_env_watcher")
        if watcher_path:
            return watcher_path[0]

        watcher_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "bin", "yt_env_watcher"))
        if os.path.exists(watcher_path):
            return watcher_path

        raise YtError("Failed to find yt_env_watcher binary")

    def _start_watcher(self):
        postrotate_commands = []
        for pid in self._all_processes.keys():
            postrotate_commands.append("\t/usr/bin/test -d /proc/{0} && kill -HUP {0} >/dev/null 2>&1 || true".format(pid))

        logrotate_options = [
            "rotate {0}".format(self.watcher_config["logs_rotate_max_part_count"]),
            "size {0}".format(self.watcher_config["logs_rotate_size"]),
            "missingok",
            "copytruncate",
            "nodelaycompress",
            "nomail",
            "noolddir",
            "compress",
            "create",
            "postrotate",
            "\n".join(postrotate_commands),
            "endscript"
        ]

        config_path = os.path.join(self.configs_path, "logs_rotator")
        with open(config_path, "w") as config_file:
            if self._enable_debug_logging:
                for service, configs in iteritems(self.configs):
                    if service.startswith("driver"):
                        continue

                    for config in flatten(configs):
                        log_config_path = "logging/writers/debug/file_name"
                        if service == "proxy":
                            log_config_path = "proxy/" + log_config_path
                        log_path = _config_safe_get(config, service, log_config_path)
                        config_file.write("{0}\n{{\n{1}\n}}\n\n".format(log_path, "\n".join(logrotate_options)))

                        if service == "node":
                            job_proxy_log_config_path = "exec_agent/job_proxy_logging/writers/debug/file_name"
                            job_proxy_log_path = _config_safe_get(config, service, job_proxy_log_config_path)
                            config_file.write("{0}\n{{\n{1}\n}}\n\n".format(job_proxy_log_path, "\n".join(logrotate_options)))

            if self.has_proxy:
                proxy_log_path = _config_safe_get(self.configs["proxy"], "proxy", "logging/filename")
                config_file.write("{0}\n{{\n{1}\n}}\n\n".format(proxy_log_path, "\n".join(logrotate_options)))

        logs_rotator_data_path = os.path.join(self.runtime_data_path, "logs_rotator")
        makedirp(logs_rotator_data_path)
        logrotate_state_file = os.path.join(logs_rotator_data_path, "state")

        watcher_path = self._get_watcher_path()

        self._run([watcher_path,
                   "--lock-file-path", os.path.join(self.path, "lock_file"),
                   "--logrotate-config-path", config_path,
                   "--logrotate-state-file", logrotate_state_file,
                   "--logrotate-interval", str(self.watcher_config["logs_rotate_interval"]),
                   "--log-path", os.path.join(self.logs_path, "watcher.log")],
                   "watcher")

        def watcher_lock_created():
            return os.path.exists(os.path.join(self.path, "lock_file"))

        self._wait_for(watcher_lock_created, "watcher")
