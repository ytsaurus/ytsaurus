from __future__ import print_function

from .configs_provider import init_logging, get_default_provision, create_configs_provider
from .helpers import versions_cmp, read_config, write_config, \
                     is_dead_or_zombie, get_open_port

from yt.common import YtError, remove_file, makedirp, set_pdeathsig, which
from yt.wrapper.client import Yt
from yt.wrapper.errors import YtResponseError
import yt.yson as yson
import yt.subprocess_wrapper as subprocess

from yt.packages.six import itervalues
from yt.packages.six.moves import xrange, map as imap, filter as ifilter
import yt.packages.requests as requests

import logging
import os
import uuid
import time
import signal
import socket
import shutil
import sys
import getpass
from collections import defaultdict
from threading import RLock
from itertools import count

logger = logging.getLogger("Yt.local")

def _get_ytserver_version():
    if not which("ytserver"):
        raise YtError("Failed to start ytserver. Make sure that ytserver binary is installed")
    # Output example: "\nytserver  version: 0.17.3-unknown~debug~0+local\n\n"
    output = subprocess.check_output(["ytserver", "--version"])
    return output.split(":", 1)[1].strip()

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
    version_str = err.split("\n")[1].split()[1]

    try:
        version = list(imap(int, version_str.split(".")))
    except ValueError:
        return None

    return version

def _config_safe_get(config, config_path, key):
    d = config
    parts = key.split("/")
    for k in parts:
        d = d.get(k)
        if d is None:
            raise YtError('Failed to get required key "{0}" from config file {1}.'.format(key, config_path))
    return d

def _configure_logger():
    logger.propagate = False

    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())

    if os.environ.get("YT_ENABLE_VERBOSE_LOGGING"):
        logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    else:
        logger.handlers[0].setFormatter(logging.Formatter("%(message)s"))

class YTInstance(object):
    def __init__(self, path, master_count=1, nonvoting_master_count=0, secondary_master_cell_count=0,
                 node_count=1, scheduler_count=1, has_proxy=False, cell_tag=0, proxy_port=None,
                 enable_debug_logging=True, preserve_working_dir=False, tmpfs_path=None,
                 port_locks_path=None, port_range_start=None, fqdn=None, jobs_memory_limit=None,
                 jobs_cpu_limit=None, jobs_user_slot_count=None, node_memory_limit_addition=None,
                 modify_configs_func=None, kill_child_processes=False):
        _configure_logger()

        self._lock = RLock()

        self.path = os.path.abspath(path)
        self.pids_filename = os.path.join(self.path, "pids.txt")

        self.configs = defaultdict(list)
        self.config_paths = defaultdict(list)
        self.log_paths = defaultdict(list)

        self._load_existing_environment = False
        if os.path.exists(self.path):
            if not preserve_working_dir:
                shutil.rmtree(self.path, ignore_errors=True)
                makedirp(self.path)
            else:
                self._load_existing_environment = True

        makedirp(self.path)

        self._stderrs_path = os.path.join(self.path, "stderrs")
        makedirp(self._stderrs_path)
        self._stderr_paths = defaultdict(list)

        self._tmpfs_path = tmpfs_path
        if self._tmpfs_path is not None:
            self._tmpfs_path = os.path.abspath(self._tmpfs_path)

        self.port_locks_path = port_locks_path
        if self.port_locks_path is not None:
            makedirp(self.port_locks_path)

        if fqdn is None:
            self._hostname = socket.getfqdn()
        else:
            self._hostname = fqdn

        self._capture_stderr_to_file = bool(int(os.environ.get("YT_CAPTURE_STDERR_TO_FILE", "0")))

        self._process_to_kill = defaultdict(list)
        self._all_processes = {}

        self._ytserver_version_long = _get_ytserver_version()
        logger.info("Logging started (ytserver version: %s)", self._ytserver_version_long)

        self._ytserver_version = self._ytserver_version_long.split("-", 1)[0].strip()

        self.master_count = master_count
        self.nonvoting_master_count = nonvoting_master_count
        self.secondary_master_cell_count = secondary_master_cell_count
        self.node_count = node_count
        self.scheduler_count = scheduler_count
        self.has_proxy = has_proxy
        self._enable_debug_logging = enable_debug_logging
        self._cell_tag = cell_tag
        self._kill_child_processes = kill_child_processes
        self._started = False

        self._prepare_environment(jobs_memory_limit, jobs_cpu_limit, jobs_user_slot_count,
                                  node_memory_limit_addition, port_range_start, proxy_port, modify_configs_func)

    def _get_ports_generator(self, port_range_start):
        def random_port_generator():
            while True:
                yield get_open_port(self.port_locks_path)

        get_open_port.busy_ports = set()
        get_open_port.lock_fds = set()

        if port_range_start and isinstance(port_range_start, int):
            return count(port_range_start)
        else:
            return random_port_generator()

    def _prepare_directories(self):
        master_dirs = []
        master_tmpfs_dirs = [] if self._tmpfs_path else None

        for cell_index in xrange(self.secondary_master_cell_count + 1):
            name = self._get_master_name("master", cell_index)
            master_dirs.append([os.path.join(self.path, name, str(i)) for i in xrange(self.master_count)])
            for dir_ in master_dirs[cell_index]:
                makedirp(dir_)

            if self._tmpfs_path is not None and not self._load_existing_environment:
                master_tmpfs_dirs.append([os.path.join(self._tmpfs_path, name, str(i)) for i in xrange(self.master_count)])
                for dir_ in master_tmpfs_dirs[cell_index]:
                    makedirp(dir_)

        scheduler_dirs = [os.path.join(self.path, "scheduler", str(i)) for i in xrange(self.scheduler_count)]
        for dir_ in scheduler_dirs:
            makedirp(dir_)

        node_dirs = [os.path.join(self.path, "node", str(i)) for i in xrange(self.node_count)]
        for dir_ in node_dirs:
            makedirp(dir_)

        proxy_dir = os.path.join(self.path, "proxy")
        makedirp(proxy_dir)

        return master_dirs, master_tmpfs_dirs, scheduler_dirs, node_dirs, proxy_dir

    def _prepare_environment(self, jobs_memory_limit, jobs_cpu_limit, jobs_user_slot_count,
                             node_memory_limit_addition, port_range_start, proxy_port, modify_configs_func):
        if self.secondary_master_cell_count > 0 and versions_cmp(self._ytserver_version, "0.18") < 0:
            raise YtError("Multicell is not supported for ytserver version < 0.18")

        logger.info("Preparing cluster instance as follows:")
        logger.info("  masters          %d (%d nonvoting)", self.master_count, self.nonvoting_master_count)
        logger.info("  nodes            %d", self.node_count)
        logger.info("  schedulers       %d", self.scheduler_count)

        if self.secondary_master_cell_count > 0:
            logger.info("  secondary cells  %d", self.secondary_master_cell_count)

        logger.info("  proxies          %d", int(self.has_proxy))
        logger.info("  working dir      %s", self.path)

        if self.master_count == 0:
            logger.warning("Master count is equal to zero. Instance is not prepared.")
            return

        configs_provider = create_configs_provider(self._ytserver_version)

        provision = get_default_provision()
        provision["master"]["cell_size"] = self.master_count
        provision["master"]["secondary_cell_count"] = self.secondary_master_cell_count
        provision["master"]["primary_cell_tag"] = self._cell_tag
        provision["master"]["cell_nonvoting_master_count"] = self.nonvoting_master_count
        provision["scheduler"]["count"] = self.scheduler_count
        provision["node"]["count"] = self.node_count
        if jobs_memory_limit is not None:
            provision["node"]["jobs_resource_limits"]["memory"] = jobs_memory_limit
        if jobs_cpu_limit is not None:
            provision["node"]["jobs_resource_limits"]["cpu"] = jobs_cpu_limit
        if jobs_user_slot_count is not None:
            provision["node"]["jobs_resource_limits"]["user_slots"] = jobs_user_slot_count
        provision["node"]["memory_limit_addition"] = node_memory_limit_addition
        provision["proxy"]["enable"] = self.has_proxy
        provision["proxy"]["http_port"] = proxy_port
        provision["fqdn"] = self._hostname
        provision["enable_debug_logging"] = self._enable_debug_logging

        master_dirs, master_tmpfs_dirs, scheduler_dirs, node_dirs, proxy_dir = self._prepare_directories()
        cluster_configuration = configs_provider.build_configs(
            self._get_ports_generator(port_range_start),
            master_dirs,
            master_tmpfs_dirs,
            scheduler_dirs,
            node_dirs,
            proxy_dir,
            provision)

        if modify_configs_func:
            modify_configs_func(cluster_configuration, self._ytserver_version)

        self._cluster_configuration = cluster_configuration

        self._prepare_masters(cluster_configuration["master"], master_dirs)
        if self.node_count > 0:
            self._prepare_nodes(cluster_configuration["node"], node_dirs)
        if self.scheduler_count > 0:
            self._prepare_schedulers(cluster_configuration["scheduler"], scheduler_dirs)
        if self.has_proxy:
            self._prepare_proxy(cluster_configuration["proxy"], cluster_configuration["ui"], proxy_dir)
        self._prepare_driver(cluster_configuration["driver"], cluster_configuration["master"])

    def start(self, use_proxy_from_package=False, start_secondary_master_cells=False, on_masters_started_func=None):
        self._process_to_kill.clear()
        self._all_processes.clear()

        if self.master_count == 0:
            logger.warning("Cannot start YT instance without masters")
            return

        self.pids_file = open(self.pids_filename, "wt")
        try:
            if self.has_proxy:
                self.start_proxy(use_proxy_from_package=use_proxy_from_package)
            self.start_all_masters(start_secondary_master_cells=start_secondary_master_cells)
            if on_masters_started_func is not None:
                on_masters_started_func()
            if self.node_count > 0:
                self.start_nodes()
            if self.scheduler_count > 0:
                self.start_schedulers()
            self._started = True

            self._write_environment_info_to_file()
        except (YtError, KeyboardInterrupt) as err:
            self.stop()
            raise YtError("Failed to start environment", inner_errors=[err])

    def stop(self):
        if not self._started:
            return

        killed_services = set()
        with self._lock:
            for name in ["proxy", "node", "scheduler", "master"]:
                if name in self.configs:
                    self.kill_service(name)
                    killed_services.add(name)
            for name in self.configs:
                if name not in killed_services:
                    self.kill_service(name)

            remove_file(self.pids_filename, force=True)

            for lock_fd in get_open_port.lock_fds:
                try:
                    os.close(lock_fd)
                except OSError as err:
                    logger.warning("Failed to close file descriptor %d: %s",
                                   lock_fd, os.strerror(err.errno))
        self._started = False

    def get_proxy_address(self):
        if not self.has_proxy:
            raise YtError("Proxy is not started")
        return "{0}:{1}".format(self._hostname, _config_safe_get(self.configs["proxy"],
                                                                 self.config_paths["proxy"], "port"))

    def kill_schedulers(self):
        self.kill_service("scheduler")

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
                logger.warning("%s (pid: %d, working directory: %s) is already terminated with exit code %d",
                               name, proc.pid, os.path.join(self.path, name), proc.returncode)
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

    def _run(self, args, name, number=None, timeout=0.1):
        with self._lock:
            number_suffix = "-" + str(number) if number is not None else ""
            stderr_path = os.path.join(self._stderrs_path, "stderr.{0}{1}".format(name, number_suffix))
            self._stderr_paths[name].append(stderr_path)

            stdout = open(os.devnull, "w")
            stderr = None
            if self._capture_stderr_to_file:
                stderr = open(stderr_path, "w")

            def preexec():
                os.setsid()
                if self._kill_child_processes:
                    set_pdeathsig()

            p = subprocess.Popen(args, shell=False, close_fds=True, preexec_fn=preexec, cwd=self.path,
                                 stdout=stdout, stderr=stderr)

            time.sleep(timeout)
            if p.poll():
                self._print_stderrs(name, number)
                raise YtError("Process {0}{1} unexpectedly terminated with error code {2}. "
                              "If the problem is reproducible please report to yt@yandex-team.ru mailing list."
                              .format(name, number_suffix, p.returncode))

            self._process_to_kill[name].append(p)
            self._all_processes[p.pid] = (p, args)
            self._append_pid(p.pid)

    def _supports_pdeath_signal(self):
        if hasattr(self, "_supports_pdeath_signal_result"):
            return self._supports_pdeath_signal_result

        help_output = subprocess.check_output(["ytserver", "-h"])
        self._supports_pdeath_signal_result = "pdeath-signal" in help_output
        return self._supports_pdeath_signal_result

    def _run_ytserver(self, service_name, name=None):
        if name is None:
            name = service_name

        logger.info("Starting %s", name)

        if not which("ytserver"):
            raise YtError("Failed to start ytserver. Make sure that ytserver binary is installed")

        for i in xrange(len(self.configs[name])):
            command = [
                "ytserver", "--" + service_name,
                "--config", self.config_paths[name][i]
            ]
            if self._supports_pdeath_signal() and self._kill_child_processes:
                command.extend(["--pdeath-signal", str(signal.SIGTERM)])
            if service_name == "node" and sys.platform.startswith("linux"):
                user_name = getpass.getuser()
                for type_ in ["cpuacct", "cpu", "blkio", "memory", "freezer"]:
                    cgroup_path = "/sys/fs/cgroup/{0}/{1}/yt/{2}/node{3}".format(
                            type_, user_name, uuid.uuid4().hex, i)
                    command.extend(["--cgroup", cgroup_path])
            self._run(command, name, i)

    def _get_master_name(self, master_name, cell_index):
        if cell_index == 0:
            return master_name
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
                config_path = os.path.join(master_dirs[cell_index][master_index], "master_config.yson")
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
                self.log_paths[master_name].append(
                    _config_safe_get(config, config_path, "logging/writers/info/file_name"))

    def start_master_cell(self, cell_index=0):
        master_name = self._get_master_name("master", cell_index)
        secondary = cell_index > 0

        self._run_ytserver("master", name=master_name)

        def quorum_ready():
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
                client.config["proxy"]["request_retry_enable"] = False
                client.get("/")
                return True
            except (requests.RequestException, YtError):
                return False
            finally:
                logger.setLevel(old_level)

        cell_ready = quorum_ready

        if secondary:
            primary_cell_client = self.create_client()
            cell_tag = int(
                self._cluster_configuration["master"]["secondary_cell_tags"][cell_index - 1])

            def quorum_ready_and_cell_registered():
                if not quorum_ready():
                    return False

                return cell_tag in primary_cell_client.get("//sys/@registered_master_cell_tags")

            cell_ready = quorum_ready_and_cell_registered

        self._wait_for(cell_ready, master_name, max_wait_time=30)

    def start_all_masters(self, start_secondary_master_cells):
        self.start_master_cell()

        if start_secondary_master_cells:
            self.start_secondary_master_cells()

    def start_secondary_master_cells(self):
        for i in xrange(self.secondary_master_cell_count):
            self.start_master_cell(i + 1)

    def _prepare_nodes(self, node_configs, node_dirs):
        for node_index in xrange(self.node_count):
            config_path = os.path.join(node_dirs[node_index], "node_config.yson")
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
            self.log_paths["node"].append(_config_safe_get(config, config_path, "logging/writers/info/file_name"))

    def start_nodes(self):
        self._run_ytserver("node")

        native_client = self.create_client()

        def nodes_ready():
            nodes = native_client.list("//sys/nodes", attributes=["state"])
            return len(nodes) == self.node_count and all(node.attributes["state"] == "online" for node in nodes)

        self._wait_for(nodes_ready, "node", max_wait_time=max(self.node_count * 6.0, 20))

    def _prepare_schedulers(self, scheduler_configs, scheduler_dirs):
        for scheduler_index in xrange(self.scheduler_count):
            config_path = os.path.join(scheduler_dirs[scheduler_index], "scheduler_config.yson")
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
            self.log_paths["scheduler"].append(_config_safe_get(config, config_path, "logging/writers/info/file_name"))

    def start_schedulers(self):
        self._remove_scheduler_lock()

        self._run_ytserver("scheduler")

        client = self.create_client()

        def schedulers_ready():
            instances = client.list("//sys/scheduler/instances")
            if len(instances) != self.scheduler_count:
                return False

            try:
                active_scheduler_orchid_path = None
                for instance in instances:
                    path = "//sys/scheduler/instances/{0}/orchid/scheduler".format(instance)
                    if client.get(path + "/connected"):
                        active_scheduler_orchid_path = path

                if active_scheduler_orchid_path is None:
                    return False

                nodes = list(itervalues(client.get(active_scheduler_orchid_path + "/nodes")))
                return len(nodes) == self.node_count and all(node["state"] == "online" for node in nodes)
            except YtResponseError as err:
                # Orchid connection refused
                if not err.contains_code(100):
                    raise
                return False

        self._wait_for(schedulers_ready, "scheduler")

    def create_client(self):
        if self.has_proxy:
            return Yt(proxy=self.get_proxy_address())
        return self.create_native_client()

    def create_native_client(self, driver_name="driver"):
        driver_config_path = self.config_paths[driver_name]

        with open(driver_config_path) as f:
            driver_config = yson.load(f)

        config = {
            "backend": "native",
            "driver_config": driver_config
        }

        try:
            import yt_driver_bindings
        except ImportError:
            raise YtError("YT driver bindings not found. Make sure you have installed "
                          "yandex-yt-python-driver package (appropriate version: {0})"
                          .format(self._ytserver_version_long))

        yt_driver_bindings.configure_logging(self.driver_logging_config)

        return Yt(config=config)

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

            config_path = os.path.join(self.path, name + ".yson")
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = driver_configs[tag]
                write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        self.driver_logging_config = init_logging(None,
            self.path, "driver", self._enable_debug_logging)

    def _prepare_proxy(self, proxy_config, ui_config, proxy_dir):
        config_path = os.path.join(proxy_dir, "proxy_config.json")
        if self._load_existing_environment:
            if not os.path.isfile(config_path):
                raise YtError("Proxy config {0} not found".format(config_path))
            config = read_config(config_path, format="json")
        else:
            config = proxy_config
            write_config(config, config_path, format="json")

        self.configs["proxy"] = config
        self.config_paths["proxy"] = config_path
        self.log_paths["proxy"] = os.path.join(proxy_dir, "http_application.log")

        # UI configuration
        web_interface_resources_path = os.environ.get("YT_LOCAL_THOR_PATH", "/usr/share/yt-thor")
        if not os.path.exists(web_interface_resources_path):
            logger.warning("Failed to configure UI, web interface resources are not installed. "
                           "Try to install yandex-yt-web-interface or set YT_LOCAL_THOR_PATH.")
            return

        ui_config_path = os.path.join(proxy_dir, "ui", "config.js")
        if not self._load_existing_environment:
            shutil.copytree(web_interface_resources_path, os.path.join(proxy_dir, "ui"))
            write_config(ui_config, ui_config_path, format=None)
        else:
            if not os.path.isfile(ui_config_path):
                logger.warning("Failed to configure UI, file {0} not found".format(ui_config_path))

    def _start_proxy_from_package(self):
        node_path = list(ifilter(lambda x: x != "", os.environ.get("NODE_PATH", "").split(":")))
        for path in node_path + ["/usr/lib/node_modules"]:
            proxy_binary_path = os.path.join(path, "yt", "bin", "yt_http_proxy")
            if os.path.exists(proxy_binary_path):
                break
        else:
            raise YtError("Failed to find YT http proxy binary. "
                          "Make sure you installed yandex-yt-http-proxy package")

        nodejs_binary_path = _find_nodejs()

        proxy_version = _get_proxy_version(nodejs_binary_path, proxy_binary_path)[:2]  # major, minor
        ytserver_version = list(imap(int, self._ytserver_version.split(".")))[:2]
        if proxy_version and proxy_version != ytserver_version:
            raise YtError("Proxy version does not match ytserver version. "
                          "Expected: {0}.{1}, actual: {2}.{3}".format(*(ytserver_version + proxy_version)))

        self._run([nodejs_binary_path,
                   proxy_binary_path,
                   "-c", self.config_paths["proxy"],
                   "-l", self.log_paths["proxy"]],
                   "proxy")

    def start_proxy(self, use_proxy_from_package=False):
        logger.info("Starting proxy")
        if use_proxy_from_package:
            self._start_proxy_from_package()
        else:
            if not which("run_proxy.sh"):
                raise YtError("Failed to start proxy from source tree. "
                              "Make sure you added directory with run_proxy.sh to PATH")
            self._run(["run_proxy.sh",
                       "-c", self.config_paths["proxy"],
                       "-l", self.log_paths["proxy"]],
                       "proxy")

        def proxy_ready():
            try:
                address = "127.0.0.1:{0}".format(self.get_proxy_address().split(":")[1])
                resp = requests.get("http://{0}/api".format(address))
                resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False

            return True

        self._wait_for(proxy_ready, "proxy", max_wait_time=20)

    def _wait_for(self, condition, name, max_wait_time=40, sleep_quantum=0.1):
        current_wait_time = 0
        logger.info("Waiting for %s...", name)
        while current_wait_time < max_wait_time:
            if condition():
                logger.info("%s ready", name.capitalize())
                return
            time.sleep(sleep_quantum)
            current_wait_time += sleep_quantum

        self._print_stderrs(name)
        raise YtError("{0} still not ready after {1} seconds. See logs in working dir for details."
                      .format(name.capitalize(), max_wait_time))
