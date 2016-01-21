from configs_provider import ConfigsProviderFactory, init_logging
from helpers import versions_cmp, is_binary_found, read_config, write_config, collect_events_from_logs, \
                    is_dead_or_zombie, get_open_port, get_lsof_diagnostic

from yt.common import update, YtError, get_value, remove_file, makedirp, set_pdeathsig
import yt.yson as yson

import logging
import os
import re
import uuid
import time
import signal
import socket
import shutil
import sys
import getpass
from collections import defaultdict
from threading import RLock
from itertools import takewhile, count

try:
    import subprocess32 as subprocess
except ImportError:
    if sys.version_info[:2] <= (2, 6):
        print >>sys.stderr, "Environment may not work properly on python of version <= 2.6 " \
                            "because subprocess32 library is not installed."
    import subprocess

logger = logging.getLogger("Yt.local")

def _get_ytserver_version():
    if not is_binary_found("ytserver"):
        raise YtError("Failed to start ytserver. Make sure that ytserver binary is installed")
    # Output example: "\nytserver  version: 0.17.3-unknown~debug~0+local\n\n"
    output = subprocess.check_output(["ytserver", "--version"])
    return output.split(":", 1)[1].strip()

def _get_proxy_version(proxy_binary_path):
    # Output example: "*** YT HTTP Proxy ***\nVersion 0.17.3\nDepends..."
    process = subprocess.Popen(["node", proxy_binary_path, "-v"], stderr=subprocess.PIPE)
    _, err = process.communicate()
    version_str = err.split("\n")[1].split()[1]

    try:
        version = map(int, version_str.split("."))
    except ValueError:
        return None

    return version

def get_busy_port_diagnostic(log_paths):
    patterns = [
        re.compile(r".*Failed to start HTTP server on port (\d+).*"),
        re.compile(r".*Failed to bind a server socket to port (\d+).*")
    ]

    try:
        for log_path in log_paths:
            if not os.path.exists(log_path):
                continue
            for line in reversed(open(log_path).readlines()):
                for pattern in patterns:
                    match = pattern.match(line)
                    if match:
                        return get_lsof_diagnostic(match.group(1))
    except:
        logger.exception("Failed to get busy port diagnostics")
    return None

def add_busy_port_diagnostic(error, log_paths):
    diagnostic = get_busy_port_diagnostic(log_paths)
    if diagnostic is not None:
        error.attributes["details"] = diagnostic

def _config_safe_get(config, config_path, key):
    d = config
    parts = key.split("/")
    for k in parts:
        d = d.get(k)
        if d is None:
            raise YtError('Failed to get required key "{0}" from config file {1}.'.format(key, config_path))
    return d

class YTEnv(object):
    NUM_MASTERS = 3
    NUM_NONVOTING_MASTERS = 0
    NUM_SECONDARY_MASTER_CELLS = 0
    START_SECONDARY_MASTER_CELLS = True
    NUM_NODES = 5
    NUM_SCHEDULERS = 0
    START_PROXY = False
    USE_PROXY_FROM_PACKAGE = False

    CONFIGS_PROVIDER_FACTORY = ConfigsProviderFactory

    DELTA_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}
    DELTA_PROXY_CONFIG = {}

    # to be redefined in successors
    def modify_master_config(self, config):
        pass

    # to be redefined in successors
    def modify_node_config(self, config):
        pass

    # to be redefined in successors
    def modify_scheduler_config(self, config):
        pass

    # to be redefined in successors
    def modify_proxy_config(self, config):
        pass

    def start(self, path_to_run, pids_filename, proxy_port=None, enable_debug_logging=True, preserve_working_dir=False,
              kill_child_processes=False, tmpfs_path=None, enable_ui=False, port_locks_path=None, ports_range_start=None,
              fqdn=None):
        self._lock = RLock()

        logger.propagate = False
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())

        if os.environ.get("YT_ENABLE_VERBOSE_LOGGING"):
            logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        else:
            logger.handlers[0].setFormatter(logging.Formatter("%(message)s"))

        self.path_to_run = os.path.abspath(path_to_run)
        self.stderrs_path = os.path.join(self.path_to_run, "stderrs")
        self.tmpfs_path = tmpfs_path
        self.port_locks_path = port_locks_path
        self.pids_filename = pids_filename

        load_existing_environment = False
        if os.path.exists(self.path_to_run):
            if not preserve_working_dir:
                shutil.rmtree(self.path_to_run, ignore_errors=True)
                makedirp(self.path_to_run)
            else:
                load_existing_environment = True

        if self.port_locks_path is not None:
            makedirp(self.port_locks_path)

        makedirp(self.stderrs_path)

        self.configs = defaultdict(list)
        self.config_paths = defaultdict(list)
        self.log_paths = defaultdict(list)

        if fqdn is None:
            self._hostname = socket.getfqdn()
        else:
            self._hostname = fqdn

        self._process_to_kill = defaultdict(list)
        self._all_processes = {}
        self._kill_previously_run_services()
        self._kill_child_processes = kill_child_processes

        ytserver_version_long = _get_ytserver_version()
        logger.info("Logging started (ytserver version: %s)", ytserver_version_long)

        self._ytserver_version = ytserver_version_long.split("-", 1)[0].strip()

        self._run_all(self.NUM_MASTERS,
                      self.NUM_NONVOTING_MASTERS,
                      self.NUM_SECONDARY_MASTER_CELLS,
                      self.NUM_NODES,
                      self.NUM_SCHEDULERS,
                      self.START_PROXY,
                      use_proxy_from_package=self.USE_PROXY_FROM_PACKAGE,
                      start_secondary_master_cells=self.START_SECONDARY_MASTER_CELLS,
                      proxy_port=proxy_port,
                      enable_debug_logging=enable_debug_logging,
                      load_existing_environment=load_existing_environment,
                      enable_ui=enable_ui,
                      ports_range_start=ports_range_start)

    def get_proxy_address(self):
        if not self.START_PROXY:
            raise YtError("Proxy is not started")
        return "{0}:{1}".format(self._hostname, _config_safe_get(self.configs["proxy"],
                                                                 self.config_paths["proxy"], "port"))

    def kill_service(self, name):
        with self._lock:
            logger.info("Killing %s", name)
            for p in self._process_to_kill[name]:
                self._kill_process(p, name)
                del self._all_processes[p.pid]
            del self._process_to_kill[name]

    def clear_environment(self):
        with self._lock:
            for name in self.configs:
                self.kill_service(name)

            remove_file(self.pids_filename, force=True)

            for lock_fd in get_open_port.lock_fds:
                try:
                    os.close(lock_fd)
                except OSError as err:
                    logger.warning("Failed to close file descriptor %d: %s",
                                   lock_fd, os.strerror(err.errno))

    def check_liveness(self, callback_func):
        with self._lock:
            for pid, info in self._all_processes.iteritems():
                proc, args = info
                proc.poll()
                if proc.returncode is not None:
                    callback_func(self, args)
                    break

    def _run_all(self, master_count, nonvoting_master_count, secondary_master_cell_count, node_count, scheduler_count, has_proxy, enable_ui=False,
                 use_proxy_from_package=False, start_secondary_master_cells=True, instance_id="", cell_tag=0,
                 proxy_port=None, enable_debug_logging=True, load_existing_environment=False, ports_range_start=None):

        master_name = "master" + instance_id
        scheduler_name = "scheduler" + instance_id
        node_name = "node" + instance_id
        driver_name = "driver" + instance_id
        console_driver_name = "console_driver" + instance_id
        proxy_name = "proxy" + instance_id

        if secondary_master_cell_count > 0 and versions_cmp(self._ytserver_version, "0.18") < 0:
            raise YtError("Multicell is not supported for ytserver version < 0.18")

        ports = self._get_ports(ports_range_start, master_count, secondary_master_cell_count,
                                scheduler_count, node_count, has_proxy, proxy_port)
        self._configs_provider = self.CONFIGS_PROVIDER_FACTORY \
                .create_for_version(self._ytserver_version, ports, enable_debug_logging, self._hostname)
        self._enable_debug_logging = enable_debug_logging
        self._load_existing_environment = load_existing_environment

        logger.info("Starting up cluster instance as follows:")
        logger.info("  masters          %d (%d nonvoting)", master_count, nonvoting_master_count)
        logger.info("  nodes            %d", node_count)
        logger.info("  schedulers       %d", scheduler_count)

        if self.START_SECONDARY_MASTER_CELLS:
            logger.info("  secondary cells  %d", secondary_master_cell_count)

        logger.info("  proxies          %d", int(has_proxy))
        logger.info("  working dir      %s", self.path_to_run)

        if master_count == 0:
            logger.info("Do nothing, because we have 0 masters")
            shutil.rmtree(self.path_to_run, ignore_errors=True)
            return

        self._started = False
        try:
            self._prepare_masters(master_count, master_name, nonvoting_master_count, secondary_master_cell_count, cell_tag)
            self._prepare_schedulers(scheduler_count, scheduler_name)
            self._prepare_nodes(node_count, node_name)
            self._prepare_proxy(has_proxy, proxy_name, enable_ui=enable_ui)
            self._prepare_driver(driver_name, secondary_master_cell_count)
            self._prepare_console_driver(console_driver_name, self.configs[driver_name])

            self.start_all_masters(master_name, secondary_master_cell_count,
                                   start_secondary_master_cells=start_secondary_master_cells)
            self.start_schedulers(scheduler_name)
            self.start_nodes(node_name)
            self.start_proxy(proxy_name, use_proxy_from_package=use_proxy_from_package)

            self._write_environment_info_to_file(has_proxy)
            self._started = True
        except (YtError, KeyboardInterrupt) as err:
            self.clear_environment()
            raise YtError("Failed to start environment", inner_errors=[err])

    def _get_ports(self, ports_range_start, master_count, secondary_master_cell_count,
                   scheduler_count, node_count, has_proxy, proxy_port):
        ports = defaultdict(list)

        def random_port_generator():
            while True:
                yield get_open_port(self.port_locks_path)

        get_open_port.busy_ports = set()
        get_open_port.lock_fds = set()

        if ports_range_start and isinstance(ports_range_start, int):
            generator = count(ports_range_start)
        else:
            generator = random_port_generator()

        if master_count > 0:
            for cell_index in xrange(secondary_master_cell_count + 1):
                cell_ports = [next(generator) for _ in xrange(2 * master_count)]  # rpc_port + monitoring_port
                ports["master"].append(cell_ports)

            if scheduler_count > 0:
                ports["scheduler"] = [next(generator) for _ in xrange(2 * scheduler_count)]
            if node_count > 0:
                ports["node"] = [next(generator) for _ in xrange(2 * node_count)]
            if has_proxy:
                if proxy_port is None:
                    ports["proxy"] = next(generator)
                else:
                    ports["proxy"] = proxy_port

        return ports

    def _write_environment_info_to_file(self, has_proxy):
        info = {}
        if has_proxy:
            info["proxy"] = {"address": self.get_proxy_address()}
        with open(os.path.join(self.path_to_run, "info.yson"), "w") as fout:
            yson.dump(info, fout, yson_format="pretty")

    def _kill_process(self, proc, name):
        proc.poll()
        if proc.returncode is not None:
            if self._started:
                logger.warning("%s (pid: %d, working directory: %s) is already terminated with exit code %d",
                               name, proc.pid, os.path.join(self.path_to_run, name), proc.returncode)
            return

        os.killpg(proc.pid, signal.SIGKILL)
        time.sleep(0.2)

        if not is_dead_or_zombie(proc.pid):
            logger.error("Failed to kill process %s (pid %d) ", name, proc.pid)

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + "\n")
        self.pids_file.flush()

    def _run(self, args, name, number=1, timeout=0.1):
        with self._lock:
            stdout = open(os.devnull, "w")
            stderr = None
            if os.environ.get("YT_CAPTURE_STDERR_TO_FILE"):
                stderr = open(os.path.join(self.stderrs_path, "stderr.{0}-{1}".format(name, number)), "w")

            def preexec():
                os.setsid()
                if self._kill_child_processes:
                    set_pdeathsig()

            p = subprocess.Popen(args, shell=False, close_fds=True, preexec_fn=preexec, cwd=self.path_to_run,
                                 stdout=stdout, stderr=stderr)

            time.sleep(timeout)
            if p.poll():
                error = YtError("Process {0}-{1} unexpectedly terminated with error code {2}. "
                                "If the problem is reproducible please report to yt@yandex-team.ru mailing list."
                                .format(name, number, p.returncode))
                add_busy_port_diagnostic(error, self.log_paths[name])
                raise error

            self._process_to_kill[name].append(p)
            self._all_processes[p.pid] = (p, args)
            self._append_pid(p.pid)

    def _run_ytserver(self, service_name, name):
        logger.info("Starting %s", name)

        if not is_binary_found("ytserver"):
            raise YtError("Failed to start ytserver. Make sure that ytserver binary is installed")

        for i in xrange(len(self.configs[name])):
            command = [
                "ytserver", "--" + service_name,
                "--config", self.config_paths[name][i]
            ]
            if service_name == "node" and sys.platform.startswith("linux"):
                user_name = getpass.getuser()
                for type_ in ["cpuacct", "cpu", "blkio", "memory", "freezer"]:
                    cgroup_path = "/sys/fs/cgroup/{0}/{1}/yt/{2}/node{3}".format(
                            type_, user_name, uuid.uuid4().hex, i)
                    command.extend(["--cgroup", cgroup_path])
            self._run(command, name, i)

    def _kill_previously_run_services(self):
        if os.path.exists(self.pids_filename):
            with open(self.pids_filename, "rt") as f:
                for pid in map(int, f.xreadlines()):
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except OSError:
                        continue
                    logger.warning("Killed alive process with pid %d from previously run instance", pid)

        dirname = os.path.dirname(self.pids_filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.pids_file = open(self.pids_filename, "wt")

    def _get_master_name(self, master_name, cell_index):
        if cell_index == 0:
            return master_name
        else:
            return master_name + "_secondary_" + str(cell_index - 1)

    def _get_master_configs(self, master_count, nonvoting_master_count, master_name, secondary_master_cell_count, cell_tag,
                            master_dirs, tmpfs_master_dirs):
        if self._load_existing_environment:
            master_configs = []
            for cell_index in xrange(secondary_master_cell_count + 1):
                name = self._get_master_name(master_name, cell_index)

                configs = []
                for master_index in xrange(master_count):
                    config_path = os.path.join(self.path_to_run, name, str(master_index), "master_config.yson")

                    if not os.path.isfile(config_path):
                        raise YtError("Master config {0} not found. "
                                      "It is possible that you requested more masters than configs exist".format(config_path))

                    configs.append(read_config(config_path))

                master_configs.append(configs)

            return master_configs
        else:
            return self._configs_provider.get_master_configs(master_count, nonvoting_master_count, master_dirs, tmpfs_master_dirs,
                                                             secondary_master_cell_count, cell_tag)

    def _prepare_masters(self, master_count, master_name, nonvoting_master_count, secondary_master_cell_count, cell_tag):
        if master_count == 0:
            return

        self._master_cell_tags = {}

        dirs = []
        tmpfs_dirs = [] if self.tmpfs_path else None

        for cell_index in xrange(secondary_master_cell_count + 1):
            name = self._get_master_name(master_name, cell_index)
            dirs.append([os.path.join(self.path_to_run, name, str(i)) for i in xrange(master_count)])
            map(makedirp, dirs[cell_index])
            if self.tmpfs_path is not None and not self._load_existing_environment:
                tmpfs_dirs.append([os.path.join(self.tmpfs_path, name, str(i)) for i in xrange(master_count)])
                map(makedirp, tmpfs_dirs[cell_index])

        configs = self._get_master_configs(master_count, nonvoting_master_count, master_name, secondary_master_cell_count, cell_tag,
                                           dirs, tmpfs_dirs)

        for cell_index in xrange(secondary_master_cell_count + 1):
            current_master_name = self._get_master_name(master_name, cell_index)

            # Will be used for waiting
            self._master_cell_tags[current_master_name] = cell_tag + cell_index

            for master_index, config in enumerate(configs[cell_index]):
                current_path = os.path.join(self.path_to_run, current_master_name, str(master_index))

                self.modify_master_config(config)
                update(config, self.DELTA_MASTER_CONFIG)

                config_path = os.path.join(current_path, "master_config.yson")
                if not self._load_existing_environment:
                    write_config(config, config_path)

                self.configs[current_master_name].append(config)
                self.config_paths[current_master_name].append(config_path)
                self.log_paths[current_master_name].append(
                    _config_safe_get(config, config_path, "logging/writers/info/file_name"))

    def start_masters(self, master_name, secondary=None):
        primary_master_name = master_name
        secondary = get_value(secondary, "secondary" in master_name)

        if secondary:
            # e.g. master_remote_secondary_0 -> master_remote
            primary_master_name = master_name[:master_name.find("secondary")-1]

        masters_count = len(self.log_paths[master_name])
        if masters_count == 0:
            return

        self._run_ytserver("master", master_name)

        is_leader_ready_marker = lambda line: "Leader active" in line or "Initial changelog rotated" in line
        is_restart_occured_marker = lambda line: "Logging started" in line or "Stopped leading" in line
        is_world_init_completed_marker = lambda line: "World initialization completed" in line
        is_follower_recovery_complete_marker = lambda line: "Follower recovery complete" in line

        # First version is less precise and will be used for quick filtering.
        is_secondary_master_registered_marker = lambda line: "Secondary master registered" in line
        secondary_master_registered_pattern = r"Secondary master registered.*CellTag: {0}"

        def is_quorum_ready(starting_master_events):
            is_world_initialization_done = False
            ready_replica_count = 0

            for replica_index, lines in enumerate(starting_master_events):
                restart_occured_and_not_ready = False

                for line in lines:
                    if is_restart_occured_marker(line):
                        restart_occured_and_not_ready = True
                        continue

                    if is_world_init_completed_marker(line):
                        is_world_initialization_done = True
                        continue

                    if is_leader_ready_marker(line) and not restart_occured_and_not_ready:
                        ready_replica_count += 1
                        if not secondary:
                            self.leader_log = self.log_paths[master_name][replica_index]
                            self.leader_id = replica_index

                    if is_follower_recovery_complete_marker(line) and not restart_occured_and_not_ready:
                        ready_replica_count += 1

            return ready_replica_count == masters_count and is_world_initialization_done

        def masters_ready():
            event_filters = [is_leader_ready_marker, is_world_init_completed_marker, is_restart_occured_marker,
                             is_follower_recovery_complete_marker]
            if secondary:
                event_filters.append(is_secondary_master_registered_marker)
            # Each element is a list with log lines of each replica of the cell.
            current_master_events = collect_events_from_logs(self.log_paths[master_name], event_filters)

            if not is_quorum_ready(current_master_events):
                return False

            if secondary:
                current_master_cell_registered_marker = re.compile(
                    secondary_master_registered_pattern.format(self._master_cell_tags[master_name]))

                for name in self._process_to_kill:
                    if name == master_name or not name.startswith(primary_master_name):
                        continue

                    master_cell_events = collect_events_from_logs(self.log_paths[name], [
                        lambda line: current_master_cell_registered_marker.search(line)])

                    if not filter(None, master_cell_events):
                        return False

                    if name != primary_master_name:
                        secondary_cell_registered_marker = re.compile(
                            secondary_master_registered_pattern.format(self._master_cell_tags[name]))

                        for event_list in current_master_events:
                            if filter(lambda line: secondary_cell_registered_marker.search(line), event_list):
                                break
                        else:
                            return False

            return True

        self._wait_for(masters_ready, name=master_name, max_wait_time=30)

        if secondary:
            logger.info("Secondary master %s registered", master_name.rsplit("_", 1)[1])
        else:
            if masters_count > 1:
                logger.info("Leader master index: %d", self.leader_id)

    def start_all_masters(self, master_name, secondary_master_cell_count, start_secondary_master_cells):
        self.start_masters(master_name)
        if start_secondary_master_cells:
            for i in xrange(secondary_master_cell_count):
                self.start_masters(self._get_master_name(master_name, i + 1), secondary=True)

    def _get_node_configs(self, node_count, node_name, node_dirs):
        if self._load_existing_environment:
            configs = []
            for i in xrange(node_count):
                config_path = os.path.join(self.path_to_run, node_name, str(i), "node_config.yson")

                if not os.path.isfile(config_path):
                    raise YtError("Node config {0} not found. "
                                  "It is possible that you requested more nodes than configs exist".format(config_path))

                configs.append(read_config(config_path))

            return configs
        else:
            return self._configs_provider.get_node_configs(node_count, node_dirs)


    def _prepare_nodes(self, node_count, node_name):
        if node_count == 0:
            return

        dirs = [os.path.join(self.path_to_run, node_name, str(i))
                for i in xrange(node_count)]
        map(makedirp, dirs)

        configs = self._get_node_configs(node_count, node_name, dirs)

        for i, config in enumerate(configs):
            current_path = os.path.join(self.path_to_run, node_name, str(i))

            self.modify_node_config(config)
            update(config, self.DELTA_NODE_CONFIG)

            config_path = os.path.join(current_path, "node_config.yson")
            if not self._load_existing_environment:
                write_config(config, config_path)

            self.configs[node_name].append(config)
            self.config_paths[node_name].append(config_path)
            self.log_paths[node_name].append(_config_safe_get(config, config_path, "logging/writers/info/file_name"))

    def start_nodes(self, node_name):
        nodes_count = len(self.log_paths[node_name])
        if nodes_count == 0:
            return

        self._run_ytserver("node", node_name)

        scheduler_logs = self.log_paths[node_name.replace("node", "scheduler", 1)]

        def all_nodes_ready():
            is_scheduler_bad_marker = lambda line: "Logging started" in line
            is_scheduler_good_marker = lambda line: "Node online" in line

            node_good_marker = re.compile(r".*Node online .*Address: ([a-zA-z0-9:_\-.]+).*")
            node_bad_marker = re.compile(r".*Node unregistered .*Address: ([a-zA-z0-9:_\-.]+).*")

            node_statuses = {}

            def update_node_status(marker, line, value):
                match = marker.match(line)
                if match:
                    address = match.group(1)
                    if address not in node_statuses:
                        node_statuses[address] = value

            for line in reversed(open(self.leader_log).readlines()):
                update_node_status(node_good_marker, line, True)
                update_node_status(node_bad_marker, line, False)

            if scheduler_logs:
                for events in collect_events_from_logs(scheduler_logs, [is_scheduler_bad_marker, is_scheduler_good_marker]):
                    filtered_events = takewhile(lambda line: not is_scheduler_bad_marker(line), events)
                    if len(list(filtered_events)) >= nodes_count:
                        break
                else:
                    return False

            return len(node_statuses) == nodes_count and all(node_statuses.values())

        self._wait_for(all_nodes_ready, name=node_name, max_wait_time=max(nodes_count * 6.0, 20))

    def _get_scheduler_configs(self, scheduler_count, scheduler_name, scheduler_dirs):
        if self._load_existing_environment:
            configs = []
            for scheduler_index in xrange(scheduler_count):
                config_path = os.path.join(self.path_to_run, scheduler_name, str(scheduler_index), "scheduler_config.yson")

                if not os.path.isfile(config_path):
                    raise YtError("Scheduler config {0} not found. "
                                  "It is possible that you requested more schedulers than configs exist".format(config_path))

                configs.append(read_config(config_path))

            return configs
        else:
            return self._configs_provider.get_scheduler_configs(scheduler_count, scheduler_dirs)

    def _prepare_schedulers(self, scheduler_count, scheduler_name):
        if scheduler_count == 0:
            return

        dirs = [os.path.join(self.path_to_run, scheduler_name, str(i))
                for i in xrange(scheduler_count)]
        map(makedirp, dirs)

        configs = self._get_scheduler_configs(scheduler_count, scheduler_name, dirs)

        for i, config in enumerate(configs):
            self.modify_scheduler_config(config)
            update(config, self.DELTA_SCHEDULER_CONFIG)

            config_path = os.path.join(self.path_to_run, scheduler_name, str(i), "scheduler_config.yson")
            if not self._load_existing_environment:
                write_config(config, config_path)

            self.configs[scheduler_name].append(config)
            self.config_paths[scheduler_name].append(config_path)
            self.log_paths[scheduler_name].append(_config_safe_get(config, config_path, "logging/writers/info/file_name"))

    def start_schedulers(self, scheduler_name):
        schedulers_count = len(self.log_paths[scheduler_name])
        if schedulers_count == 0:
            return

        self._run_ytserver("scheduler", scheduler_name)

        def scheduler_ready():
            is_scheduler_bad_marker = lambda line: "Logging started" in line
            is_primary_scheduler_good_marker = lambda line: "Master connected" in line
            secondary_scheduler_good_marker_regex = re.compile(r"Cannot.*lock.*//sys/scheduler/lock")
            is_secondary_scheduler_good_marker = lambda line: secondary_scheduler_good_marker_regex.search(line)

            is_primary_scheduler_exists = False
            ready_schedulers_count = 0

            events = collect_events_from_logs(self.log_paths[scheduler_name], [
                is_primary_scheduler_good_marker, is_secondary_scheduler_good_marker, is_scheduler_bad_marker
            ])

            for lines in events:
                filtered_lines = list(takewhile(lambda line: not is_scheduler_bad_marker(line), lines))
                if filter(is_primary_scheduler_good_marker, filtered_lines):
                    ready_schedulers_count += 1
                    is_primary_scheduler_exists = True
                    continue
                if filter(is_secondary_scheduler_good_marker, filtered_lines):
                    ready_schedulers_count += 1

            return ready_schedulers_count == schedulers_count and is_primary_scheduler_exists

        self._wait_for(scheduler_ready, name=scheduler_name)

    def _get_driver_name(self, driver_name, cell_index):
        if cell_index == 0:
            return driver_name
        else:
            return driver_name + "_secondary_" + str(cell_index - 1)

    def _get_driver_configs(self, driver_name, secondary_master_cell_count):
        if self._load_existing_environment:
            configs = []
            for cell_index in xrange(secondary_master_cell_count + 1):
                configs.append(read_config(os.path.join(self.path_to_run,
                                                        self._get_driver_name(driver_name, cell_index) + ".yson")))

            return configs
        else:
            return self._configs_provider.get_driver_configs()

    def _prepare_driver(self, driver_name, secondary_master_cell_count):
        configs = self._get_driver_configs(driver_name, secondary_master_cell_count)
        for cell_index, config in enumerate(configs):
            current_driver_name = self._get_driver_name(driver_name, cell_index)

            config_path = os.path.join(self.path_to_run, current_driver_name + ".yson")
            if not self._load_existing_environment:
                write_config(config, config_path)

            self.configs[current_driver_name] = config
            self.config_paths[current_driver_name] = config_path
            self.driver_logging_config = init_logging(None, self.path_to_run, "driver", self._enable_debug_logging)

    def _prepare_console_driver(self, console_driver_name, driver_config):
            from default_configs import get_console_driver_config

            config = get_console_driver_config()

            config["driver"] = driver_config
            config["logging"] = init_logging(config["logging"], self.path_to_run, "console_driver",
                                             self._enable_debug_logging)

            config_path = os.path.join(self.path_to_run, "console_driver_config.yson")

            write_config(config, config_path)

            self.configs[console_driver_name].append(config)
            self.config_paths[console_driver_name].append(config_path)
            self.log_paths[console_driver_name].append(config["logging"]["writers"]["info"]["file_name"])

    def _get_ui_config(self, proxy_name):
        if self._load_existing_environment:
            config_path = os.path.join(self.path_to_run, proxy_name, "ui/config.js")

            if not os.path.isfile(config_path):
                raise YtError("Proxy config {0} not found.".format(config_path))

            return read_config(config_path, format=None)
        else:
            return self._configs_provider.get_ui_config(
                "{0}:{1}".format(self._hostname, self.configs[proxy_name]["port"]))

    def _get_proxy_config(self, proxy_name, proxy_dir):
        if self._load_existing_environment:
            config_path = os.path.join(self.path_to_run, proxy_name, "proxy_config.json")

            if not os.path.isfile(config_path):
                raise YtError("Proxy config {0} not found.".format(config_path))

            return read_config(config_path, format="json")
        else:
            return self._configs_provider.get_proxy_config(proxy_dir)

    def _prepare_proxy(self, has_proxy, proxy_name, enable_ui):
        if not has_proxy:
            return

        proxy_dir = os.path.join(self.path_to_run, proxy_name)
        makedirp(proxy_dir)

        proxy_config = self._get_proxy_config(proxy_name, proxy_dir)

        self.modify_proxy_config(proxy_config)
        update(proxy_config, self.DELTA_PROXY_CONFIG)

        config_path = os.path.join(proxy_dir, "proxy_config.json")
        if not self._load_existing_environment:
            write_config(proxy_config, config_path, format="json")

        self.configs[proxy_name] = proxy_config
        self.config_paths[proxy_name] = config_path
        self.log_paths[proxy_name] = os.path.join(proxy_dir, "http_application.log")

        if enable_ui:
            web_interface_resources_path = os.environ.get("YT_LOCAL_THOR_PATH", "/usr/share/yt-thor")
            if not os.path.exists(web_interface_resources_path):
                logger.warning("Failed to configure UI, web interface resources are not installed. " \
                               "Try to install yandex-yt-web-interface or set YT_LOCAL_THOR_PATH.")
                return
            if not self._load_existing_environment:
                shutil.copytree(web_interface_resources_path, os.path.join(proxy_dir, "ui/"))
            config_path = os.path.join(proxy_dir, "ui/config.js")
            ui_config = self._get_ui_config(proxy_name)
            if not self._load_existing_environment:
                write_config(ui_config, config_path, format=None)

    def _start_proxy_from_package(self, proxy_name):
        node_path = filter(lambda x: x != "", os.environ.get("NODE_PATH", "").split(":"))
        for path in node_path + ["/usr/lib/node_modules"]:
            proxy_binary_path = os.path.join(path, "yt", "bin", "yt_http_proxy")
            if os.path.exists(proxy_binary_path):
                break
        else:
            raise YtError("Failed to find YT http proxy binary. "
                          "Make sure you installed yandex-yt-http-proxy package")

        try:
            version = subprocess.check_output(["node", "-v"])
        except subprocess.CalledProcessError:
            raise YtError("Failed to find nodejs binary to start proxy. "
                          "Make sure you added nodejs directory to PATH variable")

        if not version.startswith("v0.8"):
            raise YtError("Failed to find appropriate nodejs version (should start with 0.8)")

        proxy_version = _get_proxy_version(proxy_binary_path)[:2]  # major, minor
        ytserver_version = map(int, self._ytserver_version.split("."))[:2]
        if proxy_version and proxy_version != ytserver_version:
            raise YtError("Proxy version does not match ytserver version. "
                          "Expected: {0}.{1}, actual: {2}.{3}".format(*(ytserver_version + proxy_version)))

        self._run(["node",
                   proxy_binary_path,
                   "-c", self.config_paths[proxy_name],
                   "-l", self.log_paths[proxy_name]],
                   "proxy",
                   timeout=3.0)

    def start_proxy(self, proxy_name, use_proxy_from_package=False):
        if len(self.log_paths[proxy_name]) == 0:
            return

        logger.info("Starting %s", proxy_name)
        if use_proxy_from_package:
            self._start_proxy_from_package(proxy_name)
        else:
            if not is_binary_found("run_proxy.sh"):
                raise YtError("Failed to start proxy from source tree. "
                              "Make sure you added directory with run_proxy.sh to PATH")
            self._run(["run_proxy.sh",
                       "-c", self.config_paths[proxy_name],
                       "-l", self.log_paths[proxy_name]],
                       "proxy",
                       timeout=3.0)

        def started():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(("127.0.0.1", int(self.get_proxy_address().split(":", 1)[1])))
                sock.shutdown(2)
                return True
            except:
                return False
            finally:
                sock.close()

        self._wait_for(started, name=proxy_name, max_wait_time=20)

    def _wait_for(self, condition, max_wait_time=40, sleep_quantum=0.1, name=""):
        current_wait_time = 0
        logger.info("Waiting for %s...", name)
        while current_wait_time < max_wait_time:
            if condition():
                logger.info("%s ready", name.capitalize())
                return
            time.sleep(sleep_quantum)
            current_wait_time += sleep_quantum

        error =  YtError("{0} still not ready after {1} seconds. See logs in working dir for details."
                         .format(name.capitalize(), max_wait_time))
        add_busy_port_diagnostic(error, self.log_paths[name])
        raise error
