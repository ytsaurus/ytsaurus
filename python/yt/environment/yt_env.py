from configs_provider import ConfigsProviderFactory, init_logging
from helpers import versions_cmp

from yt.common import update, YtError
import yt.yson as yson

import logging
import os
import re
import time
import signal
import errno
import socket
import shutil
import sys
import getpass
import yt.json as json
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import chain
from copy import deepcopy

try:
    import subprocess32 as subprocess
except ImportError:
    print >>sys.stderr, "Environment may not work properly on python of version <= 2.6"
    import subprocess

GEN_PORT_ATTEMPTS = 10

logger = logging.getLogger("Yt.local")

def fix_yson_booleans(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            fix_yson_booleans(value)
            if isinstance(value, yson.YsonBoolean):
                obj[key] = True if value else False
    elif isinstance(obj, list):
        for value in obj:
            fix_yson_booleans(value)
    return obj

def _write_config(config, filename, format="yson"):
    with open(filename, "wt") as f:
        if format == "yson":
            yson.dump(config, f, yson_format="pretty", boolean_as_string=False)
        else:  # json
            json.dump(fix_yson_booleans(config), f, indent=4)

def _is_binary_found(binary_name):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.access(os.path.join(path, binary_name), os.X_OK):
            return True
    return False

def _get_ytserver_version():
    if not _is_binary_found("ytserver"):
        raise YtError("Failed to start ytserver. Make sure that ytserver binary is installed")
    # Output example: "\nytserver  version: 0.17.3-unknown~debug~0+local\n\n"
    output = subprocess.check_output(["ytserver", "--version"])
    return output.split(":", 1)[1].split("-", 1)[0]

def _makedirp(path):
    try:
        os.makedirs(path)
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise

class YTEnv(object):
    failureException = Exception

    NUM_MASTERS = 3
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

    def start(self, path_to_run, pids_filename, proxy_port=None, supress_yt_output=False, enable_debug_logging=True):
        logger.propagate = False
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())
        logger.handlers[0].setFormatter(logging.Formatter("%(message)s"))

        self.supress_yt_output = supress_yt_output
        self.path_to_run = os.path.abspath(path_to_run)
        self.pids_filename = pids_filename

        if os.path.exists(self.path_to_run):
            shutil.rmtree(self.path_to_run, ignore_errors=True)
        _makedirp(self.path_to_run)

        self.configs = defaultdict(list)
        self.config_paths = defaultdict(list)
        self.log_paths = defaultdict(list)

        self._hostname = socket.getfqdn()
        self._master_addresses = defaultdict(list)
        self._node_addresses = defaultdict(list)
        self._scheduler_addresses = defaultdict(list)
        self._proxy_address = ""
        self._process_to_kill = defaultdict(list)
        self._all_processes = {}
        self._kill_previously_run_services()

        self._ytserver_version = _get_ytserver_version()

        self._run_all(self.NUM_MASTERS,
                      self.NUM_SECONDARY_MASTER_CELLS,
                      self.NUM_NODES,
                      self.NUM_SCHEDULERS,
                      self.START_PROXY,
                      use_proxy_from_package=self.USE_PROXY_FROM_PACKAGE,
                      start_secondary_master_cells=self.START_SECONDARY_MASTER_CELLS,
                      proxy_port=proxy_port,
                      enable_debug_logging=enable_debug_logging)

    def get_master_addresses(self):
        return list(chain.from_iterable(self._master_addresses.values()))

    def get_node_addresses(self):
        return list(chain.from_iterable(self._node_addresses.values()))

    def get_scheduler_addresses(self):
        return list(chain.from_iterable(self._scheduler_addresses.values()))

    def get_proxy_address(self):
        if not self.START_PROXY:
            raise YtError("Proxy is not started")
        return self._proxy_address

    def kill_service(self, name):
        logger.info("Killing %s", name)

        ok = True
        message_parts = []
        remaining_processes = []
        for p in self._process_to_kill[name]:
            p_ok, p_message = self._kill_process(p, name)
            if not p_ok:
                ok = False
                remaining_processes.append(p)
            else:
                del self._all_processes[p.pid]
            message_parts.append(p_message)

        self._process_to_kill[name] = remaining_processes

        return ok, "".join(message_parts)

    def clear_environment(self, safe=True):
        total_ok = True
        total_message_parts = []
        for name in self.configs:
            ok, message = self.kill_service(name)
            if not ok:
                total_ok = False
                total_message_parts.append(message)
        if not total_ok:
            message = "Failed to clear clear_environment. Message: %s"
            if safe:
                raise YtError(message % "\n\n".join(total_message_parts))
            else:
                logger.warning(message, "\n\n".join(total_message_parts))

    def check_liveness(self, callback_func):
        for pid, info in self._all_processes.iteritems():
            proc, args = info
            proc.poll()
            if proc.returncode is not None:
                callback_func(self, args)
                break

    def _run_all(self, masters_count, secondary_master_cell_count, nodes_count, schedulers_count, has_proxy,
                 use_proxy_from_package=False, start_secondary_master_cells=True, instance_id="", cell_tag=0,
                 proxy_port=None, enable_debug_logging=True):

        master_name = "master" + instance_id
        scheduler_name = "scheduler" + instance_id
        node_name = "node" + instance_id
        driver_name = "driver" + instance_id
        console_driver_name = "console_driver" + instance_id
        proxy_name = "proxy" + instance_id

        if secondary_master_cell_count > 0 and versions_cmp(self._ytserver_version, "0.18") < 0:
            raise YtError("Multicell is not supported for ytserver version < 0.18")

        self._configs_provider = self.CONFIGS_PROVIDER_FACTORY \
                .create_for_version(self._ytserver_version, enable_debug_logging)
        self._enable_debug_logging = enable_debug_logging

        logger.info("Starting up cluster instance as follows:")
        logger.info("  masters          %d", masters_count)
        logger.info("  nodes            %d", nodes_count)
        logger.info("  schedulers       %d", schedulers_count)

        if self.START_SECONDARY_MASTER_CELLS:
            logger.info("  secondary cells  %d", secondary_master_cell_count)

        logger.info("  proxies          %d", int(has_proxy))
        logger.info("  working dir      %s", self.path_to_run)

        if masters_count == 0:
            logger.info("Do nothing, because we have 0 masters")
            shutil.rmtree(self.path_to_run, ignore_errors=True)
            return

        try:
            self._run_masters(masters_count, master_name, secondary_master_cell_count, cell_tag,
                              start_secondary_master_cells=start_secondary_master_cells)
            self._run_schedulers(schedulers_count, scheduler_name)
            self._run_nodes(nodes_count, node_name)
            self._prepare_driver(driver_name, secondary_master_cell_count)
            self._prepare_console_driver(console_driver_name, self.configs[driver_name])
            self._run_proxy(has_proxy, proxy_name, use_proxy_from_package=use_proxy_from_package, proxy_port=proxy_port)

            self._write_environment_info_to_file(masters_count, secondary_master_cell_count, schedulers_count,
                                                 nodes_count, has_proxy)
        except Exception as err:
            self.clear_environment(safe=False)
            raise YtError("Failed to start environment", inner_errors=[err])

    def _write_environment_info_to_file(self, masters_count, secondary_master_cell_count, schedulers_count, nodes_count, has_proxy):
        info = {}
        info["master"] = {"addresses": self.get_master_addresses()}
        if secondary_master_cell_count > 0:
            info["secondary_master_cell_count"] = secondary_master_cell_count
        if schedulers_count > 0:
            info["scheduler"] = {"addresses": self.get_scheduler_addresses()}
        if nodes_count > 0:
            info["node"] = {"addresses": self.get_node_addresses()}
        if has_proxy:
            info["proxy"] = {"address": self.get_proxy_address()}
        with open(os.path.join(self.path_to_run, "info.yson"), "w") as fout:
            yson.dump(info, fout, yson_format="pretty")

    def _kill_process(self, proc, name):
        proc.poll()
        if proc.returncode is not None:
            return False, "{0} (pid: {1}, working directory: {2}) is already terminated with exit code {3}\n"\
                .format(name, proc.pid, os.path.join(self.path_to_run), proc.returncode)
        else:
            os.killpg(proc.pid, signal.SIGKILL)

        time.sleep(0.250)

        # now try to kill unkilled process
        for i in xrange(50):
            proc.poll()
            if proc.returncode is not None:
                break
            logger.warning("%s (pid %d) was not killed by the kill command", name, proc.pid)

            os.killpg(proc.pid, signal.SIGKILL)
            time.sleep(0.100)

        if proc.returncode is None:
            return False, "Alarm! {0} (pid {1}) was not killed after 50 iterations\n".format(name, proc.pid)

        return True, ""

    def _secondary_master_name(self, master_name, index):
        return master_name + "_secondary_" + str(index)

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + "\n")
        self.pids_file.flush()

    def _run(self, args, name, number=1, timeout=0.1):
        if self.supress_yt_output:
            stdout = open("/dev/null", "w")
            stderr = open("/dev/null", "w")
        else:
            stdout = sys.stdout
            stderr = sys.stderr
        p = subprocess.Popen(args, shell=False, close_fds=True, preexec_fn=os.setsid, cwd=self.path_to_run,
                             stdout=stdout, stderr=stderr)
        self._process_to_kill[name].append(p)
        self._all_processes[p.pid] = (p, args)
        self._append_pid(p.pid)

        time.sleep(timeout)
        if p.poll():
            raise YtError("Process %s-%d unexpectedly terminated with error code %d. "
                          "If the problem is reproducible please report to yt@yandex-team.ru mailing list",
                          name, number, p.returncode)

    def _run_ytserver(self, service_name, name):
        logger.info("Starting %s", name)

        if not _is_binary_found("ytserver"):
            raise YtError("Failed to start ytserver. Make sure that ytserver binary is installed")

        for i in xrange(len(self.configs[name])):
            command = [
                "ytserver", "--" + service_name,
                "--config", self.config_paths[name][i]
            ]
            if service_name == "node":
                user_name = getpass.getuser()
                for type_ in ["cpuacct", "blkio", "memory", "freezer"]:
                    cgroup_path = "/sys/fs/cgroup/{0}/{1}/yt/node{2}".format(type_, user_name, i)
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

    def _prepare_masters(self, masters_count, master_name, secondary_master_cell_count, cell_tag):
        if masters_count == 0:
            return

        self._master_cell_tags = {}
        self._primary_masters = {}

        dirs = []
        for cell_index in xrange(secondary_master_cell_count + 1):
            if cell_index == 0:
                name = master_name
            else:
                name = self._secondary_master_name(master_name, cell_index - 1)
            dirs.append([os.path.join(self.path_to_run, name, str(i))
                         for i in xrange(masters_count)])
            map(_makedirp, dirs[cell_index])

        configs, addresses = self._configs_provider.\
                get_master_configs(masters_count, dirs, secondary_master_cell_count, cell_tag)

        for cell_index in xrange(secondary_master_cell_count + 1):
            if cell_index == 0:
                current_master_name = master_name
            else:
                current_master_name = self._secondary_master_name(master_name, cell_index - 1)

            # Will be used for waiting
            self._master_cell_tags[current_master_name] = cell_tag + cell_index
            self._primary_masters[current_master_name] = master_name

            for master_index, config in enumerate(configs[cell_index]):
                current_path = os.path.join(self.path_to_run, current_master_name, str(master_index))

                self.modify_master_config(config)
                update(config, self.DELTA_MASTER_CONFIG)

                config_path = os.path.join(current_path, "master_config.yson")
                _write_config(config, config_path)

                self.configs[current_master_name].append(config)
                self.config_paths[current_master_name].append(config_path)
                self.log_paths[current_master_name].append(config["logging"]["writers"]["info"]["file_name"])

            self._master_addresses[current_master_name] = deepcopy(addresses[cell_index])

    def start_masters(self, master_name, secondary=None):
        if secondary is None:
            secondary = "secondary" in master_name

        masters_count = len(self.log_paths[master_name])
        if masters_count == 0:
            return

        self._run_ytserver("master", master_name)
        def masters_ready():
            if versions_cmp(self._ytserver_version, "0.17.3") < 0:
                leader_ready_marker = "Leader active"
            else:
                leader_ready_marker = "Initial changelog rotated"

            world_init_completed_marker = "World initialization completed"

            bad_markers = ["Logging started", "Stopped leading"]

            secondary_master_registered = re.compile(r"Secondary master registered.*CellTag: {0}"\
                    .format(self._master_cell_tags[master_name]))

            is_world_initialization_done = False
            is_leader_ready = False

            ok = False

            master_id = 0

            for logging_file in self.log_paths[master_name]:
                if not os.path.exists(logging_file):
                    continue

                if ok:
                    break

                restart_occured_and_not_ready = False

                for line in reversed(open(logging_file).readlines()):
                    if any([bad_marker in line for bad_marker in bad_markers]) and \
                            not is_leader_ready:
                        restart_occured_and_not_ready = True

                    if world_init_completed_marker in line:
                        is_world_initialization_done = True

                    if leader_ready_marker in line \
                            and not restart_occured_and_not_ready:
                        is_leader_ready = True
                        if not secondary:
                            self.leader_log = logging_file
                            self.leader_id = master_id

                    if is_leader_ready and is_world_initialization_done:
                        ok = True
                        break

                master_id += 1

            if not ok:
                return False

            if secondary:
                ok = False
                primary_master_name = self._primary_masters[master_name]
                for logging_file in self.log_paths[primary_master_name]:
                    if not os.path.exists(logging_file):
                        continue

                    if ok:
                        break

                    for line in reversed(open(logging_file).readlines()):
                        if secondary_master_registered.search(line):
                            ok = True
                            break

            return ok

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
                self.start_masters(self._secondary_master_name(master_name, i), secondary=True)

    def _run_masters(self, masters_count, master_name, secondary_master_cell_count, cell_tag, start_secondary_master_cells):
        self._prepare_masters(masters_count, master_name, secondary_master_cell_count, cell_tag)
        self.start_all_masters(master_name, secondary_master_cell_count, start_secondary_master_cells)

    def _prepare_nodes(self, nodes_count, node_name):
        if nodes_count == 0:
            return

        dirs = [os.path.join(self.path_to_run, node_name, str(i))
                for i in xrange(nodes_count)]
        map(_makedirp, dirs)

        configs, addresses = self._configs_provider.get_node_configs(nodes_count, dirs)

        for i, config in enumerate(configs):
            current_path = os.path.join(self.path_to_run, node_name, str(i))

            self.modify_node_config(config)
            update(config, self.DELTA_NODE_CONFIG)

            config_path = os.path.join(current_path, "node_config.yson")
            _write_config(config, config_path)

            self.configs[node_name].append(config)
            self.config_paths[node_name].append(config_path)
            self.log_paths[node_name].append(config["logging"]["writers"]["info"]["file_name"])

        self._node_addresses[node_name] = deepcopy(addresses)

    def start_nodes(self, node_name):
        nodes_count = len(self.log_paths[node_name])
        if nodes_count == 0:
            return

        self._run_ytserver("node", node_name)

        scheduler_logs = self.log_paths[node_name.replace("node", "scheduler", 1)]

        def all_nodes_ready():
            nodes_status = {}

            scheduler_good_marker = re.compile(r".*Node online.*")
            node_good_marker = re.compile(r".*Node online .*Address: ([a-zA-z0-9:_\-.]+).*")
            node_bad_marker = re.compile(r".*Node unregistered .*Address: ([a-zA-z0-9:_\-.]+).*")

            def update_status(marker, line, status, value):
                match = marker.match(line)
                if match:
                    address = match.group(1)
                    if address not in status:
                        status[address] = value

            for line in reversed(open(self.leader_log).readlines()):
                update_status(node_good_marker, line, nodes_status, True)
                update_status(node_bad_marker, line, nodes_status, False)

            schedulers_ready = False
            if scheduler_logs:
                for log in scheduler_logs:
                    ready = 0
                    for line in reversed(open(log).readlines()):
                        if scheduler_good_marker.match(line):
                            ready += 1
                    if ready == nodes_count:
                        schedulers_ready = True
            else:
                schedulers_ready = True

            if len(nodes_status) != nodes_count:
                return False

            return all(nodes_status.values()) and schedulers_ready

        self._wait_for(all_nodes_ready, name=node_name,
                       max_wait_time=max(nodes_count * 6.0, 20))

    def _run_nodes(self, nodes_count, node_name):
        self._prepare_nodes(nodes_count, node_name)
        self.start_nodes(node_name)

    def _get_cache_addresses(self, instance_id):
        if len(self._node_addresses["node" + instance_id]) > 0:
            return self._node_addresses["node" + instance_id]
        else:
            return self._master_addresses["master" + instance_id]

    def _prepare_schedulers(self, schedulers_count, scheduler_name):
        if schedulers_count == 0:
            return

        dirs = [os.path.join(self.path_to_run, scheduler_name, str(i))
                for i in xrange(schedulers_count)]
        map(_makedirp, dirs)

        configs, addresses = self._configs_provider.get_scheduler_configs(schedulers_count, dirs)

        for i, config in enumerate(configs):
            current_path = os.path.join(self.path_to_run, scheduler_name, str(i))

            self.modify_scheduler_config(config)
            update(config, self.DELTA_SCHEDULER_CONFIG)

            config_path = os.path.join(current_path, "scheduler_config.yson")
            _write_config(config, config_path)

            self.configs[scheduler_name].append(config)
            self.config_paths[scheduler_name].append(config_path)
            self.log_paths[scheduler_name].append(config["logging"]["writers"]["info"]["file_name"])

        self._scheduler_addresses[scheduler_name] = deepcopy(addresses)

    def start_schedulers(self, scheduler_name):
        schedulers_count = len(self.log_paths[scheduler_name])
        if schedulers_count == 0:
            return

        self._run_ytserver("scheduler", scheduler_name)

        def scheduler_ready():
            good_marker_for_primary_scheduler = re.compile(r".*Master connected.*")
            good_marker_for_secondary_scheduler = re.compile(r"Cannot.*lock.*//sys/scheduler/lock")

            is_primary_scheduler_exists = False
            ready_schedulers_count = 0

            for log in self.log_paths[scheduler_name]:
                if not os.path.exists(log):
                    return False
                for line in reversed(open(log).readlines()):
                    if good_marker_for_primary_scheduler.search(line):
                        is_primary_scheduler_exists = True
                        ready_schedulers_count += 1
                        break
                    elif good_marker_for_secondary_scheduler.search(line):
                        ready_schedulers_count += 1
                        break
            if ready_schedulers_count != schedulers_count or not is_primary_scheduler_exists:
                return False
            return True

        self._wait_for(scheduler_ready, name=scheduler_name)

    def _run_schedulers(self, schedulers_count, scheduler_name):
        self._prepare_schedulers(schedulers_count, scheduler_name)
        self.start_schedulers(scheduler_name)

    def _prepare_driver(self, driver_name, secondary_master_cell_count):
        configs = self._configs_provider.get_driver_configs()
        for cell_index, config in enumerate(configs):
            if cell_index == 0:
                current_driver_name = driver_name
            else:
                current_driver_name = driver_name + "_secondary_" + str(cell_index - 1)

            self.configs[current_driver_name] = config
            self.driver_logging_config = init_logging(None, self.path_to_run, "driver", self._enable_debug_logging)

    def _prepare_console_driver(self, console_driver_name, driver_config):
            from default_configs import get_console_driver_config

            config = get_console_driver_config()

            config["driver"] = driver_config
            config["logging"] = init_logging(config["logging"], self.path_to_run, "console_driver",
                                              self._enable_debug_logging)

            config_path = os.path.join(self.path_to_run, "console_driver_config.yson")

            _write_config(config, config_path)

            self.configs[console_driver_name].append(config)
            self.config_paths[console_driver_name].append(config_path)
            self.log_paths[console_driver_name].append(config["logging"]["writers"]["info"]["file_name"])

    def _prepare_proxy(self, has_proxy, proxy_name, proxy_port):
        if not has_proxy:
            return

        proxy_dir = os.path.join(self.path_to_run, proxy_name)
        _makedirp(proxy_dir)

        proxy_config, proxy_address = self._configs_provider.get_proxy_config(proxy_dir, proxy_port)

        self.modify_proxy_config(proxy_config)
        update(proxy_config, self.DELTA_PROXY_CONFIG)

        config_path = os.path.join(proxy_dir, "proxy_config.json")
        _write_config(proxy_config, config_path, format="json")

        self.configs[proxy_name] = proxy_config
        self.config_paths[proxy_name] = config_path
        self.log_paths[proxy_name] = os.path.join(proxy_dir, "http_application.log")
        self._proxy_address = proxy_address

    def _start_proxy_from_package(self, proxy_name):
        node_path = os.environ.get("NODE_PATH", "").split(":")
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

        self._run(["node",
                   proxy_binary_path,
                   "-c", self.config_paths[proxy_name],
                   "-l", self.log_paths[proxy_name]],
                   "proxy",
                   timeout=3.0)

    def start_proxy(self, proxy_name, use_proxy_from_package=False):
        logger.info("Starting %s", proxy_name)
        if use_proxy_from_package:
            self._start_proxy_from_package(proxy_name)
        else:
            if not _is_binary_found("run_proxy.sh"):
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
                sock.connect(("127.0.0.1", int(self._proxy_address.split(":", 1)[1])))
                sock.shutdown(2)
                return True
            except:
                return False
            finally:
                sock.close()

        self._wait_for(started, name="proxy", max_wait_time=20)

    def _run_proxy(self, has_proxy, proxy_name, use_proxy_from_package=False, proxy_port=None):
        self._prepare_proxy(has_proxy, proxy_name, proxy_port)
        if has_proxy:
            self.start_proxy(proxy_name, use_proxy_from_package)

    def _wait_for(self, condition, max_wait_time=20, sleep_quantum=0.1, name=""):
        print_dot_timeout = timedelta(seconds=0.5)
        last_print_dot_time = datetime.now()

        current_wait_time = 0
        logger.info("Waiting for %s...", name)
        while current_wait_time < max_wait_time:
            if last_print_dot_time < datetime.now():
                last_print_dot_time += print_dot_timeout
            if condition():
                logger.info("%s ready", name.capitalize())
                return
            time.sleep(sleep_quantum)
            current_wait_time += sleep_quantum
        raise YtError("{0} still not ready after {1} seconds".format(name, max_wait_time))

