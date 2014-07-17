import configs
from helpers import unorderable_list_difference, _AssertRaisesContext, Counter

from yt.common import update
import yt.yson as yson

import logging
import os
import re
import time
import signal
import socket
import shutil
import subprocess
import sys
import errno
import simplejson as json
from collections import defaultdict
from datetime import datetime, timedelta

GEN_PORT_ATTEMPTS = 10

def init_logging(node, path, name):
    if not node:
        node = configs.get_logging_pattern()

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

def init_tracing(endpoint_port):
    config = configs.get_tracing_config()
    config['endpoint_port'] = endpoint_port
    return config

def write_config(config, filename):
    with open(filename, 'wt') as f:
        f.write(yson.dumps(config, yson_format="pretty"))

def write_with_flush(data):
    sys.stdout.write(data)
    sys.stdout.flush()

def get_open_port():
    for _ in xrange(GEN_PORT_ATTEMPTS):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("",0))
            sock.listen(1)
            port = s.getsockname()[1]
        finally:
            sock.close()

        if port in get_open_port.busy_ports:
            continue

        get_open_port.busy_ports.add(port)

        return port

    raise RuntimeError("Failed to generate random port")

class YTEnv(object):
    failureException = Exception

    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 0
    START_PROXY = False

    DELTA_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}

    # to be redefiened in successors
    def modify_master_config(self, config):
        pass

    # to be redefined in successors
    def modify_node_config(self, config):
        pass

    # to be redefined in successors
    def modify_scheduler_config(self, config):
        pass

    def set_environment(self, path_to_run, pids_filename, ports=None, supress_yt_output=False):
        logging.basicConfig(format='%(message)s')

        self.supress_yt_output = supress_yt_output
        self.path_to_run = os.path.abspath(path_to_run)
        if os.path.exists(self.path_to_run):
            shutil.rmtree(self.path_to_run, ignore_errors=True)
        try:
            os.makedirs(self.path_to_run)
        except:
            pass

        self.configs = defaultdict(lambda: [])
        self.config_paths = defaultdict(lambda: [])
        self.log_paths = defaultdict(lambda: [])

        short_hostname = socket.gethostname()
        self._hostname = socket.gethostbyname_ex(short_hostname)[0]
        self._master_addresses = defaultdict(lambda: [])
        self._node_addresses = defaultdict(lambda: [])
        self._process_to_kill = defaultdict(lambda: [])
        self._ports = defaultdict(lambda: [])
        self._pids_filename = pids_filename
        self._kill_previously_run_services()

        self._run_all(self.NUM_MASTERS, self.NUM_NODES, self.NUM_SCHEDULERS, self.START_PROXY, ports=ports)

    def _run_all(self, masters_count, nodes_count, schedulers_count, has_proxy, instance_id="", cell_id=0, ports=None):
        get_open_port.busy_ports = set()

        def list_ports(service_name, count):
            if ports is not None and service_name in ports:
                self._ports[service_name] = range(ports[service_name], ports[service_name] + count)
            else:
                self._ports[service_name] = [get_open_port() for i in xrange(count)]

        master_name = "master" + instance_id
        scheduler_name = "scheduler" + instance_id
        node_name = "node" + instance_id
        driver_name = "driver" + instance_id
        console_driver_name = "console_driver" + instance_id
        proxy_name = "proxy" + instance_id

        list_ports(master_name, 2 * masters_count)
        list_ports(scheduler_name, 2 * schedulers_count)
        list_ports(node_name, 2 * nodes_count)
        list_ports(proxy_name, 2)

        logging.info('Setting up configuration with %s masters, %s nodes, %s schedulers.' %
                     (masters_count, nodes_count, schedulers_count))
        logging.info('SANDBOX_DIR is %s', self.path_to_run)

        if masters_count == 0:
            logging.info("Do nothing, because we have 0 masters")
            return

        try:
            logging.info("Configuring...")
            self._run_masters(masters_count, master_name, cell_id)
            self._run_schedulers(schedulers_count, scheduler_name)
            self._run_nodes(nodes_count, node_name)
            self._prepare_driver(driver_name)
            self._prepare_console_driver(console_driver_name, self.configs[driver_name])
            self._run_proxy(has_proxy, proxy_name)
        except:
            self.clear_environment()
            raise

    def kill_process(self, proc, name):
        ok = True
        message = ""
        proc.poll()
        if proc.returncode is not None:
            ok = False
            message += '%s (pid %d) is already terminated with exit status %d\n' % (name, proc.pid, proc.returncode)
        else:
            os.killpg(proc.pid, signal.SIGKILL)

        time.sleep(0.250)

        # now try to kill unkilled process
        for i in xrange(50):
            proc.poll()
            if proc.returncode is not None:
                break
            logging.warning('%s (pid %d) was not killed by the kill command' % (name, proc.pid))

            os.killpg(proc.pid, signal.SIGKILL)
            time.sleep(0.100)

        if proc.returncode is None:
            ok = False
            message += 'Alarm! %s (pid %d) was not killed after 50 iterations\n' % (name, proc.pid)
        return message, ok

    def _kill_service(self, name):
        ok = True
        message = ""
        for p in self._process_to_kill[name]:
            p_message, p_ok = self.kill_process(p, name)
            if not p_ok:
                ok = False
            message += p_message

        if ok:
            self._process_to_kill[name] = []

        return ok, message


    def clear_environment(self):
        total_ok = True
        total_message = ""
        for name in self.configs:
            ok, message = self._kill_service(name)
            if not ok:
                total_ok = False
                total_message += message + "\n\n"

        assert total_ok, total_message

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + '\n')
        self.pids_file.flush();

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
        self._append_pid(p.pid)

        time.sleep(timeout)
        if p.poll():
            print >>sys.stderr, "Process %s-%d unexpectedly terminated." % (name, number)
            print >>sys.stderr, "Check that there are no other incarnations of this process."
            assert False, "Process unexpectedly terminated"

    def _run_ytserver(self, service_name, name):
        for i in xrange(len(self.configs[name])):
            self._run([
                'ytserver', "--" + service_name,
                '--config', self.config_paths[name][i]
                ],
                name, i)

    def _kill_previously_run_services(self):
        if os.path.exists(self._pids_filename):
            with open(self._pids_filename, 'rt') as f:
                for pid in map(int, f.xreadlines()):
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except OSError:
                        pass

        dirname = os.path.dirname(self._pids_filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.pids_file = open(self._pids_filename, 'wt')

    def _prepare_masters(self, masters_count, master_name, cell_id):
        if masters_count == 0:
             return

        self._master_addresses[master_name] = \
                ["%s:%s" % (self._hostname, self._ports[master_name][2 * i])
                 for i in xrange(masters_count)]

        os.mkdir(os.path.join(self.path_to_run, master_name))

        for i in xrange(masters_count):
            config = configs.get_master_config()

            current = os.path.join(self.path_to_run, master_name, str(i))
            os.mkdir(current)

            config['rpc_port'] = self._ports[master_name][2 * i]
            config['monitoring_port'] = self._ports[master_name][2 * i + 1]

            config["master"]["cell_id"] = cell_id
            config['master']['addresses'] = self._master_addresses[master_name]
            config['timestamp_provider']['addresses'] = self._master_addresses[master_name]
            config['changelogs']['path'] = os.path.join(current, 'changelogs')
            config['snapshots']['path'] = os.path.join(current, 'snapshots')
            config['logging'] = init_logging(config['logging'], current, 'master-' + str(i))
            config['tracing'] = init_tracing(config['rpc_port'])

            self.modify_master_config(config)
            update(config, self.DELTA_MASTER_CONFIG)

            config_path = os.path.join(current, 'master_config.yson')
            write_config(config, config_path)

            self.configs[master_name].append(config)
            self.config_paths[master_name].append(config_path)
            self.log_paths[master_name].append(config['logging']['writers']['debug']['file_name'])

    def start_masters(self, master_name):
        self._run_ytserver("master", master_name)

        def masters_ready():
            good_marker = "World initialization completed"
            bad_marker = "Stopped leading"

            master_id = 0
            for logging_file in self.log_paths[master_name]:
                if not os.path.exists(logging_file): continue

                for line in reversed(open(logging_file).readlines()):
                    if bad_marker in line: break
                    if good_marker in line:
                        self.leader_log = logging_file
                        self.leader_id = master_id
                        return True
                master_id += 1
            return False

        self._wait_for(masters_ready, name=master_name)
        logging.info('Leader is %d', self.leader_id)

    def _run_masters(self, masters_count, master_name, cell_id):
        self._prepare_masters(masters_count, master_name, cell_id)
        self.start_masters(master_name)


    def _prepare_nodes(self, nodes_count, node_name):
        if nodes_count == 0:
            return

        self._node_addresses[node_name] = \
                ["%s:%s" % (self._hostname, self._ports[node_name][2 * i])
                 for i in xrange(nodes_count)]

        os.mkdir(os.path.join(self.path_to_run, node_name))

        current_user = 10000;
        for i in xrange(nodes_count):
            config = configs.get_node_config()

            current = os.path.join(self.path_to_run, node_name, str(i))
            os.mkdir(current)

            config['rpc_port'] = self._ports[node_name][2 * i]
            config['monitoring_port'] = self._ports[node_name][2 * i + 1]

            config['cluster_connection']['master']['addresses'] = self._master_addresses[node_name.replace("node", "master", 1)]
            config['cluster_connection']['timestamp_provider']['addresses'] = self._master_addresses[node_name.replace("node", "master", 1)]

            config['data_node']['multiplexed_changelog']['path'] = os.path.join(current, 'multiplexed')
            config['data_node']['cache_location']['path'] = os.path.join(current, 'chunk_cache')
            config['data_node']['store_locations'].append({
                'path': os.path.join(current, 'chunk_store'),
                'low_watermark' : 0,
                'high_watermark' : 0
            })
            config['exec_agent']['slot_manager']['start_uid'] = current_user
            config['exec_agent']['slot_manager']['path'] = os.path.join(current, 'slots')
            config['tablet_node']['snapshots']['temp_path'] = os.path.join(current, 'snapshots')

            current_user += config['exec_agent']['job_controller']['resource_limits']['slots'] + 1

            config['logging'] = init_logging(config['logging'], current, 'node-%d' % i)
            config['tracing'] = init_tracing(config['rpc_port'])
            config['exec_agent']['job_proxy_logging'] = \
                init_logging(config['exec_agent']['job_proxy_logging'], current, 'job_proxy-%d' % i)

            self.modify_node_config(config)
            update(config, self.DELTA_NODE_CONFIG)

            config_path = os.path.join(current, 'node_config.yson')
            write_config(config, config_path)

            self.configs[node_name].append(config)
            self.config_paths[node_name].append(config_path)
            self.log_paths[node_name].append(config['logging']['writers']['debug']['file_name'])

    def start_nodes(self, node_name):
        self._run_ytserver("node", node_name)

        nodes_count = len(self.log_paths[node_name])
        scheduler_logs = self.log_paths[node_name.replace("node", "scheduler", 1)]

        def all_nodes_ready():
            nodes_status = {}

            scheduler_good_marker = re.compile(r".*Node online.*")
            good_marker = re.compile(r".*Node online .*NodeId: (\d+).*")
            bad_marker = re.compile(r".*Node unregistered .*NodeId: (\d+).*")

            def update_status(marker, line, status, value):
                match = marker.match(line)
                if match:
                    node_id = match.group(1)
                    if node_id not in status:
                        status[node_id] = value

            for line in reversed(open(self.leader_log).readlines()):
                update_status(good_marker, line, nodes_status, True)
                update_status(bad_marker, line, nodes_status, False)

            schedulers_ready = True
            for log in scheduler_logs:
                ready = 0
                for line in reversed(open(log).readlines()):
                    if scheduler_good_marker.match(line):
                        ready += 1
                if ready != nodes_count:
                    schedulers_ready = False

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

        os.mkdir(os.path.join(self.path_to_run, scheduler_name))

        for i in xrange(schedulers_count):
            current = os.path.join(self.path_to_run, scheduler_name, str(i))
            os.mkdir(current)

            config = configs.get_scheduler_config()
            config['cluster_connection']['master']['addresses'] = self._master_addresses[scheduler_name.replace("scheduler", "master", 1)]
            config['cluster_connection']['timestamp_provider']['addresses'] = self._get_cache_addresses(scheduler_name.replace("scheduler", "", 1))

            config['rpc_port'] = self._ports[scheduler_name][2 * i]
            config['monitoring_port'] = self._ports[scheduler_name][2 * i + 1]
            config['scheduler']['snapshot_temp_path'] = os.path.join(current, 'snapshots')
            
            config['logging'] = init_logging(config['logging'], current, 'scheduler-' + str(i))
            config['tracing'] = init_tracing(config['rpc_port'])

            self.modify_scheduler_config(config)
            update(config, self.DELTA_SCHEDULER_CONFIG)
            config_path = os.path.join(current, 'scheduler_config.yson')
            write_config(config, config_path)

            self.configs[scheduler_name].append(config)
            self.config_paths[scheduler_name].append(config_path)
            self.log_paths[scheduler_name].append(config['logging']['writers']['debug']['file_name'])

    def start_schedulers(self, scheduler_name):
        self._run_ytserver("scheduler", scheduler_name)

        def scheduler_ready():
            good_marker = 'Master connected'

            for log in self.log_paths[scheduler_name]:
                if not os.path.exists(log): return False
                ok = False
                for line in reversed(open(log).readlines()):
                    if good_marker in line:
                        ok = True
                        break
                if not ok:
                    return False;
            return True

        self._wait_for(scheduler_ready, name=scheduler_name)

    def _run_schedulers(self, schedulers_count, scheduler_name):
        self._prepare_schedulers(schedulers_count, scheduler_name)
        self.start_schedulers(scheduler_name)


    def _prepare_driver(self, driver_name):
        config = configs.get_driver_config()
        config['master']['addresses'] = self._master_addresses[driver_name.replace("driver", "master", 1)]
        config['timestamp_provider']['addresses'] = self._get_cache_addresses(driver_name.replace("driver", "", 1))
        config['master_cache']['addresses'] = self._get_cache_addresses(driver_name.replace("driver", "", 1))

        self.configs[driver_name] = config
        self.driver_logging_config = init_logging(None, self.path_to_run, "driver")
        self.driver_tracing_config = init_tracing(0)

    def _prepare_console_driver(self, console_driver_name, driver_config):
        config = configs.get_console_driver_config()
        config["driver"] = driver_config
        config['logging'] = init_logging(config['logging'], self.path_to_run, 'console_driver')
        config['tracing'] = init_tracing(0)

        config_path = os.path.join(self.path_to_run, 'console_driver_config.yson')

        write_config(config, config_path)

        self.configs[console_driver_name].append(config)
        self.config_paths[console_driver_name].append(config_path)
        self.log_paths[console_driver_name].append(config['logging']['writers']['debug']['file_name'])


    def _prepare_proxy(self, has_proxy, proxy_name):
        if not has_proxy: return

        current = os.path.join(self.path_to_run, proxy_name)
        os.mkdir(current)

        driver_config = configs.get_driver_config()
        driver_config['master']['addresses'] = self._master_addresses[proxy_name.replace("proxy", "master", 1)]
        driver_config['timestamp_provider']['addresses'] = self._get_cache_addresses(proxy_name.replace("proxy", "", 1))
        driver_config['master_cache']['addresses'] = self._get_cache_addresses(proxy_name.replace("proxy", "", 1))

        proxy_config = configs.get_proxy_config()
        proxy_config['proxy']['logging'] = init_logging(proxy_config['proxy']['logging'], current, "http_proxy")
        proxy_config['proxy']['driver'] = driver_config
        proxy_config['port'] = self._ports[proxy_name][0]
        proxy_config['log_port'] = self._ports[proxy_name][1]

        config_path = os.path.join(self.path_to_run, 'proxy_config.json')
        with open(config_path, "w") as f:
            f.write(json.dumps(proxy_config))

        self.configs[proxy_name] = proxy_config
        self.config_paths[proxy_name] = config_path
        self.log_paths[proxy_name] = os.path.join(current, "http_application.log")

    def start_proxy(self, proxy_name):
        self._run(["run_proxy.sh",
                   "-c", self.config_paths[proxy_name],
                   "-l", self.log_paths[proxy_name],
                   proxy_name],
                   "proxy",
                   timeout=3.0)

        def started():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(('127.0.0.1', self._ports[proxy_name][0]))
                sock.shutdown(2)
                return True
            except:
                return False
            finally:
                sock.close()

        self._wait_for(started, name="proxy", max_wait_time=20)

    def _run_proxy(self, has_proxy, proxy_name):
        self._prepare_proxy(has_proxy, proxy_name)
        if has_proxy:
            self.start_proxy(proxy_name)

    def _wait_for(self, condition, max_wait_time=20, sleep_quantum=0.1, name=""):
        print_dot_timeout = timedelta(seconds=0.5)
        last_print_dot_time = datetime.now()

        current_wait_time = 0
        write_with_flush('Waiting for %s' % name)
        while current_wait_time < max_wait_time:
            if last_print_dot_time < datetime.now():
                write_with_flush('.')
                last_print_dot_time += print_dot_timeout
            if condition():
                write_with_flush(' %s ready\n' % name)
                return
            time.sleep(sleep_quantum)
            current_wait_time += sleep_quantum
        assert False, "%s still not ready after %s seconds" % (name, max_wait_time)

    def get_master_addresses(self):
        if len(self._master_addresses) == 0:
            return []
        if len(self._master_addresses) == 1:
            return self._master_addresses.items()[0][1]

    # Unittest is painfull to integrate, so we simply reimplement some methods
    def assertItemsEqual(self, actual_seq, expected_seq):
        # It is simplified version of the same method of unittest.TestCase
        try:
            actual = Counter(iter(actual_seq))
            expected = Counter(iter(expected_seq))
        except TypeError:
            # Unsortable items (example: set(), complex(), ...)
            actual = list(actual_seq)
            expected = list(expected_seq)
            missing, unexpected = unorderable_list_difference(expected, actual)
        else:
            if actual == expected:
                return
            missing = list(expected - actual)
            unexpected = list(actual - expected)

        assert not missing, 'Expected, but missing:\n    %s' % repr(missing)
        assert not unexpected, 'Unexpected, but present:\n    %s' % repr(unexpected)

    def assertEqual(self, actual, expected, msg=""):
        self.assertTrue(actual == expected, msg)

    def assertTrue(self, expr, msg=""):
        assert expr, msg

    def assertFalse(self, expr, msg=""):
        assert not expr, msg

    def assertRaises(self, excClass, callableObj=None, *args, **kwargs):
        context = _AssertRaisesContext(excClass, self)
        if callableObj is None:
            return context
        with context:
            callableObj(*args, **kwargs)

    def assertAlmostEqual(self, first, second, places=7, msg="", delta=None):
        """ check equality of float numbers with some precision """
        if first == second:
            return

        if delta is not None:
            assert (abs(first - second) <= delta), msg if msg else "|{0} - {1}| > {2}".format(first, second, delta)
        else:
            rounding = round(abs(second - first), places)
            assert (rounding == 0), msg if msg else "rounding is {0} (not zero!)".format(rounding)
