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

GEN_PORT_ATTEMPTS = 10

def init_logging(node, path, name):
    for key, suffix in [('file', '.log'), ('raw', '.debug.log')]:
        node['writers'][key]['file_name'] = os.path.join(path, name + suffix)

def write_config(config, filename):
    with open(filename, 'wt') as f:
        f.write(yson.dumps(config, yson_format="pretty"))

def write_with_flush(data):
    sys.stdout.write(data)
    sys.stdout.flush()

def get_open_port():
    for _ in xrange(GEN_PORT_ATTEMPTS):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("",0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()

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

        self._cgroup_types = filter(lambda x: x != "cpuset", os.listdir("/sys/fs/cgroup"))
        self._cgroup_roots = dict()
        for line in open("/proc/self/cgroup").read().split("\n"):
            if line:
                _, name, root = line.split(":", 2)
                if root is not None and root.startswith("/"):
                    self._cgroup_roots[name] = root[1:]

        self.configs = defaultdict(lambda: [])
        self.config_paths = defaultdict(lambda: [])
        self.log_paths = defaultdict(lambda: [])

        self._master_addresses = defaultdict(lambda: [])
        self._process_to_kill = defaultdict(lambda: [])
        self._ports = defaultdict(lambda: [])
        self._pids_filename = pids_filename
        self._kill_previously_run_services()

        self._run_all(self.NUM_MASTERS, self.NUM_NODES, self.NUM_SCHEDULERS, self.START_PROXY, set_driver=True, ports=ports)

    def _run_all(self, masters_count, nodes_count, schedulers_count, has_proxy, set_driver, identifier="", cell_id=0, ports=None):
        get_open_port.busy_ports = set()

        def list_ports(service_name, count):
            if ports is not None and service_name in ports:
                self._ports[service_name] = range(ports[service_name], ports[service_name] + count)
            else:
                self._ports[service_name] = [get_open_port() for i in xrange(count)]

        master_name = "master" + identifier
        scheduler_name = "scheduler" + identifier
        node_name = "node" + identifier
        driver_name = "driver" + identifier
        proxy_name = "proxy" + identifier

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
            self._prepare_driver(driver_name, set_driver)
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

    def _run(self, args, name, number=1, timeout=0.5):
        if self.supress_yt_output:
            stdout = open("/dev/null", "w")
            stderr = open("/dev/null", "w")
        else:
            stdout = sys.stdout
            stderr = sys.stderr
        p = subprocess.Popen(args, shell=False, close_fds=True, preexec_fn=os.setsid,
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
            cgroups_params = []
            for type_ in self._cgroup_types:
                cgroup = os.path.join("/sys/fs/cgroup", type_, self._cgroup_roots[type_], service_name, str(i))
                try:
                    os.makedirs(cgroup, mode=0775)
                except OSError, ex:
                    if ex.errno != errno.EEXIST:
                        raise
                cgroups_params += ["--cgroup", cgroup]
            self._run([
                'ytserver', "--" + service_name,
                '--config', self.config_paths[name][i]
                ]
                  +
                cgroups_params,
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

        hostname = socket.getfqdn()

        self._master_addresses[master_name] = \
                ["%s:%s" % (hostname, self._ports[master_name][2 * i])
                 for i in xrange(masters_count)]

        os.mkdir(os.path.join(self.path_to_run, master_name))

        for i in xrange(masters_count):
            config = configs.get_master_config()

            current = os.path.join(self.path_to_run, master_name, str(i))
            os.mkdir(current)

            config["object_manager"]["cell_id"] = cell_id

            config['meta_state']['cell']['rpc_port'] = self._ports[master_name][2 * i]
            config['monitoring_port'] = self._ports[master_name][2 * i + 1]

            config['meta_state']['cell']['addresses'] = self._master_addresses[master_name]
            config['meta_state']['changelogs']['path'] = \
                os.path.join(current, 'logs')
            config['meta_state']['snapshots']['path'] = \
                    os.path.join(current, 'snapshots')
            init_logging(config['logging'], current, 'master-' + str(i))

            self.modify_master_config(config)
            update(config, self.DELTA_MASTER_CONFIG)

            config_path = os.path.join(current, 'master_config.yson')
            write_config(config, config_path)

            self.configs[master_name].append(config)
            self.config_paths[master_name].append(config_path)
            self.log_paths[master_name].append(config['logging']['writers']['file']['file_name'])

    def start_masters(self, master_name):
        self._run_ytserver("master", master_name)

        def masters_ready():
            good_marker = "World initialization completed"
            bad_marker = "Active quorum lost"

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
        logging.info('(Leader is: %d)', self.leader_id)


    def _run_masters(self, masters_count, master_name, cell_id):
        self._prepare_masters(masters_count, master_name, cell_id)
        self.start_masters(master_name)


    def _prepare_nodes(self, nodes_count, node_name):
        if nodes_count == 0:
            return

        os.mkdir(os.path.join(self.path_to_run, node_name))

        current_user = 10000;
        for i in xrange(nodes_count):
            config = configs.get_node_config()

            current = os.path.join(self.path_to_run, node_name, str(i))
            os.mkdir(current)

            config['rpc_port'] = self._ports[node_name][2 * i]
            config['monitoring_port'] = self._ports[node_name][2 * i + 1]

            config['masters']['addresses'] = self._master_addresses[node_name.replace("node", "master", 1)]
            config['data_node']['cache_location']['path'] = \
                os.path.join(current, 'chunk_cache')
            config['data_node']['store_locations'].append(
                {'path': os.path.join(current, 'chunk_store'),
                 'low_watermark' : 0,
                 'high_watermark' : 0})
            config['exec_agent']['slot_manager']['start_uid'] = current_user
            config['exec_agent']['slot_manager']['path'] = \
                os.path.join(current, 'slot')

            current_user += config['exec_agent']['job_controller']['resource_limits']['slots'] + 1

            init_logging(config['logging'], current, 'node-%d' % i)
            init_logging(config['exec_agent']['job_proxy_logging'], current, 'job_proxy-%d' % i)

            self.modify_node_config(config)
            update(config, self.DELTA_NODE_CONFIG)

            config_path = os.path.join(current, 'node_config.yson')
            write_config(config, config_path)

            self.configs[node_name].append(config)
            self.config_paths[node_name].append(config_path)
            self.log_paths[node_name].append(config['logging']['writers']['file']['file_name'])

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

    def _prepare_schedulers(self, schedulers_count, scheduler_name):
        if schedulers_count == 0:
            return

        os.mkdir(os.path.join(self.path_to_run, scheduler_name))

        for i in xrange(schedulers_count):
            current = os.path.join(self.path_to_run, scheduler_name, str(i))
            os.mkdir(current)

            config = configs.get_scheduler_config()
            config['masters']['addresses'] = self._master_addresses[scheduler_name.replace("scheduler", "master", 1)]
            init_logging(config['logging'], current, 'scheduler-' + str(i))

            config['rpc_port'] = self._ports[scheduler_name][2 * i]
            config['monitoring_port'] = self._ports[scheduler_name][2 * i + 1]

            config['scheduler']['snapshot_temp_path'] = os.path.join(current, 'snapshots')

            self.modify_scheduler_config(config)
            update(config, self.DELTA_SCHEDULER_CONFIG)
            config_path = os.path.join(current, 'scheduler_config.yson')
            write_config(config, config_path)

            self.configs[scheduler_name].append(config)
            self.config_paths[scheduler_name].append(config_path)
            self.log_paths[scheduler_name].append(config['logging']['writers']['file']['file_name'])

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


    def _prepare_driver(self, driver_name, set_driver):
        config = configs.get_driver_config()
        config['masters']['addresses'] = self._master_addresses[driver_name.replace("driver", "master", 1)]
        config_path = os.path.join(self.path_to_run, driver_name + '_config.yson')
        write_config(config, config_path)

        self.configs[driver_name] = config
        self.config_paths[driver_name] = config_path

        if set_driver:
            os.environ['YT_CONFIG'] = config_path


    def _prepare_proxy(self, has_proxy, proxy_name):
        if not has_proxy: return

        current = os.path.join(self.path_to_run, proxy_name)
        os.mkdir(current)

        driver_config = configs.get_driver_config()
        driver_config['masters']['addresses'] = self._master_addresses[proxy_name.replace("proxy", "master", 1)]
        init_logging(driver_config['logging'], current, 'node')

        proxy_config = configs.get_proxy_config()
        proxy_config['proxy']['logging'] = driver_config['logging']
        del driver_config['logging']
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
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect(('127.0.0.1', self._ports[proxy_name][0]))
                s.shutdown(2)
                return True
            except:
                return False

        self._wait_for(started, name="proxy", max_wait_time=20)

    def _run_proxy(self, has_proxy, proxy_name):
        self._prepare_proxy(has_proxy, proxy_name)
        if has_proxy:
            self.start_proxy(proxy_name)

    def _wait_for(self, condition, max_wait_time=20, sleep_quantum=0.5, name=""):
        current_wait_time = 0
        write_with_flush('Waiting for %s' % name)
        while current_wait_time < max_wait_time:
            write_with_flush('.')
            if condition():
                write_with_flush(' %s ready\n' % name)
                return
            time.sleep(sleep_quantum)
            current_wait_time += sleep_quantum
        assert False, "%s still not ready after %s seconds" % (name, max_wait_time)

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
