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
import simplejson as json


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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("",0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port

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

        self._pids_filename = pids_filename
        self._kill_previously_run_services()

        def list_ports(service_name, count):
            if ports is not None and service_name in ports:
                self._ports[service_name] = range(ports[service_name], ports[service_name] + count)
            else:
                self._ports[service_name] = [get_open_port() for i in xrange(count)]

        self._ports = {}
        list_ports("master", 2 * self.NUM_MASTERS)
        list_ports("node", 2 * self.NUM_NODES)
        list_ports("scheduler", 2 * self.NUM_SCHEDULERS)
        list_ports("proxy", 2)

        logging.info('Setting up configuration with %s masters, %s nodes, %s schedulers.' %
                     (self.NUM_MASTERS, self.NUM_NODES, self.NUM_SCHEDULERS))
        logging.info('SANDBOX_DIR is %s', self.path_to_run)

        self.process_to_kill = []

        if self.NUM_MASTERS == 0:
            logging.info("Do nothing, because we have 0 masters")
            return

        try:
            logging.info("Configuring...")
            self._run_masters()
            self._run_schedulers()
            self._run_nodes()
            self._prepare_driver()
            self._run_proxy()
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

    def clear_environment(self):
        ok = True
        message = ""
        for p, name in reversed(self.process_to_kill):
            p_message, p_ok = self.kill_process(p, name)
            if not p_ok: ok = False
            message += p_message

        assert ok, message

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + '\n')
        self.pids_file.flush();

    def _run(self, args, name, timeout=0.5):
        if self.supress_yt_output:
            stdout = open("/dev/null", "w")
            stderr = open("/dev/null", "w")
        else:
            stdout = sys.stdout
            stderr = sys.stderr
        p = subprocess.Popen(args, shell=False, close_fds=True, preexec_fn=os.setsid,
                             stdout=stdout, stderr=stderr)
        self.process_to_kill.append((p, name))
        self._append_pid(p.pid)

        time.sleep(timeout)
        if p.poll():
            print >>sys.stderr, "Process %s unexpectedly terminated." % name
            print >>sys.stderr, "Check that there are no other incarnations of this process."
            assert False, "Process unexpectedly terminated"

    def _run_ytserver(self, service_name, configs):
        for i in xrange(len(configs)):
            self._run([
                'ytserver', "--" + service_name,
                '--config', configs[i]],
                "%s-%d" % (service_name, i))

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


    def _run_masters(self, prepare_files=True):
        if self.NUM_MASTERS == 0: return

        short_hostname = socket.gethostname()
        hostname = socket.gethostbyname_ex(short_hostname)[0]

        self._master_addresses = ["%s:%s" % (hostname, self._ports["master"][2 * i])
                                  for i in xrange(self.NUM_MASTERS)]
        self._master_configs = []

        logs = []
        config_paths = []

        if prepare_files:
            os.mkdir(os.path.join(self.path_to_run, 'master'))

        for i in xrange(self.NUM_MASTERS):
            config = configs.get_master_config()

            current = os.path.join(self.path_to_run, 'master', str(i))
            if prepare_files:
                os.mkdir(current)

            config['rpc_port'] = self._ports["master"][2 * i]
            config['monitoring_port'] = self._ports["master"][2 * i + 1]

            config['masters']['addresses'] = self._master_addresses
            config['timestamp_provider']['addresses'] = self._master_addresses
            config['changelogs']['path'] = \
                os.path.join(current, 'logs')
            config['snapshots']['path'] = \
                    os.path.join(current, 'snapshots')
            init_logging(config['logging'], current, 'master-' + str(i))

            self.modify_master_config(config)
            update(config, self.DELTA_MASTER_CONFIG)

            logs.append(config['logging']['writers']['file']['file_name'])

            self._master_configs.append(config)

            config_path = os.path.join(current, 'master_config.yson')
            if prepare_files:
                write_config(config, config_path)
            config_paths.append(config_path)


        self._run_ytserver('master', config_paths)

        def masters_ready():
            good_marker = "World initialization completed"
            bad_marker = "Active quorum lost"

            master_id = 0
            for logging_file in logs:
                if not os.path.exists(logging_file): continue

                for line in reversed(open(logging_file).readlines()):
                    if bad_marker in line: continue
                    if good_marker in line:
                        self.leader_log = logging_file
                        self.leader_id = master_id
                        return True
                master_id += 1
            return False

        self._wait_for(masters_ready, name = "masters")
        logging.info('(Leader is: %d)', self.leader_id)


    def _run_nodes(self, prepare_files=True):
        if self.NUM_NODES == 0: return

        self.node_configs = []

        config_paths = []

        if prepare_files:
            os.mkdir(os.path.join(self.path_to_run, 'node'))

        current_user = 10000;
        for i in xrange(self.NUM_NODES):
            config = configs.get_node_config()

            current = os.path.join(self.path_to_run, 'node', str(i))
            os.mkdir(current)

            config['rpc_port'] = self._ports["node"][2 * i]
            config['monitoring_port'] = self._ports["node"][2 * i + 1]

            config['masters']['addresses'] = self._master_addresses
            config['timestamp_provider']['addresses'] = self._master_addresses
            config['data_node']['cache_location']['path'] = \
                os.path.join(current, 'chunk_cache')
            config['data_node']['store_locations'].append(
                {'path': os.path.join(current, 'chunk_store'),
                 'low_watermark' : 0,
                 'high_watermark' : 0})
            config['exec_agent']['slot_manager']['start_uid'] = current_user
            config['exec_agent']['slot_manager']['path'] = \
                os.path.join(current, 'slots')
            config['tablet_node']['changelogs']['path'] = \
                os.path.join(current, 'changelogs')
            config['tablet_node']['snapshots']['path'] = \
                os.path.join(current, 'snapshots')

            current_user += config['exec_agent']['job_controller']['resource_limits']['slots'] + 1

            init_logging(config['logging'], current, 'node-%d' % i)
            init_logging(config['exec_agent']['job_proxy_logging'], current, 'job_proxy-%d' % i)

            self.modify_node_config(config)
            update(config, self.DELTA_NODE_CONFIG)

            self.node_configs.append(config)

            config_path = os.path.join(current, 'node_config.yson')
            write_config(config, config_path)
            config_paths.append(config_path)

        self._run_ytserver('node', config_paths)


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
            for log in self.scheduler_logs:
                ready = 0
                for line in reversed(open(log).readlines()):
                    if scheduler_good_marker.match(line):
                        ready += 1
                if ready != self.NUM_NODES:
                    schedulers_ready = False

            if len(nodes_status) != self.NUM_NODES: return False
            return all(nodes_status.values()) and schedulers_ready

        self._wait_for(all_nodes_ready, name = "nodes",
                       max_wait_time = max(self.NUM_NODES * 6.0, 20))


    def _run_schedulers(self):
        self.scheduler_log = None

        logs = []
        config_paths = []
        for i in xrange(self.NUM_SCHEDULERS):
            current = os.path.join(self.path_to_run, 'scheduler-' + str(i))
            os.mkdir(current)

            config = configs.get_scheduler_config()
            config['masters']['addresses'] = self._master_addresses
            config['timestamp_provider']['addresses'] = self._master_addresses
            init_logging(config['logging'], current, 'scheduler-' + str(i))

            config['rpc_port'] = self._ports["scheduler"][2 * i]
            config['monitoring_port'] = self._ports["scheduler"][2 * i + 1]

            config['scheduler']['snapshot_temp_path'] = os.path.join(current, 'snapshots')
            
            logs.append(config['logging']['writers']['file']['file_name'])

            self.modify_scheduler_config(config)
            update(config, self.DELTA_SCHEDULER_CONFIG)
            config_path = os.path.join(current, 'scheduler_config.yson')
            config_paths.append(config_path)
            write_config(config, config_path)

        self._run_ytserver('scheduler', config_paths)

        self.scheduler_logs = logs

        def scheduler_ready():
            good_marker = 'Master connected'

            for log in logs:
                if not os.path.exists(log): return False
                ok = False
                for line in reversed(open(log).readlines()):
                    if good_marker in line:
                        ok = True
                        break
                if not ok:
                    return False;
            return True

        self._wait_for(scheduler_ready, name = "scheduler")

    def _prepare_driver(self):
        path = os.path.join(self.path_to_run, "driver")
        os.mkdir(path)

        config = configs.get_driver_config()
        config['masters']['addresses'] = self._master_addresses
        config['timestamp_provider']['addresses'] = self._master_addresses
        init_logging(config['logging'], path, 'driver')

        config_path = os.path.join(path, 'driver_config.yson')
        write_config(config, config_path)
        os.environ['YT_CONFIG'] = config_path

    def _run_proxy(self):
        if not self.START_PROXY: return

        current = os.path.join(self.path_to_run, "proxy")
        os.mkdir(current)

        driver_config = configs.get_driver_config()
        driver_config['masters']['addresses'] = self._master_addresses
        driver_config['timestamp_provider']['addresses'] = self._master_addresses
        init_logging(driver_config['logging'], current, 'node')

        proxy_config = configs.get_proxy_config()
        proxy_config['proxy']['logging'] = driver_config['logging']
        del driver_config['logging']
        proxy_config['proxy']['driver'] = driver_config
        proxy_config['port'] = self._ports["proxy"][0]
        proxy_config['log_port'] = self._ports["proxy"][1]

        config_path = os.path.join(self.path_to_run, 'proxy_config.json')
        with open(config_path, "w") as f:
            f.write(json.dumps(proxy_config))

        log = os.path.join(current, "http_application.log")
        self._run(['run_proxy.sh', "-c", config_path, "-l", log], "proxy", timeout=3.0)

        def started():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect(('127.0.0.1', self._ports["proxy"][0]))
                s.shutdown(2)
                return True
            except:
                return False

        self._wait_for(started, name="proxy", max_wait_time=90)

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
