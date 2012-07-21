import configs

from tools.common import update
import yson

import unittest

import copy
import os
import re
import time
import signal
import socket
import subprocess
import sys
import simplejson as json

from collections import defaultdict

def init_logging(node, path, name):
    for key, suffix in [('file', '.log'), ('raw', '.debug.log')]:
        node['writers'][key]['file_name'] = os.path.join(path, name + suffix)

def write_config(config, filename):
    with open(filename, 'wt') as f:
        f.write(yson.dumps(config, indent = '    '))

class YTEnv(unittest.TestCase):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    START_SCHEDULER = False
    START_PROXY = False

    DELTA_MASTER_CONFIG = {}
    DELTA_HOLDER_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}

    # needed for compatibility with unittest.TestCase
    def runTest(self):
        pass

    # to be redefiened in successors
    def modify_master_config(self, config):
        pass

    # to be redefined in successors
    def modify_node_config(self, config):
        pass

    # to be redefined in successors
    def modify_scheduler_config(self, config):
        pass

    def set_environment(self, path_to_run, pids_filename, ports=None):
        # TODO(panin): add option for this
        # os.system('killall MasterMain')
        # os.system('killall NodeMain')
        # os.system('killall SchedulerMain')
        self._pids_filename = pids_filename
        self._kill_previously_run_services()
        self._ports = {
            "master": 8001,
            "node": 7001,
            "scheduler": 8101,
            "proxy": 8080}
        if ports is not None:
            self._ports.update(ports)

        self.path_to_run = os.path.abspath(path_to_run)
        print 'Setting up configuration with %s masters, %s nodes, %s schedulers at %s' % \
            (self.NUM_MASTERS, self.NUM_HOLDERS, self.START_SCHEDULER, self.path_to_run)
        self.process_to_kill = []

        if os.path.exists(self.path_to_run):
            shutil.rmtree(self.path_to_run)
        os.makedirs(self.path_to_run)

        try:
            self._run_masters()
            self._run_nodes()
            self._run_schedulers()
            self._prepare_driver()
            self._run_proxy()
        except:
            self.clear_environment()
            raise

    def clear_environment(self):
        print 'Tearing down'
        ok = True
        message = ""
        for p, name in self.process_to_kill:
            p.poll()
            if p.returncode is not None:
                ok = False
                message += '%s (pid %d) is already terminated with exit status %d\n' % (name, p.pid, p.returncode)
            else:
                os.killpg(p.pid, signal.SIGTERM)

            time.sleep(0.250)

            # now try to kill unkilled process
            for i in xrange(50):
                p.poll()
                if p.returncode is not None:
                    break
                print '%s (pid %d) was not killed by the kill command' % (name, p.pid)

                os.killpg(p.pid, signal.SIGKILL)
                time.sleep(0.100)

            if p.returncode is None:
                ok = False
                message += 'Alarm! %s (pid %d) was not killed after 50 iterations\n' % (name, p. pid)

        assert ok, message

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + '\n')
        self.pids_file.flush();

    def _run(self, args, name):
        p = subprocess.Popen(args, shell=False, close_fds=True, preexec_fn=os.setsid)
        self.process_to_kill.append((p, name))
        self._append_pid(p.pid)

    def _run_ytserver(self, service_name, configs, start_port):
        for i in xrange(len(configs)):
            self._run([
                'ytserver', "--" + service_name,
                '--config', configs[i],
                '--port', str(start_port + i)],
                "%s-%d" % (service_name, i))

    def _kill_previously_run_services(self):
        if os.path.exists(self._pids_filename):
            with open(self._pids_filename, 'rt') as f:
                for pid in map(int, f.xreadlines()):
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except OSError:
                        pass

        self.pids_file = open(self._pids_filename, 'wt')


    def _run_masters(self):
        if self.NUM_MASTERS == 0: return

        short_hostname = socket.gethostname()
        hostname = socket.gethostbyname_ex(short_hostname)[0]

        self._master_addresses = ["%s:%s" % (hostname, self._ports["master"] + i)
                                  for i in xrange(self.NUM_MASTERS)]

        logs = []
        config_paths = []

        os.mkdir(os.path.join(self.path_to_run, 'master'))
        for i in xrange(self.NUM_MASTERS):
            config = configs.get_master_config()

            current = os.path.join(self.path_to_run, 'master', str(i))
            os.mkdir(current)

            config['meta_state']['cell']['addresses'] = self._master_addresses
            config['meta_state']['changelogs']['path'] = \
                os.path.join(current, 'logs')
            config['meta_state']['snapshots']['path'] = \
                    os.path.join(current, 'snapshots')
            init_logging(config['logging'], current, 'master-' + str(i))

            self.modify_master_config(config)
            update(config, self.DELTA_MASTER_CONFIG)

            logs.append(config['logging']['writers']['file']['file_name'])

            config_path = os.path.join(current, 'master_config.yson')
            write_config(config, config_path)
            config_paths.append(config_path)


        self._run_ytserver('master', config_paths, self._ports["master"])

        def masters_ready():
            good_marker = "World initialization completed"
            bad_marker = "Active quorum lost"

            master_id = 0
            for logging_file in logs:
                if not os.path.exists(logging_file): continue

                for line in reversed(open(logging_file).readlines()):
                    if bad_marker in line: continue
                    if good_marker in line:
                        print 'Found leader: ', master_id
                        self.leader_log = logging_file
                        return True
                master_id += 1
            return False

        self._wait_for(masters_ready, name = "masters")

    def _run_nodes(self):
        if self.NUM_HOLDERS == 0: return

        self.node_configs = []

        config_paths = []

        os.mkdir(os.path.join(self.path_to_run, 'node'))
        for i in xrange(self.NUM_HOLDERS):
            config = configs.get_node_config()

            current = os.path.join(self.path_to_run, 'node', str(i))
            os.mkdir(current)

            config['masters']['addresses'] = self._master_addresses
            config['data_node']['cache_location']['path'] = \
                os.path.join(current, 'chunk_cache')
            config['data_node']['store_locations'].append(
                {'path': os.path.join(current, 'chunk_store')})
            config['exec_agent']['job_manager']['slot_location'] = \
                os.path.join(current, 'slot')

            init_logging(config['logging'], current, 'node-%d' % i)
            init_logging(config['exec_agent']['job_proxy_logging'], current, 'job_proxy-%d' % i)

            self.modify_node_config(config)
            update(config, self.DELTA_HOLDER_CONFIG)

            self.node_configs.append(config)

            config_path = os.path.join(current, 'node_config.yson')
            write_config(config, config_path)
            config_paths.append(config_path)

        self._run_ytserver('node', config_paths, self._ports["node"])


        def all_nodes_ready():
            nodes_status = {}

            good_marker = re.compile(r".*Holder online .* HolderId: (\d+).*")
            bad_marker = re.compile(r".*Holder unregistered .* HolderId: (\d+).*")

            def update_status(marker, line, status, value):
                match = marker.match(line)
                if match:
                    node_id = match.group(1)
                    if node_id not in status:
                        status[node_id] = value

            for line in reversed(open(self.leader_log).readlines()):
                update_status(good_marker, line, nodes_status, True)
                update_status(bad_marker, line, nodes_status, False)

            if len(nodes_status) != self.NUM_HOLDERS: return False
            return all(nodes_status.values())

        self._wait_for(all_nodes_ready, name = "nodes",
                       max_wait_time = self.NUM_HOLDERS * 6.0)


    def _run_schedulers(self):
        if not self.START_SCHEDULER: return

        current = os.path.join(self.path_to_run, 'scheduler')
        os.mkdir(current)

        config = configs.get_scheduler_config()
        config['masters']['addresses'] = self._master_addresses
        init_logging(config['logging'], current, 'scheduler')

        self.modify_scheduler_config(config)
        update(config, self.DELTA_SCHEDULER_CONFIG)
        config_path = os.path.join(current, 'scheduler_config.yson')
        write_config(config, config_path)

        self._run_ytserver('scheduler', [config_path], self._ports["scheduler"])

        def scheduler_ready():
            good_marker = 'Scheduler address published'

            log = config['logging']['writers']['file']['file_name']
            if not os.path.exists(log): return False
            for line in reversed(open(log).readlines()):
                if good_marker in line:
                    return True
            return False

        self._wait_for(scheduler_ready, name = "scheduler")

    def _prepare_driver(self):
        config = configs.get_driver_config()
        config['masters']['addresses'] = self._master_addresses
        config_path = os.path.join(self.path_to_run, 'driver_config.yson')
        write_config(config, config_path)
        os.environ['YT_CONFIG'] = config_path

    def _run_proxy(self):
        if not self.START_PROXY: return
        driver_config = configs.get_driver_config()
        driver_config['masters']['addresses'] = self._master_addresses

        proxy_config = configs.get_proxy_config()
        proxy_config['driver'] = driver_config
        proxy_config['port'] = self._ports["proxy"]

        config_path = os.path.join(self.path_to_run, 'proxy_config.json')
        with open(config_path, "w") as f:
            f.write(json.dumps(proxy_config))

        self._run(['run_proxy.sh', "-c", config_path], "proxy")

        def started():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect(('127.0.0.1', self._ports["proxy"]))
                s.shutdown(2)
                return True
            except:
                return False

        self._wait_for(started)

    def _wait_for(self, condition, max_wait_time=20, sleep_quantum=0.5, name=""):
        current_wait_time = 0
        print 'Waiting for {0}'.format(name),
        while current_wait_time < max_wait_time:
            sys.stdout.write('.')
            sys.stdout.flush()
            if condition():
                print ' %s ready' % name
                return
            time.sleep(sleep_quantum)
            current_wait_time += sleep_quantum
        print
        assert False, "%s still not ready after %s seconds" % (name, max_wait_time)

