from tools.common import update
import configs

import unittest

import copy
import os
import re
import time
import signal
import socket
import subprocess
import sys
import getpass
import simplejson as json

from collections import defaultdict

def init_logging(node, path, name):
    for key, suffix in [('file', '.log'), ('raw', '.debug.log')]:
        node['writers'][key]['file_name'] = os.path.join(path, name + '.log')


class YTEnv(unittest.TestCase):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 0
    NUM_PROXY = 0

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

    def set_environment(self, path_to_run, pids_filename):
        # TODO(panin): add option for this
        # os.system('killall MasterMain')
        # os.system('killall NodeMain')
        # os.system('killall SchedulerMain')
        self._pids_filename = pids_filename
        self._kill_previously_run_services()

        print 'Setting up configuration with %s masters, %s nodes, %s schedulers' % \
            (self.NUM_MASTERS, self.NUM_HOLDERS, self.NUM_SCHEDULERS)
        try:
            self._set_path(path_to_run)
            self._clean_run_path()
            self._prepare_configs()
            self._run_masters()
            self._wait_for_ready_masters()
            self._run_nodes()
            self._wait_for_ready_nodes()
            self._run_schedulers()
            self._wait_for_ready_schedulers()
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
                message = '%s (pid %d) is already terminated with exit status %d' % (name, p.pid, p.returncode)
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
                message = 'Alarm! %s (pid %d) was not killed after 50 iterations' % (name, p. pid)

        assert ok, message

    def _set_path(self, path_to_run, configs_dir):
        path_to_run = os.path.abspath(path_to_run)
        print 'Initializing at', path_to_run
        self.process_to_kill = []

        self.path_to_run = path_to_run

        self.master_config = read_config(os.path.join(configs_dir, 'default_master_config.yson'))
        self.node_config = read_config(os.path.join(configs_dir, 'default_node_config.yson'))
        self.scheduler_config = read_config(os.path.join(configs_dir, 'default_scheduler_config.yson'))
        self.driver_config = read_config(os.path.join(configs_dir, 'default_driver_config.yson'))
        self.proxy_config = json.loads(open(os.path.join(configs_dir, 'default_proxy_config.json')).read())

        short_hostname = socket.gethostname()
        hostname = socket.gethostbyname_ex(short_hostname)[0]

        master_addresses = [hostname + ':' + str(8001 + i) for i in xrange(self.NUM_MASTERS)]

        self.master_config['meta_state']['cell']['addresses'] = master_addresses
        self.node_config['masters']['addresses'] = master_addresses
        self.scheduler_config['masters']['addresses'] = master_addresses
        self.driver_config['masters']['addresses'] = master_addresses

        self.config_paths = {}
        self.configs = defaultdict(lambda : [])

        #TODO(panin): refactor
        self.master_logging_file = []

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + '\n')
        self.pids_file.flush();

    def _run_ytserver(self, service_name, process_count, start_port):
        for i in xrange(process_count):
            p = subprocess.Popen([
                'ytserver', "--" + service_name,
                '--config', self.config_paths[service_name][i],
                '--port', str(start_port + i)],
                shell=False, close_fds=True, preexec_fn=os.setsid)
            self.process_to_kill.append((p, "%s-%d" % (service_name, i)))
            self._append_pid(p.pid)


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
        self._run_ytserver('master', self.NUM_MASTERS, 8001)

    # TODO(panin): think about refactoring this part
    def _wait_for_ready_masters(self):
        if self.NUM_MASTERS == 0: return
        self._wait_for(self._all_masters_ready, name = "masters")

    def _all_masters_ready(self):
        good_marker = "World initialization completed"
        bad_marker = "Active quorum lost"

        master_id = 0
        for logging_file in self.master_logging_file:
            if (not os.path.exists(logging_file)): continue

            for line in reversed(open(logging_file).readlines()):
                if bad_marker in line: continue
                if good_marker in line:
                    print 'Found leader: ', master_id
                    self.leader_log = logging_file
                    return True
            master_id += 1
        return False

    def _run_nodes(self):
        self._run_ytserver('node', self.NUM_HOLDERS, 7001)

    def _wait_for_ready_nodes(self):
        if self.NUM_HOLDERS == 0: return
        self._wait_for(self._all_nodes_ready, name = "nodes", max_wait_time = 30)

    def _all_nodes_ready(self):
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


    def _run_schedulers(self):
        self._run_ytserver('scheduler', self.NUM_SCHEDULERS, 8101)

    def _wait_for_ready_schedulers(self):
        if self.NUM_SCHEDULERS == 0: return
        self._wait_for(self._all_schedulers_ready, name = "schedulers", max_wait_time = 30)

    def _all_schedulers_ready(self):
        good_marker = 'Scheduler address published'

        if (not os.path.exists(self.scheduler_log)): return False
        for line in reversed(open(self.scheduler_log).readlines()):
            if good_marker in line:
                return True
        return False

    def _clean_run_path(self):
        os.system('rm -rf ' + self.path_to_run)
        os.makedirs(self.path_to_run)

    def _prepare_configs(self):
        self._prepare_masters_config()
        self._prepare_nodes_config()
        self._prepare_schedulers_config()
        self._prepare_driver_config()
        self._prepare_proxy_config()

    def _prepare_masters_config(self):
        self.config_paths['master'] = []

        os.mkdir(os.path.join(self.path_to_run, 'master'))
        for i in xrange(self.NUM_MASTERS):
            master_config = copy.deepcopy(self.master_config)

            current = os.path.join(self.path_to_run, 'master', str(i))
            os.mkdir(current)

            log_path = os.path.join(current, 'logs')
            snapshot_path = os.path.join(current, 'snapshots')

            logging_file_name = os.path.join(current, 'master-' + str(i) + '.log')
            debugging_file_name = os.path.join(current, 'master-' + str(i) + '.debug.log')

            master_config['meta_state']['changelogs']['path'] = log_path
            master_config['meta_state']['snapshots']['path'] = snapshot_path
            master_config['logging']['writers']['file']['file_name'] = logging_file_name
            master_config['logging']['writers']['raw']['file_name'] = debugging_file_name

            self.modify_master_config(master_config)
            update(master_config, self.DELTA_MASTER_CONFIG)

            config_path = os.path.join(current, 'master_config.yson')
            write_config(master_config, config_path)
            self.config_paths['master'].append(config_path)

            self.master_logging_file.append(logging_file_name)


    def _prepare_nodes_config(self):
        self.config_paths['node'] = []

        os.mkdir(os.path.join(self.path_to_run, 'node'))
        for i in xrange(self.NUM_HOLDERS):
            node_config = copy.deepcopy(self.node_config)

            current = os.path.join(self.path_to_run, 'node', str(i))
            os.mkdir(current)

            chunk_cache = os.path.join(current, 'chunk_cache')
            chunk_store = os.path.join(current, 'chunk_store')
            slot_location = os.path.join(current, 'slot')

            node_config['data_node']['cache_location']['path'] = chunk_cache
            node_config['data_node']['store_locations'].append( {'path': chunk_store})
            node_config['exec_agent']['job_manager']['slot_location'] = slot_location

            init_logging(node_config['logging'], current, 'node-%d' % i)
            init_logging(node_config['exec_agent']['job_proxy_logging'], current, 'job_proxy-%d' % i)

            self.modify_node_config(node_config)
            update(node_config, self.DELTA_HOLDER_CONFIG)

            config_path = os.path.join(current, 'node_config.yson')
            write_config(node_config, config_path)

            self.config_paths['node'].append(config_path)
            self.configs['node'].append(node_config)

    def _prepare_schedulers_config(self):
        self.config_paths['scheduler'] = []

        os.mkdir(os.path.join(self.path_to_run, 'scheduler'))
        for i in xrange(self.NUM_SCHEDULERS):
            config = copy.deepcopy(self.scheduler_config)

            current = os.path.join(self.path_to_run, 'scheduler', str(i))
            os.mkdir(current)

            logging_file_name = os.path.join(current, 'scheduler-%s.log' % i)
            debugging_file_name = os.path.join(current, 'scheduler-' + str(i) + '.debug.log')

            config['logging']['writers']['file']['file_name'] = logging_file_name
            config['logging']['writers']['raw']['file_name'] = debugging_file_name

            self.scheduler_log = logging_file_name

            self.modify_scheduler_config(config)
            update(config, self.DELTA_SCHEDULER_CONFIG)

            config_path = os.path.join(current, 'scheduler_config.yson')
            write_config(config, config_path)
            self.config_paths['scheduler'].append(config_path)

    def _prepare_driver_config(self):
        config_path = os.path.join(self.path_to_run, 'driver_config.yson')
        write_config(self.driver_config, config_path)
        os.environ['YT_CONFIG'] = config_path
    
    def _prepare_proxy_config(self):
        config_path = os.path.join(self.path_to_run, 'proxy_config.json')
        self.proxy_config["driver"] = self.driver_config
        #self.proxy_config["user"] = self.proxy_config["group"] = getpass.getuser()
        with open(config_path, "w") as f:
            f.write(json.dumps(self.proxy_config))


    def _run_proxy(self):
        p = subprocess.Popen(['run_proxy.sh', "-c", os.path.join(self.path_to_run, 'proxy_config.json')],
            shell=False, close_fds=True, preexec_fn=os.setsid)
        self.process_to_kill.append((p, "proxy"))
        self._append_pid(p.pid)

        def started():
            host = '127.0.0.1'
            port = 8080
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((host, port))
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

