
import sys
#TODO:get rid of it
sys.path.append('../yson')

import unittest

import yson_parser
import yson

import copy
import os
import subprocess
import signal
import re
import time
import socket

from collections import defaultdict

SANDBOX_ROOTDIR = os.path.abspath('tests.sandbox')
CONFIGS_ROOTDIR = os.path.abspath('default_configs')


def deepupdate(d, other):
    for key, value in other.iteritems():
        if key in d and isinstance(value, dict):
            deepupdate(d[key], value)
        else:
            d[key] = value
    return d

def read_config(filename):
    f = open(filename, 'rt')
    config = yson_parser.parse_string(f.read())
    f.close()
    return config

def write_config(config, filename):
    f = open(filename, 'wt')
    f.write(yson.dumps(config, indent = '    '))
    f.close()

def init_logging(node, path, name):
    logging_file_name = os.path.join(path, name + '.log')
    debugging_file_name = os.path.join(path, name + '.debug.log')

    node['writers']['file']['file_name'] = logging_file_name
    node['writers']['raw']['file_name'] = debugging_file_name


class YTEnv(unittest.TestCase):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 0

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
    def modify_holder_config(self, config):
        pass

    # to be redefined in successors
    def modify_scheduler_config(self, config):
        pass

    def my_setUp(self, path_to_run):
        # TODO(panin): add option for this
        os.system('killall MasterMain')
        os.system('killall NodeMain')
        os.system('killall SchedulerMain')
        
        print 'Setting up configuration with %s masters, %s holders, %s schedulers' % (
            self.NUM_MASTERS, self.NUM_HOLDERS, self.NUM_SCHEDULERS
            )
        try:
            self._set_path(path_to_run)
            self._clean_run_path()
            self._prepare_configs()
            self._run_masters()
            self._wait_for_ready_masters()
            self._run_holders()
            self._wait_for_ready_holders()
            self._run_schedulers()
            self._wait_for_ready_schedulers()
        except:
            self.tearDown()
            raise

    def my_tearDown(self):
        print 'Tearing down'
        for p, name in self.process_to_kill:
            p.poll()
            if p.returncode is not None:
                print '%s (pid %d) is already terminated with exit status %d' % (name, p.pid, p.returncode)
                continue
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
                assert False, 'Alarm! %s (pid %d) was not killed after 50 iterations' % (name, p. pid)

    def _set_path(self, path_to_run):
        path_to_run = os.path.abspath(path_to_run)
        print 'Initializing at', path_to_run
        self.process_to_kill = []

        self.path_to_run = path_to_run

        self.master_config = read_config(os.path.join(CONFIGS_ROOTDIR, 'default_master_config.yson'))
        self.holder_config = read_config(os.path.join(CONFIGS_ROOTDIR, 'default_holder_config.yson'))
        self.scheduler_config = read_config(os.path.join(CONFIGS_ROOTDIR, 'default_scheduler_config.yson'))
        self.driver_config = read_config(os.path.join(CONFIGS_ROOTDIR, 'default_driver_config.yson'))

        short_hostname = socket.gethostname()
        hostname = socket.gethostbyname_ex(short_hostname)[0]

        master_addresses = [hostname + ':' + str(8001 + i) for i in xrange(self.NUM_MASTERS)]
        
        self.master_config['meta_state']['cell']['addresses'] = master_addresses
        self.holder_config['masters']['addresses'] = master_addresses
        self.scheduler_config['masters']['addresses'] = master_addresses
        self.driver_config['masters']['addresses'] = master_addresses

        self.config_paths = {}
        self.configs = defaultdict(lambda : [])

        #TODO(panin): refactor
        self.master_logging_file = []

    def _run_masters(self):
        for i in xrange(self.NUM_MASTERS):
            p = subprocess.Popen([
                'ytserver', '--master',
                '--config', self.config_paths['master'][i],
                '--port', str(8001 + i)],
                shell=False, close_fds=True, preexec_fn=os.setsid)
            self.process_to_kill.append((p, "master-%d" % (i)))

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

    def _run_holders(self):
        for i in xrange(self.NUM_HOLDERS):
            p = subprocess.Popen([
                'ytserver', '--node',
                '--config', self.config_paths['holder'][i],
                '--port', str(7001 + i)],
                shell=False, close_fds=True, preexec_fn=os.setsid)
            self.process_to_kill.append((p, "holder-%d" % (i)))

    def _wait_for_ready_holders(self):
        if self.NUM_HOLDERS == 0: return
        self._wait_for(self._all_holders_ready, name = "holders", max_wait_time = 30)

    def _all_holders_ready(self):
        holders_status = {}

        good_marker = re.compile(r".*Holder online .* HolderId: (\d+).*")
        bad_marker = re.compile(r".*Holder unregistered .* HolderId: (\d+).*")

        def update_status(marker, line, status, value):
            match = marker.match(line)
            if match:
                holder_id = match.group(1)
                if holder_id not in status:
                    status[holder_id] = value

        for line in reversed(open(self.leader_log).readlines()):
            update_status(good_marker, line, holders_status, True)
            update_status(bad_marker, line, holders_status, False)
        
        if len(holders_status) != self.NUM_HOLDERS: return False
        return all(holders_status.values())

    def _run_schedulers(self):
        for i in xrange(self.NUM_SCHEDULERS):
            p = subprocess.Popen([
                'ytserver', '--scheduler',
                '--config', self.config_paths['scheduler'][i],
                '--port', str(8101 + i)],
                shell=False, close_fds=True, preexec_fn=os.setsid)
            self.process_to_kill.append((p, "scheduler-%d" % (i)))

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
        self._prepare_holders_config()
        self._prepare_schedulers_config()
        self._prepare_driver_config()

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
            deepupdate(master_config, self.DELTA_MASTER_CONFIG)

            config_path = os.path.join(current, 'master_config.yson')
            write_config(master_config, config_path)
            self.config_paths['master'].append(config_path)

            self.master_logging_file.append(logging_file_name)
           

    def _prepare_holders_config(self):
        self.config_paths['holder'] = []

        os.mkdir(os.path.join(self.path_to_run, 'holder'))
        for i in xrange(self.NUM_HOLDERS):
            holder_config = copy.deepcopy(self.holder_config)
            
            current = os.path.join(self.path_to_run, 'holder', str(i))
            os.mkdir(current)

            chunk_cache = os.path.join(current, 'chunk_cache')
            chunk_store = os.path.join(current, 'chunk_store')
            slot_location = os.path.join(current, 'slot')

            holder_config['chunk_holder']['cache_location']['path'] = chunk_cache
            holder_config['chunk_holder']['store_locations'].append( {'path': chunk_store})
            holder_config['exec_agent']['job_manager']['slot_location'] = slot_location

            init_logging(holder_config['logging'], current, 'holder-%d' % i)
            init_logging(holder_config['exec_agent']['job_proxy_logging'], current, 'job_proxy-%d' % i)

            self.modify_holder_config(holder_config)
            deepupdate(holder_config, self.DELTA_HOLDER_CONFIG)

            config_path = os.path.join(current, 'holder_config.yson')
            write_config(holder_config, config_path)

            self.config_paths['holder'].append(config_path)
            self.configs['holder'].append(holder_config)

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
            deepupdate(config, self.DELTA_SCHEDULER_CONFIG)

            config_path = os.path.join(current, 'scheduler_config.yson')
            write_config(config, config_path)
            self.config_paths['scheduler'].append(config_path)

    def _prepare_driver_config(self):
        config_path = os.path.join(self.path_to_run, 'driver_config.yson')
        write_config(self.driver_config, config_path)
        os.environ['YT_CONFIG'] = config_path

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
   
