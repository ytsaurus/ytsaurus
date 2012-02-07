
import sys
#TODO:get rid of it
sys.path.append('../yson')

import yson_parser
import yson

import copy
import os
import subprocess
import signal
import time

def read_config(filename):
    f = open(filename, 'rt')
    config = yson_parser.parse_string(f.read())
    f.close()
    return config

def write_config(config, filename):
    f = open(filename, 'wt')
    f.write(yson.dumps(config, indent = ' '))
    f.close()

class YTEnv:
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    SETUP_TIMEOUT = 7

    def __init__(self, path_to_tests, path_to_run):
        self.process_to_kill = []

        self.path_to_run = path_to_run

        self.master_config = read_config(os.path.join(path_to_tests, 'default_master_config.yson'))
        self.holder_config = read_config(os.path.join(path_to_tests, 'default_holder_config.yson'))
        self.driver_config = read_config(os.path.join(path_to_tests, 'default_driver_config.yson'))

        master_addresses = ["localhost:" + str(8001 + i) for i in xrange(self.NUM_MASTERS)]
        self.master_config['meta_state']['cell']['addresses'] = master_addresses
        self.holder_config['masters']['addresses'] = master_addresses
        self.driver_config['masters']['addresses'] = master_addresses

        self.config_paths = {}

    def setUp(self):
        print 'Setting up configuration with', self.NUM_MASTERS, 'masters and', self.NUM_HOLDERS, 'holders'
        self._clean_run_path()
        self._prepare_configs()
        self._run_services()
        time.sleep(self. SETUP_TIMEOUT)

    def _run_services(self):
        self._run_masters()
        self._run_holders()

    def _run_masters(self):
        for i in xrange(self.NUM_MASTERS):
            port = 8001 + i
            config_path = self.config_paths['master'][i]
            p = subprocess.Popen('ytserver --cell-master --config {config_path}  --port {port} --id {i}'.format(**vars()).split())
            self.process_to_kill.append(p)

    def _run_holders(self):
        for i in xrange(self.NUM_HOLDERS):
            port = 7001 + i
            config_path = self.config_paths['holder'][i]
            p = subprocess.Popen('ytserver --chunk-holder --config {config_path} --port {port}'.format(**vars()).split())
            self.process_to_kill.append(p)

    def _clean_run_path(self):
        os.system('rm -rf ' + self.path_to_run)
        os.makedirs(self.path_to_run)

    def _prepare_configs(self):
        self._prepare_masters_config()
        self._prepare_holders_config()
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

            master_config['meta_state']['log_path'] = log_path
            master_config['meta_state']['snapshot_path'] = snapshot_path
            master_config['logging']['writers']['file']['file_name'] = logging_file_name

            self.modify_master_config(master_config)

            config_path = os.path.join(current, 'master_config.yson')
            write_config(master_config, config_path)
            self.config_paths['master'].append(config_path)

    def _prepare_holders_config(self):
        self.config_paths['holder'] = []

        os.mkdir(os.path.join(self.path_to_run, 'holder'))
        for i in xrange(self.NUM_HOLDERS):
            holder_config = copy.deepcopy(self.holder_config)
            
            current = os.path.join(self.path_to_run, 'holder', str(i))
            os.mkdir(current)

            chunk_store = os.path.join(current, 'chunk_store')
            chunk_cache = os.path.join(current, 'chunk_cache')
            logging_file_name = os.path.join(current, 'holder-' + str(i) + '.log')

            holder_config['chunk_cache_location']['path'] = chunk_cache
            store_location = holder_config['chunk_store_locations']
            store_location = store_location[0:1]
            store_location[0]['path'] = chunk_store
            holder_config['chunk_store_locations'] = store_location
            holder_config['logging']['writers']['file']['file_name'] = logging_file_name

            config_path = os.path.join(current, 'holder_config.yson')
            write_config(holder_config, config_path)
            self.config_paths['holder'].append(config_path)

    def _prepare_driver_config(self):
        config_path = os.path.join(self.path_to_run, '.ytdriver.config.yson')
        write_config(self.driver_config, config_path)

    def tearDown(self):
        for p in self.process_to_kill:
            p.kill()

    # to be redefiened in succeccors
    def modify_master_config(self, config):
        pass

    # to be redefiened in succeccors
    def modify_holder_config(self, config):
        pass
