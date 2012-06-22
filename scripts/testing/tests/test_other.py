import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import yson_parser
import yson

import time
import os

class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 1

    def _check_service(self, path_to_orchid, service_name):
        path_to_value = path_to_orchid + '/value'

        assert get_py(path_to_orchid + '/@service_name') == service_name

        some_map = '{"a"=1;"b"=2}'

        set(path_to_value, some_map)
        assert get(path_to_value) == some_map

        self.assertItemsEqual(yson2py(ls(path_to_value)), ['a', 'b'])
        remove(path_to_value)
        with pytest.raises(YTError): get(path_to_value)


    def _check_orchid(self, path, num_services, service_name):
        result = ls(path)
        services = yson2py(result)
        q = '"'
        
        assert len(services) == num_services
        for service in services:
            path_to_orchid = path + '/'  + q + service + q + '/orchid'
            self._check_service(path_to_orchid, service_name)

    def test_on_masters(self):
        self._check_orchid('//sys/masters', self.NUM_MASTERS, "master")

    def test_on_holders(self):
        self._check_orchid('//sys/holders', self.NUM_HOLDERS, "node")

    def test_on_scheduler(self):
        self._check_service('//sys/scheduler/orchid', "scheduler")
    

###################################################################################

class TestCanceledUpload(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 3

    DELTA_HOLDER_CONFIG = {'chunk_holder' : {'session_timeout': 100}}

    # should be called on empty holders
    def test(self):
        tx_id = start_transaction(opt = '/timeout=2000')

        # uploading from empty stream will fail
        process = run_command('upload', '//tmp/file', tx = tx_id)
        time.sleep(1)
        process.kill()
        time.sleep(1)

        # now check that there are no temp files
        for i in xrange(self.NUM_HOLDERS):
            # TODO(panin): refactor
            holder_config = self.Env.configs['holder'][i]
            chunk_store_path = holder_config['chunk_holder']['store_locations'][0]['path']
            self._check_no_temp_file(chunk_store_path)

    def _check_no_temp_file(self, chunk_store):
        for root, dirs, files in os.walk(chunk_store):
            for file in files:
                assert not file.endswith('~'), 'Found temporary file: ' + file  
