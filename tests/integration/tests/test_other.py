import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time
import os

##################################################################

class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    START_SCHEDULER = True

    def _check_service(self, path_to_orchid, service_name):
        path_to_value = path_to_orchid + '/value'

        assert get(path_to_orchid + '/@service_name') == service_name

        some_map = {"a": 1, "b": 2}

        set(path_to_value, some_map)
        assert get(path_to_value) == some_map

        self.assertItemsEqual(ls(path_to_value), ['a', 'b'])
        remove(path_to_value)
        with pytest.raises(YTError): get(path_to_value)


    def _check_orchid(self, path, num_services, service_name):
        services = ls(path)
        q = '"'
        
        assert len(services) == num_services
        for service in services:
            path_to_orchid = path + '/'  + q + service + q + '/orchid'
            self._check_service(path_to_orchid, service_name)

    def test_on_masters(self):
        self._check_orchid('//sys/masters', self.NUM_MASTERS, "master")

    def test_on_nodes(self):
        self._check_orchid('//sys/nodes', self.NUM_NODES, "node")

    def test_on_scheduler(self):
        self._check_service('//sys/scheduler/orchid', "scheduler")
    

###################################################################################

# TODO(panin): unite with next
class TestResourceLeak(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {'data_node' : {'session_timeout': 100}}

    def _check_no_temp_file(self, chunk_store):
        for root, dirs, files in os.walk(chunk_store):
            for file in files:
                assert not file.endswith('~'), 'Found temporary file: ' + file  

    # should be called on empty nodes
    def test_canceled_upload(self):
        tx = start_transaction(opt = '/timeout=2000')

        # uploading from empty stream will fail
        process = run_command('upload', '//tmp/file', tx = tx)
        time.sleep(1)
        process.kill()
        time.sleep(1)

        # now check that there are no temp files
        for i in xrange(self.NUM_NODES):
            # TODO(panin): refactor
            node_config = self.Env.node_configs[i]
            chunk_store_path = node_config['data_node']['store_locations'][0]['path']
            self._check_no_temp_file(chunk_store_path)

# TODO(panin): check chunks
class TestResourceLeak2(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5

    def test_abort_snapshot_lock(self):
        upload('//tmp/file', 'some_data')

        tx = start_transaction()

        lock('//tmp/file', mode='snapshot', tx=tx)
        remove('//tmp/file')
        abort_transaction(tx)

    def test_commit_snapshot_lock(self):
        upload('//tmp/file', 'some_data')

        tx = start_transaction()

        lock('//tmp/file', mode='snapshot', tx=tx)
        remove('//tmp/file')
        commit_transaction(tx)
