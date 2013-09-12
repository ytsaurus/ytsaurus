import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time
import os
import sys

##################################################################

class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    START_SCHEDULER = True

    def _check_service(self, path_to_orchid, service_name):
        path_to_value = path_to_orchid + '/value'

        assert get(path_to_orchid + '/service/name') == service_name

        some_map = {"a": 1, "b": 2}

        set(path_to_value, some_map)
        assert get(path_to_value) == some_map

        self.assertItemsEqual(ls(path_to_value), ['a', 'b'])
        remove(path_to_value)
        with pytest.raises(YTError): get(path_to_value)


    def _check_orchid(self, path, num_services, service_name):
        services = ls(path)
        assert len(services) == num_services
        for service in services:
            path_to_orchid = path + '/' + service + '/orchid'
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
                assert not file.endswith('~') or file == 'health_check~', 'Found temporary file: ' + file

    # should be called on empty nodes
    def test_canceled_upload(self):
        tx = start_transaction(opt = '/timeout=2000')

        # uploading from empty stream will fail
        create('file', '//tmp/file')
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
        create('file', '//tmp/file')
        upload('//tmp/file', 'some_data')

        tx = start_transaction()

        lock('//tmp/file', mode='snapshot', tx=tx)
        remove('//tmp/file')
        abort_transaction(tx)

    def test_commit_snapshot_lock(self):
        create('file', '//tmp/file')
        upload('//tmp/file', 'some_data')

        tx = start_transaction()

        lock('//tmp/file', mode='snapshot', tx=tx)
        remove('//tmp/file')
        commit_transaction(tx)

###################################################################################

class TestVirtualMaps(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    def test_chunks(self):
        gc_collect()
        assert get('//sys/chunks/@count') == 0
        assert get('//sys/underreplicated_chunks/@count') == 0
        assert get('//sys/overreplicated_chunks/@count') == 0

###################################################################################

class TestAttributes(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    START_SCHEDULER = True

    def test1(self):
        table = '//tmp/t'
        create('table', table)
        set_str('//tmp/t/@compression_codec', 'snappy')
        write_str(table, '{foo=bar}')

        for i in xrange(8):
            merge(in_=[table, table], out="<append=true>" + table)

        chunk_count = 3**8
        assert len(get('//tmp/t/@chunk_ids')) == chunk_count
        codec_info = get('//tmp/t/@compression_statistics')
        assert codec_info['snappy']['chunk_count'] == chunk_count

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test2(self):
        tableA = '//tmp/a'
        create('table', tableA)
        write_str(tableA, '{foo=bar}')

        tableB = '//tmp/b'
        create('table', tableB)
        set_str(tableB + '/@compression_codec', 'snappy')

        map(in_=[tableA], out=[tableB], command="cat")

        codec_info = get(tableB + '/@compression_statistics')
        assert codec_info.keys() == ['snappy']

    def test3(self): #regression
        ls_str('//sys/nodes', attr=['statistics'])

###################################################################################

class TestChunkServer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20
    START_SCHEDULER = False

    def test_owning_nodes1(self):
        create('table', '//tmp/t')
        write('//tmp/t', {'a' : 'b'})
        chunk_ids = get('//tmp/t/@chunk_ids')
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get('#' + chunk_id + '/@owning_nodes') == ['//tmp/t']

    def test_owning_nodes2(self):
        create('table', '//tmp/t')
        tx = start_transaction()
        write('//tmp/t', {'a' : 'b'}, tx=tx)
        chunk_ids = get('//tmp/t/@chunk_ids', tx=tx)
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get('#' + chunk_id + '/@owning_nodes') == ['//tmp/t']

    def _test_decommission(self, erasure_codec, replica_count):
        create('table', '//tmp/t')
        set('//tmp/t/@erasure_codec', erasure_codec)
        write('//tmp/t', {'a' : 'b'})

        time.sleep(1) # wait for background replication

        chunk_ids = get('//tmp/t/@chunk_ids')
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        nodes = get('#%s/@stored_replicas' % chunk_id)
        assert len(nodes) == replica_count

        node_to_decommission = nodes[0]
        assert get('//sys/nodes/%s/@stored_replica_count' % node_to_decommission) == 1

        set('//sys/nodes/%s/@decommissioned' % node_to_decommission, 'true')

        time.sleep(3) # wait for background replication

        assert get('//sys/nodes/%s/@stored_replica_count' % node_to_decommission) == 0
        assert len(get('#%s/@stored_replicas' % chunk_id)) == replica_count

    def test_decommission_regular(self):
        self._test_decommission('none', 3)

    def test_decommission_erasure(self):
        self._test_decommission('lrc_12_2_2', 16)

###################################################################################

class TestNodeTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    START_SCHEDULER = False

    def test_ban(self):
        nodes = ls('//sys/nodes')
        assert len(nodes) == 3

        test_node = nodes[0]
        assert get('//sys/nodes/%s/@state' % test_node) == 'online'

        set('//sys/nodes/%s/@banned' % test_node, 'true')
        time.sleep(1)      
        assert get('//sys/nodes/%s/@state' % test_node) == 'offline'

        set('//sys/nodes/%s/@banned' % test_node, 'false')
        time.sleep(1)
        assert get('//sys/nodes/%s/@state' % test_node) == 'online'
