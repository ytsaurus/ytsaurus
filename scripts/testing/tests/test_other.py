import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import yson_parser
import yson

import time
import os


#TODO(panin): tests of scheduler
class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5

    def test_on_masters(self):
        result = ls('//sys/masters')
        masters = yson_parser.parse_string(result)
        assert len(masters) == self.NUM_MASTERS

        q = '"'
        for master in masters:
            path_to_orchid = '//sys/masters/'  + q + master + q + '/orchid'
            path = path_to_orchid + '/value'

            assert get(path_to_orchid + '/@service_name') == '"master"'

            some_map = '{"a"=1;"b"=2}'

            set(path, some_map)
            assert get(path) == some_map
            assertItemsEqual(yson2py(ls(path)), ['a', 'b'])
            remove(path)
            with pytest.raises(YTError): get(path)


    def test_on_holders(self):
        result = ls('//sys/holders')
        holders = yson_parser.parse_string(result)
        assert len(holders) == self.NUM_HOLDERS

        q = '"'
        for holder in holders:
            path_to_orchid = '//sys/holders/'  + q + holder + q + '/orchid'
            path = path_to_orchid + '/value'

            assert get(path_to_orchid + '/@service_name') == '"node"'

            some_map = '{"a"=1;"b"=2}'

            set(path, some_map)
            assert get(path) == some_map
            assertItemsEqual(yson2py(ls(path)), ['a', 'b'])
            remove(path)
            with pytest.raises(YTError): get(path)

###################################################################################

class TestCanceledUpload(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 3

    DELTA_HOLDER_CONFIG = {'chunk_holder' : {'session_timeout': 100}}

    # should be called on empty holders
    #@pytest.mark.xfail(run = False, reason = 'Replace blocking read from empty stream with something else')
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
