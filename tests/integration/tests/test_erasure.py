import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestErasure(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16

    def _do_test_simple(self, option):
        create('table', '//tmp/table')

        assert read('//tmp/table') == []
        assert get('//tmp/table/@row_count') == 0
        assert get('//tmp/table/@chunk_count') == 0

        write_str('//tmp/table', '{b="hello"}', config_opt=option)
        assert read('//tmp/table') == [{"b":"hello"}]
        assert get('//tmp/table/@row_count') == 1
        assert get('//tmp/table/@chunk_count') == 1

        write_str(
            '<append=true>//tmp/table', '{b="2";a="1"};{x="10";y="20";a="30"}',
            config_opt=option)
        assert read('//tmp/table') == [{"b": "hello"}, {"a":"1", "b":"2"}, {"a":"30", "x":"10", "y":"20"}]
        assert get('//tmp/table/@row_count') == 3
        assert get('//tmp/table/@chunk_count') == 2

    def test_reed_solomon(self):
        self._do_test_simple('/table_writer/erasure_codec=reed_solomon')

    def test_lrc(self):
        self._do_test_simple('/table_writer/erasure_codec=lrc')

