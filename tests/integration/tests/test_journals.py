import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestJournals(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

    def test_create_success(self):
        create('journal', '//tmp/j')
        assert get('//tmp/j/@replication_factor') == 3
        assert get('//tmp/j/@read_quorum') == 2
        assert get('//tmp/j/@write_quorum') == 2
        assert get('//tmp/j/@resource_usage/disk_space') == 0        
        assert get('//tmp/j/@sealed') == 'true'
        assert get('//tmp/j/@row_count') == 0
        assert get('//tmp/j/@chunk_ids') == []

    def test_create_failure(self):
        with pytest.raises(YtError): create('journal', '//tmp/j', opt=['/attributes/replication_factor=1'])
        with pytest.raises(YtError): create('journal', '//tmp/j', opt=['/attributes/read_quorum=4'])
        with pytest.raises(YtError): create('journal', '//tmp/j', opt=['/attributes/write_quorum=4'])
        with pytest.raises(YtError): create('journal', '//tmp/j', opt=['/attributes/replication_factor=4'])
