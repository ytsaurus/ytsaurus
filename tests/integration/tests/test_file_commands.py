import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestFileCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5

    def test_simple(self):
        content = "some_data"
        upload('//tmp/file', content)
        assert download('//tmp/file') == content

        chunk_id = get('//tmp/file/@chunk_id')
        assert ls('//sys/chunks') == [chunk_id]
        assert get('//tmp/file/@size') == 9

        # check that chunk was deleted
        remove('//tmp/file')
        assert ls('//sys/chunks') == []
    
    # TODO(panin): check codecs