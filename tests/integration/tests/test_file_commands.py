import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestFileCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

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
    
    def test_copy(self):
        content = "some_data"
        upload('//tmp/f', content)

        assert download('//tmp/f') == content
        copy('//tmp/f', '//tmp/f2')
        assert download('//tmp/f2') == content

        remove('//tmp/f')
        assert download('//tmp/f2') == content

        remove('//tmp/f2')
        assert ls('//sys/chunks') == []

	def test_copy_tx(self):
        content = "some_data"
        upload('//tmp/f', content)

        tx = start_transaction()
        assert download('//tmp/f', tx=tx) == content
        copy('//tmp/f', '//tmp/f2', tx=tx)
        assert download('//tmp/f2', tx=tx) == content
        commit_transaction(tx)

        assert download('//tmp/f2') == content

        remove('//tmp/f')
        assert download('//tmp/f2') == content

        remove('//tmp/f2')
        assert ls('//sys/chunks') == []
