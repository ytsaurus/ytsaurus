import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestFiles(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

    def test_invalid_type(self):
        with pytest.raises(YtError): download('//tmp')
        with pytest.raises(YtError): upload('//tmp', '')

    def test_simple(self):
        content = "some_data"
        create('file', '//tmp/file')
        upload('//tmp/file', content)
        assert download('//tmp/file') == content

        chunk_ids = get('//tmp/file/@chunk_ids')
        assert get_chunks() == chunk_ids
        assert get('//tmp/file/@uncompressed_data_size') == len(content)

        # check that chunk was deleted
        remove('//tmp/file')
        assert get_chunks() == []

    def test_empty(self):
        content = ""
        create('file', '//tmp/file')
        upload('//tmp/file', content)
        assert download('//tmp/file') == content

        chunk_ids = get('//tmp/file/@chunk_ids')
        assert get_chunks() == chunk_ids
        assert get('//tmp/file/@uncompressed_data_size') == len(content)

        # check that chunk was deleted
        remove('//tmp/file')
        assert get_chunks() == []

    def test_read_interval(self):
        content = ''.join(["data"] * 100)
        create('file', '//tmp/file')
        upload('//tmp/file', content, file_writer={"block_size": 8})

        offset = 9
        length = 212
        assert download('//tmp/file', offset=offset) == content[offset:]
        assert download('//tmp/file', length=length) == content[:length]
        assert download('//tmp/file', offset=offset, length=length) == content[offset:offset + length]

        chunk_ids = get('//tmp/file/@chunk_ids')
        assert get_chunks() == chunk_ids
        assert get('//tmp/file/@uncompressed_data_size') == len(content)

        # check that chunk was deleted
        remove('//tmp/file')
        assert get_chunks() == []

    def test_copy(self):
        content = "some_data"
        create('file', '//tmp/f')
        upload('//tmp/f', content)

        assert download('//tmp/f') == content
        copy('//tmp/f', '//tmp/f2')
        assert download('//tmp/f2') == content

        assert get('//tmp/f2/@resource_usage') == get('//tmp/f/@resource_usage')
        assert get('//tmp/f2/@replication_factor') == get('//tmp/f/@replication_factor')

        remove('//tmp/f')
        assert download('//tmp/f2') == content

        remove('//tmp/f2')
        assert get_chunks() == []

    def test_copy_tx(self):
        content = "some_data"
        create('file', '//tmp/f')
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
        assert get_chunks() == []

    def test_replication_factor_attr(self):
        content = "some_data"
        create('file', '//tmp/f')
        upload('//tmp/f', content)

        get('//tmp/f/@replication_factor')

        with pytest.raises(YtError): remove('//tmp/f/@replication_factor')
        with pytest.raises(YtError): set('//tmp/f/@replication_factor', 0)
        with pytest.raises(YtError): set('//tmp/f/@replication_factor', {})

        tx = start_transaction()
        with pytest.raises(YtError): set('//tmp/f/@replication_factor', 2, tx=tx)

    def test_append(self):
        content = "some_data"
        create('file', '//tmp/f')
        upload('//tmp/f', content)
        upload('<append=true>//tmp/f', content)

        assert len(get('//tmp/f/@chunk_ids')) == 2
        assert get('//tmp/f/@uncompressed_data_size') == 18
        assert download('//tmp/f') == content + content

    def test_upload_inside_tx(self):
        create('file', '//tmp/f')

        tx = start_transaction()

        content = "some_data"
        upload('//tmp/f', content, tx=tx)

        assert download('//tmp/f') == ""
        assert download('//tmp/f', tx=tx) == content

        commit_transaction(tx)

        assert download('//tmp/f') == content
