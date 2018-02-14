import hashlib
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestFiles(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

    def test_invalid_type(self):
        with pytest.raises(YtError): read_file("//tmp")
        with pytest.raises(YtError): write_file("//tmp", "")

    def test_simple(self):
        content = "some_data"
        create("file", "//tmp/file")
        write_file("//tmp/file", content)
        assert read_file("//tmp/file") == content

        chunk_ids = get("//tmp/file/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert get("//tmp/file/@uncompressed_data_size") == len(content)

        remove("//tmp/file")

        gc_collect()
        multicell_sleep()
        assert not exists("#%s" % chunk_id)

    def test_empty(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "")
        assert read_file("//tmp/file") == ""

        assert get("//tmp/file/@chunk_ids") == []
        assert get("//tmp/file/@uncompressed_data_size") == 0

    def test_read_interval(self):
        content = "".join(["data"] * 100)
        create("file", "//tmp/file")
        write_file("//tmp/file", content, file_writer={"block_size": 8})

        offset = 9
        length = 212
        assert read_file("//tmp/file", offset=offset) == content[offset:]
        assert read_file("//tmp/file", length=length) == content[:length]
        assert read_file("//tmp/file", offset=offset, length=length) == content[offset:offset + length]

        with pytest.raises(YtError):
            assert read_file("//tmp/file", length=-1)

        with pytest.raises(YtError):
            assert read_file("//tmp", length=0)

        with pytest.raises(YtError):
            assert read_file("//tmp", length=1)

        assert get("//tmp/file/@uncompressed_data_size") == len(content)

    def test_read_all_intervals(self):
        content = "".join(chr(c) for c in range(ord("a"), ord("a") + 8))
        create("file", "//tmp/file")
        write_file("//tmp/file", content, file_writer={"block_size": 3})

        for offset in range(len(content)):
            for length in range(0, len(content) - offset):
                assert read_file("//tmp/file", offset=offset, length=length) == content[offset:offset + length]

    def test_copy(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)

        chunk_ids = get("//tmp/f/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert read_file("//tmp/f") == content
        copy("//tmp/f", "//tmp/f2")
        assert read_file("//tmp/f2") == content

        assert get("//tmp/f2/@resource_usage") == get("//tmp/f/@resource_usage")
        assert get("//tmp/f2/@replication_factor") == get("//tmp/f/@replication_factor")

        remove("//tmp/f")
        assert read_file("//tmp/f2") == content

        remove("//tmp/f2")

        gc_collect()
        multicell_sleep()
        assert not exists("#%s" % chunk_id)

    def test_copy_tx(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)

        chunk_ids = get("//tmp/f/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        tx = start_transaction()
        assert read_file("//tmp/f", tx=tx) == content
        copy("//tmp/f", "//tmp/f2", tx=tx)
        assert read_file("//tmp/f2", tx=tx) == content
        commit_transaction(tx)

        assert read_file("//tmp/f2") == content

        remove("//tmp/f")
        assert read_file("//tmp/f2") == content

        remove("//tmp/f2")
        
        gc_collect()
        multicell_sleep()
        assert not exists("#%s" % chunk_id)

    def test_replication_factor_attr(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)

        get("//tmp/f/@replication_factor")

        with pytest.raises(YtError): remove("//tmp/f/@replication_factor")
        with pytest.raises(YtError): set("//tmp/f/@replication_factor", 0)
        with pytest.raises(YtError): set("//tmp/f/@replication_factor", {})

        tx = start_transaction()
        with pytest.raises(YtError): set("//tmp/f/@replication_factor", 2, tx=tx)

    def test_append(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)
        write_file("<append=true>//tmp/f", content)

        assert len(get("//tmp/f/@chunk_ids")) == 2
        assert get("//tmp/f/@uncompressed_data_size") == 18
        assert read_file("//tmp/f") == content + content

    def test_overwrite(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)
        write_file("//tmp/f", content)

        assert len(get("//tmp/f/@chunk_ids")) == 1
        assert get("//tmp/f/@uncompressed_data_size") == 9
        assert read_file("//tmp/f") == content

    def test_upload_inside_tx(self):
        create("file", "//tmp/f")

        tx = start_transaction()

        content = "some_data"
        write_file("//tmp/f", content, tx=tx)

        assert read_file("//tmp/f") == ""
        assert read_file("//tmp/f", tx=tx) == content

        commit_transaction(tx)

        assert read_file("//tmp/f") == content

    def test_concatenate(self):
        create("file", "//tmp/fa")
        write_file("//tmp/fa", "a")
        assert read_file("//tmp/fa") == "a"

        create("file", "//tmp/fb")
        write_file("//tmp/fb", "b")
        assert read_file("//tmp/fb") == "b"

        create("file", "//tmp/f")

        concatenate(["//tmp/fa", "//tmp/fb"], "//tmp/f")
        assert read_file("//tmp/f") == "ab"

        concatenate(["//tmp/fa", "//tmp/fb"], "<append=true>//tmp/f")
        assert read_file("//tmp/f") == "abab"

    def test_concatenate_incorrect_types(self):
        create("file", "//tmp/f1")
        create("file", "//tmp/f2")
        create("table", "//tmp/t")

        with pytest.raises(YtError):
            concatenate(["//tmp/f1", "//tmp/f2"], "//tmp/t")

        with pytest.raises(YtError):
            concatenate(["//tmp/f1", "//tmp/t"], "//tmp/t")

        with pytest.raises(YtError):
            concatenate(["//tmp", "//tmp/t"], "//tmp/t")

    def test_set_compression(self):
        create("file", "//tmp/f")

        write_file("<compression_codec=lz4>//tmp/f", "a")
        assert get("//tmp/f/@compression_codec") == "lz4"

        write_file("<compression_codec=none>//tmp/f", "a")
        assert get("//tmp/f/@compression_codec") == "none"

        with pytest.raises(YtError):
            write_file("<append=true;compression_codec=none>//tmp/f", "a")

    def test_compute_hash(self):
        create("file", "//tmp/fcache")
        create("file", "//tmp/fcache2")
        create("file", "//tmp/fcache3")
        create("file", "//tmp/fcache4")
        create("file", "//tmp/fcache5")

        write_file("<append=%true>//tmp/fcache", "abacaba", compute_md5=True)

        assert get("//tmp/fcache/@md5") == "129296d4fd2ade2b2dbc402d4564bf81" == hashlib.md5("abacaba").hexdigest()
        assert exists("//tmp/fcache/@md5")

        write_file("<append=%true>//tmp/fcache", "new", compute_md5=True)
        assert get("//tmp/fcache/@md5") == "12ef1dfdbbb50c2dfd2b4119bac9dee5" == hashlib.md5("abacabanew").hexdigest()

        write_file("//tmp/fcache2", "abacaba")
        assert not exists("//tmp/fcache2/@md5")

        write_file("//tmp/fcache3", "test", compute_md5=True)
        assert get("//tmp/fcache3/@md5") == "098f6bcd4621d373cade4e832627b4f6" == hashlib.md5("test").hexdigest()

        write_file("//tmp/fcache3", "test2", compute_md5=True)
        assert get("//tmp/fcache3/@md5") == hashlib.md5("test2").hexdigest()

        concatenate(["//tmp/fcache", "//tmp/fcache3"], "//tmp/fcache4")
        assert not exists("//tmp/fcache4/@md5")

        with pytest.raises(YtError):
            write_file("<append=%true>//tmp/fcache4", "data", compute_md5=True)

        with pytest.raises(YtError):
            set("//tmp/fcache/@md5", "test")

        write_file("//tmp/fcache5", "", compute_md5=True)
        assert get("//tmp/fcache5/@md5") == "d41d8cd98f00b204e9800998ecf8427e" == hashlib.md5("").hexdigest()


##################################################################

class TestFilesMulticell(TestFiles):
    NUM_SECONDARY_MASTER_CELLS = 2
