# -*- coding: utf-8 -*-

from .helpers import TEST_DIR, set_config_option, set_config_options

import yt.zip as zip
from yt.packages.six import PY3

import yt.wrapper as yt

import os
import pytest
import tempfile

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

@pytest.mark.usefixtures("yt_env")
class TestFileCommands(object):
    def test_file_commands(self):
        with pytest.raises(yt.YtError):
            yt.write_file(TEST_DIR + "/dir/file", b"")

        file_path = TEST_DIR + "/file"
        yt.write_file(file_path, b"")
        assert yt.read_file(file_path).read() == b""

        yt.write_file(file_path, b"0" * 1000)
        assert yt.read_file(file_path).read() == b"0" * 1000

        _, filename = tempfile.mkstemp()
        with open(filename, "w") as fout:
            fout.write("some content")

        destinationA = yt.smart_upload_file(filename, placement_strategy="hash")
        assert destinationA.startswith(yt.file_commands._get_remote_temp_files_directory())

        destinationB = yt.smart_upload_file(filename, placement_strategy="hash")
        assert destinationA == destinationB

        destination = yt.smart_upload_file(filename, placement_strategy="random")
        path = os.path.join(os.path.basename(filename), yt.file_commands._get_remote_temp_files_directory())
        assert destination.startswith(path)

        destination = TEST_DIR + "/file_dir/some_file"
        yt.smart_upload_file(filename, destination=destination, placement_strategy="ignore")
        assert yt.get_attribute(destination, "file_name") == "some_file"

        with pytest.raises(yt.YtError):
            yt.smart_upload_file(filename, destination=destination, placement_strategy="random")
        with pytest.raises(yt.YtError):
            yt.smart_upload_file(filename, destination=destination, placement_strategy="hash")

        assert yt.read_file(destination, length=4).read() == b"some"
        assert yt.read_file(destination, offset=5).read() == b"content"

        destination = yt.smart_upload_file(filename, placement_strategy="ignore")
        yt.smart_upload_file(filename, placement_strategy="ignore")
        assert yt.read_file(destination).read() == b"some content"

        with set_config_option("write_retries/chunk_size", 2):
            yt.write_file(file_path, b"abacaba")
            assert yt.read_file(file_path).read() == b"abacaba"

            yt.remove(file_path)
            yt.write_file(file_path, BytesIO(b"abacaba"))
            assert yt.read_file(file_path).read() == b"abacaba"

        with set_config_option("prefix", TEST_DIR + "/"):
            yt.smart_upload_file(filename, destination="subdir/abc", placement_strategy="replace")
            assert yt.read_file("subdir/abc").read() == b"some content"

    @pytest.mark.parametrize("use_tmp_dir_for_intermediate_data", [True, False])
    def test_parallel_write_file(self, use_tmp_dir_for_intermediate_data):
        override_options = {
            "write_parallel/concatenate_size": 7,
            "write_parallel/enable": True,
            "write_parallel/use_tmp_dir_for_intermediate_data": use_tmp_dir_for_intermediate_data,
            "write_retries/chunk_size": 20
        }
        with set_config_options(override_options):
            self.test_file_commands()

    def test_unicode(self):
        data = u"строка"
        path = TEST_DIR + "/filename"
        yt.create("file", path)
        if not PY3:
            yt.write_file(path, data)
            assert yt.read_file(path).read().decode("utf-8") == data
        else:
            with pytest.raises(yt.YtError):
                yt.write_file(path, data)

    def test_write_compressed_file_data(self):
        fd, filename = tempfile.mkstemp()
        os.close(fd)

        with zip.GzipFile(filename, "w", 5) as fout:
            fout.write(b"test write compressed file data")

        with set_config_option("proxy/content_encoding", "gzip"):
            with open(filename, "rb") as f:
                if yt.config["backend"] == "native":
                    with pytest.raises(yt.YtError):  # not supported for native backend
                        yt.write_file(TEST_DIR + "/file", f, is_stream_compressed=True)
                else:
                    yt.write_file(TEST_DIR + "/file", f, is_stream_compressed=True)
                    assert b"test write compressed file data" == yt.read_file(TEST_DIR + "/file").read()
