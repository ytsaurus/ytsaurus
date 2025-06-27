# -*- coding: utf-8 -*-

from .conftest import authors
from .helpers import TEST_DIR, set_config_option, set_config_options, failing_heavy_request

from yt.wrapper.common import MB
from yt.wrapper.driver import make_request
from yt.wrapper import heavy_commands, parallel_writer

import yt.wrapper as yt

import os
import gzip
import pytest
import tempfile

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestFileCommands(object):
    @authors("asaitgalin")
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
        self._test_chunk_attributes()

    def _test_chunk_attributes(self):
        attributes = {"compression_codec": "brotli_4"}
        file_with_attributes = TEST_DIR + "/file_dir/file_with_attributes"
        yt.create("file", file_with_attributes, attributes=attributes)
        yt.write_file(file_with_attributes, b"some content")
        assert yt.read_file(file_with_attributes).read() == b"some content"
        for chunk_id in yt.get(file_with_attributes + "/@chunk_ids"):
            actual_attributes = yt.get("#{}/@".format(chunk_id), attributes=list(attributes.keys()))
            assert attributes == actual_attributes

    @authors("ostyakov", "ignat")
    def test_parallel_read_file(self, yt_env_with_rpc):
        override_options = {
            "read_parallel/enable": True,
            "read_parallel/data_size_per_thread": 6
        }
        with set_config_options(override_options):
            self.test_file_commands()

    @authors("ostyakov")
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

    @authors("asaitgalin", "ignat")
    def test_unicode(self):
        data = u"строка"
        path = TEST_DIR + "/filename"
        yt.create("file", path)
        with pytest.raises(yt.YtError):
            yt.write_file(path, data)

    @authors("asaitgalin")
    def test_write_compressed_file_data(self):
        fd, filename = tempfile.mkstemp()
        os.close(fd)

        with gzip.GzipFile(filename, "w", 5) as fout:
            fout.write(b"test write compressed file data")

        with set_config_option("proxy/content_encoding", "gzip"):
            with open(filename, "rb") as f:
                if yt.config["backend"] in ("native", "rpc"):
                    with pytest.raises(yt.YtError):  # supported only for http backend
                        yt.write_file(TEST_DIR + "/file", f, is_stream_compressed=True)
                else:
                    yt.write_file(TEST_DIR + "/file", f, is_stream_compressed=True)
                    assert b"test write compressed file data" == yt.read_file(TEST_DIR + "/file").read()

    @authors("ignat")
    def test_etag(self):
        if yt.config["backend"] in ("native", "rpc"):
            pytest.skip()

        file_path = TEST_DIR + "/file"
        yt.write_file(file_path, b"")
        assert yt.read_file(file_path).read() == b""

        id = yt.get(file_path + "/@id")
        revision = yt.get(file_path + "/@revision")
        response_stream = make_request("read_file", {"path": file_path}, return_content=False)
        response = response_stream._get_response()

        expected_etag = "{}:{}".format(id, revision)

        assert response.headers["ETag"] == expected_etag

        import yt.packages.requests as requests
        response = requests.get("http://{0}/api/v3/read_file".format(yt.config["proxy"]["url"]),
                                params={"path": file_path}, headers={"If-None-Match": expected_etag})
        assert response.status_code == 304

    @authors("se4min")
    def test_write_file_retries(self):
        chunks = [b"abc", b"def", b"xyz"]
        chunks_generator = (chunk for chunk in chunks)

        file_path = TEST_DIR + "/file"
        with failing_heavy_request(heavy_commands, n_fails=2, assert_exhausted=True):
            yt.write_file(file_path, chunks_generator)

        assert b"".join(chunks) == tuple(yt.read_file(file_path))[0]

    @authors("se4min")
    @pytest.mark.parametrize("progress_bar", (False, True))
    def test_read_file_retries(self, progress_bar):
        data = b"abc" * 50
        file_path = TEST_DIR + "/file"
        yt.write_file(file_path, data)

        with set_config_option("read_retries/enable", True):
            with set_config_option("read_progress_bar/enable", progress_bar):
                with failing_heavy_request(heavy_commands, n_fails=2, assert_exhausted=True):
                    assert tuple(yt.read_file(file_path))[0] == data

    @pytest.mark.parametrize("parallel,progress_bar", [(False, False), (True, False), (False, True),
                                                       (True, True)])
    @authors("se4min")
    def test_write_big_file_retries(self, parallel, progress_bar):
        with set_config_option("write_parallel/enable", parallel):
            with set_config_option("write_progress_bar/enable", progress_bar):
                with set_config_option("write_retries/chunk_size", 3 * MB):
                    string_length = 4 * MB
                    chunks = [
                        b"1" * string_length,
                        b"2" * string_length,
                        b"3" * string_length,
                        b"4" * string_length,
                        b"5" * string_length,
                    ]
                    chunks_generator = (chunk for chunk in chunks)

                    if parallel:
                        module = parallel_writer
                    else:
                        module = heavy_commands

                    file_path = TEST_DIR + "/file"
                    with failing_heavy_request(module, n_fails=2, assert_exhausted=True):
                        yt.write_file(file_path, chunks_generator)

                    assert b"".join(chunks) == tuple(yt.read_file(file_path))[0]

    @authors("ignat")
    def test_lz4_content_encoding(self):
        with set_config_option("proxy/content_encoding", "z-lz4"):
            string_length = 4 * MB
            chunks = [
                b"1" * string_length,
                b"2" * string_length,
                b"3" * string_length,
                b"4" * string_length,
                b"5" * string_length,
            ]
            chunks_generator = (chunk for chunk in chunks)

            file_path = TEST_DIR + "/file"

            yt.write_file(file_path, chunks_generator)

            assert b"".join(chunks) == tuple(yt.read_file(file_path))[0]

    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_storage_attributes_preserved_on_multi_chunk(self, write_parallel):
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 256):
                yt.write_file(yt.YPath("<compression_codec=zlib_3>//tmp/file"), b"1" * 10 * MB)

        assert yt.get("//tmp/file/@compression_codec") == "zlib_3"
        assert yt.get("//tmp/file/@compression_statistics").keys() == {"zlib_3"}

    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_storage_attributes_from_parent_preserved_on_multi_chunk(self, write_parallel):
        yt.create("map_node", "//tmp/zlib3_compressed", attributes={"compression_codec": "zlib_3"}, recursive=True)
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 256):
                yt.write_file(yt.YPath("//tmp/zlib3_compressed/file"), b"1" * 10 * MB)

        assert yt.get("//tmp/zlib3_compressed/file/@compression_codec") == "zlib_3"
        assert yt.get("//tmp/zlib3_compressed/file/@compression_statistics").keys() == {"zlib_3"}

    @authors("denvr")
    def test_temp_user_dir(self):
        yt.file_commands.TEMP_DIR_CREATED_PATH = dict()
        assert "//tmp/yt_wrapper/file_storage" == yt.file_commands._get_remote_temp_files_directory()


@pytest.fixture(scope="function")
def custom_medium(yt_env_additional_media):
    medium = 'custom_medium'
    if not yt.exists(f"//sys/media/{medium}"):
        yt.create("domestic_medium", attributes={"name": medium})
        yt.set(f"//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{medium}", 10 * 1024**3)

    return medium


class TestCustomMediumWrite:
    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_storage_attributes_from_parent_preserved_on_multi_chunk(self, write_parallel, custom_medium):

        yt.create("map_node", "//tmp/custom_medium_dir", attributes={"primary_medium": custom_medium}, recursive=True)
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 10):
                yt.write_file(yt.YPath("//tmp/custom_medium_dir/file"), b"1" * (MB // 2))

        assert yt.get("//tmp/custom_medium_dir/file/@primary_medium") == custom_medium
        assert yt.get("//tmp/custom_medium_dir/file/@media").keys() == {custom_medium}

    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_storage_attributes_from_file_preserved_on_multi_chunk(self, write_parallel, custom_medium):
        yt.create("file", "//tmp/custom_medium_file", attributes={"primary_medium": custom_medium}, recursive=True)
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 10):
                yt.write_file(yt.YPath("//tmp/custom_medium_file"), b"1" * (MB // 2))

        assert yt.get("//tmp/custom_medium_file/@primary_medium") == custom_medium
        assert yt.get("//tmp/custom_medium_file/@media").keys() == {custom_medium}

    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_storage_attributes_from_path_override_directory(self, write_parallel, custom_medium):
        yt.create(
            "map_node",
            "//tmp/custom_storage_attributes_dir",
            attributes={"primary_medium": custom_medium, "compression_codec": "zlib_3", "erasure_codec": "isa_lrc_12_2_2"},
            recursive=True
        )
        path_attributes = {"primary_medium": "default", "compression_codec": "lz4", "erasure_codec": "none"}
        path = yt.YPath("//tmp/custom_storage_attributes_dir/file", attributes=path_attributes)
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 10):
                yt.write_file(path, b"1" * (MB // 2))

        attribtues = yt.get(path + "/@", attributes=["primary_medium", "compression_codec", "erasure_codec"])
        # TODO(YT-23841): Use primary medium from write query
        assert attribtues == path_attributes | {"primary_medium": custom_medium}
