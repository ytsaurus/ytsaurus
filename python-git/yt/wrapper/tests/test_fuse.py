from .helpers import TEST_DIR

from yt.wrapper.cypress_fuse import CachedYtClient, Cypress

from yt.packages.fuse import fuse_file_info, FuseOSError
from yt.packages.six import iterkeys, PY3
from yt.packages.six.moves import xrange

import yt.wrapper as yt

import pytest
import random
import json

@pytest.mark.usefixtures("yt_env")
class TestCachedYtClient(object):
    def test_list(self):
        client = CachedYtClient(config=yt.config.config)
        assert sorted(client.list("/")) == sorted(yt.list("/"))

    def test_list_nonexistent(self):
        client = CachedYtClient(config=yt.config.config)
        with pytest.raises(yt.YtError):
            client.list("//nonexistent")

    def test_get_attributes_empty(self):
        client = CachedYtClient(config=yt.config.config)
        assert client.get_attributes("//sys", []) == {}

    def test_get_attributes_nonexistent(self):
        client = CachedYtClient(config=yt.config.config)
        assert client.get_attributes("//sys", ["nonexistent"]) == {}
        # Get cached attribute again.
        assert client.get_attributes("//sys", ["nonexistent"]) == {}

    def test_get_attributes_list(self, yt_env):
        client = CachedYtClient(config=yt.config.config)

        real_attributes = yt.get("//home/@")
        # TODO(acid): Replace this with single get when YT-3522 is ready.
        for attribute in list(real_attributes):
            if isinstance(real_attributes[attribute], yt.yson.YsonEntity):
                real_attributes[attribute] = yt.get("//home/@" + attribute)

        cached_attributes = client.get_attributes("//home", list(real_attributes))
        ephemeral_attributes = ["access_time", "access_counter"]
        if yt_env.version >= "19.3":
            ephemeral_attributes += "ephemeral_ref_counter"
        else:
            ephemeral_attributes += "weak_ref_counter"

        for attribute in ephemeral_attributes:
            for attributes in [real_attributes, cached_attributes]:
                attributes.pop(attribute, None)
        assert real_attributes == cached_attributes

        sample_names = random.sample(list(real_attributes), len(real_attributes) // 2)
        cached_attributes = client.get_attributes("//home", sample_names)
        assert sorted(iterkeys(cached_attributes)) == sorted(sample_names)

        for name in sample_names:
            assert real_attributes[name] == cached_attributes[name]


@pytest.mark.usefixtures("yt_env")
class TestCypress(object):
    def test_readdir(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=False)

        assert sorted(cypress.readdir("/", None)) == sorted(yt.list("/"))

    def test_read_file(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=False)

        filepath = TEST_DIR + "/file"
        content = b"Hello, world!" * 100
        yt.write_file(filepath, content)

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]
        cypress.open(fuse_filepath, fi)

        fuse_content = cypress.read(fuse_filepath, len(content), 0, fi)
        assert fuse_content == content

        offset = len(content) // 2
        fuse_content = cypress.read(fuse_filepath, len(content), offset, fi)
        assert fuse_content == content[offset:]

        length = len(content) // 2
        fuse_content = cypress.read(fuse_filepath, length, 0, fi)
        assert fuse_content == content[:length]

        cypress.release(fuse_filepath, fi)

    def test_read_table(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=False)

        filepath = TEST_DIR + "/file"
        content = ""
        for i in xrange(100):
            data = {"a": i, "b": 2 * i, "c": 3 * i}
            content += json.dumps(data, separators=(",", ":"), sort_keys=True)
            content += "\n"
        if PY3:
            content = content.encode("utf-8")
        yt.write_table(filepath, content, format=yt.JsonFormat(), raw=True)

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]
        cypress.open(fuse_filepath, fi)

        fuse_content = cypress.read(fuse_filepath, len(content), 0, fi)
        assert fuse_content == content

        offset = len(content) // 2
        fuse_content = cypress.read(fuse_filepath, len(content), offset, fi)
        assert fuse_content == content[offset:]

        length = len(content) // 2
        fuse_content = cypress.read(fuse_filepath, length, 0, fi)
        assert fuse_content == content[:length]

        cypress.release(fuse_filepath, fi)

    def test_create_file(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=True)

        filepath = TEST_DIR + "/file"

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]
        cypress.create(fuse_filepath, 0o755, fi)
        cypress.release(fuse_filepath, fi)

        assert yt.read_file(filepath).read() == b""

    def test_unlink_file(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=True)

        filepath = TEST_DIR + "/file"

        yt.create("file", filepath)
        fuse_filepath = filepath[1:]
        fuse_test_dir = TEST_DIR[1:]
        cypress.unlink(fuse_filepath)

        assert "file" not in yt.list(TEST_DIR)
        assert "file" not in cypress.readdir(fuse_test_dir, None)

    def test_truncate_file(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=True)

        filepath = TEST_DIR + "/file"
        content = b"Hello, world!" * 100
        yt.write_file(filepath, content)

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]

        for truncated_length in [len(content), len(content) // 2, 0]:
            cypress.open(fuse_filepath, fi)
            cypress.truncate(fuse_filepath, truncated_length)
            cypress.flush(fuse_filepath, fi)
            cypress.release(fuse_filepath, fi)

            assert yt.read_file(filepath).read() == content[:truncated_length]

    def test_write_file(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=True)

        filepath = TEST_DIR + "/file"
        content = b"Hello, world!" * 100

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]

        cypress.create(fuse_filepath, 0o755, fi)
        cypress.write(fuse_filepath, content, 0, fi)
        cypress.flush(fuse_filepath, fi)
        cypress.release(fuse_filepath, fi)

        assert yt.read_file(filepath).read() == content

    def test_write_multipart_file(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=True)

        filepath = TEST_DIR + "/file"
        content = b"Hello, world!" * 100

        parts = []
        part_length = 17
        offset = 0
        while offset < len(content):
            length = min(part_length, len(content) - offset)
            parts.append((offset, length))
            offset += length
        random.shuffle(parts)

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]

        cypress.create(fuse_filepath, 0o755, fi)

        for offset, length in parts:
            cypress.write(fuse_filepath, content[offset:offset + length], offset, fi)

        cypress.flush(fuse_filepath, fi)
        cypress.release(fuse_filepath, fi)

        assert yt.read_file(filepath).read() == content

    def test_create_directory(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=True)

        dirpath = TEST_DIR + "/dir"
        fuse_dirpath = dirpath[1:]

        cypress.mkdir(fuse_dirpath, 0o755)
        assert "dir" in yt.list(TEST_DIR)

        cypress.rmdir(fuse_dirpath)
        assert "dir" not in yt.list(TEST_DIR)

        cypress.mkdir(fuse_dirpath, 0o755)
        assert "dir" in yt.list(TEST_DIR)

    def test_remove_directory(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=True)

        dirpath = TEST_DIR + "/dir"
        filepath = dirpath + "/file"
        yt.create("map_node", dirpath)
        yt.create("file", filepath)

        # Try to remove non-empty directory.
        fuse_dirpath = dirpath[1:]
        fuse_test_dir = TEST_DIR[1:]
        with pytest.raises(FuseOSError):
            cypress.rmdir(fuse_dirpath)
        assert "dir" in yt.list(TEST_DIR)
        assert "dir" in cypress.readdir(fuse_test_dir, None)

        # Remove empty directory.
        yt.remove(filepath)
        cypress.rmdir(fuse_dirpath)
        assert "dir" not in yt.list(TEST_DIR)
        assert "dir" not in cypress.readdir(fuse_test_dir, None)

    def test_write_access_guards(self):
        cypress = Cypress(
            CachedYtClient(config=yt.config.config),
            enable_write_access=False)

        filepath = TEST_DIR + "/file"
        dirpath = TEST_DIR + "/dir"
        fuse_filepath = filepath[1:]
        fuse_dirpath = dirpath[1:]

        fi = fuse_file_info()
        with pytest.raises(FuseOSError):
            cypress.create(fuse_filepath, 0o755, fi)
        assert "file" not in yt.list(TEST_DIR)

        with pytest.raises(FuseOSError):
            cypress.mkdir(fuse_dirpath, 0o755)
        assert "dir" not in yt.list(TEST_DIR)

        yt.create("file", filepath)
        with pytest.raises(FuseOSError):
            cypress.unlink(fuse_filepath)
        assert "file" in yt.list(TEST_DIR)

        yt.create("map_node", dirpath)
        with pytest.raises(FuseOSError):
            cypress.rmdir(fuse_dirpath)
        assert "dir" in yt.list(TEST_DIR)
