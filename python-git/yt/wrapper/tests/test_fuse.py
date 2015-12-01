from yt.wrapper.cypress_fuse import CachedYtClient, Cypress
from yt.wrapper.common import parse_bool
from yt.wrapper.http import get_proxy_url
import yt.wrapper as yt

from helpers import TEST_DIR

from fuse import fuse_file_info

import pytest
import random
import json

@pytest.mark.usefixtures("yt_env")
class TestCachedYtClient(object):

    def test_list(self):
        client = CachedYtClient(proxy = get_proxy_url(), config=yt.config.config)
        assert sorted(client.list("/")) == sorted(yt.list("/"))

    def test_list_nonexistent(self):
        client = CachedYtClient(proxy = get_proxy_url(), config=yt.config.config)
        with pytest.raises(yt.YtError):
            client.list("//nonexistent")

    def test_get_attributes_empty(self):
        client = CachedYtClient(proxy = get_proxy_url(), config=yt.config.config)
        assert client.get_attributes("//sys", []) == {}

    def test_get_attributes_nonexistent(self):
        client = CachedYtClient(proxy = get_proxy_url(), config=yt.config.config)
        assert client.get_attributes("//sys", ["nonexistent"]) == {}
        # Get cached attribute again.
        assert client.get_attributes("//sys", ["nonexistent"]) == {}

    def test_get_attributes_list(self):
        client = CachedYtClient(proxy = get_proxy_url(), config=yt.config.config)
        real_attributes = yt.get("//home/@")
        cached_attributes = client.get_attributes("//home", real_attributes.keys())
        for attribute in ["access_time", "access_counter", "weak_ref_counter"]:
            for attributes in [real_attributes, cached_attributes]:
                del attributes[attribute]
        assert real_attributes == cached_attributes

        sample_names = random.sample(real_attributes.keys(), len(real_attributes) / 2)
        cached_attributes = client.get_attributes("//home", sample_names)
        assert sorted(cached_attributes.keys()) == sorted(sample_names)

        for name in sample_names:
            assert real_attributes[name] == cached_attributes[name]


@pytest.mark.usefixtures("yt_env")
class TestCypress(object):
    def test_readdir(self):
        cypress = Cypress(
            CachedYtClient(proxy = get_proxy_url(), config=yt.config.config))

        assert sorted(cypress.readdir("/", None)) == sorted(yt.list("/"))

    def test_read_file(self):
        cypress = Cypress(
            CachedYtClient(proxy = get_proxy_url(), config=yt.config.config))

        filepath = TEST_DIR + "/file"
        content = "Hello, world!" * 100
        yt.write_file(filepath, content)

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]
        cypress.open(fuse_filepath, fi)

        fuse_content = cypress.read(fuse_filepath, len(content), 0, fi)
        assert fuse_content == content

        offset = len(content) / 2
        fuse_content = cypress.read(fuse_filepath, len(content), offset, fi)
        assert fuse_content == content[offset:]

        length = len(content) / 2
        fuse_content = cypress.read(fuse_filepath, length, 0, fi)
        assert fuse_content == content[:length]

        cypress.release(fuse_filepath, fi)

    def test_read_table(self):
        cypress = Cypress(
            CachedYtClient(proxy = get_proxy_url(), config=yt.config.config))

        filepath = TEST_DIR + "/file"
        content = ""
        for i in range(100):
            data = {"a": i, "b": 2 * i, "c": 3 * i}
            content += json.dumps(data, separators=(',', ':'), sort_keys=True)
            content += "\n"
        yt.write_table(filepath, content, format=yt.JsonFormat())

        fi = fuse_file_info()
        fuse_filepath = filepath[1:]
        cypress.open(fuse_filepath, fi)

        fuse_content = cypress.read(fuse_filepath, len(content), 0, fi)
        assert fuse_content == content

        offset = len(content) / 2
        fuse_content = cypress.read(fuse_filepath, len(content), offset, fi)
        assert fuse_content == content[offset:]

        length = len(content) / 2
        fuse_content = cypress.read(fuse_filepath, length, 0, fi)
        assert fuse_content == content[:length]

        cypress.release(fuse_filepath, fi)
