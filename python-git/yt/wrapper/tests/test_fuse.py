from yt.wrapper.cypress_fuse import CachedYtClient, Cypress
from yt.wrapper.common import parse_bool
from yt.wrapper.http import get_proxy_url
import yt.wrapper as yt

from helpers import TEST_DIR

import pytest

import random

@pytest.mark.usefixtures("yt_env")
class TestCachedYtClient(object):

    def test_list(self):
        client = CachedYtClient(proxy = get_proxy_url(), config=yt.config.config)
        assert sorted(client.list("/")) == sorted(yt.list("/"))

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
        for attribute in ["access_time", "access_counter"]:
            for attributes in [real_attributes, cached_attributes]:
                del attributes[attribute]
        assert real_attributes == cached_attributes

        sample_names = random.sample(real_attributes.keys(), len(real_attributes) / 2)
        cached_attributes = client.get_attributes("//home", sample_names)
        assert sorted(cached_attributes.keys()) == sorted(sample_names)

        for name in sample_names:
            assert real_attributes[name] == cached_attributes[name]
