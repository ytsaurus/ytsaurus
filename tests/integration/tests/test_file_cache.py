from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt.common import date_string_to_datetime

import hashlib
import pytest

##################################################################

class TestFileCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5

    @authors("ignat")
    def test_get_file_from_cache(self):
        cache_path = "//tmp/file_cache"
        create("file", "//tmp/file")
        write_file("//tmp/file", "abacaba", compute_md5=True)
        md5 = hashlib.md5("abacaba").hexdigest()
        move("//tmp/file", "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5), recursive=True)

        assert get_file_from_cache(md5, cache_path) == "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5)

        create("file", "//tmp/file")
        write_file("//tmp/file", "aba", compute_md5=True)
        move("//tmp/file", "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5), recursive=True, force=True)

        assert get_file_from_cache(md5, cache_path) == ""

        remove("//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        create("file", "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        assert get_file_from_cache(md5, cache_path) == ""

        remove("//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        assert get_file_from_cache(md5, cache_path) == ""

        assert get_file_from_cache("invalid_md5", cache_path) == ""
        assert get_file_from_cache("", cache_path) == ""


    @authors("ignat")
    def test_put_file_to_cache(self):
        create("map_node", "//tmp/cache")
        create("file", "//tmp/file")
        write_file("//tmp/file", "abacaba")

        with pytest.raises(YtError):
            put_file_to_cache("//tmp/file", hashlib.md5("abacaba").hexdigest(), cache_path="//tmp/cache")

        write_file("//tmp/file", "abacaba", compute_md5=True)

        with pytest.raises(YtError):
            put_file_to_cache("//tmp/file", "invalid_hash", cache_path="//tmp/cache")

        with pytest.raises(YtError):
            put_file_to_cache("//tmp/file", hashlib.md5("abacaba").hexdigest())

        path = put_file_to_cache("//tmp/file", hashlib.md5("abacaba").hexdigest(), cache_path="//tmp/cache")
        assert read_file(path) == "abacaba"
        assert get(path + "/@touched")
        assert exists("//tmp/file")

        write_file("//tmp/file", "abacaba", compute_md5=True)
        set(path + "/@touched", False)

        path2 = put_file_to_cache("//tmp/file", hashlib.md5("abacaba").hexdigest(), cache_path="//tmp/cache")
        assert path2 == path
        assert get(path + "/@touched")

        write_file("//tmp/file", "aba", compute_md5=True)

        create("file", "//tmp/file2")
        write_file("//tmp/file2", "abacaba", compute_md5=True)

        path3 = put_file_to_cache("//tmp/file2", hashlib.md5("abacaba").hexdigest(), cache_path="//tmp/cache")
        assert read_file(path3) == "abacaba"
        assert path3 == path2

        write_file("//tmp/file", "abacaba", compute_md5=True)
        path4 = put_file_to_cache("//tmp/file", hashlib.md5("abacaba").hexdigest(), cache_path="//tmp/cache")

        assert read_file(path4) == "abacaba"
        assert path4 == path3

        write_file("//tmp/file", "aba", compute_md5=True)

        create_user("u")
        create("map_node", "//tmp/cache2", attributes={
            "inherit_acl": False,
            "acl" : [make_ace("deny", "u", "use")]})

        with pytest.raises(YtError):
            put_file_to_cache("//tmp/file", hashlib.md5("aba").hexdigest(), cache_path="//tmp/cache2", authenticated_user="u")

        set("//tmp/cache2/@acl/end", make_ace("allow", "u", "write"))
        put_file_to_cache("//tmp/file", hashlib.md5("aba").hexdigest(), cache_path="//tmp/cache2", authenticated_user="u")


    @authors("levysotsky")
    def test_put_file_to_cache_no_overwriting(self):
        content = "abacaba"
        content_md5 = hashlib.md5(content).hexdigest()

        create("map_node", "//tmp/cache")
        create("file", "//tmp/file1")
        write_file("//tmp/file1", content, compute_md5=True)
        create("file", "//tmp/file2")
        write_file("//tmp/file2", content, compute_md5=True)

        path1 = put_file_to_cache("//tmp/file1", content_md5, cache_path="//tmp/cache")
        id1 = get(path1 + "/@id")
        path2 = put_file_to_cache("//tmp/file2", content_md5, cache_path="//tmp/cache")
        id2 = get(path2 + "/@id")
        assert id1 == id2


    @authors("ignat", "levysotsky")
    def test_file_cache(self):
        content = "abacaba"
        content_md5 = hashlib.md5("abacaba").hexdigest()

        create("map_node", "//tmp/cache")
        create("file", "//tmp/file")
        write_file("//tmp/file", content, compute_md5=True)
        path = put_file_to_cache("//tmp/file", content_md5, cache_path="//tmp/cache")
        path2 = get_file_from_cache(content_md5, "//tmp/cache")
        assert path == path2
        assert read_file(path) == content

        modification_time = date_string_to_datetime(get(path + "/@modification_time"))
        path3 = get_file_from_cache(content_md5, "//tmp/cache")
        assert path2 == path3
        assert read_file(path) == content
        assert date_string_to_datetime(get(path + "/@modification_time")) > modification_time

    @authors("ignat")
    def test_under_transaction(self):
        tx = start_transaction()

        content = "abacaba"
        content_md5 = hashlib.md5("abacaba").hexdigest()

        create("map_node", "//tmp/cache", tx=tx)
        create("file", "//tmp/file", tx=tx)
        write_file("//tmp/file", content, compute_md5=True, tx=tx)
        path = put_file_to_cache("//tmp/file", content_md5, cache_path="//tmp/cache", tx=tx)
        path2 = get_file_from_cache(content_md5, "//tmp/cache", tx=tx)
        assert path == path2
        assert read_file(path, tx=tx) == content

        abort_transaction(tx)

        assert not exists(path)

##################################################################

class TestFileCacheRpcProxy(TestFileCache):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
