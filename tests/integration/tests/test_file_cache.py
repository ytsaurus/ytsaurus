from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt.common import date_string_to_datetime

import hashlib
import pytest

##################################################################

class TestFileCache(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

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

    def test_file_cache(self):
        create("map_node", "//tmp/cache")
        create("file", "//tmp/file")
        write_file("//tmp/file", "abacaba", compute_md5=True)
        path = put_file_to_cache("//tmp/file", hashlib.md5("abacaba").hexdigest(), cache_path="//tmp/cache")
        path2 = get_file_from_cache(hashlib.md5("abacaba").hexdigest(), "//tmp/cache")
        assert path == path2
        assert read_file(path) == "abacaba"

        modification_time = date_string_to_datetime(get(path + "/@modification_time"))
        path3 = get_file_from_cache(hashlib.md5("abacaba").hexdigest(), "//tmp/cache")
        assert path2 == path3
        assert read_file(path) == "abacaba"
        assert date_string_to_datetime(get(path + "/@modification_time")) > modification_time

