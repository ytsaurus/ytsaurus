from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, get, exists, set, create, move, remove, read_file, write_file,
    get_file_from_cache, put_file_to_cache, start_transaction, abort_transaction,
    make_ace, create_user)

from yt.common import date_string_to_datetime, YtError

from yt_sequoia_helpers import not_implemented_in_sequoia

import hashlib
import pytest
import time

##################################################################


@pytest.mark.enabled_multidaemon
class TestFileCache(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 5

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_get_file_from_cache(self):
        cache_path = "//tmp/file_cache"
        create("file", "//tmp/file")
        write_file("//tmp/file", b"abacaba", compute_md5=True)
        md5 = hashlib.md5(b"abacaba").hexdigest()
        move(
            "//tmp/file",
            "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5),
            recursive=True,
        )

        assert get_file_from_cache(md5, cache_path) == "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5)

        create("file", "//tmp/file")
        write_file("//tmp/file", b"aba", compute_md5=True)
        move(
            "//tmp/file",
            "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5),
            recursive=True,
            force=True,
        )

        assert get_file_from_cache(md5, cache_path) == ""

        remove("//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        create("file", "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        assert get_file_from_cache(md5, cache_path) == ""

        remove("//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        assert get_file_from_cache(md5, cache_path) == ""

        assert get_file_from_cache("invalid_md5", cache_path) == ""
        assert get_file_from_cache("", cache_path) == ""

    @authors("ignat")
    @not_implemented_in_sequoia
    def test_put_file_to_cache(self):
        create("map_node", "//tmp/cache")
        create("file", "//tmp/file")
        write_file("//tmp/file", b"abacaba")

        with pytest.raises(YtError):
            put_file_to_cache(
                "//tmp/file",
                hashlib.md5(b"abacaba").hexdigest(),
                cache_path="//tmp/cache",
            )

        write_file("//tmp/file", b"abacaba", compute_md5=True)

        with pytest.raises(YtError):
            put_file_to_cache("//tmp/file", "invalid_hash", cache_path="//tmp/cache")

        with pytest.raises(YtError):
            put_file_to_cache("//tmp/file", hashlib.md5(b"abacaba").hexdigest())

        path = put_file_to_cache("//tmp/file", hashlib.md5(b"abacaba").hexdigest(), cache_path="//tmp/cache")
        assert read_file(path) == b"abacaba"
        assert get(path + "/@touched")
        assert exists("//tmp/file")

        write_file("//tmp/file", b"abacaba", compute_md5=True)
        set(path + "/@touched", False)

        path2 = put_file_to_cache("//tmp/file", hashlib.md5(b"abacaba").hexdigest(), cache_path="//tmp/cache")
        assert path2 == path
        assert get(path + "/@touched")

        write_file("//tmp/file", b"aba", compute_md5=True)

        create("file", "//tmp/file2")
        write_file("//tmp/file2", b"abacaba", compute_md5=True)

        path3 = put_file_to_cache("//tmp/file2", hashlib.md5(b"abacaba").hexdigest(), cache_path="//tmp/cache")
        assert read_file(path3) == b"abacaba"
        assert path3 == path2

        write_file("//tmp/file", b"abacaba", compute_md5=True)
        path4 = put_file_to_cache("//tmp/file", hashlib.md5(b"abacaba").hexdigest(), cache_path="//tmp/cache")

        assert read_file(path4) == b"abacaba"
        assert path4 == path3

        write_file("//tmp/file", b"aba", compute_md5=True)

        create_user("u")
        create(
            "map_node",
            "//tmp/cache2",
            attributes={"inherit_acl": False, "acl": [make_ace("deny", "u", "use")]},
        )

        with pytest.raises(YtError):
            put_file_to_cache(
                "//tmp/file",
                hashlib.md5(b"aba").hexdigest(),
                cache_path="//tmp/cache2",
                authenticated_user="u",
            )

        set("//tmp/cache2/@acl/end", make_ace("allow", "u", "write"))
        put_file_to_cache(
            "//tmp/file",
            hashlib.md5(b"aba").hexdigest(),
            cache_path="//tmp/cache2",
            authenticated_user="u",
        )

    @authors("levysotsky")
    def test_put_file_to_cache_no_overwriting(self):
        content = b"abacaba"
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
        content = b"abacaba"
        content_md5 = hashlib.md5(content).hexdigest()

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

        content = b"abacaba"
        content_md5 = hashlib.md5(content).hexdigest()

        create("map_node", "//tmp/cache", tx=tx)
        create("file", "//tmp/file", tx=tx)
        write_file("//tmp/file", content, compute_md5=True, tx=tx)
        path = put_file_to_cache("//tmp/file", content_md5, cache_path="//tmp/cache", tx=tx)
        path2 = get_file_from_cache(content_md5, "//tmp/cache", tx=tx)
        assert path == path2
        assert read_file(path, tx=tx) == content

        abort_transaction(tx)

        assert not exists(path)

    @authors("egor-gutrov")
    def test_dont_preserve_expiration_timeout(self):
        content = b"abacaba"
        content_md5 = hashlib.md5(content).hexdigest()

        create("map_node", "//tmp/cache")
        create("file", "//tmp/file")
        write_file("//tmp/file", content, compute_md5=True)
        set("//tmp/file/@expiration_timeout", 1000)
        path = put_file_to_cache("//tmp/file", content_md5, cache_path="//tmp/cache")
        assert exists("//tmp/file/@expiration_timeout")
        assert not exists(path + "/@expiration_timeout")

    @authors("egor-gutrov")
    def test_preserve_expiration_timeout(self):
        content = b"abacaba"
        content_md5 = hashlib.md5(content).hexdigest()

        create("map_node", "//tmp/cache")
        create("file", "//tmp/file")
        write_file("//tmp/file", content, compute_md5=True)
        set("//tmp/file/@expiration_timeout", 1000)
        path = put_file_to_cache(
            "//tmp/file",
            content_md5,
            cache_path="//tmp/cache",
            preserve_expiration_timeout=True,
        )
        assert exists("//tmp/file/@expiration_timeout")
        assert exists(path + "/@expiration_timeout")
        time.sleep(2)
        assert not exists("//tmp/file")
        assert not exists(path)

##################################################################


class TestFileCacheRpcProxy(TestFileCache):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


##################################################################


class TestFileCacheSequoia(TestFileCache):
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["sequoia_node_host", "transaction_coordinator"]},
        "12": {"roles": ["chunk_host"]},
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "testing": {
            "enable_ground_update_queues_sync": True,
            "enable_user_directory_per_request_sync": True,
        }
    }
