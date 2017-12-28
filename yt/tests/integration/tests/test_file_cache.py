from yt_env_setup import YTEnvSetup
from yt_commands import *

import hashlib

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

        assert get_file_from_cache(md5, cache_path) == '"//tmp/file_cache/{0}/{1}"'.format(md5[-2:], md5)

        create("file", "//tmp/file")
        write_file("//tmp/file", "aba", compute_md5=True)
        move("//tmp/file", "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5), recursive=True, force=True)

        assert get_file_from_cache(md5, cache_path) == '""'

        remove("//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        create("file", "//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        assert get_file_from_cache(md5, cache_path) == '""'

        remove("//tmp/file_cache/{0}/{1}".format(md5[-2:], md5))
        assert get_file_from_cache(md5, cache_path) == '""'

        assert get_file_from_cache("invalid_md5", cache_path) == '""'
        assert get_file_from_cache("", cache_path) == '""'


##################################################################

class TestFileCacheMulticell(TestFileCache):
    NUM_SECONDARY_MASTER_CELLS = 2
