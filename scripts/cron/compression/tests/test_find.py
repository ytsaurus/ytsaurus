from yt.local import LocalYt 

from yt.packages.six.moves import xrange

import os
import sys
import subprocess
import pytest

FIND_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../find_tables_to_compress.py")
FIND_SCRIPT_DATA_PATH = os.path.join(os.path.dirname(__file__), "compress_script_cypress")
SANDBOX_PATH = os.path.join(os.path.dirname(__file__), "../tests.sandbox")


@pytest.mark.parametrize("worker_count", [1, 3, 15, 120])
def test_find_script(worker_count):
    with LocalYt(local_cypress_dir=FIND_SCRIPT_DATA_PATH, path=SANDBOX_PATH) as client:
        client.create("map_node", "//tasks_root")
        for index in xrange(worker_count):
            client.create("list_node", "//tasks_root/" + str(index))
        client.set("//tasks_root/@alive_workers", list(map(str, xrange(worker_count))))

        p = subprocess.Popen([sys.executable, FIND_SCRIPT_PATH, "--tasks-root", "//tasks_root"],
                             env={"YT_PROXY": client.config["proxy"]["url"]})
        assert p.wait() == 0

        tables = []
        for lst in client.list("//tasks_root"):
            tables.extend(client.get("//tasks_root/" + lst))

        assert sorted([obj["table"] for obj in tables]) == sorted([
            "//test/dir_with_force_compress/table",
            "//test/dir_with_no_force_compress/table",
            "//test/dir_with_opaque/table",
            "//test/dir_with_recursive_compress_spec/table",
            "//test/dir_with_recursive_compress_spec/other_subdir/table",
            "//test/dir_with_custom_codecs/table",
            "//test/dir_with_none_erasure_codec/table"])

        zlib6_tables = [obj["table"] for obj in tables if obj["compression_codec"] == "zlib_6"]
        assert zlib6_tables == ["//test/dir_with_custom_codecs/table"]

        none_erasure_codec_tables = [obj["table"] for obj in tables if obj["erasure_codec"] == "none"]
        assert none_erasure_codec_tables == ["//test/dir_with_none_erasure_codec/table"]
