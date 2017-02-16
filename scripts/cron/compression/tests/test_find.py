import yt.local as yt_local

from yt.packages.six.moves import xrange

import yt.wrapper as yt

import os
import sys
import subprocess

FIND_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../find_tables_to_compress.py")
FIND_SCRIPT_DATA_PATH = os.path.join(os.path.dirname(__file__), "compress_script_cypress")
SANDBOX_PATH = os.path.join(os.path.dirname(__file__), "../tests.sandbox")

def test_find_script():
    yt_env = yt_local.start(local_cypress_dir=FIND_SCRIPT_DATA_PATH,
                            path=SANDBOX_PATH)
    yt.config["proxy"]["url"] = yt_env.get_proxy_address()
    try:
        for worker_count in (1, 3, 15, 120):
            yt.create("map_node", "//tasks_root")
            for index in xrange(worker_count):
                yt.create("list_node", "//tasks_root/" + str(index))
            yt.set("//tasks_root/@alive_workers", list(map(str, xrange(worker_count))))

            p = subprocess.Popen([sys.executable, FIND_SCRIPT_PATH, "--tasks-root", "//tasks_root"],
                                 env={"YT_PROXY": yt_env.get_proxy_address()})
            assert p.wait() == 0

            tables = []
            for lst in yt.list("//tasks_root"):
                tables.extend(yt.get("//tasks_root/" + lst))

            assert sorted([obj["table"] for obj in tables]) == sorted([
                "//test/dir_with_force_compress/table",
                "//test/dir_with_no_force_compress/table",
                "//test/dir_with_opaque/table",
                "//test/dir_with_recursive_compress_spec/table",
                "//test/dir_with_recursive_compress_spec/other_subdir/table",
                "//test/dir_with_custom_codecs/table"])

            zlib6_tables = [obj["table"] for obj in tables if obj["compression_codec"] == "zlib_6"]
            assert zlib6_tables == ["//test/dir_with_custom_codecs/table"]

            yt.remove("//tasks_root", recursive=True)
    finally:
        yt_local.stop(yt_env.id, remove_working_dir=True, path=SANDBOX_PATH)
