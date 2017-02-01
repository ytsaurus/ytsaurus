import yt.local as yt_local
import yt.wrapper as yt

import os
import subprocess

COMPRESS_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../compress.py")
COMPRESS_SCRIPT_DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "compress_script_cypress")
SANDBOX_PATH = os.path.join(os.path.dirname(__file__), "sandbox")

def test_compress_script():
    yt_env = yt_local.start(local_cypress_dir=COMPRESS_SCRIPT_DATA_PATH,
                            path=SANDBOX_PATH)
    yt.config["proxy"]["url"] = yt_env.get_proxy_address()
    try:
        p = subprocess.Popen(["python", COMPRESS_SCRIPT_PATH, "find", "--queue", "//tmp/table_queue"],
                             env={"YT_PROXY": yt_env.get_proxy_address()})
        assert p.wait() == 0
        assert sorted([obj["table"] for obj in yt.get("//tmp/table_queue")]) == sorted([
            "//test/dir_with_force_compress/table",
            "//test/dir_with_no_force_compress/table",
            "//test/dir_with_opaque/table",
            "//test/dir_with_recursive_compress_spec/table",
            "//test/dir_with_recursive_compress_spec/other_subdir/table",
            "//test/dir_with_custom_codecs/table"])

        zlib6_tables = [obj["table"] for obj in yt.get("//tmp/table_queue")
                        if obj["compression_codec"] == "zlib_6"]
        assert zlib6_tables == ["//test/dir_with_custom_codecs/table"]
    finally:
        yt_local.stop(yt_env.id, remove_working_dir=True, path=SANDBOX_PATH)
