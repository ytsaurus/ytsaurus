import yt.local as yt_local

import yt.wrapper as yt

import os
import subprocess

PRUNE_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../prune_empty_nodes.py")
PRUNE_SCRIPT_DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "prune_script_cypress")
SANDBOX_PATH = os.path.join(os.path.dirname(__file__), "sandbox")

def test_prune_script():
    yt_env = yt_local.start(local_cypress_dir=PRUNE_SCRIPT_DATA_PATH,
                            use_proxy_from_yt_source=True,
                            path=SANDBOX_PATH)
    yt.config["proxy"]["url"] = yt_env.get_proxy_address()
    try:
        p = subprocess.Popen(["python", PRUNE_SCRIPT_PATH],
                             env={"YT_PROXY": yt_env.get_proxy_address()})
        assert p.wait() == 0
        assert not yt.exists("//dir1")

        assert yt.exists("//dir2")
        assert yt.exists("//dir2/subdir")
        assert yt.exists("//dir2/table1")
        assert not yt.exists("//dir2/table2")

        assert not yt.exists("//dir3")
    finally:
        yt_local.stop(yt_env.id, path=SANDBOX_PATH)
