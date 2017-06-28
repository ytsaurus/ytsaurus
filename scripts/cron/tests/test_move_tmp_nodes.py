from yt.local import LocalYt

from yt.common import makedirp
import yt.wrapper as yt

import os
import subprocess
import time

SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../move_tmp_nodes.py")
SANDBOX_PATH = os.path.join(os.path.dirname(__file__), "../tests.sandbox")

def test_move_tmp_nodes():
    makedirp(SANDBOX_PATH)
    os.environ["YT_LOCAL_ROOT_PATH"] = SANDBOX_PATH

    with LocalYt() as client:
        client.create("table", "//home/node/test_table", attributes={"account": "tmp"}, recursive=True)
        client.create("table", "//home/node/test_table2", attributes={"account": "tmp"})
        client.create("file", "//home/some_file", attributes={"account": "tmp_files"})
        client.create("file", "//home/test_file", attributes={"account": "tmp"})

        environment = os.environ
        environment["YT_PROXY"] = client.config["proxy"]["url"]

        run_script = lambda: subprocess.check_call([
            SCRIPT_PATH,
            "--root",
            "/",
            "--trash-dir",
            "//tmp/trash_by_cron",
            "--account",
            "tmp"

        ], env=environment)

        run_script()

        assert client.get("//home/@count") == 2
        assert client.get("//tmp/trash_by_cron/@count") == 1
        assert client.exists("//home/some_file")
        assert client.exists("//home/node")


        run_script()
        assert client.get("//home/@count") == 2
        assert client.get("//tmp/trash_by_cron/@count") == 1

        client.create("table", "//home/node1/node2/node3/node4/node5/node6/node7/test_table",
                      attributes={"account": "tmp"}, recursive=True)

        time.sleep(2)
        run_script()

        assert client.get("//home/@count") == 3
        assert client.get("//tmp/trash_by_cron/@count") == 2

        client.create("table", "//home/node/test_table", attributes={"account": "tmp"}, recursive=True)
        time.sleep(2)

        with client.Transaction():
            client.lock("//home/node")
            run_script()

        assert client.get("//home/@count") == 3
        assert client.get("//tmp/trash_by_cron/@count") == 3
