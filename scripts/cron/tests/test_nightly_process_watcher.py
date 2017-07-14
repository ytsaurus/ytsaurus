from yt.local import LocalYt

from yt.common import makedirp
import yt.wrapper as yt

import os
import subprocess
import time

SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../nightly_process_watcher.py")
COLLECTOR_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "data", "test_collector.py")
WORKER_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "data", "test_worker.py")
SANDBOX_PATH = os.path.join(os.path.dirname(__file__), "../tests.sandbox")

def test_nightly_process_watcher():
    makedirp(SANDBOX_PATH)
    os.environ["YT_LOCAL_ROOT_PATH"] = SANDBOX_PATH

    root = "//tmp/test_watcher"

    collector_log_path = os.path.join(SANDBOX_PATH, "collector_log")
    worker_log_path = os.path.join(SANDBOX_PATH, "worker_log")

    with LocalYt() as client:
        client.create("map_node", root, recursive=True)
        client.create("map_node", yt.ypath_join(root, "watcher"))
        client.create("map_node", yt.ypath_join(root, "tasks"))
        client.create("table", "//tmp/wtable")

        watcher = subprocess.Popen([
            SCRIPT_PATH,
            "--worker-command", WORKER_SCRIPT_PATH,
            "--worker-count", "3",
            "--worker-log-path", worker_log_path,
            "--tasks-root", yt.ypath_join(root, "tasks"),
            "--watcher-root", yt.ypath_join(root, "watcher"),
            "--collector-command", COLLECTOR_SCRIPT_PATH,
            "--collector-period", "3",
            "--collector-log-path", collector_log_path
        ], env={"YT_PROXY": client.config["proxy"]["url"]})

        time.sleep(15.0)

        assert watcher.poll() is None
        watcher.terminate()
        time.sleep(1.0)
        assert watcher.poll() is not None

        assert client.get("//tmp/wtable/@row_count") == 3
