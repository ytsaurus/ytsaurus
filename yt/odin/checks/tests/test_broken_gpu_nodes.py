from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher


def test_no_broken_gpu_nodes(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("broken_gpu_nodes")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "broken_gpu_nodes")

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE
