from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks

TABLE_PATH = "//sys/admin/odin/read_static_table_commands/table"
SEED_ROW = {"key": "read_static_table_commands", "value": "seed"}


def test_read_static_table_commands(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_env.yt_client.config["proxy"]["url"]
    checks_path = make_check_dir("read_static_table_commands")
    storage = configure_and_run_checks(proxy_url, checks_path)

    assert abs(storage.get_service_states("read_static_table_commands")[-1] - FULLY_AVAILABLE_STATE) <= 0.001
    assert yt_client.exists(TABLE_PATH)
    assert SEED_ROW in list(yt_client.read_table(TABLE_PATH))
