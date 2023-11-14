import mock

from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin,  run_checks, CheckWatcher

from yt_odin_checks.lib.master_chunk_management import get_bool_attribute_if_exists
from yt_odin_checks.lib.master_chunk_management import get_results
from yt_odin_checks.lib.master_chunk_management import NOT_EXISTS, EXISTS_AND_FALSE, EXISTS_AND_TRUE


def test_get_bool_attribute_if_exists():
    yt_client = mock.MagicMock()
    yt_client.exists = mock.MagicMock(return_value=False)
    assert get_bool_attribute_if_exists(yt_client, "//some/path") == NOT_EXISTS

    yt_client.exists = mock.MagicMock(return_value=True)
    yt_client.get = mock.MagicMock(return_value=True)
    assert get_bool_attribute_if_exists(yt_client, "//some/path") == EXISTS_AND_TRUE

    yt_client.get = mock.MagicMock(return_value=False)
    assert get_bool_attribute_if_exists(yt_client, "//some/path") == EXISTS_AND_FALSE


def test_get_results():
    states = mock.MagicMock()
    states.FULLY_AVAILABLE_STATE = FULLY_AVAILABLE_STATE
    states.UNAVAILABLE_STATE = UNAVAILABLE_STATE

    state, message = get_results(NOT_EXISTS, states)
    assert states.FULLY_AVAILABLE_STATE == state

    state, message = get_results(EXISTS_AND_TRUE, states)
    assert states.FULLY_AVAILABLE_STATE == state

    state, message = get_results(EXISTS_AND_FALSE, states)
    assert states.UNAVAILABLE_STATE == state

    state, message = get_results("SOME_UNKNOWN_KEY", states)
    assert states.UNAVAILABLE_STATE == state


def test_master_chunk_management_is_working(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]
    checks_path = make_check_dir("master_chunk_management")

    # chunk_refresh_enabled = "//sys/@chunk_refresh_enabled"
    # chunk_requisition_update_enabled = "//sys/@chunk_requisition_update_enabled"

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "master_chunk_management")

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE
