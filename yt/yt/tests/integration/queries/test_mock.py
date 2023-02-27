from yt_env_setup import YTEnvSetup

from yt_commands import (authors, raises_yt_error, wait)

from queries.environment import start_query


class TestQueriesMock(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("max42")
    def test_fail(self, query_tracker):
        query = start_query("mock", "fail")
        with raises_yt_error("failed"):
            query.track()
        assert query.get_state() == "failed"

        query = start_query("mock", "fail_on_start")
        with raises_yt_error("failed"):
            query.track()
        assert query.get_state() == "failed"

    @authors("max42")
    def test_abort(self, query_tracker):
        query = start_query("mock", "run_forever")
        wait(lambda: query.get_state() == "running")
        query.abort()
        with raises_yt_error("aborted"):
            query.track()
        assert query.get_state() == "aborted"
