from yt_env_setup import YTEnvSetup

from yt_commands import wait, authors, ls, YtError

import yt_error_codes

from queries.environment import QueryTracker


class TestEnvironment(YTEnvSetup):
    NUM_QUERY_TRACKERS = 3

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    def _ls_instances(self):
        try:
            return ls("//sys/query_tracker/instances", verbose=False, verbose_error=False)
        except YtError as err:
            if err.contains_code(yt_error_codes.ResolveErrorCode):
                return []
            raise

    def _check_liveness(self, instance_count):
        wait(lambda: len(self._ls_instances()) == instance_count)

    def _check_cleanliness(self):
        assert len(self._ls_instances()) == 0

    @authors("max42")
    def test_fixture(self, query_tracker):
        self._check_liveness(3)

    @authors("max42")
    def test_context_manager(self, query_tracker_environment):
        self._check_cleanliness()
        with QueryTracker(self.Env, 1):
            self._check_liveness(1)
        self._check_cleanliness()
        with QueryTracker(self.Env, 2):
            self._check_liveness(2)
        self._check_cleanliness()
