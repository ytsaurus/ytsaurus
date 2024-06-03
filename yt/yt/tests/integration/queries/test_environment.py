from conftest_lib.conftest_queries import QueryTracker

from yt_env_setup import YTEnvSetup

from yt.environment.init_query_tracker_state import get_latest_version, create_tables_required_version, run_migration

from yt.common import YtError

from yt_commands import wait, authors, ls, get, set, assert_yt_error, remove, select_rows, insert_rows

import yt_error_codes


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

    @authors("mpereskokova")
    def test_alerts(self, query_tracker):
        alerts_path = f"//sys/query_tracker/instances/{query_tracker.addresses[0]}/orchid/alerts"
        version_path = "//sys/query_tracker/@version"
        latest_version = get_latest_version()

        assert get(version_path) == latest_version
        assert len(get(alerts_path)) == 0

        set(version_path, latest_version - 1)
        wait(lambda: "query_tracker_invalid_state" in get(alerts_path))
        assert_yt_error(YtError.from_dict(get(alerts_path)["query_tracker_invalid_state"]),
                        "Min required state version is not met")


class TestMigration(YTEnvSetup):
    NUM_QUERY_TRACKERS = 1
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 3

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("mpereskokova")
    def test_aco_migration(self, query_tracker):
        remove("//sys/query_tracker", recursive=True, force=True)

        client = query_tracker.env.create_native_client()
        create_tables_required_version(client, 7)

        insert_rows("//sys/query_tracker/active_queries", [{"query_id": "test_query_id", "access_control_object": "test_aco"}])
        insert_rows("//sys/query_tracker/finished_queries", [{"query_id": "test_query_id"}])
        insert_rows("//sys/query_tracker/finished_queries_by_start_time", [{"query_id": "test_query_id", "start_time": 0, "access_control_object": "test_aco"}])
        run_migration(client, 8)

        active_queries = list(select_rows("* from [//sys/query_tracker/active_queries]"))
        assert len(active_queries) == 1
        assert active_queries[0]["query_id"] == "test_query_id"
        assert active_queries[0]["access_control_objects"] == ["test_aco"]
        assert "access_control_object" not in active_queries[0]

        finished_queries = list(select_rows("* from [//sys/query_tracker/finished_queries]"))
        assert len(finished_queries) == 1
        assert finished_queries[0]["access_control_objects"] == []
        assert "access_control_object" not in finished_queries[0]

        finished_queries_by_start_time = list(select_rows("* from [//sys/query_tracker/finished_queries_by_start_time]"))
        assert len(finished_queries_by_start_time) == 1
        assert finished_queries_by_start_time[0]["access_control_objects"] == ["test_aco"]
        assert "access_control_object" not in finished_queries_by_start_time[0]
