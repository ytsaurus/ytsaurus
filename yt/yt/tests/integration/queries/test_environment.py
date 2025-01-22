from conftest_lib.conftest_queries import QueryTracker

from yt_env_setup import YTEnvSetup

from yt.environment.init_query_tracker_state import get_latest_version, create_tables_required_version, run_migration

from yt.common import YtError

from yt_commands import (wait, authors, ls, get, set, assert_yt_error, remove, select_rows, insert_rows, exists,
                         create_tablet_cell_bundle, sync_create_cells)

import yt_error_codes

import pytest


@pytest.mark.enabled_multidaemon
class TestEnvironment(YTEnvSetup):
    NUM_QUERY_TRACKERS = 3

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    ENABLE_MULTIDAEMON = True

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
    def test_context_manager(self):
        self._check_cleanliness()
        with QueryTracker(self.Env, 1):
            self._check_liveness(1)
        self._check_cleanliness()
        with QueryTracker(self.Env, 2):
            self._check_liveness(2)
        self._check_cleanliness()

    @authors("mpereskokova")
    def test_alerts(self, query_tracker):
        alerts_path = f"//sys/query_tracker/instances/{query_tracker.query_tracker.addresses[0]}/orchid/alerts"
        version_path = "//sys/query_tracker/@version"
        latest_version = get_latest_version()

        assert get(version_path) == latest_version
        assert len(get(alerts_path)) == 0

        set(version_path, latest_version - 1)
        wait(lambda: "query_tracker_invalid_state" in get(alerts_path))
        assert_yt_error(YtError.from_dict(get(alerts_path)["query_tracker_invalid_state"]),
                        "Min required state version is not met")


@pytest.mark.enabled_multidaemon
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

    ENABLE_MULTIDAEMON = True

    @authors("mpereskokova")
    def test_aco_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)

        client = query_tracker.query_tracker.env.create_native_client()
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

    @authors("mpereskokova")
    def test_finished_queries_by_start_time_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)

        client = query_tracker.query_tracker.env.create_native_client()
        create_tables_required_version(client, 8)

        insert_rows("//sys/query_tracker/finished_queries_by_start_time", [{"query_id": "test_query_id", "start_time": 1, "user": "user", "access_control_objects": ["aco1", "aco2"]}])
        run_migration(client, 9)

        queries_by_aco = list(select_rows("* from [//sys/query_tracker/finished_queries_by_aco_and_start_time] order by (access_control_object, minus_start_time, query_id) LIMIT 3"))
        assert len(queries_by_aco) == 2

        assert queries_by_aco[0]["query_id"] == "test_query_id"
        assert queries_by_aco[0]["access_control_object"] == "aco1"
        assert queries_by_aco[0]["minus_start_time"] == -1
        assert "access_control_objects" not in queries_by_aco[0]
        assert "start_time" not in queries_by_aco[0]

        assert queries_by_aco[1]["query_id"] == "test_query_id"
        assert queries_by_aco[1]["access_control_object"] == "aco2"
        assert queries_by_aco[1]["minus_start_time"] == -1
        assert "access_control_objects" not in queries_by_aco[1]
        assert "start_time" not in queries_by_aco[1]

        queries_by_user = list(select_rows("* from [//sys/query_tracker/finished_queries_by_user_and_start_time]"))
        assert len(queries_by_user) == 1

        assert queries_by_user[0]["user"] == "user"
        assert queries_by_user[0]["minus_start_time"] == -1
        assert "access_control_objects" not in queries_by_user[0]
        assert "start_time" not in queries_by_user[0]

        assert exists("//sys/query_tracker/finished_queries_by_start_time")

    @authors("mpereskokova")
    def test_dyntables_params_migration(self, query_tracker):
        tables = [
            "finished_queries_by_aco_and_start_time",
            "finished_queries_by_user_and_start_time",
            "finished_queries_by_start_time",
            "finished_queries",
            "finished_queries_results",
            "active_queries",
        ]

        for table in tables:
            assert get(f"//sys/query_tracker/{table}/@min_data_ttl") == 60000
            assert get(f"//sys/query_tracker/{table}/@merge_rows_on_flush")
            assert get(f"//sys/query_tracker/{table}/@auto_compaction_period") == 3600000
