from conftest_lib.conftest_queries import QueryTracker

from yt_env_setup import YTEnvSetup

from yt.environment.init_query_tracker_state import get_latest_version, create_tables_required_version, run_migration

from yt.common import YtError

from yt_commands import (wait, authors, ls, get, set, assert_yt_error, remove, select_rows, insert_rows, exists,
                         create_tablet_cell_bundle, sync_create_cells)

from yt_queries import get_query, get_query_tracker_info

from yt.yson import YsonEntity, YsonMap

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
            "finished_query_results",
            "active_queries",
        ]

        for table in tables:
            assert get(f"//sys/query_tracker/{table}/@min_data_ttl") == 60000
            assert get(f"//sys/query_tracker/{table}/@merge_rows_on_flush")
            assert get(f"//sys/query_tracker/{table}/@auto_compaction_period") == 3600000

    @authors("lucius")
    def test_full_result_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)

        client = query_tracker.query_tracker.env.create_native_client()
        create_tables_required_version(client, 11)

        insert_rows("//sys/query_tracker/finished_query_results", [{"query_id": "test_query_id", "result_index": 1}])
        run_migration(client, 12)

        results = list(select_rows("* from [//sys/query_tracker/finished_query_results]"))
        assert len(results) == 1

        assert results[0]["query_id"] == "test_query_id"
        assert results[0]["result_index"] == 1
        assert "full_result" in results[0]
        assert results[0]["full_result"] == YsonEntity()

    @authors("lucius")
    def test_fix_query_results_table_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)
        client = query_tracker.query_tracker.env.create_native_client()

        create_tables_required_version(client, 12)
        assert exists("//sys/query_tracker/finished_queries_results")
        assert exists("//sys/query_tracker/finished_query_results")

        run_migration(client, 13)
        assert not exists("//sys/query_tracker/finished_queries_results")
        assert exists("//sys/query_tracker/finished_query_results")

    @authors("kirsiv40")
    def test_assigned_query_attr_in_finished_queries_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)
        client = query_tracker.query_tracker.env.create_native_client()

        create_tables_required_version(client, 15)
        assert exists("//sys/query_tracker/finished_queries")

        insert_rows("//sys/query_tracker/finished_queries", [{"query_id": "test_query_id"}])
        rows_before_migration = list(select_rows("* from [//sys/query_tracker/finished_queries]"))
        assert len(rows_before_migration) == 1
        assert len(rows_before_migration[0]) == 15
        assert "assigned_tracker" not in rows_before_migration[0]

        run_migration(client, 16)
        assert exists("//sys/query_tracker/finished_queries")
        rows_after_migration = list(select_rows("* from [//sys/query_tracker/finished_queries]"))
        assert len(rows_after_migration) == 1
        assert len(rows_after_migration[0]) == 16
        assert "assigned_tracker" in rows_after_migration[0]

    @authors("kirsiv40")
    def test_progress_compression_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)
        client = query_tracker.query_tracker.env.create_native_client()

        create_tables_required_version(client, 16)

        assert get_query_tracker_info(attributes=["cluster_name"])['expected_tables_version'] >= 17
        assert client.get_query_tracker_info(attributes=["cluster_name"]) is not None

        progress_before_migration = YsonMap({"abc": "def", "qwe": 123, "zxc": [1, "abc", 2, ',:="\'\'"', ",:=\"\'''\'\""]})
        insert_rows("//sys/query_tracker/finished_queries", [{"query_id": "12345678-12345678-12345678-12345678", "progress": progress_before_migration, "user": "some-user"}])

        run_migration(client, 17)

        rows_after_migration = list(select_rows("* from [//sys/query_tracker/finished_queries]"))
        raw_progress_after_migration = rows_after_migration[0]["progress"]
        assert not isinstance(raw_progress_after_migration, dict)

        progress_after_migration = get_query("12345678-12345678-12345678-12345678")["progress"]
        assert progress_after_migration == progress_before_migration

    @authors("kirsiv40")
    def test_search_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)
        client = query_tracker.query_tracker.env.create_native_client()

        create_tables_required_version(client, 17)
        assert not exists("//sys/query_tracker/search_inverted_index")
        assert not exists("//sys/query_tracker/search_meta")

        insert_rows("//sys/query_tracker/finished_queries", [
            {
                "query_id": "test_query_id",
                "query": "((some_token 23 some_token 1))",
                "access_control_objects": ["aco-1", "aco-2", "nobody"],
                "user": "kirsiv40",
                "start_time": 1,
                "engine": "yql",
            },
        ])

        insert_rows("//sys/query_tracker/active_queries", [
            {
                "query_id": "test_query_id_2",
                "query": "some_token 23 select from",
                "access_control_objects": ["aco-1"],
                "user": "kirsiv40",
                "start_time": 2,
                "engine": "spyt",
            },
            {
                "query_id": "test_query_id",
                "query": "((some_token 23 some_token 1))",
                "access_control_objects": ["aco-1", "aco-2", "nobody"],
                "user": "kirsiv40",
                "start_time": 1,
                "engine": "yql",
            },
        ])

        run_migration(client, 18)

        assert not exists("//sys/query_tracker/search_inverted_index/@auto_compaction_period")
        assert not exists("//sys/query_tracker/search_meta/@auto_compaction_period")

        inverted_index_records = list(select_rows("* from [//sys/query_tracker/search_inverted_index] order by (access_scope, token, engine, user, minus_start_time) limit 1000"))
        # 'nobody' aco doesn't go to inverted indices tables
        assert inverted_index_records == [
            {"access_scope": "aco:aco-1", "token": "23", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "23", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "spyt", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "23", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-1", "token": "aco", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "aco", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "spyt", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "aco", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "spyt", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},

            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "spyt", "user": "kirsiv40", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},

            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},


            {"access_scope": "aco:aco-2", "token": "23", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "23", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-2", "token": "23", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "23", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-2", "token": "aco", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "aco", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-2", "token": "aco", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "aco", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},
            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},

            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},
            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "yql", "user": "kirsiv40", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},


            {"access_scope": "su", "token": "23", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "23", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "su", "token": "23", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "23", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "su", "token": "aco", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "aco", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "su", "token": "aco", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "aco", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "su", "token": "aco:aco-1", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "aco:aco-1", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "su", "token": "aco:aco-1", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "aco:aco-1", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "su", "token": "aco:aco-2", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "su", "token": "aco:aco-2", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "su", "token": "some_token", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "some_token", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},

            {"access_scope": "su", "token": "some_token", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "su", "token": "some_token", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},


            {"access_scope": "user:kirsiv40", "token": "23", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "23", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "user:kirsiv40", "token": "23", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "23", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "user:kirsiv40", "token": "aco", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "aco", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "user:kirsiv40", "token": "aco", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "aco", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "user:kirsiv40", "token": "aco:aco-1", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "aco:aco-1", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},

            {"access_scope": "user:kirsiv40", "token": "aco:aco-1", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "aco:aco-1", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "user:kirsiv40", "token": "aco:aco-2", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "aco:aco-2", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 1},


            {"access_scope": "user:kirsiv40", "token": "some_token", "engine": "", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "some_token", "engine": "", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},

            {"access_scope": "user:kirsiv40", "token": "some_token", "engine": "spyt", "user": "", "minus_start_time": -2, "query_id": "test_query_id_2", "occurrences": 1},
            {"access_scope": "user:kirsiv40", "token": "some_token", "engine": "yql", "user": "", "minus_start_time": -1, "query_id": "test_query_id", "occurrences": 2},
        ]

        meta_records = list(select_rows("* from [//sys/query_tracker/search_meta] order by (access_scope, token, engine, user) limit 100"))
        assert meta_records == [
            {"access_scope": "aco:aco-1", "token": "23", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "", "user": "kirsiv40", "total_occurrences": 2, "unique_queries": 2},

            {"access_scope": "aco:aco-1", "token": "23", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "spyt", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-1", "token": "23", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "23", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-1", "token": "aco", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "", "user": "kirsiv40", "total_occurrences": 2, "unique_queries": 2},

            {"access_scope": "aco:aco-1", "token": "aco", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "spyt", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-1", "token": "aco", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "aco", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "", "user": "kirsiv40", "total_occurrences": 2, "unique_queries": 2},

            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "spyt", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-1", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "aco:aco-2", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "", "user": "", "total_occurrences": 3, "unique_queries": 2},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "", "user": "kirsiv40", "total_occurrences": 3, "unique_queries": 2},

            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "spyt", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "yql", "user": "", "total_occurrences": 2, "unique_queries": 1},
            {"access_scope": "aco:aco-1", "token": "some_token", "engine": "yql", "user": "kirsiv40", "total_occurrences": 2, "unique_queries": 1},


            {"access_scope": "aco:aco-2", "token": "23", "engine": "", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "23", "engine": "", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-2", "token": "23", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "23", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-2", "token": "aco", "engine": "", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "aco", "engine": "", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-2", "token": "aco", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "aco", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-1", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "aco:aco-2", "engine": "yql", "user": "kirsiv40", "total_occurrences": 1, "unique_queries": 1},


            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "", "user": "kirsiv40", "total_occurrences": 2, "unique_queries": 1},

            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "yql", "user": "", "total_occurrences": 2, "unique_queries": 1},
            {"access_scope": "aco:aco-2", "token": "some_token", "engine": "yql", "user": "kirsiv40", "total_occurrences": 2, "unique_queries": 1},


            {"access_scope": "su", "token": "23", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "su", "token": "23", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "su", "token": "23", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "su", "token": "aco", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "su", "token": "aco", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "su", "token": "aco", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "su", "token": "aco:aco-1", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "su", "token": "aco:aco-1", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "su", "token": "aco:aco-1", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "su", "token": "aco:aco-2", "engine": "", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "su", "token": "aco:aco-2", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "su", "token": "some_token", "engine": "", "user": "", "total_occurrences": 3, "unique_queries": 2},
            {"access_scope": "su", "token": "some_token", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "su", "token": "some_token", "engine": "yql", "user": "", "total_occurrences": 2, "unique_queries": 1},

            {"access_scope": "user:kirsiv40", "token": "23", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "user:kirsiv40", "token": "23", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "user:kirsiv40", "token": "23", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "user:kirsiv40", "token": "aco", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "user:kirsiv40", "token": "aco", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "user:kirsiv40", "token": "aco", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "user:kirsiv40", "token": "aco:aco-1", "engine": "", "user": "", "total_occurrences": 2, "unique_queries": 2},
            {"access_scope": "user:kirsiv40", "token": "aco:aco-1", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "user:kirsiv40", "token": "aco:aco-1", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "user:kirsiv40", "token": "aco:aco-2", "engine": "", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "user:kirsiv40", "token": "aco:aco-2", "engine": "yql", "user": "", "total_occurrences": 1, "unique_queries": 1},

            {"access_scope": "user:kirsiv40", "token": "some_token", "engine": "", "user": "", "total_occurrences": 3, "unique_queries": 2},
            {"access_scope": "user:kirsiv40", "token": "some_token", "engine": "spyt", "user": "", "total_occurrences": 1, "unique_queries": 1},
            {"access_scope": "user:kirsiv40", "token": "some_token", "engine": "yql", "user": "", "total_occurrences": 2, "unique_queries": 1},
        ]

    @authors("kirsiv40")
    def test_do_not_index_flag_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)
        client = query_tracker.query_tracker.env.create_native_client()

        create_tables_required_version(client, 18)

        insert_rows("//sys/query_tracker/finished_queries", [{"query_id": "test_query_id"}])
        rows_before_migration = list(select_rows("* from [//sys/query_tracker/finished_queries]"))
        assert len(rows_before_migration) == 1
        assert len(rows_before_migration[0]) == 16
        assert "is_indexed" not in rows_before_migration[0]

        run_migration(client, 19)
        assert exists("//sys/query_tracker/finished_queries")
        rows_after_migration = list(select_rows("* from [//sys/query_tracker/finished_queries]"))
        assert len(rows_after_migration) == 1
        assert len(rows_after_migration[0]) == 17
        assert "is_indexed" in rows_after_migration[0]
        assert str(rows_after_migration[0]["is_indexed"]) == "true"

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_is_tutorial_flag_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)
        client = query_tracker.query_tracker.env.create_native_client()

        create_tables_required_version(client, 19)

        insert_rows("//sys/query_tracker/active_queries", [{"query_id": "test_query_id"}])
        insert_rows("//sys/query_tracker/finished_queries", [{"query_id": "test_query_id"}])
        insert_rows("//sys/query_tracker/finished_queries_by_start_time", [{"query_id": "test_query_id", "minus_start_time": 0}])
        insert_rows("//sys/query_tracker/finished_queries_by_user_and_start_time", [{"user": "user", "query_id": "test_query_id", "minus_start_time": 0}])
        insert_rows("//sys/query_tracker/finished_queries_by_aco_and_start_time", [{"access_control_object": "aco", "query_id": "test_query_id", "minus_start_time": 0}])

        run_migration(client, 20)
        for table in ["active_queries", "finished_queries", "finished_queries_by_start_time"]:
            assert exists(f"//sys/query_tracker/{table}")
            rows_after_migration = list(select_rows(f"* from [//sys/query_tracker/{table}]"))
            assert len(rows_after_migration) == 1
            assert "is_tutorial" in rows_after_migration[0]
            assert str(rows_after_migration[0]["is_tutorial"]) == "false"

    @authors("mpereskokova")
    def test_ttl_migration(self, query_tracker):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        remove("//sys/query_tracker", recursive=True, force=True)
        client = query_tracker.query_tracker.env.create_native_client()

        create_tables_required_version(client, 20)

        insert_rows("//sys/query_tracker/finished_queries", [{"query_id": "test_query_id"}])
        insert_rows("//sys/query_tracker/finished_query_results", [{"query_id": "test_query_id", "result_index": 1}])

        run_migration(client, 21)
        for table in ["finished_queries", "finished_query_results"]:
            assert exists(f"//sys/query_tracker/{table}")
            rows_after_migration = list(select_rows(f"* from [//sys/query_tracker/{table}]"))
            assert len(rows_after_migration) == 1
