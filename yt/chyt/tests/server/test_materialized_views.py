from base import ClickHouseTestBase, Clique, QueryFailedError

from helpers import get_breakpoint_node, release_breakpoint, wait_breakpoint

from yt_commands import (abort_transaction, authors, create, create_user, exists, get, lock, ls,
                         raises_yt_error, read_table, remove, start_transaction, sync_mount_table,
                         wait, write_table)

import yt.yson as yson

import threading
import time


class TestMaterializedViews(ClickHouseTestBase):
    SCHEMA = [
        {"name": "key", "type": "int64"},
        {"name": "value", "type": "string"},
    ]

    CREATE_MV_QUERY = 'CREATE MATERIALIZED VIEW mv TO "//tmp/target" AS SELECT key, value FROM "//tmp/source"'
    SELECT_MV_QUERY = "SELECT key, value FROM mv ORDER BY key"

    PERSISTED_MV_STATEMENT = (
        "ATTACH MATERIALIZED VIEW mv TO YT.`//tmp/target`\n"
        "(\n"
        "    `key` Nullable(Int64),\n"
        "    `value` Nullable(String)\n"
        ")\n"
        "DEFINER = root SQL SECURITY DEFINER\n"
        "AS SELECT\n"
        "    key,\n"
        "    value\n"
        "FROM YT.`//tmp/source`\n"
    )

    def setup_method(self, method):
        super().setup_method(method)
        create("table", "//tmp/source", attributes={"schema": self.SCHEMA})
        create("table", "//tmp/target", attributes={"schema": self.SCHEMA})

    @staticmethod
    def _statement_path(clique, database="YT"):
        return clique.storage_artifacts_path + "/{}.mv".format(database)

    @authors("buyval01")
    def test_lifecycle(self):
        rows = [{"key": i, "value": str(i)} for i in range(3)]
        write_table("//tmp/target", rows)

        config_patch = {"yt": {"materialized_views": {"scan_period": 100}}}
        with Clique(2, config_patch=config_patch, export_query_log=True) as clique:
            instances = clique.get_active_instances()
            statement_path = self._statement_path(clique)
            progress_root = clique.materialized_views_path + "/progress"

            clique.make_query(self.CREATE_MV_QUERY)

            assert exists(statement_path)
            assert get(statement_path + "/@chyt_object_type") == "materialized_view"
            persisted_config = yson.loads(yson.get_bytes(get(statement_path + "/@value")))
            assert persisted_config["create_statement"] == self.PERSISTED_MV_STATEMENT
            assert persisted_config["source_path"] == "//tmp/source"
            assert persisted_config["target_path"] == "//tmp/target"

            view_id = get(statement_path + "/@id")
            progress_path = progress_root + "/" + view_id
            assert exists(progress_path)
            persisted_progress = get(progress_path)
            assert persisted_progress["next_row_index"] == 0
            assert persisted_progress["last_error"] == ""

            for instance in instances:
                assert clique.make_direct_query(instance, self.SELECT_MV_QUERY) == rows

            assert clique.make_query("EXISTS TABLE mv") == [{"result": 1}]
            show_create = clique.make_query("SHOW CREATE TABLE mv")
            assert "CREATE MATERIALIZED VIEW" in show_create[0]["statement"]
            assert "//tmp/target" in show_create[0]["statement"]

            clique.make_direct_query(instances[0], "DROP TABLE mv")
            assert not exists(statement_path)
            with raises_yt_error(code=QueryFailedError):
                clique.make_direct_query(instances[1], self.SELECT_MV_QUERY)

            clique.make_query(self.CREATE_MV_QUERY)
            clique.make_query("DROP VIEW mv")
            assert not exists(statement_path)

            with raises_yt_error(code=QueryFailedError):
                clique.make_query("DROP TABLE no_such_mv")

    @authors("buyval01")
    def test_rejections(self):
        dict_schema = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "int64", "required": True},
        ]
        create("table", "//tmp/t", attributes={"schema": dict_schema})
        write_table("//tmp/t", [{"a": 0, "b": 1}])
        create_dictionary_query = (
            "CREATE DICTIONARY {} (`a` Int64, `b` Int64) PRIMARY KEY a "
            "SOURCE(Yt(Path '//tmp/t')) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 600)"
        )

        with Clique(1) as clique:
            with raises_yt_error(code=QueryFailedError):
                clique.make_query('CREATE MATERIALIZED VIEW mv ENGINE = YtTable() AS SELECT key, value FROM "//tmp/source"')
            with raises_yt_error(message_pattern="target table must exist"):
                clique.make_query('CREATE MATERIALIZED VIEW mv TO "//tmp/no_such_target" AS SELECT key, value FROM "//tmp/source"')

            # An explicit definer matching the query user is allowed.
            clique.make_query(
                'CREATE MATERIALIZED VIEW mv TO "//tmp/target" '
                'DEFINER = root SQL SECURITY DEFINER '
                'AS SELECT key, value FROM "//tmp/source"')
            clique.make_query("DROP TABLE mv")

            # Non-definer security types are rejected.
            with raises_yt_error(code=QueryFailedError):
                clique.make_query(
                    'CREATE MATERIALIZED VIEW mv TO "//tmp/target" '
                    'SQL SECURITY NONE '
                    'AS SELECT key, value FROM "//tmp/source"')

            # Setting a foreign definer is rejected by the ClickHouse SET_DEFINER access check.
            create_user("u2")
            clique.make_query("SELECT 1", user="u2")
            with raises_yt_error(code=QueryFailedError):
                clique.make_query(
                    'CREATE MATERIALIZED VIEW mv TO "//tmp/target" '
                    'DEFINER = u2 SQL SECURITY DEFINER '
                    'AS SELECT key, value FROM "//tmp/source"')

            # Names are claimed atomically across clique object kinds.
            clique.make_query(create_dictionary_query.format("obj"))
            with raises_yt_error(code=QueryFailedError):
                clique.make_query('CREATE MATERIALIZED VIEW obj TO "//tmp/target" AS SELECT key, value FROM "//tmp/source"')
            clique.make_query(self.CREATE_MV_QUERY)
            with raises_yt_error(code=QueryFailedError):
                clique.make_query(create_dictionary_query.format("mv"))

        remove("//tmp/source")
        create("table", "//tmp/source", attributes={
            "dynamic": True,
            "schema": self.SCHEMA,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/source")
        with Clique(1) as clique:
            with raises_yt_error(message_pattern="source table must be static"):
                clique.make_query(
                    'CREATE MATERIALIZED VIEW dynamic_mv TO "//tmp/target" '
                    'AS SELECT key, value FROM "//tmp/source"')

        remove("//tmp/source")
        create("table", "//tmp/source", attributes={"schema": self.SCHEMA})
        with Clique(1, enable_object_repository=False) as clique:
            with raises_yt_error(message_pattern="Clique has no configured CypressObjectRepository"):
                clique.make_query(self.CREATE_MV_QUERY)

    @authors("buyval01")
    def test_persistence(self):
        rows = [{"key": i, "value": str(i)} for i in range(2)]
        write_table("//tmp/target", rows)

        test_alias = "mv_persistence_alias"
        with Clique(1, alias=test_alias, remove_storage_artifacts_on_exit=False) as clique:
            clique.make_query(self.CREATE_MV_QUERY)

        # A new clique incarnation with the same alias picks the view up from Cypress.
        with Clique(1, alias=test_alias) as clique:
            wait(lambda: clique.make_query("EXISTS TABLE mv") == [{"result": 1}])
            assert clique.make_query(self.SELECT_MV_QUERY) == rows
            assert "CREATE MATERIALIZED VIEW" in clique.make_query("SHOW CREATE TABLE mv")[0]["statement"]

    @authors("buyval01")
    def test_background_refresh(self):
        write_table("//tmp/source", [
            {"key": 0, "value": "initial-0"},
            {"key": 1, "value": "initial-1"},
        ])
        expected_rows = [
            {"key": i, "value": "new-{}".format(i)}
            for i in range(2, 34)
        ]
        config_patch = {
            "yt": {
                "materialized_views": {
                    "scan_period": 100,
                    "max_rows_per_refresh": 1,
                },
            },
        }

        with Clique(2, config_patch=config_patch, export_query_log=True) as clique:
            clique.make_query(self.CREATE_MV_QUERY)
            assert clique.make_query(self.SELECT_MV_QUERY) == []

            statement_path = self._statement_path(clique)
            view_id = get(statement_path + "/@id")
            progress_root = clique.materialized_views_path + "/progress"
            progress_path = progress_root + "/" + view_id
            assert get(progress_path)["next_row_index"] == 2
            assert read_table("//tmp/target") == []

            write_table("<append=%true>//tmp/source", expected_rows)
            wait(lambda: read_table("//tmp/target") == expected_rows)

            assert ls(progress_root) == [view_id]
            assert get(progress_path + "/@type") == "document"
            progress = get(progress_path)
            assert progress["next_row_index"] == 34

            target_id = get("//tmp/target/@id")

            def get_refresh_queries():
                return {
                    row["query_id"]
                    for row in read_table(clique.query_log_table_path)
                    if row.get("type") == "QueryFinish"
                    and row.get("query_kind") == "Insert"
                    and row.get("is_initial_query") == 1
                    and target_id in row.get("query", "")
                }

            # max_rows_per_refresh is 1, so each refresh query must move 1 row.
            wait(lambda: len(get_refresh_queries()) == len(expected_rows))

    @authors("buyval01")
    def test_background_refresh_persists_initial_validation_error(self):
        config_patch = {
            "yt": {
                "materialized_views": {
                    "scan_period": 3000,
                },
            },
        }

        with Clique(2, config_patch=config_patch, export_query_log=True) as clique:
            clique.make_query(self.CREATE_MV_QUERY)
            view_id = get(self._statement_path(clique) + "/@id")
            progress_path = clique.materialized_views_path + "/progress/" + view_id

            remove("//tmp/source")
            create("table", "//tmp/source", attributes={"schema": self.SCHEMA})

            wait(
                lambda: "source table was replaced" in get(progress_path)["last_error"],
                ignore_exceptions=True,
                timeout=10)

    @authors("buyval01")
    def test_background_refresh_lock_contention(self):
        config_patch = {"yt": {"materialized_views": {"scan_period": 1000}}}
        expected_rows = [{"key": 1, "value": "new-1"}]

        with Clique(2, config_patch=config_patch, export_query_log=True) as clique:
            clique.make_query(self.CREATE_MV_QUERY)
            transaction_id = start_transaction(timeout=10000)
            try:
                lock("//tmp/target", mode="exclusive", tx=transaction_id)
                write_table("<append=%true>//tmp/source", expected_rows)
                time.sleep(2)
                assert read_table("//tmp/target") == []
                clique.make_query("SELECT 1")
            finally:
                abort_transaction(transaction_id)

            wait(lambda: read_table("//tmp/target") == expected_rows, timeout=10)

    @authors("buyval01")
    def test_background_refresh_query_failure(self):
        int_schema = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "int64"},
        ]
        create("table", "//tmp/int_target", attributes={"schema": int_schema})
        config_patch = {
            "yt": {
                "materialized_views": {
                    "scan_period": 100,
                },
            },
        }

        with Clique(1, config_patch=config_patch, export_query_log=True) as clique:
            clique.make_query(
                'CREATE MATERIALIZED VIEW mv TO "//tmp/int_target" '
                'AS SELECT key, accurateCast(value, \'Int64\') AS value FROM "//tmp/source"')
            view_id = get(self._statement_path(clique) + "/@id")
            progress_path = clique.materialized_views_path + "/progress/" + view_id

            write_table("//tmp/source", [{"key": 1, "value": "not-an-integer"}])

            wait(
                lambda: bool(get(progress_path)["last_error"]),
                ignore_exceptions=True,
                timeout=10)
            assert read_table("//tmp/int_target") == []
            assert get(progress_path)["next_row_index"] == 0

            expected_rows = [{"key": 2, "value": 42}]
            write_table("//tmp/source", [{"key": 2, "value": "42"}])
            wait(lambda: read_table("//tmp/int_target") == expected_rows, timeout=10)
            assert get(progress_path)["last_error"] == ""

    @authors("buyval01")
    def test_background_refresh_parent_transaction_abort(self):
        config_patch = {
            "yt": {
                "settings": {
                    "testing": {
                        "throw_exception_after_refresh_query": True,
                    },
                },
                "materialized_views": {
                    "scan_period": 100,
                },
            },
        }

        with Clique(1, config_patch=config_patch, export_query_log=True) as clique:
            clique.make_query(self.CREATE_MV_QUERY)
            view_id = get(self._statement_path(clique) + "/@id")
            progress_path = clique.materialized_views_path + "/progress/" + view_id
            target_id = get("//tmp/target/@id")

            write_table("//tmp/source", [{"key": 1, "value": "new-1"}])
            wait(
                lambda: "Testing exception after materialized view refresh query"
                in get(progress_path)["last_error"],
                ignore_exceptions=True,
                timeout=10)

            wait(
                lambda: any(
                    row.get("type") == "QueryFinish"
                    and row.get("is_initial_query") == 1
                    and target_id in row.get("query", "")
                    for row in read_table(clique.query_log_table_path)),
                ignore_exceptions=True)

            assert read_table("//tmp/target") == []
            assert get(progress_path)["next_row_index"] == 0

    @authors("buyval01")
    def test_database_scoping(self):
        create("map_node", "//tmp/my_db")
        create("table", "//tmp/target2", attributes={"schema": self.SCHEMA})
        write_table("//tmp/target", [{"key": 1, "value": "yt"}])
        write_table("//tmp/target2", [{"key": 2, "value": "my_db"}])

        config_patch = {"yt": {"database_directories": {"my_db": "//tmp/my_db"}}}
        with Clique(1, config_patch=config_patch) as clique:
            clique.make_query(
                'CREATE MATERIALIZED VIEW YT.mv TO "//tmp/target" '
                'AS SELECT key, value FROM "//tmp/source"')
            clique.make_query(
                'CREATE MATERIALIZED VIEW my_db.mv TO "//tmp/target2" '
                'AS SELECT key, value FROM "//tmp/source"')

            assert exists(self._statement_path(clique, "YT"))
            assert exists(self._statement_path(clique, "my_db"))
            assert clique.make_query("SELECT key, value FROM YT.mv") == [{"key": 1, "value": "yt"}]
            assert clique.make_query("SELECT key, value FROM my_db.mv") == [{"key": 2, "value": "my_db"}]

            clique.make_query("DROP VIEW my_db.mv")
            assert exists(self._statement_path(clique, "YT"))
            assert not exists(self._statement_path(clique, "my_db"))
            assert clique.make_query("SELECT key, value FROM YT.mv") == [{"key": 1, "value": "yt"}]

    @authors("buyval01")
    def test_drop_does_not_remove_recreated_view(self):
        create("table", "//tmp/target2", attributes={"schema": self.SCHEMA})

        with Clique(2) as clique:
            instances = clique.get_active_instances()
            clique.make_query(self.CREATE_MV_QUERY)

            def replace_view():
                wait_breakpoint("drop_mv")
                clique.make_direct_query(instances[0], "DROP VIEW mv")
                clique.make_direct_query(
                    instances[0],
                    'CREATE MATERIALIZED VIEW mv TO "//tmp/target2" '
                    'AS SELECT key, value FROM "//tmp/source"')
                release_breakpoint("drop_mv")

            thread = threading.Thread(target=replace_view)
            thread.start()

            with raises_yt_error(code=QueryFailedError):
                clique.make_direct_query(
                    instances[1],
                    "DROP VIEW mv",
                    settings={"chyt.testing.drop_table_breakpoint": get_breakpoint_node("drop_mv")})

            thread.join()

            statement_path = self._statement_path(clique)
            assert exists(statement_path)
            persisted_config = yson.loads(yson.get_bytes(get(statement_path + "/@value")))
            assert persisted_config["target_path"] == "//tmp/target2"
