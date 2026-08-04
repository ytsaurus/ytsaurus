from base import ClickHouseTestBase, Clique, QueryFailedError

from helpers import get_breakpoint_node, release_breakpoint, wait_breakpoint

from yt_commands import authors, create, create_user, exists, get, raises_yt_error, wait, write_table

import threading


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

        with Clique(2) as clique:
            instances = clique.get_active_instances()
            statement_path = self._statement_path(clique)

            clique.make_query(self.CREATE_MV_QUERY)

            assert exists(statement_path)
            assert get(statement_path + "/@chyt_object_type") == "materialized_view"
            assert get(statement_path + "/@value") == self.PERSISTED_MV_STATEMENT
            assert get(statement_path + "/@source_path") == "//tmp/source"
            assert get(statement_path + "/@target_path") == "//tmp/target"

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

        with Clique(1, enable_object_repository=False) as clique:
            with raises_yt_error(message_pattern="Clique doesn't have configured CypressObjectRepository"):
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
            assert get(statement_path + "/@target_path") == "//tmp/target2"
