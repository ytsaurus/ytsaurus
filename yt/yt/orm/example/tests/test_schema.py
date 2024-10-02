import yt.wrapper as yt

import pytest


class TestSchema:
    @pytest.mark.parametrize(
        "table_name,fields",
        [
            pytest.param("books_to_publishers", "[meta.id], [meta.id2]"),
            pytest.param("history_events", "object_type, object_id"),
            pytest.param(
                "history_index", "object_type, history_event_type, index_event_type, object_id"
            ),
        ],
    )
    def test_hash_column(self, example_env, table_name, fields):
        path = yt.ypath_join("//home/example/db", table_name, "@schema")
        schema = example_env.yt_client.get(path)
        assert "hash" == schema[0]["name"]
        assert f"farm_hash({fields})" == schema[0]["expression"]

    @pytest.mark.parametrize(
        "table_name,has_spec_etc,has_spec",
        [
            pytest.param("mother_ships", True, False, id="without_no_etc_and_without_columns"),
            pytest.param("authors", True, False, id="without_no_etc_and_all_columns"),
            pytest.param("illustrators", False, True, id="with_no_etc_and_without_columns"),
        ],
    )
    def test_etc(self, example_env, table_name, has_spec_etc, has_spec):
        def is_column_exist(schema, column_name):
            return any([field for field in schema if field["name"] == column_name])

        schema = example_env.yt_client.get("//home/example/db/{}/@schema".format(table_name))
        assert has_spec_etc == is_column_exist(schema, "spec.etc")
        assert has_spec == is_column_exist(schema, "spec")

    def test_status_and_one_to_many(self, example_env):
        schema = example_env.yt_client.get("//home/example/db/editors/@schema")
        assert [column for column in schema if column["name"] == "status.etc"]
