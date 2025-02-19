from base import ClickHouseTestBase, Clique

from yt_commands import (authors, create, exists, read_table, sync_unmount_table, get, alter_table, write_table, print_debug)

from yt.common import wait


class TestQueryLog(ClickHouseTestBase):
    @staticmethod
    def _get_query_log_patch(root_dir=None, max_rows_to_keep=None):
        return {
            "yt": {
                "system_log_table_exporters": {
                    "cypress_root_directory": root_dir or "",
                    "default": {
                        "enabled": root_dir is not None,
                        "max_rows_to_keep": max_rows_to_keep or 100000,
                    },
                },
            },
        }

    @authors("dakovalkov")
    def test_query_log_rotation(self):
        with Clique(1, config_patch=self._get_query_log_patch(max_rows_to_keep=10)) as clique:
            for i in range(11):
                query_id = clique.make_query("select queryID() as query_id")[0]["query_id"]

                wait(lambda: {"query_id": query_id} in clique.make_query("select query_id from system.query_log"))

                log_size = len(clique.make_query("select query_id from system.query_log"))
                assert 0 < log_size and log_size <= 10

    @authors("dakovalkov")
    def test_query_log_exporter_simple(self):
        root_dir = "//tmp/exporter"
        create("map_node", root_dir)

        with Clique(1, config_patch=self._get_query_log_patch(root_dir)) as clique:
            query_id = clique.make_query("select queryID() as query_id")[0]["query_id"]
            table_path = root_dir + "/query_log/0"
            wait(lambda: exists(table_path))
            wait(lambda: {"query_id": query_id} in read_table(table_path + "{query_id}"))

    @authors("dakovalkov")
    def test_change_log_version(self):
        root_dir = "//tmp/exporter"
        create("map_node", root_dir)

        table_path_0 = root_dir + "/query_log/0"
        table_path_1 = root_dir + "/query_log/1"

        patch = self._get_query_log_patch(root_dir)

        with Clique(1, config_patch=patch) as clique:
            clique.make_query("select 1")
            wait(lambda: exists(table_path_0))

        sync_unmount_table(table_path_0)

        with Clique(1, config_patch=patch) as clique:
            clique.make_query("select 1")
            wait(lambda: get(table_path_0 + "/@tablet_state") == "mounted")

        assert not exists(table_path_1)

        sync_unmount_table(table_path_0)
        schema = get(table_path_0 + "/@schema")
        schema.append({"name": "new_super_puper_column", "type": "string"})
        alter_table(table_path_0, schema=schema)

        with Clique(1, config_patch=patch) as clique:
            clique.make_query("select 1")
            wait(lambda: exists(table_path_1))

    @authors("dakovalkov")
    def test_log_table_extender(self):
        root_dir = "//tmp/exporter"
        create("map_node", root_dir)

        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])

        with Clique(1, config_patch=self._get_query_log_patch(root_dir)) as clique:
            query_id = clique.make_query("select initialQueryID() as query_id, a from `//tmp/t`")[0]["query_id"]

            table_path = root_dir + "/query_log/0"
            wait(lambda: exists(table_path))
            wait(lambda: len(read_table(table_path)) > 0)

            row = read_table(table_path)[0]
            assert len(row["chyt_version"]) > 0
            assert row["chyt_instance_cookie"] == 0

            def match(row, fields):
                for key, value in fields.items():
                    if key not in row or row[key] != value:
                        return False
                return True

            initial_query_fields = {"query_id": query_id, "type": "QueryFinish"}
            secondary_query_fields = {"initial_query_id": query_id, "type": "QueryFinish", "is_initial_query": 0}

            wait(lambda: len([r for r in read_table(table_path) if match(r, initial_query_fields)]) == 1)
            wait(lambda: len([r for r in read_table(table_path) if match(r, secondary_query_fields)]) == 1)

            initial_query_entry = [r for r in read_table(table_path) if match(r, initial_query_fields)][0]
            secondary_query_entry = [r for r in read_table(table_path) if match(r, secondary_query_fields)][0]

            print_debug(initial_query_entry)
            print_debug(secondary_query_entry)

            assert secondary_query_entry["chyt_secondary_query_ids"] == []
            assert initial_query_entry["chyt_secondary_query_ids"] == [secondary_query_entry["query_id"]]

            assert 'storage_distributor' in initial_query_entry["chyt_query_statistics"]
            assert 'storage_subquery' not in initial_query_entry["chyt_query_statistics"]
            assert 'secondary_query_source' not in initial_query_entry["chyt_query_statistics"]

            assert 'storage_distributor' not in secondary_query_entry["chyt_query_statistics"]
            assert 'storage_subquery' in secondary_query_entry["chyt_query_statistics"]
            assert 'secondary_query_source' in secondary_query_entry["chyt_query_statistics"]

    @authors("a-dyu")
    def test_http_header_logs(self):
        root_dir = "//tmp/exporter"
        create("map_node", root_dir)
        patch = self._get_query_log_patch(root_dir)
        patch["yt"]["http_header_blacklist"] = "authentication|x-clickhouse-user"

        def make_query_and_get_loged_query_headers(patch):
            with Clique(1, config_patch=patch) as clique:
                query_id = clique.make_query(
                    "select queryID() as query_id",
                    headers={
                        "Authentication": "some value",
                        "X-Clickhouse-User": "some value",
                        "Allowed-Header": "some value",
                    },
                )[0]["query_id"]
                table_path = root_dir + "/query_log/0"
                wait(lambda: exists(table_path))

                def match(row):
                    return row["query_id"] == query_id and row["type"] == "QueryFinish"

                wait(lambda: len([r for r in read_table(table_path) if match(r)]) > 0)

                row = [r for r in read_table(table_path) if match(r)][0]
                query_headers = row["http_headers"]
                return query_headers

        def get_headers(query_headers, expected_header):
            for [header, value] in query_headers:
                if header == expected_header:
                    return value
            return None

        query_headers = make_query_and_get_loged_query_headers(patch)
        assert get_headers(query_headers, "Authentication") is None
        assert get_headers(query_headers, "X-Clickhouse-User") is None
        assert get_headers(query_headers, "Allowed-Header") is None
        patch["yt"]["enable_http_header_log"] = True
        query_headers = make_query_and_get_loged_query_headers(patch)
        assert get_headers(query_headers, "Authentication") is None
        assert get_headers(query_headers, "X-Clickhouse-User") is None
        assert get_headers(query_headers, "Allowed-Header") == "some value"
