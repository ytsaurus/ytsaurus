from base import ClickHouseTestBase, Clique

from yt_commands import (authors, create, exists, read_table, sync_unmount_table, get, alter_table)

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
