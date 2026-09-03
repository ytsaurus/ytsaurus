from helpers import get_breakpoint_node, release_breakpoint, wait_breakpoint

from yt_commands import authors, create, get_driver, raises_yt_error, remove, write_table

from base import ClickHouseTestBase, Clique

import threading


@authors("a-romanov")
class TestClickHouseCrossCluster(ClickHouseTestBase):
    NUM_REMOTE_CLUSTERS = 1
    NUM_TEST_PARTITIONS = 1

    def test_static_table_join(self):
        remote_driver = get_driver(cluster="remote_0")
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]

        create("table", "//tmp/local", attributes={"schema": schema})
        create("table", "//tmp/remote", attributes={"schema": schema}, driver=remote_driver)
        write_table("//tmp/local", [{"key": 1, "value": "local-1"}, {"key": 2, "value": "local-2"}])
        write_table(
            "//tmp/remote",
            [{"key": 2, "value": "remote-2"}, {"key": 3, "value": "remote-3"}],
            driver=remote_driver)

        with Clique(2) as clique:
            assert clique.make_query(
                "select key, value from `remote_0://tmp/remote` order by key") == [
                    {"key": 2, "value": "remote-2"},
                    {"key": 3, "value": "remote-3"},
                ]

            assert clique.make_query("""
                select l.key as key, l.value as local_value, r.value as remote_value
                from `//tmp/local` as l
                join `remote_0://tmp/remote` as r using key
                order by key
            """) == [
                {"key": 2, "local_value": "local-2", "remote_value": "remote-2"},
            ]

            description = clique.make_query(
                "describe table `remote_0://tmp/remote`",
                settings={
                    "chyt.conversion.low_cardinality.mode": "from_statistics",
                    "chyt.conversion.low_cardinality.threshold": 10,
                })
            value_column = next(column for column in description if column["name"] == "value")
            assert value_column["type"].startswith("LowCardinality")

    def test_rejects_unsupported_remote_table_operations(self):
        remote_driver = get_driver(cluster="remote_0")
        schema = [{"name": "key", "type": "int64"}]
        create("table", "//tmp/static", attributes={"schema": schema}, driver=remote_driver)
        create(
            "table",
            "//tmp/dynamic",
            attributes={"dynamic": True, "schema": schema},
            driver=remote_driver)

        with Clique(1) as clique:
            with raises_yt_error("Cross-cluster reads support static tables only"):
                clique.make_query("select * from `remote_0://tmp/dynamic`")

            with raises_yt_error("Cross-cluster tables are supported only in SELECT queries"):
                clique.make_query("insert into `remote_0://tmp/static` values (1)")

            create("table", "//tmp/output", attributes={"schema": schema})
            with raises_yt_error("Cross-cluster tables are supported only in SELECT queries"):
                clique.make_query(
                    "insert into `//tmp/output` select * from `remote_0://tmp/static`")

    def test_independent_cross_cluster_snapshots(self):
        remote_driver = get_driver(cluster="remote_0")
        schema = [{"name": "key", "type": "int64", "sort_order": "ascending"}]
        rows = [{"key": 1}, {"key": 2}]

        create("table", "//tmp/local_snapshot", attributes={"schema": schema})
        create(
            "table",
            "//tmp/remote_snapshot",
            attributes={"schema": schema},
            driver=remote_driver)
        write_table("//tmp/local_snapshot", rows)
        write_table("//tmp/remote_snapshot", rows, driver=remote_driver)

        with Clique(1) as clique:
            def remove_tables():
                wait_breakpoint("cross_cluster_snapshot")
                remove("//tmp/local_snapshot")
                remove("//tmp/remote_snapshot", driver=remote_driver)
                release_breakpoint("cross_cluster_snapshot")

            thread = threading.Thread(target=remove_tables)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": "sync",
                "chyt.testing.chunk_spec_fetcher_breakpoint": get_breakpoint_node("cross_cluster_snapshot"),
            }
            result = clique.make_query("""
                select l.key as key
                from `//tmp/local_snapshot` as l
                join `remote_0://tmp/remote_snapshot` as r using key
                order by key
            """, settings=settings)

            thread.join()
            assert result == rows
