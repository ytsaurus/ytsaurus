from helpers import get_breakpoint_node, release_breakpoint, wait_breakpoint

from yt_commands import authors, create, get_driver, raises_yt_error, remove, write_table
from yt.common import wait

from base import ClickHouseTestBase, Clique

import threading


@authors("a-romanov")
class TestClickHouseCrossCluster(ClickHouseTestBase):
    NUM_REMOTE_CLUSTERS = 2
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

    def test_remote_schema_cache(self):
        remote_driver = get_driver(cluster="remote_0")
        schema = [{"name": "key", "type": "int64"}]
        rows = [{"key": 1}]
        create("table", "//tmp/schema_cache", attributes={"schema": schema}, driver=remote_driver)
        write_table("//tmp/schema_cache", rows, driver=remote_driver)

        patch = {
            "yt": {
                "table_schema_cache": {
                    "capacity": 10 * 1024**2,
                },
            },
        }

        with Clique(1, config_patch=patch) as clique:
            hit_counter = clique.get_profiler().counter(
                "clickhouse/yt/table_schema_cache/hit",
                tags={"remote_cluster": "remote_0"})

            before = hit_counter.get_delta()
            assert clique.make_query("select * from `remote_0://tmp/schema_cache`") == rows
            wait(lambda: hit_counter.get_delta() == before)

            assert clique.make_query("select * from `remote_0://tmp/schema_cache`") == rows
            wait(lambda: hit_counter.get_delta() > before)

    def test_set_operations_over_three_clusters(self):
        schema = [{"name": "key", "type": "int64"}]
        table_path = "//tmp/set_operations"

        create("table", table_path, attributes={"schema": schema})
        write_table(table_path, [{"key": 1}, {"key": 2}])

        remote_0_driver = get_driver(cluster="remote_0")
        create("table", table_path, attributes={"schema": schema}, driver=remote_0_driver)
        write_table(table_path, [{"key": 2}, {"key": 3}], driver=remote_0_driver)

        remote_1_driver = get_driver(cluster="remote_1")
        create("table", table_path, attributes={"schema": schema}, driver=remote_1_driver)
        write_table(table_path, [{"key": 2}, {"key": 4}], driver=remote_1_driver)

        with Clique(1) as clique:
            assert clique.make_query(f"""
                select key from (
                    select key from `{table_path}`
                    union all
                    select key from `remote_0:{table_path}`
                    union all
                    select key from `remote_1:{table_path}`
                ) order by key
            """) == [
                {"key": 1},
                {"key": 2},
                {"key": 2},
                {"key": 2},
                {"key": 3},
                {"key": 4},
            ]

            assert clique.make_query(f"""
                select key from (
                    select key from `{table_path}`
                    intersect distinct
                    select key from `remote_0:{table_path}`
                    intersect distinct
                    select key from `remote_1:{table_path}`
                ) order by key
            """) == [{"key": 2}]

            assert clique.make_query(f"""
                select key from (
                    select key from `{table_path}`
                    except distinct
                    select key from `remote_0:{table_path}`
                    except distinct
                    select key from `remote_1:{table_path}`
                ) order by key
            """) == [{"key": 1}]
