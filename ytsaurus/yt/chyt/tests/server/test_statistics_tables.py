from yt_commands import (create, read_table, write_table, authors, sync_mount_table, print_debug, select_rows)
from yt.common import wait
from yt.wrapper import yson

from base import ClickHouseTestBase, Clique

import re
import pytest


def validate_statistics_row(row, expected):
    for key, value in expected.items():
        if key in ("secondary_query_ids"):
            assert sorted(row[key]) == sorted(value)
        elif key in ("select_queries"):
            assert len(row[key]) == len(value)
            for i in range(len(value)):
                assert re.match(value[i], row[key][i]) is not None
        else:
            assert row[key] == value


# ClickHouse can call read() several times, what leads to rubbish in 'select_queries' and 'secondary_query_ids'.
# Until ClickHouse fixed it, we remove them from query rows manually.
# CHYT-565
def remove_fake_subqueries(distributed_query, secondary_queries):
    real_subquery_ids = [query["query_id"] for query in secondary_queries]
    real_select_query_indexes = [query["select_query_index"] for query in secondary_queries]

    filtered_secondary_query_ids = []
    for query_id in distributed_query["secondary_query_ids"]:
        if query_id in real_subquery_ids:
            filtered_secondary_query_ids.append(query_id)
    distributed_query["secondary_query_ids"] = filtered_secondary_query_ids

    filtered_select_queries = []
    for i in range(len(distributed_query["select_queries"])):
        if i in real_select_query_indexes:
            filtered_select_queries.append(distributed_query["select_queries"][i])
    distributed_query["select_queries"] = filtered_select_queries


def get_secondary_query_count(expected_structure):
    result = len(expected_structure)
    for query_text, secondary_queries in expected_structure:
        result += get_secondary_query_count(secondary_queries)

    return result


def get_distributed_query_count(expected_structure):
    result = 1 if len(expected_structure) > 0 else 0
    for query_text, secondary_queries in expected_structure:
        result += get_distributed_query_count(secondary_queries)

    return result


def validate_query_statistics(clique, query, settings=None, expected_structure=[]):
    initial_query_id = clique.make_query(query, settings=settings, full_response=True).headers["X-ClickHouse-Query-Id"]

    def get_query_rows(table):
        return select_rows('* from [{}] where initial_query_id = "{}"'.format(table, initial_query_id))

    wait(lambda: len(get_query_rows("//tmp/distributed_queries")) == get_distributed_query_count(expected_structure))
    wait(lambda: len(get_query_rows("//tmp/secondary_queries")) == get_secondary_query_count(expected_structure))
    wait(lambda: len(get_query_rows("//tmp/ancestor_query_ids")) == get_secondary_query_count(expected_structure))

    instances = clique.get_active_instances()
    # Validation with several instances is more difficult.
    assert len(instances) == 1
    instance_address = instances[0].attributes["host"]
    clique_id = clique.get_clique_id()

    print_debug(yson.dumps(read_table("//tmp/distributed_queries"), yson_format="pretty"))

    distributed_queries = get_query_rows("//tmp/distributed_queries")
    secondary_queries = get_query_rows("//tmp/secondary_queries")
    ancestor_query_ids = get_query_rows("//tmp/ancestor_query_ids")

    print_debug(yson.dumps(distributed_queries, yson_format="pretty"))
    print_debug(yson.dumps(secondary_queries, yson_format="pretty"))
    print_debug(yson.dumps(ancestor_query_ids, yson_format="pretty"))

    def do_validate(query_id, expected_structure):
        filtered_distributed_queries = [query for query in distributed_queries if query["query_id"] == query_id]

        filtered_secondary_queries = [query for query in secondary_queries if query["parent_query_id"] == query_id]
        filtered_secondary_queries.sort(key=lambda query: query["select_query_index"])

        filtered_ancestor_query_ids = [row for row in ancestor_query_ids if row["parent_query_id"] == query_id]
        filtered_ancestor_query_ids.sort(key=lambda row: row["secondary_query_id"])

        # Query did not initialize any subqueries.
        if len(expected_structure) == 0:
            assert filtered_distributed_queries == []
            assert filtered_secondary_queries == []
            assert filtered_ancestor_query_ids == []
            return

        assert len(filtered_distributed_queries) == 1
        distributed_query = filtered_distributed_queries[0]
        remove_fake_subqueries(distributed_query, filtered_secondary_queries)

        validate_statistics_row(distributed_query, {
            "initial_query_id": initial_query_id,
            "query_id": query_id,
            "instance_cookie": 0,
            "instance_address": instance_address,
            "clique_id": clique_id,
            # "start_time": 0,
            # "finish_time": 1,
            # "statistics": {},
            "select_queries": [query for query, _ in expected_structure],
            "secondary_query_ids": [query["query_id"] for query in filtered_secondary_queries],
            # "proxy_address": "",
        })

        for i in range(len(filtered_secondary_queries)):
            secondary_query = filtered_secondary_queries[i]
            validate_statistics_row(secondary_query, {
                "initial_query_id": initial_query_id,
                "parent_query_id": query_id,
                # "query_id": "",
                "instance_cookie": 0,
                "instance_address": instance_address,
                # "start_time": 0,
                # "finish_time": 1,
                # "select_query_index": i,
                # "statistics": {},
            })
            do_validate(secondary_query["query_id"], expected_structure[i][1])

    do_validate(initial_query_id, expected_structure)


class TestStatistisTables(ClickHouseTestBase):
    CONFIG_PATCH = {
        "yt": {
            "query_statistics_reporter": {
                "enabled": True,
                "max_items_in_batch": 1,

                "distributed_queries_handler": {
                    "path": "//tmp/distributed_queries",
                },
                "secondary_queries_handler": {
                    "path": "//tmp/secondary_queries",
                },
                "ancestor_query_ids_handler": {
                    "path": "//tmp/ancestor_query_ids",
                },
            },
        },
    }

    def setup_method(self, method):
        super().setup_method(method)

        create(
            "table",
            "//tmp/distributed_queries",
            attributes={
                "dynamic": True,
                "atomicity": "none",
                "enable_dynamic_store_read": True,
                "schema": [
                    {"name": "initial_query_id", "type": "string", "sort_order": "ascending"},
                    {"name": "query_id", "type": "string", "sort_order": "ascending"},
                    {"name": "instance_cookie", "type": "int64"},
                    {"name": "instance_address", "type": "string"},
                    {"name": "clique_id", "type": "string"},
                    {"name": "start_time", "type": "timestamp"},
                    {"name": "finish_time", "type": "timestamp"},
                    {"name": "statistics", "type": "any"},
                    {"name": "select_queries", "type": "any"},
                    {"name": "secondary_query_ids", "type": "any"},
                    {"name": "proxy_address", "type": "string"},
                ],
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/distributed_queries")

        create(
            "table",
            "//tmp/secondary_queries",
            attributes={
                "dynamic": True,
                "atomicity": "none",
                "enable_dynamic_store_read": True,
                "schema": [
                    {"name": "initial_query_id", "type": "string", "sort_order": "ascending"},
                    {"name": "parent_query_id", "type": "string", "sort_order": "ascending"},
                    {"name": "query_id", "type": "string", "sort_order": "ascending"},
                    {"name": "instance_cookie", "type": "int64"},
                    {"name": "instance_address", "type": "string"},
                    {"name": "start_time", "type": "timestamp"},
                    {"name": "finish_time", "type": "timestamp"},
                    {"name": "select_query_index", "type": "int64"},
                    {"name": "statistics", "type": "any"},
                ],
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/secondary_queries")

        create(
            "table",
            "//tmp/ancestor_query_ids",
            attributes={
                "dynamic": True,
                "atomicity": "none",
                "enable_dynamic_store_read": True,
                "schema": [
                    {"name": "secondary_query_id", "type": "string", "sort_order": "ascending"},
                    {"name": "parent_query_id", "type": "string"},
                    {"name": "initial_query_id", "type": "string"},
                ],
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/ancestor_query_ids")

        for i in range(3):
            path = "//tmp/t{}".format(i)
            create(
                "table",
                path,
                attributes={
                    "schema": [
                        {"name": "a", "type": "int64", "sort_order": "ascending"},
                        {"name": "b", "type": "int64"},
                    ]
                }
            )
            write_table(path, [{"a": 0, "b": 1}])

    @authors("dakovalkov")
    def test_simple(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            query = 'select a from "//tmp/t0"'
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a FROM `//tmp/t0`$", []),
            ])

            query = 'select * from "//tmp/t0"'
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a, b FROM `//tmp/t0`$", []),
            ])

            query = 'select * from (select * from (select * from "//tmp/t0"))'
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a, b FROM `//tmp/t0`$", []),
            ])

    @authors("dakovalkov")
    def test_joins(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            query = 'select a from "//tmp/t0" as a join "//tmp/t1" as b using a'
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a FROM `//tmp/t0` AS a ALL INNER JOIN `//tmp/t1` AS b USING \\(a\\)$", []),
            ])

            query = 'select a from "//tmp/t0" as a join (select * from "//tmp/t1") as b using a'
            settings = {"chyt.execution.distribute_only_global_and_sorted_join": 0}
            validate_query_statistics(clique, query, settings, expected_structure=[
                ("^SELECT a FROM `//tmp/t0` AS a ALL INNER JOIN \\(SELECT \\* FROM `//tmp/t1`\\) AS b USING \\(a\\)$", [
                    ("^SELECT a FROM `//tmp/t1`$", []),
                ]),
            ])

            query = 'select a from "//tmp/t0" as a global join "//tmp/t1" as b using a'
            validate_query_statistics(clique, query, expected_structure=[
                # XXX(dakovalkov): global join reads redundant column 'b'.
                # https://github.com/ClickHouse/ClickHouse/issues/21478
                ("^SELECT a(, b)? FROM (YT.)?`//tmp/t1`$", []),
                ("^SELECT a FROM `//tmp/t0` AS a GLOBAL ALL INNER JOIN .* AS b USING \\(a\\)$", []),
            ])

            query = 'select a from "//tmp/t0" as a global join (select * from "//tmp/t1") as b using a'
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a(, b)? FROM `//tmp/t1`$", []),
                ("^SELECT a FROM `//tmp/t0` AS a GLOBAL ALL INNER JOIN .* AS b USING \\(a\\)$", []),
            ])

    @authors("dakovalkov")
    def test_nested_subqueries(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            # TODO(dakovalkov): When use_index_for_in_with_subqueries=1, there is an extra select query on the coordinator.
            # This subquery is ill-formed, since tables with the same name on the coordinator and other instances can mismatch.
            # May be ClickHouse should disable this optimization for remote storages.
            query = 'select a from "//tmp/t0" where a in (select a from "//tmp/t1") settings use_index_for_in_with_subqueries=0'
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a FROM `//tmp/t0` WHERE a IN \\(.*\\) SETTINGS use_index_for_in_with_subqueries = 0$", [
                    ("^SELECT a FROM `//tmp/t1`$", []),
                ]),
            ])

            query = 'select a from "//tmp/t0" where a global in (select a from "//tmp/t1")'
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a FROM `//tmp/t1`$", []),
                ("^SELECT a FROM `//tmp/t0` WHERE a GLOBAL IN \\(.*\\)$", []),
            ])

            query = '''select a from "//tmp/t0" where a global in (
                select a from "//tmp/t1" where a global in (
                    select a from "//tmp/t2"
                )
            )'''
            validate_query_statistics(clique, query, expected_structure=[
                ("^SELECT a FROM `//tmp/t2`$", []),
                ("^SELECT a FROM `//tmp/t1` WHERE a GLOBAL IN \\(.*\\)$", []),
                ("^SELECT a FROM `//tmp/t0` WHERE a GLOBAL IN \\(.*\\)$", []),
            ])

    @pytest.mark.xfail(reason="Fails due to KeyError: 'x-clickhouse-query-id'")
    @authors("max42")
    def test_distributed_insert_select(self):
        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": i} for i in range(100)]
        write_table("//tmp/t_in", rows, verbose=False)
        create("table", "//tmp/t_out", attributes={"schema": [{"name": "a", "type": "int64"}]})

        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            query = 'insert into `//tmp/t_out` select a from `//tmp/t_in` ' \
                    'settings parallel_distributed_insert_select = 1'
            validate_query_statistics(clique, query, expected_structure=[
                ("^INSERT INTO `//tmp/t_out` SELECT a FROM `//tmp/t_in` "
                 "SETTINGS parallel_distributed_insert_select = 1$", [
                     ("^INSERT INTO `//tmp/t_out` SELECT a from \\(.*\\)$", [])
                 ])
            ])
