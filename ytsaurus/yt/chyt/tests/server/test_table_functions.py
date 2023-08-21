from yt_commands import (create, authors, write_table, get,
                         raises_yt_error)

from base import ClickHouseTestBase, Clique, QueryFailedError

import yt.yson as yson


class TestTableFunctions(ClickHouseTestBase):
    @authors("dakovalkov")
    def test_yt_list_dir(self):
        create("map_node", "//tmp/dir")

        create(
            "table",
            "//tmp/dir/t0",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                ],
            },
        )
        create(
            "table",
            "//tmp/dir/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                ],
            },
        )
        write_table("//tmp/dir/t1", [{"a": 1}, {"a": 2}, {"a": 3}])

        create("map_node", "//tmp/dir/subdir")

        with Clique(1) as clique:
            # Explicit attributes.
            query = '''select $key, $path, type, dynamic, row_count from ytListNodes('//tmp/dir') order by $key'''
            assert clique.make_query(query) == [
                {
                    "$key": "subdir",
                    "$path": "//tmp/dir/subdir",
                    "type": "map_node",
                    "dynamic": None,
                    "row_count": None,
                },
                {
                    "$key": "t0",
                    "$path": "//tmp/dir/t0",
                    "type": "table",
                    "dynamic": 0,
                    "row_count": 0,
                },
                {
                    "$key": "t1",
                    "$path": "//tmp/dir/t1",
                    "type": "table",
                    "dynamic": 0,
                    "row_count": 3,
                },
            ]

            query = '''select key from ytListTables('//tmp/dir') order by key'''
            assert clique.make_query(query) == [{"key": "t0"}, {"key": "t1"}]

            # Implicit attributes and resource_usage attributes.
            query = '''select $key, id, tablet_count, disk_space, compression_ratio
                from ytListTables('//tmp/dir') order by $key'''
            assert clique.make_query(query) == [
                {
                    "$key": "t0",
                    "id": get("//tmp/dir/t0/@id"),
                    "tablet_count": 0,
                    "disk_space": 0,
                    "compression_ratio": 0,
                },
                {
                    "$key": "t1",
                    "id": get("//tmp/dir/t1/@id"),
                    "tablet_count": 0,
                    "disk_space": get("//tmp/dir/t1/@resource_usage/disk_space"),
                    "compression_ratio": get("//tmp/dir/t1/@compression_ratio"),
                },
            ]

            # Yson attributes.
            query = '''select ConvertYson(resource_usage, 'text') as resource_usage from ytListTables('//tmp/dir') order by $key'''
            resource_usages = [yson.loads(x["resource_usage"].encode()) for x in clique.make_query(query)]
            assert resource_usages == [
                get("//tmp/dir/t0/@resource_usage"),
                get("//tmp/dir/t1/@resource_usage"),
            ]

    @authors("dakovalkov")
    def test_yt_list_dir_link(self):
        create("map_node", "//tmp/dir")
        create("map_node", "//tmp/dir/subdir")
        create("table", "//tmp/dir/table")
        create("link", "//tmp/dir/table_link", attributes={"target_path": "//tmp/dir/table"})
        create("link", "//tmp/dir/subdir_link", attributes={"target_path": "//tmp/dir/subdir"})
        create("link", "//tmp/dir/broken_link", attributes={"target_path": "//this_path_does_not_exist"})

        with Clique(1) as clique:
            query = '''select $key, type from {}('//tmp/dir') order by $key'''

            assert clique.make_query(query.format('ytListNodes')) == [
                {"$key": "broken_link", "type": "link"},
                {"$key": "subdir", "type": "map_node"},
                {"$key": "subdir_link", "type": "link"},
                {"$key": "table", "type": "table"},
                {"$key": "table_link", "type": "link"},
            ]

            assert clique.make_query(query.format('ytListTables')) == [
                {"$key": "table", "type": "table"},
            ]

            assert clique.make_query(query.format('ytListNodesL')) == [
                {"$key": "broken_link", "type": "link"},
                {"$key": "subdir", "type": "map_node"},
                {"$key": "subdir_link", "type": "map_node"},
                {"$key": "table", "type": "table"},
                {"$key": "table_link", "type": "table"},
            ]

            assert clique.make_query(query.format('ytListTablesL')) == [
                {"$key": "table", "type": "table"},
                {"$key": "table_link", "type": "table"},
            ]

            query = '''select $key, key, type from ytListNodesL('//tmp/dir') order by $key'''
            assert clique.make_query(query) == [
                {"$key": "broken_link", "key": "broken_link", "type": "link"},
                {"$key": "subdir", "key": "subdir", "type": "map_node"},
                {"$key": "subdir_link", "key": "subdir", "type": "map_node"},
                {"$key": "table", "key": "table", "type": "table"},
                {"$key": "table_link", "key": "table", "type": "table"},
            ]

    @authors("dakovalkov")
    def test_yt_node_attributes(self):
        create("table", "//tmp/t0")
        create("table", "//tmp/t1")
        create("link", "//tmp/link", attributes={"target_path": "//tmp/t1"})

        with Clique(1) as clique:
            query = '''select $key, key, type from ytNodeAttributes('//tmp/t0')'''
            assert clique.make_query(query) == [
                {"$key": "t0", "key": "t0", "type": "table"},
            ]
            query = '''select $key, key, type
                from ytNodeAttributes('//tmp/t0', '//tmp/t1', '//tmp/t1', '//tmp/link', '//tmp/link&')'''
            assert clique.make_query(query) == [
                {"$key": "t0", "key": "t0", "type": "table"},
                {"$key": "t1", "key": "t1", "type": "table"},
                {"$key": "t1", "key": "t1", "type": "table"},
                {"$key": "link", "key": "t1", "type": "table"},
                {"$key": "link&", "key": "link", "type": "link"},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query('select key from ytNodeAttributes()')

            with raises_yt_error(QueryFailedError):
                clique.make_query("select key from ytNodeAttributes('//this_table_does_not_exist')")

    @authors("dakovalkov")
    def test_yt_list_log_tables(self):
        def create_log_table(path, table_num):
            create(
                "table",
                path,
                attributes={
                    "schema": [
                        {"name": "a", "type": "int64"},
                    ],
                },
            )
            write_table(path, [{"a": table_num}])

        create("map_node", "//tmp/dir1")
        create("map_node", "//tmp/dir1/1d")
        create("map_node", "//tmp/dir1/1h")
        create("map_node", "//tmp/dir1/30min")
        create("map_node", "//tmp/dir1/stream")
        create("map_node", "//tmp/dir1/stream/5min")

        table_paths = [
            "//tmp/dir1/1d/2021-01-01",
            "//tmp/dir1/1d/2021-01-02",
            "//tmp/dir1/1h/2021-01-03T00:00:00",
            "//tmp/dir1/1h/2021-01-03T01:00:00",
            "//tmp/dir1/30min/2021-01-03T02:00:00",
            "//tmp/dir1/30min/2021-01-03T02:30:00",
            "//tmp/dir1/stream/5min/2021-01-03T03:00:00",
            "//tmp/dir1/stream/5min/2021-01-03T03:05:00",
        ]

        for path in table_paths:
            create("table", path)

        create("map_node", "//tmp/dir2")
        create("map_node", "//tmp/dir2/30min")

        create("map_node", "//tmp/dir3")

        with Clique(1) as clique:
            query = "select $path from ytListLogTables('//tmp/dir1') order by $key"
            assert clique.make_query(query) == [{"$path": path} for path in table_paths]

            # Some time intervals already covered by existing tables.
            create("table", "//tmp/dir1/1h/2021-01-02T05:00:00")
            create("table", "//tmp/dir1/30min/2021-01-03T01:00:00")
            create("table", "//tmp/dir1/stream/5min/2021-01-03T02:30:00")
            create("table", "//tmp/dir1/stream/5min/2021-01-03T02:35:00")

            assert clique.make_query(query) == [{"$path": path} for path in table_paths]

            query = "select $path from ytListLogTables('//tmp/dir1', '2021-01-02') order by $key"
            assert clique.make_query(query) == [{"$path": path} for path in table_paths[1:]]

            query = "select $path from ytListLogTables('//tmp/dir1', '2021-01-02T20:00', '2021-01-03 02:40') order by $key"
            assert clique.make_query(query) == [{"$path": path} for path in table_paths[1:-2]]

            query = "select $path from ytListLogTables('//tmp/dir1', '', '2021-01-03 02:30') order by $key"
            assert clique.make_query(query) == [{"$path": path} for path in table_paths[:-3]]

            # Covers all 1d, 30min and 5min tables.
            create("table", "//tmp/dir1/1d/2021-01-03")

            query = "select $path from ytListLogTables('//tmp/dir1') order by $key"
            assert clique.make_query(query) == [
                {"$path": "//tmp/dir1/1d/2021-01-01"},
                {"$path": "//tmp/dir1/1d/2021-01-02"},
                {"$path": "//tmp/dir1/1d/2021-01-03"},
            ]

            query = "select $path from ytListLogTables('//tmp/dir2')"
            assert clique.make_query(query) == []

            create("file", "//tmp/dir2/30min/2021-01-03T01:00:00")

            assert clique.make_query(query) == []

            create("table", "//tmp/dir2/30min/2021-01-03T02:00:00")

            assert clique.make_query(query) == [
                {"$path": "//tmp/dir2/30min/2021-01-03T02:00:00"},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query("select $path from ytListLogTables('//this_dir_does_not_exist')")

            with raises_yt_error(QueryFailedError):
                clique.make_query("select $path from ytListLogTables('//tmp/dir3')")

    @authors("dakovalkov")
    def test_yt_tables(self):
        create("map_node", "//tmp/dir1")
        create("map_node", "//tmp/dir2")

        def create_test_table(path, table_num):
            create(
                "table",
                path,
                attributes={
                    "schema": [
                        {"name": "a", "type": "int64"},
                    ],
                },
            )
            write_table(path, [{"a": table_num}])

        create_test_table("//tmp/dir1/t0", 0)
        create_test_table("//tmp/dir1/t1", 1)
        create_test_table("//tmp/dir2/t2", 2)
        create_test_table("//tmp/dir2/t3", 3)
        create("map_node", "//tmp/dir1/subdir")

        create("map_node", "//tmp/log_dir")
        create("map_node", "//tmp/log_dir/1d")
        create("map_node", "//tmp/log_dir/1h")
        create_test_table("//tmp/log_dir/1d/2021-01-01", 0)
        create_test_table("//tmp/log_dir/1h/2021-01-01T23:00:00", 1)
        create_test_table("//tmp/log_dir/1h/2021-01-02T00:00:00", 2)

        with Clique(1) as clique:
            query = '''select * from ytTables('//tmp/dir1/t0', '//tmp/dir2/t2') order by a'''
            assert clique.make_query(query) == [{"a": 0}, {"a": 2}]

            query = '''select * from ytTables(ytListTables('//tmp/dir1')) order by a'''
            assert clique.make_query(query) == [{"a": 0}, {"a": 1}]

            query = '''
                select * from ytTables((
                    select path from ytListNodes('//tmp/dir2')
                    where key = 't2'
                ))
                order by a'''
            assert clique.make_query(query) == [{"a": 2}]

            query = '''select * from ytTables(ytListLogTables('//tmp/log_dir')) order by a'''
            assert clique.make_query(query) == [
                {"a": 0},
                {"a": 2},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query('select * form ytTables()')

            with raises_yt_error(QueryFailedError):
                # dir1 contains subdir.
                clique.make_query("select * from ytTables(ytListNodes('//tmp/dir1'))")
