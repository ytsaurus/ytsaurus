from yt_env_setup import YTEnvSetup
from yt_commands import *


class TestExplainQuery(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    def _create_test_table(self, path):
        test_schema = make_schema(
            [
                {"name": "hash", "type": "int64", "sort_order": "ascending", "expression": "int64(farm_hash(a))"},
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"}
            ])

        create_dynamic_table(path, schema=test_schema)

    @authors("avmatrosov")
    def test_explain_query_group_by_primary_key(self):
        sync_create_cells(1)
        self._create_test_table("//tmp/t")
        sync_mount_table("//tmp/t")

        response = explain_query("* from [//tmp/t] group by hash, a, b")
        assert "common_prefix_with_primary_key" in response["query"]

        response = explain_query("* from [//tmp/t] group by a, b")
        assert "common_prefix_with_primary_key" in response["query"]

        response = explain_query("* from [//tmp/t] group by a, c")
        assert "common_prefix_with_primary_key" not in response["query"]

    @authors("avmatrosov")
    def test_explain_sort_merge_join(self):
        sync_create_cells(1)
        first_test_schema = make_schema(
            [
                {"name": "hash", "type": "int64", "sort_order": "ascending", "expression": "int64(farm_hash(cid))"},
                {"name": "cid", "type": "int64", "sort_order": "ascending"},
                {"name": "pid", "type": "int64", "sort_order": "ascending"},
                {"name": "__shard__", "type": "int64"},
                {"name": "PhraseID", "type": "int64"}
            ])

        create_dynamic_table("//tmp/first", schema=first_test_schema)

        second_test_schema = make_schema(
            [
                {"name": "ExportIDHash", "type": "int64", "sort_order": "ascending", "expression": "int64(farm_hash(ExportID))"},
                {"name": "ExportID", "type": "int64", "sort_order": "ascending"},
                {"name": "GroupExportID", "type": "int64", "sort_order": "ascending"},
                {"name": "PhraseID", "type": "uint64", "sort_order": "ascending"},
                {"name": "UpdateTime", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "int64"},
            ])

        create_dynamic_table("//tmp/second", schema=second_test_schema)

        third_test_schema = make_schema(
            [
                {"name": "hash", "type": "int64", "sort_order": "ascending", "expression": "int64(farm_hash(pid))"},
                {"name": "pid", "type": "int64", "sort_order": "ascending"},
                {"name": "__shard__", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "int64"}
            ])

        create_dynamic_table("//tmp/third", schema=third_test_schema)

        fourth_test_schema = make_schema(
            [
                {"name": "hash", "type": "int64", "sort_order": "ascending", "expression": "int64(farm_hash(cid))"},
                {"name": "cid", "type": "int64", "sort_order": "ascending"},
                {"name": "__shard__", "type": "int64", "sort_order": "ascending"},
                {"name": "ExportID", "type": "int64"}
            ])

        create_dynamic_table("//tmp/fourth", schema=fourth_test_schema)

        sync_mount_table("//tmp/first")
        sync_mount_table("//tmp/second")
        sync_mount_table("//tmp/third")
        sync_mount_table("//tmp/fourth")

        def _check_response(query_string, foreign_key_prefixes, common_key_prefixes):
            response = explain_query(query_string)
            joins = response["query"]["joins"][0]
            size = len(foreign_key_prefixes)

            assert len(foreign_key_prefixes) == len(common_key_prefixes)
            assert len(joins) == size
            for index in range(size):
                assert(joins[index]["foreign_key_prefix"] == foreign_key_prefixes[index])
                assert(joins[index]["common_key_prefix"] == common_key_prefixes[index])

        query_string = """* from [//tmp/first] D
            left join [//tmp/fourth] C on D.cid = C.cid
            left join [//tmp/second] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)
            left join [//tmp/third] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)"""

        _check_response(query_string, [2, 4, 3], [2, 2, 0])

        query_string = """* from [//tmp/first] D
            left join [//tmp/fourth] C on (D.cid,D.__shard__) = (C.cid,C.__shard__)
            left join [//tmp/second] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)
            left join [//tmp/third] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)"""

        _check_response(query_string, [3, 4, 3], [2, 2, 0])

        query_string = """* from [//tmp/first] D
            left join [//tmp/second] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)
            left join [//tmp/fourth] C on (D.cid,D.__shard__) = (C.cid,C.__shard__)
            left join [//tmp/third] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)"""

        _check_response(query_string, [4, 3, 3], [3, 2, 0])

    @authors("avmatrosov")
    def test_explain_order_by_primary_key_prefix(self):
        sync_create_cells(1)
        self._create_test_table("//tmp/t")
        sync_mount_table("//tmp/t")

        response = explain_query("* from [//tmp/t] order by hash, a limit 10")
        assert response["query"]["is_ordered_scan"]

        response = explain_query("* from [//tmp/t] order by hash, a, b limit 10")
        assert response["query"]["is_ordered_scan"]

        response = explain_query("* from [//tmp/t] order by a, b limit 10")
        assert not response["query"]["is_ordered_scan"]

    @authors("avmatrosov")
    def test_explain_where_expression(self):
        sync_create_cells(1)
        self._create_test_table("//tmp/t")
        sync_mount_table("//tmp/t")

        response = explain_query("* from [//tmp/t] where a < b AND b > c")
        assert response["query"]["where_expression"] == "(a < b) AND (b > c)"

    @authors("avmatrosov")
    def test_explain_limits(self):
        sync_create_cells(1)

        test_schema = make_schema(
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"},
            ])

        create_dynamic_table("//tmp/t", schema=test_schema)
        sync_mount_table("//tmp/t")

        response = explain_query("* from [//tmp/t] where a IN (1, 2, 10) AND b BETWEEN (1 and 9)")

        expected_ranges = [['[0#1, 1#1]', '[0#1, 1#9, 0#<Max>]'],
                           ['[0#2, 1#1]', '[0#2, 1#9, 0#<Max>]'],
                           ['[0#10, 1#1]', '[0#10, 1#9, 0#<Max>]']]
        expected_key_trie = "(key0, {  })\n0#1:\n  (key1, { [0#1:0#9] })\n0#2:\n  (key1, { [0#1:0#9] })\n0#10:\n  (key1, { [0#1:0#9] })"

        assert(response["query"]["ranges"] == expected_ranges)
        assert(response["query"]["key_trie"] == expected_key_trie)
