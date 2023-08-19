from yt_env_setup import YTEnvSetup

from yt_commands import (
    alter_table, authors, concatenate, create, get_driver, get, insert_rows, map as yt_map,
    map_reduce as yt_map_reduce, merge, read_table, reduce as yt_reduce, set as yt_set, set_node_banned,
    sort as yt_sort, sync_create_cells, sync_freeze_table, sync_mount_table, wait, write_file, write_table)

from yt.common import YtError

import pytest

import time

CELL_TAG_CONVERSION = 10


def _assert_conditional_equality(lhs, rhs, equality_expected):
    if equality_expected:
        assert lhs == rhs
    else:
        assert lhs != rhs


def _test_schemas_match(table_path, schema_equality, schema_id_equality=None, expected_schema=None):
    if schema_id_equality is None:
        schema_id_equality = schema_equality

    table_schema = get("{}/@schema".format(table_path))
    if expected_schema is not None:
        table_schema = expected_schema
        assert schema_equality is True  # Sanity check

    is_external = get("{}/@external".format(table_path))
    chunk_ids = get("{}/@chunk_ids".format(table_path))

    assert len(chunk_ids) > 0

    for chunk_id in chunk_ids:
        chunk_schema = get("#{}/@schema".format(chunk_id))
        _assert_conditional_equality(table_schema, chunk_schema, schema_equality)

        chunk_schema_id = get("#{}/@schema_id".format(chunk_id))

        if not is_external:
            table_schema_id = get("{}/@schema_id".format(table_path))
            _assert_conditional_equality(table_schema_id, chunk_schema_id, schema_id_equality)
        else:
            external_cell_tag = get("{}/@external_cell_tag".format(table_path))

            table_id = get("{}/@id".format(table_path))
            table_schema_id = get("#{}/@schema_id".format(table_id), driver=get_driver(external_cell_tag - CELL_TAG_CONVERSION))
            table_schema_id != chunk_schema_id


##################################################################


class TestChunkSchemas(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    @authors("h0pless")
    def test_simple_file(self):
        content = b"something very funky indeed"
        create("file", "//tmp/file")
        write_file("//tmp/file", content)
        chunk_ids = get("//tmp/file/@chunk_ids")
        for chunk_id in chunk_ids:
            with pytest.raises(YtError, match="Attribute \"schema_id\" is not found"):
                get("#{}/@schema_id".format(chunk_id))

    @authors("h0pless")
    def test_simple_static_1(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", {"key": 123, "value": "hi"})
        write_table("//tmp/table", {"key": 1234, "value": "hello"})
        _test_schemas_match("//tmp/table", schema_equality=True)

    @authors("h0pless")
    def test_simple_static_2(self):
        create("table", "//tmp/table", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]})
        write_table("//tmp/table", {"key": 123, "value": "hello"})
        write_table("//tmp/table", {"key": 1234, "value": "hello"})
        _test_schemas_match("//tmp/table", schema_equality=True)

    @authors("h0pless")
    def test_simple_static_3(self):
        create("table", "//tmp/table", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]})

        write_table(
            "<chunk_sort_columns=[{name=key;sort_order=descending};{name=value;sort_order=descending}];append=true>//tmp/table",
            {"key": 123, "value": "howdy"})
        write_table(
            "<chunk_sort_columns=[{name=key;sort_order=descending};{name=value;sort_order=descending}];append=true>//tmp/table",
            {"key": 1234, "value": "howdydo"})
        _test_schemas_match("//tmp/table", schema_equality=False)

    @authors("h0pless")
    def test_simple_static_alter(self):
        create("table", "//tmp/table", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]})
        write_table("//tmp/table", {"key": 123, "value": "hello"})
        write_table("//tmp/table", {"key": 1234, "value": "hello"})

        schema = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
            {"name": "something_new", "type": "double"},
        ]

        alter_table("//tmp/table", schema=schema)
        _test_schemas_match("//tmp/table", schema_equality=False)

    @authors("h0pless")
    def test_simple_dynamic(self):
        sync_create_cells(1)
        create("table", "//tmp/dynam", attributes={
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]})
        sync_mount_table("//tmp/dynam")
        insert_rows("//tmp/dynam", [{"key": 123, "value": "hi"}])
        insert_rows("//tmp/dynam", [{"key": 1234, "value": "hello"}])
        sync_freeze_table("//tmp/dynam")
        _test_schemas_match("//tmp/dynam", schema_equality=True)


##################################################################


class ChunkSchemasMulticellBase(YTEnvSetup):
    NUM_MASTERS = 1
    # Using six nodes to test repair jobs
    NUM_NODES = 7
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1

    @authors("h0pless")
    def test_multicell_static_alter(self):
        create("table", "//tmp/table", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "external_cell_tag": 12})
        write_table("//tmp/table", {"key": 123, "value": "hello"})
        write_table("//tmp/table", {"key": 1234, "value": "hello"})

        schema = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
            {"name": "something_new", "type": "double"},
        ]

        alter_table("//tmp/table", schema=schema)
        _test_schemas_match("//tmp/table", schema_equality=False)

    @authors("h0pless")
    def test_teleportation_through_merge_operation(self):
        create("table", "//tmp/input", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "external_cell_tag": 11})
        create("table", "//tmp/output", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
                {"name": "something_else", "type": "string"}
            ],
            "external_cell_tag": 12})

        write_table("<append=true>//tmp/input", {"key": 123, "value": "hi"})

        merge(mode="unordered", in_=["//tmp/input"], out="//tmp/output")

        expected_schema = get("//tmp/input/@schema")
        _test_schemas_match("//tmp/output", schema_equality=True, schema_id_equality=False, expected_schema=expected_schema)

    @authors("h0pless")
    @pytest.mark.parametrize('external', [True, False])
    def test_map_operation(self, external):
        create("table", "//tmp/input", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]}, external=external)
        create("table", "//tmp/output", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
                {"name": "something_else", "type": "string"}
            ]}, external=external)

        write_table("//tmp/input", {"key": 123, "value": "hi"})

        yt_map(in_="//tmp/input", out="<append=true>//tmp/output", command="cat")

        _test_schemas_match("//tmp/output", schema_equality=True)

    @authors("h0pless")
    def test_sort_operation(self):
        create("table", "//tmp/input", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]})

        write_table("//tmp/input", {"key": 420, "value": "hello"})
        write_table("//tmp/input", {"key": 80085, "value": "howdy"})
        write_table("//tmp/input", {"key": 1, "value": "hi"})
        write_table("//tmp/input", {"key": 1337, "value": "Ehehe"})

        create("table", "//tmp/output", attributes={
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
                {"name": "something_else", "type": "string"}
            ]})

        yt_sort(in_="//tmp/input", out="<append=%true>//tmp/output", sort_by=["key"])
        _test_schemas_match("//tmp/output", schema_equality=True)

    @authors("h0pless")
    def test_reduce_operation(self):
        create("table", "//tmp/input", attributes={
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ]})

        create("table", "//tmp/output", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
                {"name": "something_else", "type": "string"}
            ]})

        write_table("//tmp/input", {"key": 1, "value": "hi"})
        write_table("//tmp/input", {"key": 420, "value": "hello"})
        write_table("//tmp/input", {"key": 1337, "value": "Ehehe"})
        write_table("//tmp/input", {"key": 80085, "value": "howdy"})

        yt_reduce(in_=["//tmp/input"], out=["<append=%true>//tmp/output"], reduce_by=["key"], command="cat")
        _test_schemas_match("//tmp/output", schema_equality=True)

    @authors("h0pless")
    def test_map_reduce_operation(self):
        create("table", "//tmp/input", attributes={
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ]})

        create("table", "//tmp/output", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
                {"name": "something_else", "type": "string"}
            ]})

        write_table("//tmp/input", {"key": 1, "value": "hi"})
        write_table("//tmp/input", {"key": 420, "value": "hello"})
        write_table("//tmp/input", {"key": 1337, "value": "Ehehe"})
        write_table("//tmp/input", {"key": 80085, "value": "howdy"})

        yt_map_reduce(in_=["//tmp/input"], out=["<append=%true>//tmp/output"], sort_by=["key"], reducer_command="cat")
        _test_schemas_match("//tmp/output", schema_equality=True)

    @authors("h0pless")
    def test_replication_job(self):
        create("table", "//tmp/table", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "replication_factor": self.NUM_NODES,
            "external_cell_tag": 11})
        write_table("//tmp/table", {"key": 123, "value": "hi"})

        _test_schemas_match("//tmp/table", schema_equality=True)

    def _wait_for_merge(self, table_path, merge_mode, account="tmp"):
        yt_set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        rows = read_table(table_path)
        assert get("{}/@resource_usage/chunk_count".format(table_path)) > 1

        yt_set("{}/@chunk_merger_mode".format(table_path), merge_mode)
        yt_set("//sys/accounts/{}/@merge_job_rate_limit".format(account), 10)
        yt_set("//sys/accounts/{}/@chunk_merger_node_traversal_concurrency".format(account), 1)
        wait(lambda: get("{}/@resource_usage/chunk_count".format(table_path)) == 1)
        assert read_table(table_path) == rows

    @authors("h0pless")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_merge_chunks(self, merge_mode):
        create("table", "//tmp/table", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]})
        write_table("<append=true>//tmp/table", {"key": 123, "value": "hi"})
        write_table("<append=true>//tmp/table", {"key": 1234, "value": "hello"})
        write_table("<append=true>//tmp/table", {"key": 1337, "value": "leet"})
        write_table("<append=true>//tmp/table", {"key": 80085, "value": "teehee"})

        self._wait_for_merge("//tmp/table", merge_mode)

        _test_schemas_match("//tmp/table", schema_equality=True)

    def get_node_address(self, node_id=0, cluster_index=0):
        if cluster_index == 0:
            env = self.Env
        else:
            env = self.remote_envs[cluster_index - 1]
        return "localhost:" + str(env.configs["node"][node_id]["rpc_port"])

    @authors("h0pless")
    def test_repair_chunk(self):
        create("table", "//tmp/table", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "erasure_codec": "reed_solomon_3_3"})
        write_table("//tmp/table", [{"key": i, "value": "ello ere chum"} for i in range(100)])

        chunk_ids = get("//tmp/table/@chunk_ids")
        chunk_id = chunk_ids[0]
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        address_to_ban = str(replicas[3])
        set_node_banned(address_to_ban, True)
        time.sleep(3.0)

        read_result = read_table("//tmp/table",
                                 table_reader={
                                     "unavailable_chunk_strategy": "restore",
                                     "pass_count": 1,
                                     "retry_count": 1,
                                 })
        assert read_result == [{"key": i, "value": "ello ere chum"} for i in range(100)]

        replicas = set(map(str, replicas))
        new_replicas = set(map(str, get("#{0}/@stored_replicas".format(chunk_id))))

        has_repaired_replica = False
        for node_id in range(self.NUM_NODES):
            address = self.get_node_address(node_id)
            if address in new_replicas and address not in replicas:
                has_repaired_replica = True

        assert has_repaired_replica

        _test_schemas_match("//tmp/table", schema_equality=True)

        set_node_banned(address_to_ban, False)


##################################################################


class TestChunkSchemasMulticell(ChunkSchemasMulticellBase):
    @authors("h0pless")
    def test_multicell_static_1(self):
        create("table", "//tmp/output", attributes={"external_cell_tag": 10})
        create("table", "//tmp/table1", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "external_cell_tag": 11})
        write_table("//tmp/table1", {"key": 123, "value": "hi"})

        create("table", "//tmp/table2", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "external_cell_tag": 12})
        write_table("//tmp/table2", {"key": 1234, "value": "hello"})

        create("table", "//tmp/table3", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "external_cell_tag": 10})
        write_table("//tmp/table3", {"key": 12345, "value": "howdy"})

        create("table", "//tmp/table_wtih_a_known_schema", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ],
            "external_cell_tag": 10})

        concatenate(["//tmp/table1", "//tmp/table2"], "//tmp/output")

        _test_schemas_match("//tmp/table1", schema_equality=True)
        _test_schemas_match("//tmp/table2", schema_equality=True)
        _test_schemas_match("//tmp/table3", schema_equality=True)
        _test_schemas_match("//tmp/output", schema_equality=True, schema_id_equality=False)


##################################################################


class TestChunkSchemasMulticellPortal(ChunkSchemasMulticellBase):
    ENABLE_TMP_PORTAL = True
