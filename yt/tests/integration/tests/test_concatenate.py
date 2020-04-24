from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

import pytest

##################################################################

class TestConcatenate(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @authors("ermolovd")
    def test_simple_concatenate(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})
        assert read_table("//tmp/t1") == [{"key": "x"}]

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"key": "y"})
        assert read_table("//tmp/t2") == [{"key": "y"}]

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}] * 2

    @authors("ermolovd")
    def test_sorted(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})
        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        assert read_table("//tmp/t1") == [{"key": "x"}]
        assert get("//tmp/t1/@sorted", "true")

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"key": "y"})
        sort(in_="//tmp/t2", out="//tmp/t2", sort_by="key")
        assert read_table("//tmp/t2") == [{"key": "y"}]
        assert get("//tmp/t2/@sorted", "true")

        create("table", "//tmp/union")
        sort(in_="//tmp/union", out="//tmp/union", sort_by="key")
        assert get("//tmp/union/@sorted", "true")

        concatenate(["//tmp/t2", "//tmp/t1"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "y"}, {"key": "x"}]
        assert get("//tmp/union/@sorted", "false")

    @authors("ermolovd")
    def test_infer_schema(self):
        create("table", "//tmp/t1",
           attributes = {
               "schema": [{"name": "key", "type": "string"}]
           })
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2",
           attributes = {
               "schema": [{"name": "key", "type": "string"}]
           })
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert get("//tmp/union/@schema") == get("//tmp/t1/@schema")

    @authors("ermolovd")
    def test_infer_schema_many_columns(self):
        row = {"a": "1", "b": "2", "c": "3", "d": "4"}
        for table_path in ["//tmp/t1", "//tmp/t2"]:
            create("table", table_path,
               attributes = {
                   "schema": [
                       {"name": "b", "type": "string"},
                       {"name": "a", "type": "string"},
                       {"name": "d", "type": "string"},
                       {"name": "c", "type": "string"},
                   ]
               })
            write_table(table_path, [row])

        create("table", "//tmp/union")
        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert read_table("//tmp/union") == [row] * 2
        assert normalize_schema(get("//tmp/union/@schema")) == make_schema([
            {"name": "a", "type": "string", "required": False},
            {"name": "b", "type": "string", "required": False},
            {"name": "c", "type": "string", "required": False},
            {"name": "d", "type": "string", "required": False},
        ], strict=True, unique_keys=False)

    @authors("ermolovd")
    def test_conflict_missing_output_schema_append(self):
        create("table", "//tmp/t1",
           attributes = {
               "schema": [{"name": "key", "type": "string"}]
           })
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2",
           attributes = {
               "schema": [{"name": "key", "type": "string"}]
           })
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union")
        empty_schema = get("//tmp/union/@schema")

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        assert get("//tmp/union/@schema_mode") == "weak"
        assert get("//tmp/union/@schema") == empty_schema

    @authors("ermolovd")
    @pytest.mark.parametrize("append", [False, True])
    def test_output_schema_same_as_input(self, append):
        schema =  [{"name": "key", "type": "string"}]
        create("table", "//tmp/t1", attributes = {"schema": schema})
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2", attributes = {"schema": schema})
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union", attributes = {"schema": schema})
        old_schema = get("//tmp/union/@schema")

        if append:
            concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        else:
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert get("//tmp/union/@schema") == old_schema

    @authors("ermolovd")
    def test_impossibility_to_concatenate_into_sorted_table(self):
        schema =  [{"name": "key", "type": "string"}]
        create("table", "//tmp/t1", attributes = {"schema": schema})
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2", attributes = {"schema": schema})
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union", attributes = {"schema": schema})
        sort(in_="//tmp/union", out="//tmp/union", sort_by=["key"])

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

    @authors("ermolovd")
    @pytest.mark.parametrize("append", [False, True])
    def test_compatible_schemas(self, append):
        create("table", "//tmp/t1",
           attributes = {
               "schema": [{"name": "key", "type": "string"}]
           })
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2",
           attributes = {
               "schema": [{"name": "value", "type": "string"}]
           })
        write_table("//tmp/t2", {"value": "y"})

        create("table", "//tmp/union",
           attributes = {
               "schema": [{"name": "key", "type": "string"}, {"name": "value", "type": "string"}]
           })
        old_schema = get("//tmp/union/@schema")

        if append:
            concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        else:
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert read_table("//tmp/union") == [{"key": "x"}, {"value": "y"}]
        assert get("//tmp/union/@schema") == old_schema

    @authors("ermolovd")
    def test_incompatible_schemas(self):
        create("table", "//tmp/t1",
           attributes = {
               "schema": [{"name": "key", "type": "string"}]
           })

        create("table", "//tmp/t2",
           attributes = {
               "schema": [{"name": "value", "type": "int64"}]
           })

        create("table", "//tmp/t3",
           attributes = {
               "schema": [{"name": "other_column", "type": "string"}]
           })

        create("table", "//tmp/union",
           attributes = {
               "schema": [{"name": "key", "type": "string"}, {"name": "value", "type": "string"}]
           })

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t3"], "//tmp/union")

    @authors("ermolovd")
    def test_different_input_schemas_no_output_schema(self):
        create("table", "//tmp/t1",
           attributes = {
               "schema": [{"name": "key", "type": "string"}]
           })

        create("table", "//tmp/t2",
           attributes = {
               "schema": [{"name": "value", "type": "int64"}]
           })

        create("table", "//tmp/union")
        empty_schema = get("//tmp/union/@schema")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert get("//tmp/union/@schema_mode") == "weak"
        assert get("//tmp/union/@schema") == empty_schema

    @authors("ermolovd")
    def test_strong_output_schema_weak_input_schemas(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        create("table", "//tmp/union",
           attributes = {
               "schema": [{"name": "value", "type": "int64"}]
           })

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

    @authors("ermolovd")
    def test_append_to_sorted_weak_schema(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/union")

        write_table("//tmp/union", {"key": "y"})
        sort(in_="//tmp/union", out="//tmp/union", sort_by="key")
        assert get("//tmp/union/@schema_mode", "weak")
        assert get("//tmp/union/@sorted", "true")

        concatenate(["//tmp/t1"], "<append=true>//tmp/union")

        assert get("//tmp/union/@sorted", "false")

        assert read_table("//tmp/t1") == [{"key": "x"}]
        assert read_table("//tmp/union") == [{"key": "y"}, {"key": "x"}]

    @authors("ermolovd")
    def test_concatenate_unique_keys(self):
        create("table", "//tmp/t1",
           attributes = {
               "schema": make_schema([{"name": "key", "type": "string", "sort_order": "ascending"}], unique_keys=True)
           })
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2",
           attributes = {
               "schema": make_schema([{"name": "key", "type": "string", "sort_order": "ascending"}], unique_keys=True)
           })
        write_table("//tmp/t2", {"key": "x"})

        create("table", "//tmp/union")
        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert get("//tmp/union/@sorted", "false")

        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "x"}]
        assert get("//tmp/union/@schema/@unique_keys", "false")

    @authors("ermolovd", "kiselyovp")
    def test_empty_concatenate(self):
        create("table", "//tmp/union")
        orig_schema = get("//tmp/union/@schema")
        concatenate([], "//tmp/union")
        assert get("//tmp/union/@schema_mode") == "weak"
        assert get("//tmp/union/@schema") == orig_schema

    @authors("ermolovd")
    def test_lost_complex_column(self):
        create("table", "//tmp/t1",
           attributes = {
               "schema": make_schema([
                   {"name": "list_column", "type_v3": list_type("int64")},
                   {"name": "int_column", "type_v3": "int64"},
               ])
           })

        create("table", "//tmp/union",
           attributes = {
               "schema": make_schema([
                   {"name": "int_column", "type_v3": "int64"},
               ], strict=False)
           })
        with raises_yt_error("is missing in strict part of output schema"):
            concatenate(["//tmp/t1"], "//tmp/union")

    @authors("gritukan")
    def test_sorted_concatenation(self):
        def make_rows(values):
            return [{"a": value} for value in values]

        create("table", "//tmp/in",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}])
           })

        for x in [1, 3, 2]:
            write_table("<chunk_key_column_count=1;append=true>//tmp/in", make_rows([x]))
        assert get("//tmp/in/@chunk_count") == 3

        assert read_table("//tmp/in") == make_rows([1, 3, 2])

        create("table", "//tmp/out",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])
           })

        concatenate(["//tmp/in"], "//tmp/out")

        assert read_table("//tmp/out") == make_rows([1, 2, 3])
        assert get("//tmp/out/@sorted")

    @authors("gritukan")
    def test_sorted_concatenation_comparator(self):
        create("table", "//tmp/in",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}])
           })

        write_table("<chunk_key_column_count=1;append=true>//tmp/in", [{"a": 1, "b": "x"}, {"a": 2, "b": "z"}])
        write_table("<chunk_key_column_count=1;append=true>//tmp/in", [{"a": 1, "b": "y"}, {"a": 1, "b": "z"}])
        assert read_table("//tmp/in") == [
            {"a": 1, "b": "x"},
            {"a": 2, "b": "z"},
            {"a": 1, "b": "y"},
            {"a": 1, "b": "z"},
        ]

        create("table", "//tmp/out",
           attributes={
               "schema": make_schema(
                   [
                       {"name": "a", "type": "int64", "sort_order": "ascending"},
                       {"name": "b", "type": "string"},
                   ])
           })

        concatenate(["//tmp/in"], "//tmp/out")
        assert read_table("//tmp/out") == [
            {"a": 1, "b": "y"},
            {"a": 1, "b": "z"},
            {"a": 1, "b": "x"},
            {"a": 2, "b": "z"},
        ]
        assert get("//tmp/out/@sorted")

    @authors("gritukan")
    def test_sorted_concatenation_with_overlapping_ranges(self):
        def make_rows(values):
            return [{"a": value} for value in values]

        create("table", "//tmp/in",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}])
           })

        write_table("<chunk_key_column_count=1;append=true>//tmp/in", make_rows([1, 3]))
        write_table("<chunk_key_column_count=1;append=true>//tmp/in", make_rows([2, 4]))
        assert get("//tmp/in/@chunk_count") == 2

        assert read_table("//tmp/in") == make_rows([1, 3, 2, 4])

        create("table", "//tmp/out1",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])
           })

        create("table", "//tmp/out2",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}])
           })

        with raises_yt_error(SortOrderViolation):
            concatenate(["//tmp/in"], "//tmp/out1")

        concatenate(["//tmp/in"], "//tmp/out2")
        assert read_table("//tmp/out2") == make_rows([1, 3, 2, 4])
        assert get("//tmp/out2/@sorted") == False

    @authors("gritukan")
    def test_sorted_concatenation_schema(self):
        def make_row(a, b, c):
            return {"a": a, "b": b, "c": c}

        create("table", "//tmp/in1",
           attributes={
               "schema": make_schema(
                   [
                       {"name": "a", "type": "int64"},
                       {"name": "b", "type": "int64"},
                       {"name": "c", "type": "int64"},
                   ])
           })

        write_table("<chunk_key_column_count=3;append=true>//tmp/in1", [make_row(3, 3, 3), make_row(4, 4, 4)])
        write_table("<chunk_key_column_count=2;append=true>//tmp/in1", [make_row(1, 1, 1), make_row(2, 2, 2)])
        assert read_table("//tmp/in1") == [make_row(3, 3, 3), make_row(4, 4, 4), make_row(1, 1, 1), make_row(2, 2, 2)]

        create("table", "//tmp/out1",
           attributes={
               "schema": make_schema(
                   [
                       {"name": "a", "type": "int64", "sort_order": "ascending"},
                       {"name": "b", "type": "int64", "sort_order": "ascending"},
                       {"name": "c", "type": "int64"},
                   ])
           })

        concatenate(["//tmp/in1"], "//tmp/out1")
        assert read_table("//tmp/out1") == [make_row(1, 1, 1), make_row(2, 2, 2), make_row(3, 3, 3), make_row(4, 4, 4)]

        create("table", "//tmp/out2",
           attributes={
               "schema": make_schema(
                   [
                       {"name": "a", "type": "int64", "sort_order": "ascending"},
                       {"name": "b", "type": "int64", "sort_order": "ascending"},
                       {"name": "c", "type": "int64", "sort_order": "ascending"},
                   ])
           })

        with raises_yt_error(SchemaViolation):
            concatenate(["//tmp/in1"], "//tmp/out2")

        create("table", "//tmp/in2",
            attributes={
                "schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])
            })
        write_table("//tmp/in2", [{"a": 1}, {"a": 3}])

        create("table", "//tmp/in3",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])
           })
        write_table("//tmp/in3", [{"a": 2}, {"a": 4}])

        create("table", "//tmp/out3")
        concatenate(["//tmp/in2", "//tmp/in3"], "//tmp/out3")
        assert get("//tmp/out3/@sorted") == False
        assert read_table("//tmp/out3") == [{"a": 1}, {"a": 3}, {"a": 2}, {"a": 4}]

    @authors("gritukan")
    def test_sorted_concatenation_append(self):
        def make_rows(values):
            return [{"a": value} for value in values]

        create("table", "//tmp/in1",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}])
           })
        write_table("<chunk_key_column_count=1;append=true>//tmp/in1", make_rows([1, 2]))

        create("table", "//tmp/in2",
               attributes={
                   "schema": make_schema([{"name": "a", "type": "int64"}])
               })
        write_table("<chunk_key_column_count=1;append=true>//tmp/in2", make_rows([5, 6]))

        create("table", "//tmp/out1",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])
           })
        write_table("//tmp/out1", make_rows([3, 4]))

        with raises_yt_error(SortOrderViolation):
            concatenate(["//tmp/in1"], "<append=true>//tmp/out1")

        concatenate(["//tmp/in2"], "<append=true>//tmp/out1")
        assert read_table("//tmp/out1") == make_rows([3, 4, 5, 6])

        create("table", "//tmp/in3",
           attributes={
               "schema": make_schema(
                   [
                       {"name": "a", "type": "int64", "sort_order": "ascending"},
                   ],
                   unique_keys=False)
           })

        write_table("<chunk_key_column_count=1;append=true>//tmp/in3", make_rows([2, 2]))

        concatenate(["//tmp/in3", "//tmp/in3"], "<append=true>//tmp/in3")
        assert read_table("//tmp/in3") == make_rows([2, 2, 2, 2, 2, 2])

        create("table", "//tmp/in4",
            attributes={
             "schema": make_schema(
                 [
                     {"name": "a", "type": "int64", "sort_order": "ascending"},
                 ],
                 unique_keys=True)
            })
        write_table("<chunk_key_column_count=1;append=true>//tmp/in4", make_rows([2]))
        with raises_yt_error(UniqueKeyViolation):
            concatenate(["//tmp/in4", "//tmp/in4"], "<append=true>//tmp/in4")

    @authors("gritukan")
    def test_sorted_concatenate_unique_keys(self):
        def make_rows(values):
            return [{"a": value} for value in values]

        create("table", "//tmp/in1",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}])
           })
        for x in [1, 4, 2, 3]:
            write_table("<chunk_key_column_count=1;chunk_unique_keys=true;append=true>//tmp/in1", make_rows([x]))

        create("table", "//tmp/out1",
           attributes={
               "schema": make_schema(
               [
                   {"name": "a", "type": "int64", "sort_order": "ascending"}
               ],
               unique_keys=True)
           })
        concatenate(["//tmp/in1"], "//tmp/out1")
        assert read_table("//tmp/out1") == make_rows([1, 2, 3, 4])

        write_table("<chunk_key_column_count=1;append=true>//tmp/in1", make_rows([5]))
        with raises_yt_error(SchemaViolation):
            concatenate(["//tmp/in1"], "//tmp/out1")

        create("table", "//tmp/in2",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}])
           })
        for x in [1, 3, 2, 2, 4, 5]:
            write_table("<chunk_key_column_count=1;chunk_unique_keys=true;append=true>//tmp/in2", make_rows([x]))

        with raises_yt_error(UniqueKeyViolation):
            concatenate(["//tmp/in2"], "//tmp/out1")

        create("table", "//tmp/in3",
           attributes={
               "schema": make_schema([{"name": "a", "type": "int64"}])
           })
        for x in [4, 3, 2]:
            write_table("<chunk_key_column_count=1;chunk_unique_keys=true;append=true>//tmp/in3", make_rows([x]))

        create("table", "//tmp/out2",
           attributes={
               "schema": make_schema(
               [
                   {"name": "a", "type": "int64", "sort_order": "ascending"}
               ],
               unique_keys=True)
           })
        write_table("//tmp/out2", make_rows([1, 2]))
        with raises_yt_error(UniqueKeyViolation):
            concatenate(["//tmp/in3"], "<append=true>//tmp/out2")

        concatenate(["//tmp/in3"], "//tmp/out2")
        assert read_table("//tmp/out2") == make_rows([2, 3, 4])

    @authors("gritukan")
    def test_input_with_custom_transaction(self):
        custom_tx = start_transaction()

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        with pytest.raises(YtError):
            concatenate(["//tmp/in"], "//tmp/out")
        concatenate(['<transaction_id="{}">//tmp/in'.format(custom_tx)], "//tmp/out")

        assert read_table("//tmp/out") == [{"foo": "bar"}]

##################################################################

class TestConcatenateMulticell(TestConcatenate):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("gritukan")
    def test_concatenate_imported_chunks(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 1})
        write_table("//tmp/t1", [{"a": "b"}])

        create("table", "//tmp/t2", attributes={"external_cell_tag": 2})
        op = merge(mode="unordered",
              in_=["//tmp/t1"],
              out="//tmp/t2")
        op.track()
        chunk_id = get_singular_chunk_id("//tmp/t2")
        assert len(get("#" + chunk_id + "/@exports")) > 0

        tx1 = start_transaction()
        lock("//tmp/t2", mode="exclusive", tx=tx1)
        remove("//tmp/t2", tx=tx1)

        create("table", "//tmp/t3", attributes={"external_cell_tag": 1})

        tx2 = start_transaction()
        concatenate(['<transaction_id="{}">//tmp/t2'.format(tx2)], "//tmp/t3")

        assert read_table("//tmp/t3") == [{"a": "b"}]

        abort_transaction(tx1)
        assert read_table("//tmp/t3") == [{"a": "b"}]

    @authors("shakurov")
    def test_concatenate_between_primary_and_secondary_shards(self):
        create("table", "//tmp/src1", attributes={"external": False})
        write_table("//tmp/src1", [{"a": "b"}])
        create("table", "//tmp/src2", attributes={"external_cell_tag": 1})
        write_table("//tmp/src2", [{"c": "d"}])

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 2})
        create("table", "//tmp/p/dst", attributes={"exit_cell_tag": 1})

        tx = start_transaction()
        concatenate(["//tmp/src1", "//tmp/src2"], "//tmp/p/dst", tx=tx)
        commit_transaction(tx)

        assert read_table("//tmp/p/dst") == [{"a": "b"}, {"c": "d"}]

class TestConcatenatePortal(TestConcatenateMulticell):
    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("shakurov")
    def test_concatenate_between_secondary_shards(self):
        create("table", "//tmp/src1", attributes={"external_cell_tag": 1})
        write_table("//tmp/src1", [{"a": "b"}])
        create("table", "//tmp/src2", attributes={"external_cell_tag": 2})
        write_table("//tmp/src2", [{"c": "d"}])

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 3})
        create("table", "//tmp/p/dst", attributes={"external_cell_tag": 1})

        tx = start_transaction()
        concatenate(["//tmp/src1", "//tmp/src2"], "//tmp/p/dst", tx=tx)
        commit_transaction(tx)

        assert read_table("//tmp/p/dst") == [{"a": "b"}, {"c": "d"}]

class TestConcatenateRpcProxy(TestConcatenate):
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

