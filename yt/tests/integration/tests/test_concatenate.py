from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

from yt.yson import to_yson_type, loads
from yt.environment.helpers import assert_items_equal

from time import sleep
import pytest

##################################################################

class TestConcatenate(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

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
        assert get("//tmp/union/@schema") == make_schema([
            {"name": "a", "type": "string"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "string"},
            {"name": "d", "type": "string"},
        ], strict=True, unique_keys=False)

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

    def test_strong_output_schema_weak_input_schemas(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        create("table", "//tmp/union",
           attributes = {
               "schema": [{"name": "value", "type": "int64"}]
           })

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

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
