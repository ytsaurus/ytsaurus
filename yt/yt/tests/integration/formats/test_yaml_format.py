from yt_env_setup import YTEnvSetup

from yt_commands import (authors, create, read_table, write_table, get, set, sync_create_cells, merge,
                         sync_freeze_table, select_rows, wait, raises_yt_error)
import yt.environment.init_operations_archive as init_operations_archive

import yaml

import yt.yson as yson

import pytest

# Most of the logic is tested in unittests, see yaml_{writer,parser}_ut.cpp.
# Here we just test that the format is correctly registered and can be used in
# structured and tabular commands by various comparisons between the results
# of commands in native format and in YAML format.


@authors("max42")
@pytest.mark.enabled_multidaemon
class TestYamlFormat(YTEnvSetup):
    # We use operation archive table as a source of both complex structured data
    # and complex tabular data by retrieving its attributes and rows respectively.

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    TABLE_PATH = "//sys/operations_archive/ordered_by_id"

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_operation_heavy_attributes_archivation": True,
            "operations_cleaner": {
                "enable": True,
                "analysis_period": 100,
                "archive_batch_timeout": 100,
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
                "clean_delay": 50,
                "fetch_batch_size": 1,
                "max_operation_age": 100,
            },
        },
    }
    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        # Initialize operation archive table and fill it with some data.
        super(TestYamlFormat, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": 1}])

        def get_row_count():
            rows = select_rows(f"sum(1) from [{self.TABLE_PATH}] group by 1")
            if len(rows) > 0:
                return rows[0]["sum(1)"]
            return 0

        for i in range(2):
            merge(in_="//tmp/t", out="//tmp/t", spec={"force_transform": True})

        wait(lambda: get_row_count() >= 2)
        sync_freeze_table(self.TABLE_PATH)

        # Teach yaml how to interpret !yt/attrnode tag.
        def construct_attrnode(loader, node):
            lst = loader.construct_sequence(node, deep=True)
            assert len(lst) == 2
            return yson.to_yson_type(lst[1], attributes=lst[0])
        yaml.SafeLoader.add_constructor("!yt/attrnode", construct_attrnode)

        # Work around yaml being too smart to recognize ISO timestamps - we do not want to parse
        # datetime objecs from timestamps to simplify comparisong of parsed YAML and native data.
        def construct_timestamp(loader, node):
            return loader.construct_scalar(node)
        yaml.SafeLoader.add_constructor("tag:yaml.org,2002:timestamp", construct_timestamp)

    def get_complex_struct_native(self):
        return get(self.TABLE_PATH + "/@")

    def get_complex_struct_yaml(self, write_uint_tag=False):
        return get(self.TABLE_PATH + "/@", is_raw=True, output_format=yson.to_yson_type("yaml", attributes={"write_uint_tag": write_uint_tag}))

    def read_complex_table_native(self):
        return read_table(self.TABLE_PATH)

    def read_complex_table_yaml(self, write_uint_tag=False):
        return read_table(self.TABLE_PATH, is_raw=True, output_format=yson.to_yson_type("yaml", attributes={"write_uint_tag": write_uint_tag}))

    def sanitize_struct(self, struct):
        keys_to_del = ("unflushed_timestamp", "access_time", "access_counter", "flush_lag_time")
        for key in keys_to_del:
            if key in struct:
                del struct[key]
        return struct

    def test_get(self):
        # Read a complex structure in YAML and in native format and compare them as Python dicts
        from_native = self.get_complex_struct_native()
        from_yaml = yaml.safe_load(self.get_complex_struct_yaml())
        assert self.sanitize_struct(from_native) == self.sanitize_struct(from_yaml)

    def test_set(self):
        # Read a complex structure in YAML and write it back to a document; compare the content with
        # the original structure in native format.
        create("document", "//tmp/d", attributes={"value": {}})
        yaml_content = self.get_complex_struct_yaml(write_uint_tag=True)
        from_native = self.get_complex_struct_native()
        set("//tmp/d", yaml_content, is_raw=True, input_format="yaml")
        assert self.sanitize_struct(get("//tmp/d")) == self.sanitize_struct(from_native)

    def test_unssupported_tabular_commands(self):
        # Test that tabular commands are not supported in YAML format.
        with raises_yt_error("YAML is supported only for structured data"):
            read_table(self.TABLE_PATH, is_raw=True, output_format="yaml")
        create("table", "//tmp/t2")
        with raises_yt_error("YAML is supported only for structured data"):
            write_table("//tmp/t2", b"a: 1", is_raw=True, input_format="yaml")

    # Tests below are commented because we decided to not support YAML format for tables for now,
    # but they may be useful in the future.
    #
    # def test_read_table(self):
    #     # Read a complex table row sequence in YAML and in native format and compare them as Python dicts.
    #     from_native = self.read_complex_table_native()
    #     from_yaml = list(yaml.load_all(self.read_complex_table_yaml(), Loader=yaml.SafeLoader))
    #     assert len(from_native) >= 2
    #     assert from_native == from_yaml
    #
    # def test_write_table(self):
    #     create("table", "//tmp/t2")
    #     yaml_content = self.read_complex_table_yaml(write_uint_tag=True)
    #     from_native = self.read_complex_table_native()
    #     assert len(from_native) >= 2
    #     write_table("//tmp/t2", yaml_content, is_raw=True, input_format="yaml")
    #     assert read_table("//tmp/t2") == from_native
