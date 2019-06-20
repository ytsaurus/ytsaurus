from .helpers import TEST_DIR, set_config_option, check

from yt.wrapper.spec_builders import MapSpecBuilder

import yt.wrapper as yt

import pytest

import os

@pytest.mark.usefixtures("yt_env")
class TestTmpfs(object):
    def test_tmpfs_configuration(self):
        def mapper(row):
            yield row

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])

        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command(mapper) \
            .end_mapper() \
            .input_table_paths(table) \
            .output_table_paths(table) \
            .ordered(False)

        with set_config_option("pickling/enable_tmpfs_archive", True):
            spec = spec_builder.build()

        assert spec["mapper"]["tmpfs_path"] == "tmpfs"
        assert "tmpfs_size" in spec["mapper"]

    def test_tmpfs_usage(self):
        def mapper(row):
            assert os.path.exists("tmpfs")
            assert "tmpfs" in yt.__file__
            yield row

        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [{"xyz": 1}])
        with set_config_option("pickling/enable_tmpfs_archive", True):
            yt.run_map(mapper, input_table, output_table)

        check([{"xyz": 1}], yt.read_table(output_table), ordered=False)
