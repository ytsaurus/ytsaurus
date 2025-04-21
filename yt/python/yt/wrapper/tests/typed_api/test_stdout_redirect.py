import os
import pytest
import sys
import typing

from yt.testlib import authors
from yt.wrapper.prepare_operation import TypedJob
from yt.testlib.helpers import set_config_option

import yt.wrapper as yt


@yt.yt_dataclass
class Row1:
    str_field: str
    int32_field: int


_dupped_stdout_fd = os.dup(sys.stdout.fileno())
_dupped_stderr_fd = os.dup(sys.stderr.fileno())


class SimpleMapperWithStdout(TypedJob):
    os.write(_dupped_stdout_fd, b"Some stdout print in def")
    os.write(_dupped_stderr_fd, b"Some stderr print in def")

    def __call__(self, row: Row1) -> typing.Iterator[Row1]:
        os.write(_dupped_stdout_fd, b"Some stdout print in mapper")
        os.write(_dupped_stderr_fd, b"Some stderr print in mapper")
        yield Row1(row.str_field + "_1", row.int32_field + 1)


@pytest.mark.usefixtures("yt_env_v4")
class TestYtMisc(object):

    @authors("denvr")
    @pytest.mark.parametrize("method", ["config", "spec"])
    @pytest.mark.parametrize("redirect", [True, False])
    def test_stderr_redirect_in_operations(self, method, redirect):
        table_in = "//tmp/table_row_in"
        table_out = "//tmp/table_row_out"
        table_err = "//tmp/table_row_err"

        yt.write_table_structured(table_in, Row1, [
            Row1(int32_field=10, str_field="key1"),
            Row1(int32_field=20, str_field="key2"),
        ])

        try:
            if method == "spec":
                yt.run_map(
                    SimpleMapperWithStdout(),
                    source_table=table_in,
                    destination_table=table_out,
                    stderr_table=table_err,
                    spec={
                        "mapper": {
                            "redirect_stdout_to_stderr": redirect,
                        },
                        "max_failed_job_count": 1,
                    },
                )
            elif method == "config":
                with set_config_option("pickling/redirect_stdout_to_stderr", redirect):
                    yt.run_map(
                        SimpleMapperWithStdout(),
                        source_table=table_in,
                        destination_table=table_out,
                        stderr_table=table_err,
                        spec={
                            "max_failed_job_count": 1,
                        },
                    )
        except yt.YtError:
            raised = True
        else:
            raised = False

        if redirect:
            assert list(yt.read_table_structured(table_out, Row1)) == [
                Row1(int32_field=11, str_field="key1_1"),
                Row1(int32_field=21, str_field="key2_1"),
            ]

            error_data = list(yt.read_table(table_err))[0]["data"]
            assert "Some stdout print in mapper" in error_data
            assert "Some stderr print in mapper" in error_data
            assert "Some stdout print in def" in error_data
            assert "Some stderr print in def" in error_data
        else:
            assert raised, "Exceptin w/o redirect"

        yt.remove(table_out, force=True)
        yt.remove(table_err, force=True)

    @authors("denvr")
    def test_stderr_redirect_in_operations_defaults(self):
        table_in = "//tmp/table2_row_in"
        table_out = "//tmp/table2_row_out"

        yt.write_table_structured(table_in, Row1, [
            Row1(int32_field=10, str_field="key1"),
            Row1(int32_field=20, str_field="key2"),
        ])

        def _build_spec(binary, source_table, destination_table, format, spec):
            file_paths = []
            stderr_table = None
            input_format = output_format = format

            spec_builder = yt.run_operation_commands.MapSpecBuilder() \
                .input_table_paths(source_table) \
                .output_table_paths(destination_table) \
                .stderr_table_path(stderr_table) \
                .begin_mapper() \
                    .command(binary) \
                    .file_paths(file_paths) \
                    .format(format) \
                    .input_format(input_format) \
                    .output_format(output_format) \
                .end_mapper() \
                .spec(spec)  # noqa
            return spec_builder.build(client=None)

        spec_python = _build_spec(SimpleMapperWithStdout(), table_in, table_out, None, {})
        assert spec_python["mapper"].get("redirect_stdout_to_stderr", False)

        spec_cap = _build_spec("cat", table_in, table_out, "yson", {})
        assert not spec_cap["mapper"].get("redirect_stdout_to_stderr", False)

        spec_yamr = _build_spec(SimpleMapperWithStdout(), table_in, table_out, None, {"mapper": {"use_yamr_descriptors": True}})
        assert not spec_yamr["mapper"].get("redirect_stdout_to_stderr", False)
