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
class TestTypedApi(object):

    @authors("denvr")
    @pytest.mark.parametrize("method", ["config", "spec"])
    def test_stderr_redirect(self, method):
        table_in = "//tmp/table_row_in"
        table_out = "//tmp/table_row_out"
        table_err = "//tmp/table_row_err"

        yt.write_table_structured(table_in, Row1, [
            Row1(int32_field=10, str_field="key1"),
            Row1(int32_field=20, str_field="key2"),
        ])

        if method == "spec":
            yt.run_map(
                SimpleMapperWithStdout(),
                source_table=table_in,
                destination_table=table_out,
                stderr_table=table_err,
                spec={
                    "mapper": {
                        "redirect_stdout_to_stderr": True,
                    },
                    "max_failed_job_count": 1,
                },
            )
        elif method == "config":
            with set_config_option("pickling/redirect_stdout_to_stderr", True):
                yt.run_map(
                    SimpleMapperWithStdout(),
                    source_table=table_in,
                    destination_table=table_out,
                    stderr_table=table_err,
                    spec={
                        "max_failed_job_count": 1,
                    },
                )

        assert list(yt.read_table_structured(table_out, Row1)) == [
            Row1(int32_field=11, str_field="key1_1"),
            Row1(int32_field=21, str_field="key2_1"),
        ]

        error_data = list(yt.read_table(table_err))[0]["data"]
        assert "Some stdout print in mapper" in error_data
        assert "Some stderr print in mapper" in error_data
        assert "Some stdout print in def" in error_data
        assert "Some stderr print in def" in error_data
