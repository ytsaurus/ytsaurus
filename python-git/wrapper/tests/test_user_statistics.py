from .helpers import TEST_DIR, check, yatest_common

from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message

import yt.wrapper as yt

import pytest


@pytest.mark.skipif(yatest_common is not None, reason="It is not supported inside arcadia")
@pytest.mark.usefixtures("yt_env_with_rpc")
class TestUserStatistics(object):
    @add_failed_operation_stderrs_to_error_message
    def test_user_statistics_in_jobs(self):
        def write_statistics(row):
            yt.write_statistics({"row_count": 1})
            assert yt.get_blkio_cgroup_statistics()
            assert not yt.get_memory_cgroup_statistics()
            yield row

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}, {"y": 2}])
        op = yt.run_map(write_statistics, table, table, format=None, sync=False)
        op.wait()
        assert op.get_job_statistics()["custom"]["row_count"] == {"$": {"completed": {"map": {"count": 2, "max": 1, "sum": 2, "min": 1}}}}
        check(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

