from spyt.submit import SubmissionStatus
from spyt.testing.common.helpers import assert_items_equal, upload_job_file
from spyt.testing.public.base import SpytPublicTestBase

import pytest


class TestSpytJobs(SpytPublicTestBase):
    @pytest.mark.timeout(90)
    def test_id_job_cluster_mode(self):
        rows = [{"a": i} for i in range(10)]
        self.YT.create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64"}]})
        self.YT.write_table("//tmp/t_in", rows)

        upload_job_file(self.YT, 'yt/spark/spark-over-yt/e2e-test/src/test/yt/jobs/id.py', '//tmp/id.py')

        with self.spyt_cluster() as cluster:
            status = cluster.submit_cluster_job('//tmp/id.py')
            assert status is SubmissionStatus.FINISHED

        result = self.YT.read_table("//tmp/t_out")
        assert_items_equal(result, rows)
