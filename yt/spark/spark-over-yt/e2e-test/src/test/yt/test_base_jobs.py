from spyt.submit import SubmissionStatus

from yt_commands import (create, write_table, read_table, authors)

from yt.test_helpers import assert_items_equal

from base import SpytCluster, SpytTestBase

import pytest


class TestSpytBaseJobs(SpytTestBase):
    @authors("alex-shishkin")
    @pytest.mark.timeout(90)
    def test_id_job_cluster_mode(self):
        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": i} for i in range(10)]
        write_table("//tmp/t_in", rows, verbose=False)

        self.upload_job_file('yt/spark/spark-over-yt/e2e-test/src/test/yt/jobs/id.py', '//tmp/id.py')

        with SpytCluster(self.SPARK_USER, self.SPARK_TOKEN) as cluster:
            status = cluster.submit_cluster_job('//tmp/id.py')
            assert status is SubmissionStatus.FINISHED

        assert_items_equal(read_table("//tmp/t_out"), rows)
