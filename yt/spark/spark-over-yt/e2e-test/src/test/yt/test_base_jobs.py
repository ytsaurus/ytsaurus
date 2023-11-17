from spyt.submit import java_gateway, SparkSubmissionClient, SubmissionStatus

from yt_commands import (create, create_user, write_file, write_table, issue_token, read_table, print_debug, authors)

from yt.test_helpers import assert_items_equal

from base import SpytCluster, SpytTestBase

import pytest


class TestSpytBaseJobs(SpytTestBase):
    @authors("alex-shishkin")
    @pytest.mark.timeout(120)
    def test_id_job_cluster_mode(self):
        user = "spark_robot"
        create_user(user)
        token, _ = issue_token(user)

        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": i} for i in range(10)]
        write_table("//tmp/t_in", rows, verbose=False)
        script = \
            "from spyt import spark_session\n" \
            "with spark_session() as spark:\n" \
            "    spark.read.yt('//tmp/t_in').write.yt('//tmp/t_out')"
        create("file", '//tmp/id.py')
        write_file('//tmp/id.py', script.encode())

        with SpytCluster() as cluster:
            print_debug("Starting java gateway")
            with java_gateway() as gateway:
                submission_client = \
                    SparkSubmissionClient(gateway, cluster.proxy_address, cluster.discovery_path, "1.0.0", user, token)
                launcher = submission_client.new_launcher()
                launcher.set_app_resource("yt:///tmp/id.py")
                launcher.set_conf("spark.executor.cores", "1")
                launcher.set_conf("spark.executor.memory", "1g")
                launcher.set_conf("spark.pyspark.python", cluster.python_int_path)
                app_id = submission_client.submit(launcher)
                status = submission_client.wait_final(app_id)
                assert status is SubmissionStatus.FINISHED
            print_debug("Job finished")

        assert_items_equal(read_table("//tmp/t_out"), rows)
