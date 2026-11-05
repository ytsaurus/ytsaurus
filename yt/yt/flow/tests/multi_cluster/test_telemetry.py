import pytest

from yt.yt.flow.tests.test_telemetry.common.jobtruck import assert_jobtruck_tables, check_jobtruck_logs

from .test_pipeline import MultiClusterTestBase


class TestTelemetry(MultiClusterTestBase):
    @pytest.mark.authors(["pechatnov"])
    def test_telemetry(self):
        replica_cluster = self.first_replica_cluster_name
        replica_client = self.cluster_name_to_client[replica_cluster]
        self.prepare_environment(pipeline_replica_cluster=replica_cluster)
        assert_jobtruck_tables(
            self.client,
            self.pipeline_path,
            "replicated_table",
            replica_cluster=replica_cluster,
            replica_client=replica_client,
        )
        pipeline_config_path = self.prepare_pipeline_config()
        with self.start_flow_process_federation(
            node_config={"logging": {"flush_period": "100ms"}},
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_pipeline_state("working")
            check_jobtruck_logs(self.client, self.pipeline_path, replica_client=replica_client)
