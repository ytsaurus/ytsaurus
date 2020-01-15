from . import templates

import pytest

@pytest.mark.usefixtures("yp_env")
class TestMultiClusterReplicaSets(object):
    def test_permissions(self, yp_env):
        templates.permissions_test_template(yp_env, "multi_cluster_replica_set")

    def test_update_spec(self, yp_env):
        templates.update_spec_revision_test_template(yp_env.yp_client, "multi_cluster_replica_set")

    def test_network_project_permissions(self, yp_env):
        templates.replica_set_network_project_permissions_test_template(yp_env, "multi_cluster_replica_set")

    def test_node_segment_id_update(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account")
        segment_id_1 = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": ""}})
        segment_id_2 = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": ""}})
        rs_id = yp_client.create_object(object_type="multi_cluster_replica_set", attributes={"spec": {"account_id": account_id, "node_segment_id": segment_id_1}})
        yp_client.update_object("multi_cluster_replica_set", rs_id, set_updates=[{"path": "/spec/node_segment", "value": segment_id_2}])
