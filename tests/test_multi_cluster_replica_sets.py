from . import templates

import pytest

@pytest.mark.usefixtures("yp_env")
class TestMultiClusterReplicaSets(object):
    def test_permissions(self, yp_env):
        templates.permissions_test_template(yp_env, "multi_cluster_replica_set")

    def test_update_spec(self, yp_env):
        templates.update_spec_revision_test_template(yp_env.yp_client, "multi_cluster_replica_set")
