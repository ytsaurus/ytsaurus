from yp.common import YtResponseError

from yt.yson import YsonEntity

import pytest

@pytest.mark.usefixtures("yp_env")
class TestMultiClusterReplicaSets(object):
    def test_permissions(self, yp_env):
        yp_client = yp_env.yp_client

        rs_id = yp_client.create_object("multi_cluster_replica_set")

        with pytest.raises(YtResponseError):
            yp_client.update_object("multi_cluster_replica_set" , rs_id, set_updates=[{"path": "/spec/account_id", "value": ""}])

        account_id = yp_client.create_object("account")
        user_id = yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            client.create_object("multi_cluster_replica_set")
            with pytest.raises(YtResponseError):
                client.create_object("multi_cluster_replica_set", attributes={"spec": {"account_id": account_id}})

        yp_client.update_object("account", account_id, set_updates=[
            {"path": "/meta/acl/end", "value": {"action": "allow", "permissions": ["use"], "subjects": [user_id]}}
        ])
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            client.create_object("multi_cluster_replica_set", attributes={"spec": {"account_id": account_id}})

    def test_update_spec(self, yp_env):
        yp_client = yp_env.yp_client

        rs_id = yp_client.create_object(
            object_type="multi_cluster_replica_set",
            attributes={
                "spec": {
                    "revision_id": "1"
                }
            })

        yp_client.update_object("multi_cluster_replica_set", rs_id, set_updates=[
            {"path": "/spec/revision_id", "value": "2"},
        ])

        assert yp_client.get_object("multi_cluster_replica_set", rs_id, selectors=["/spec/revision_id"])[0][0] == "2"
