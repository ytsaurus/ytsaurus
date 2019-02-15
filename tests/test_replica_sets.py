from yp.common import YtResponseError

from yt.yson import YsonEntity

import pytest

@pytest.mark.usefixtures("yp_env")
class TestReplicaSets(object):
    def test_permissions(self, yp_env):
        yp_client = yp_env.yp_client

        rs_id = yp_client.create_object("replica_set")

        with pytest.raises(YtResponseError):
            yp_client.update_object("replica_set" , rs_id, set_updates=[{"path": "/spec/account_id", "value": ""}])

        account_id = yp_client.create_object("account")
        user_id = yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            client.create_object("replica_set")
            with pytest.raises(YtResponseError):
                client.create_object("replica_set", attributes={"spec": {"account_id": account_id}})

        yp_client.update_object("account", account_id, set_updates=[
            {"path": "/meta/acl/end", "value": {"action": "allow", "permissions": ["use"], "subjects": [user_id]}}
        ])
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            client.create_object("replica_set", attributes={"spec": {"account_id": account_id}})

    def test_update_spec(self, yp_env):
        yp_client = yp_env.yp_client

        rs_id = yp_client.create_object(
            object_type="replica_set",
            attributes={
                "spec": {
                    "revision_id": "1"
                }
            })

        yp_client.update_object("replica_set", rs_id, set_updates=[
            {"path": "/spec/revision_id", "value": "2"},
        ])

        assert yp_client.get_object("replica_set", rs_id, selectors=["/spec/revision_id"])[0][0] == "2"

    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        rs_id = yp_client.create_object(
            object_type="replica_set",
            attributes={
                "spec": {
                    "revision_id": "42",
                    "replica_count": 32,
                    "deployment_strategy": {
                        "min_available": 21,
                        "max_unavailable": 11,
                        "max_surge": 13,
                    },
                },
            })

        result = yp_client.get_object("replica_set", rs_id, selectors=["/meta", "/spec"])
        assert result[0]["id"] == rs_id
        assert result[1]["revision_id"] == "42"
        assert result[1]["replica_count"] == 32
        assert result[1]["deployment_strategy"]["min_available"] == 21
        assert result[1]["deployment_strategy"]["max_unavailable"] == 11
        assert result[1]["deployment_strategy"]["max_surge"] == 13

        status = {
            "in_progress": {
                "pod_count": 31,
            },
            "ready": {
                "pod_count": 1,
            },
            "revisions": {
                "123456": {
                    "revision_id": "123456",
                    "in_progress": {
                        "pod_count": 2,
                        "condition": {
                            "status": "FAILED",
                            "reason": "not_implemented",
                            "message": "Not implemented",
                        },
                    },
                },
            },
        }

        yp_client.update_object("replica_set", rs_id, set_updates=[{"path": "/status", "value": status}])

        result = yp_client.get_object("replica_set", rs_id, selectors=["/status"])[0]
        assert result == status
