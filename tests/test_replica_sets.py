import pytest

@pytest.mark.usefixtures("yp_env")
class TestReplicaSets(object):
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
