import pytest


@pytest.mark.usefixtures("yp_env")
class TestDaemonSets(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        ds_id = yp_client.create_object(
            object_type="daemon_set",
            attributes={"meta": {"pod_set_id": "foo"}, "spec": {"strong": True}},
        )

        result = yp_client.get_object("daemon_set", ds_id, selectors=["/meta", "/spec"])
        assert result[0]["id"] == ds_id
        assert result[0]["pod_set_id"] == "foo"
        assert result[1]["strong"] == True  # noqa

        yp_client.update_object(
            "daemon_set", ds_id, set_updates=[{"path": "/meta/pod_set_id", "value": "bar"}]
        )

        result = yp_client.get_object("daemon_set", ds_id, selectors=["/meta/pod_set_id"])
        assert result == ["bar"]
