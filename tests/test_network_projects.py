import pytest

from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestNetworkProjects(object):
    def test_project_id_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YpResponseError): yp_client.create_object(object_type="network_project")

    def test_cannot_update_project_id(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("network_project", attributes={
            "meta": {"id": "MYPROJECT"},
            "spec": {"project_id": 123}
        })
        with pytest.raises(YpResponseError): yp_client.update_object("network_project", id, set_updates=[{"path": "/spec/project_id", "value": 1234}])

    def test_get(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("network_project", attributes={
            "meta": {"id": "MYPROJECT"},
            "spec": {"project_id": 123}
        })
        result = yp_client.get_object("network_project", id, selectors=[
            "/meta/id",
            "/spec/project_id"
        ])
        assert result[0] == id
        assert result[1] == 123
