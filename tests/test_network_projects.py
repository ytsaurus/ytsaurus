from .conftest import create_user

from yp.common import YtResponseError

import pytest


@pytest.mark.usefixtures("yp_env")
class TestNetworkProjects(object):
    def test_project_id_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object(object_type="network_project")

    def test_get(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object(
            "network_project", attributes={"meta": {"id": "MYPROJECT"}, "spec": {"project_id": 123}}
        )
        result = yp_client.get_object(
            "network_project", id, selectors=["/meta/id", "/spec/project_id"]
        )
        assert result[0] == id
        assert result[1] == 123

    def test_must_have_use_permission(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object(
            "network_project", attributes={"meta": {"id": "MYPROJECT"}, "spec": {"project_id": 123}}
        )

        create_user(yp_client, "u", grant_create_permission_for_types=("pod_set", "pod"))
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            pod_set_id = yp_client1.create_object("pod_set")

            def create_pod():
                pod_id = yp_client1.create_object(
                    "pod",
                    attributes={
                        "meta": {"pod_set_id": pod_set_id},
                        "spec": {
                            "ip6_address_requests": [
                                {"vlan_id": "somevlan", "network_id": "MYPROJECT"}
                            ]
                        },
                    },
                )

            with pytest.raises(YtResponseError):
                create_pod()

            yp_client.update_object(
                "network_project",
                "MYPROJECT",
                set_updates=[
                    {
                        "path": "/meta/acl/end",
                        "value": {"action": "allow", "permissions": ["use"], "subjects": ["u"]},
                    }
                ],
            )

            create_pod()
