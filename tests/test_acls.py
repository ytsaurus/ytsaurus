import pytest

from yp.common import YpAuthorizationError
from yt.environment.helpers import assert_items_equal

@pytest.mark.usefixtures("yp_env")
class TestAcls(object):
    def test_owner(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client.create_object("user", attributes={"meta": {"id": "u2"}})

        yp_client1 = yp_env.yp_instance.create_client(config={"user": "u1"})
        yp_client2 = yp_env.yp_instance.create_client(config={"user": "u2"})
        yp_env.sync_access_control()

        id = yp_client1.create_object("pod_set")
        yp_client1.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])
        with pytest.raises(YpAuthorizationError):
            yp_client2.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])

    def test_groups_immediate(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client.create_object("group", attributes={"meta": {"id": "g"}})
        yp_client1 = yp_env.yp_instance.create_client(config={"user": "u1"})
        yp_env.sync_access_control()

        id = yp_client.create_object("pod_set", attributes={
                "meta": {
                    "acl": [
                        {"action": "allow", "permissions": ["write"], "subjects": ["g"]}
                    ]
                }
            })
        with pytest.raises(YpAuthorizationError):
            yp_client1.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])

        yp_client.update_object("group", "g", set_updates=[{"path": "/spec/members", "value": ["u1"]}])
        yp_env.sync_access_control()

        yp_client1.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])

    def test_groups_recursive(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client.create_object("group", attributes={"meta": {"id": "g1"}})
        yp_client.create_object("group", attributes={"meta": {"id": "g2"}})
        yp_client1 = yp_env.yp_instance.create_client(config={"user": "u1"})
        yp_env.sync_access_control()

        id = yp_client.create_object("pod_set", attributes={
                "meta": {
                    "acl": [
                        {"action": "allow", "permissions": ["write"], "subjects": ["g1"]}
                    ]
                }
            })
        with pytest.raises(YpAuthorizationError):
            yp_client1.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])

        yp_client.update_object("group", "g1", set_updates=[{"path": "/spec/members", "value": ["g2"]}])
        yp_client.update_object("group", "g2", set_updates=[{"path": "/spec/members", "value": ["u1"]}])
        yp_env.sync_access_control()

        yp_client1.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])

    def test_inherit_acl(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client1 = yp_env.yp_instance.create_client(config={"user": "u1"})
        yp_env.sync_access_control()

        pod_set_id = yp_client1.create_object("pod_set")
        pod_id = yp_client1.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id, "acl": []}})
        yp_client1.update_object("pod", pod_id, set_updates=[{"path": "/labels/a", "value": "b"}])

        yp_client.update_object("pod", pod_id, set_updates=[{"path": "/meta/inherit_acl", "value": False}])
        with pytest.raises(YpAuthorizationError):
            yp_client1.update_object("pod", pod_id, set_updates=[{"path": "/labels/a", "value": "b"}])

    def test_endpoint_inherits_from_endpoint_set(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client1 = yp_env.yp_instance.create_client(config={"user": "u1"})
        yp_env.sync_access_control()

        endpoint_set_id = yp_client.create_object("endpoint_set")
        endpoint_id = yp_client.create_object("endpoint", attributes={
                "meta": {
                    "endpoint_set_id": endpoint_set_id
                }
            })

        with pytest.raises(YpAuthorizationError):
            yp_client1.update_object("endpoint", endpoint_id, set_updates=[{"path": "/labels/a", "value": "b"}])
        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[
            {
                "path": "/meta/acl",
                "value": [
                    {"action": "allow", "permissions": ["write"], "subjects": ["u1"]}
                ]
            }])
        yp_client1.update_object("endpoint", endpoint_id, set_updates=[{"path": "/labels/a", "value": "b"}])

    def test_check_permissions(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        endpoint_set_id = yp_client.create_object("endpoint_set", attributes={
                "meta": {
                    "acl": [
                        {"action": "allow", "subjects": ["u"], "permissions": ["write"]}
                    ]
                }
            })

        assert \
            yp_client.check_object_permissions([
                {"object_type": "endpoint_set", "object_id": endpoint_set_id, "subject_id": "u", "permission": "read"},
                {"object_type": "endpoint_set", "object_id": endpoint_set_id, "subject_id": "u", "permission": "write"},
                {"object_type": "endpoint_set", "object_id": endpoint_set_id, "subject_id": "u", "permission": "ssh_access"}
            ]) == \
            [
                {"action": "allow", "subject_id": "everyone", "object_type": "schema", "object_id": "endpoint_set"},
                {"action": "allow", "subject_id": "u", "object_type": "endpoint_set", "object_id": endpoint_set_id},
                {"action": "deny"}
            ]

    def test_create_at_schema(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client1 = yp_env.yp_instance.create_client(config={"user": "u1"})

        yp_env.sync_access_control()

        yp_client.update_object("schema", "endpoint_set", set_updates=[{
                "path": "/meta/acl/end",
                "value": {"action": "deny", "permissions": ["create"], "subjects": ["u1"]}
            }])

        with pytest.raises(YpAuthorizationError):
            yp_client1.create_object("endpoint_set")

        yp_client.update_object("schema", "endpoint_set", remove_updates=[{
                "path": "/meta/acl/-1"
            }])

        yp_client1.create_object("endpoint_set")

    def test_create_requires_write_at_parent(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client1 = yp_env.yp_instance.create_client(config={"user": "u1"})

        yp_env.sync_access_control()

        endpoint_set_id = yp_client1.create_object("endpoint_set")

        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[{
                "path": "/meta/acl",
                "value": [{"action": "deny", "permissions": ["write"], "subjects": ["u1"]}]
            }])

        with pytest.raises(YpAuthorizationError):
            yp_client1.create_object("endpoint", attributes={"meta": {"endpoint_set_id": endpoint_set_id}})            

        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[{
                "path": "/meta/acl",
                "value": [{"action": "allow", "permissions": ["write"], "subjects": ["u1"]}]
            }])

        yp_client1.create_object("endpoint", attributes={"meta": {"endpoint_set_id": endpoint_set_id}})            
