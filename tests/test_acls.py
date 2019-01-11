from .conftest import ZERO_RESOURCE_REQUESTS

from yp.common import YpAuthorizationError

from yt.environment.helpers import assert_items_equal

import pytest


@pytest.mark.usefixtures("yp_env")
class TestAcls(object):
    def test_owner(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client.create_object("user", attributes={"meta": {"id": "u2"}})

        with yp_env.yp_instance.create_client(config={"user": "u1"}) as yp_client1:
            yp_env.sync_access_control()

            id = yp_client1.create_object("pod_set")
            yp_client1.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])

        with yp_env.yp_instance.create_client(config={"user": "u2"}) as yp_client2:
            yp_env.sync_access_control()

            with pytest.raises(YpAuthorizationError):
                yp_client2.update_object("pod_set", id, set_updates=[{"path": "/labels/a", "value": "b"}])

    def test_groups_immediate(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client.create_object("group", attributes={"meta": {"id": "g"}})

        with yp_env.yp_instance.create_client(config={"user": "u1"}) as yp_client1:
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
        with yp_env.yp_instance.create_client(config={"user": "u1"}) as yp_client1:
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
        with yp_env.yp_instance.create_client(config={"user": "u1"}) as yp_client1:
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
        with yp_env.yp_instance.create_client(config={"user": "u1"}) as yp_client1:
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
        with yp_env.yp_instance.create_client(config={"user": "u1"}) as yp_client1:
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

        with yp_env.yp_instance.create_client(config={"user": "u1"}) as yp_client1:
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

    def test_get_object_access_allowed_for(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u1"}})
        yp_client.create_object("user", attributes={"meta": {"id": "u2"}})
        yp_client.create_object("user", attributes={"meta": {"id": "u3"}})

        yp_client.create_object("group", attributes={"meta": {"id": "g1"}, "spec": {"members": ["u1", "u2"]}})
        yp_client.create_object("group", attributes={"meta": {"id": "g2"}, "spec": {"members": ["u2", "u3"]}})
        yp_client.create_object("group", attributes={"meta": {"id": "g3"}, "spec": {"members": ["g1", "g2"]}})

        yp_env.sync_access_control()

        endpoint_set_id = yp_client.create_object("endpoint_set", attributes={"meta": {"inherit_acl": False}})

        assert_items_equal(
            yp_client.get_object_access_allowed_for([
                {"object_id": endpoint_set_id, "object_type": "endpoint_set", "permission": "read"}
            ])[0]["user_ids"],
            ["root"])

        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[{
                "path": "/meta/acl",
                "value": [{"action": "allow", "permissions": ["read"], "subjects": ["u1", "u2"]}]
            }])

        assert_items_equal(
            yp_client.get_object_access_allowed_for([
                {"object_id": endpoint_set_id, "object_type": "endpoint_set", "permission": "read"}
            ])[0]["user_ids"],
            ["root", "u1", "u2"])

        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[{
                "path": "/meta/acl",
                "value": [{"action": "deny", "permissions": ["read"], "subjects": ["root"]}]
            }])

        assert_items_equal(
            yp_client.get_object_access_allowed_for([
                {"object_id": endpoint_set_id, "object_type": "endpoint_set", "permission": "read"}
            ])[0]["user_ids"],
            ["root"])

        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[{
                "path": "/meta/acl",
                "value": [
                    {"action": "allow", "permissions": ["read"], "subjects": ["g3"]},
                    {"action": "deny", "permissions": ["read"], "subjects": ["g2"]}
                ]
            }])

        assert_items_equal(
            yp_client.get_object_access_allowed_for([
                {"object_id": endpoint_set_id, "object_type": "endpoint_set", "permission": "read"}
            ])[0]["user_ids"],
            ["root", "u1"])

    def test_only_superuser_can_force_assign_pod1(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        yp_env.sync_access_control()

        node_id = yp_client.create_object("node")
        pod_set_id = yp_client.create_object("pod_set", attributes={
                "meta": {
                    "acl": [{"action": "allow", "permissions": ["write"], "subjects": ["u"]}]
                }
            })

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            def try_create():
                yp_client1.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "resource_requests": ZERO_RESOURCE_REQUESTS,
                        "node_id": node_id
                    }
                })

            with pytest.raises(YpAuthorizationError):
                try_create()

            yp_client.update_object("group", "superusers", set_updates=[
                {"path": "/spec/members", "value": ["u"]}
            ])

            yp_env.sync_access_control()

            try_create()

    def test_only_superuser_can_force_assign_pod2(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        yp_env.sync_access_control()

        node_id = yp_client.create_object("node")
        pod_set_id = yp_client.create_object("pod_set", attributes={
                "meta": {
                    "acl": [{"action": "allow", "permissions": ["write"], "subjects": ["u"]}]
                }
            })
        pod_id = yp_client.create_object("pod", attributes={
                "meta": {
                    "pod_set_id": pod_set_id
                },
                "spec": {
                    "resource_requests": ZERO_RESOURCE_REQUESTS
                }
            })

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            def try_update():
                yp_client1.update_object("pod", pod_id, set_updates=[
                    {"path": "/spec/node_id", "value": node_id}
                ])

            with pytest.raises(YpAuthorizationError):
                try_update()

            yp_client.update_object("group", "superusers", set_updates=[
                {"path": "/spec/members", "value": ["u"]}
            ])

            yp_env.sync_access_control()

            try_update()

    def test_only_superuser_can_request_subnet_without_network_project(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        yp_env.sync_access_control()

        node_id = yp_client.create_object("node")
        pod_set_id = yp_client.create_object("pod_set", attributes={
                "meta": {
                    "acl": [{"action": "allow", "permissions": ["write"], "subjects": ["u"]}]
                }
            })

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            def try_create():
                yp_client1.create_object("pod", attributes={
                    "meta": {
                        "pod_set_id": pod_set_id
                    },
                    "spec": {
                        "resource_requests": ZERO_RESOURCE_REQUESTS,
                        "ip6_subnet_requests": [
                            {"vlan_id": "somevlan"}
                        ]
                    }
                })

            with pytest.raises(YpAuthorizationError):
                try_create()

            yp_client.update_object("group", "superusers", set_updates=[
                {"path": "/spec/members", "value": ["u"]}
            ])

            yp_env.sync_access_control()

            try_create()
