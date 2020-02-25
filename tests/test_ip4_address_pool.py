from . import templates
from .conftest import (
    create_nodes,
    create_pod_with_boilerplate,
    wait_pod_is_assigned,
)

from yp.local import DEFAULT_IP4_ADDRESS_POOL_ID

from yp.common import YpAuthorizationError

from contextlib import contextmanager
import pytest


@pytest.mark.usefixtures("yp_env")
class TestIP4AddressPools(object):
    def test_update_oject(self, yp_env):
        yp_client = yp_env.yp_client
        pool_id = yp_client.create_object(
            "ip4_address_pool", attributes={"meta": {"id": "mega_pool"}}
        )
        yp_client.remove_object("ip4_address_pool", pool_id)

        templates.update_spec_test_template(
            yp_client=yp_client,
            object_type="ip4_address_pool",
            initial_spec={},
            update_path="/meta/acl",
            update_value=[
                {"action": "allow", "permissions": ["read", "write",], "subjects": ["root",]}
            ],
        )

    def test_create_new_address(self, yp_env):
        yp_client = yp_env.yp_client
        yt_client = yp_env.yt_client

        pool_id = yp_client.create_object("ip4_address_pool", attributes={"meta": {"id": "pool"}})
        assert pool_id == "pool"

        addr_id = yp_client.create_object(
            "internet_address",
            attributes={
                "meta": {"ip4_address_pool_id": pool_id,},
                "spec": {"ip4_address": "1.3.5.7", "network_module_id": "VLA-100.500",},
            },
        )
        parent_records = yt_client.select_rows(
            '* from [//yp/db/parents] where object_id="{}" AND object_type=11 AND parent_id="pool"'.format(
                addr_id
            )
        )
        assert len(list(parent_records)) == 1

    def _create_pod_with_ip4_address_and_acl(
        self, yp_env, ip4_address, ace_action=None, ip4_address_pool_id=None
    ):
        yp_client = yp_env.yp_client

        user_name = yp_client.create_object("user")

        vlan_id = "somevlan"
        network_id = yp_client.create_object(
            "network_project",
            attributes={
                "spec": {"project_id": 123,},
                "meta": {
                    "acl": [{"action": "allow", "permissions": ["use",], "subjects": [user_name,]}],
                },
            },
        )

        if ip4_address_pool_id is None:
            assert ace_action is not None

            acl = [{"action": ace_action, "permissions": ["use",], "subjects": [user_name,]}]

            ip4_address_pool_id = yp_client.create_object(
                "ip4_address_pool", attributes={"meta": {"acl": acl,},}
            )

        network_module_id = "VLA-100.500"

        create_nodes(yp_client, node_count=1, vlan_id=vlan_id, network_module_id=network_module_id)

        yp_client.create_object(
            "internet_address",
            attributes={
                "meta": {"ip4_address_pool_id": ip4_address_pool_id,},
                "spec": {"ip4_address": ip4_address, "network_module_id": network_module_id,},
            },
        )

        pod_set_id = yp_client.create_object(
            "pod_set",
            attributes={
                "meta": {
                    "acl": [
                        {"action": "allow", "permissions": ["write",], "subjects": [user_name,]}
                    ],
                },
            },
        )

        with yp_env.yp_instance.create_client(config={"user": user_name}) as yp_client:
            yp_env.sync_access_control()
            return create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec={
                    "ip6_address_requests": [
                        {
                            "vlan_id": vlan_id,
                            "network_id": network_id,
                            "ip4_address_pool_id": ip4_address_pool_id,
                        },
                    ],
                    "enable_scheduling": True,
                },
            )

    def test_acl_allow(self, yp_env):
        yp_client = yp_env.yp_client
        pod_id = self._create_pod_with_ip4_address_and_acl(yp_env, "1.2.3.4", ace_action="allow")
        wait_pod_is_assigned(yp_client, pod_id)

    def test_acl_deny(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YpAuthorizationError):
            self._create_pod_with_ip4_address_and_acl(yp_env, "1.3.5.7", ace_action="deny")

    def test_acl_for_default_pool(self, yp_env):
        yp_client = yp_env.yp_client
        pod_id = self._create_pod_with_ip4_address_and_acl(
            yp_env, "1.4.7.10", ip4_address_pool_id=DEFAULT_IP4_ADDRESS_POOL_ID
        )
        wait_pod_is_assigned(yp_client, pod_id)

    def test_create_pool_as_user(self, yp_env):
        yp_client = yp_env.yp_client

        @contextmanager
        def allow_pool_creation(user_name):
            try:
                yp_client.update_object(
                    "schema",
                    "ip4_address_pool",
                    set_updates=[
                        {
                            "path": "/meta/acl/end",
                            "value": {
                                "action": "allow",
                                "permissions": ["create", "write"],
                                "subjects": [user_name],
                            },
                        },
                    ],
                )
                yield
            finally:
                yp_client.update_object(
                    "schema", "ip4_address_pool", remove_updates=[{"path": "/meta/acl/-1",},]
                )

        user_name = yp_client.create_object("user")
        with allow_pool_creation(user_name):
            with yp_env.yp_instance.create_client(config={"user": user_name}) as user_yp_client:
                yp_env.sync_access_control()
                user_yp_client.create_object("ip4_address_pool")
