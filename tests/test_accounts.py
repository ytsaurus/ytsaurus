from yp.common import YtResponseError

from yt.environment.helpers import assert_items_equal

from .conftest import ZERO_RESOURCE_REQUESTS

import pytest


@pytest.mark.usefixtures("yp_env")
class TestAccounts(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("account", attributes={"meta": {"id": "a"}})
        assert yp_client.get_object("account", "a", selectors=["/meta/id"])[0] == "a"

        yp_client.create_object("account", attributes={"meta": {"id": "b"}})
        assert yp_client.get_object("account", "b", selectors=["/meta/id"])[0] == "b"

        assert yp_client.get_object("account", "a", selectors=["/spec/parent_id"])[0] == ""
        yp_client.update_object("account", "a", set_updates=[{"path": "/spec/parent_id", "value": "b"}])
        assert yp_client.get_object("account", "a", selectors=["/spec/parent_id"])[0] == "b"

        yp_client.remove_object("account", "b")
        assert yp_client.get_object("account", "a", selectors=["/spec/parent_id"])[0] == ""

    def test_builtin_accounts(self, yp_env):
        yp_client = yp_env.yp_client

        accounts = [x[0] for x in yp_client.select_objects("account", selectors=["/meta/id"])]
        assert_items_equal(accounts, ["tmp"])

        for account in accounts:
            with pytest.raises(YtResponseError):
                yp_client.remove_object("account", account)

    def test_cannot_set_null_account(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        with pytest.raises(YtResponseError):
            yp_client.update_object("pod_set" ,pod_set_id, set_updates=[{"path": "/spec/account_id", "value": ""}])

    def test_cannot_create_with_null_account(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object("pod_set", attributes={
                    "spec": {
                        "account_id": ""
                    }
                })
    def test_must_have_use_permission1(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account", attributes={
            "spec": {}
        })

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            def create_pod_set():
                yp_client1.create_object("pod_set", attributes={
                    "spec": {"account_id": account_id}
                })

            with pytest.raises(YtResponseError):
                create_pod_set()

            yp_client.update_object("account", account_id, set_updates=[
                {"path": "/meta/acl/end", "value": {"action": "allow", "permissions": ["use"], "subjects": ["u"]}}
            ])

            create_pod_set()

    def test_must_have_use_permission2(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account", attributes={
            "spec": {}
        })

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            pod_set_id = yp_client1.create_object("pod_set")

            def create_pod():
                yp_client1.create_object("pod", attributes={
                    "meta": {"pod_set_id": pod_set_id},
                    "spec": {
                        "account_id": account_id,
                        "resource_requests": ZERO_RESOURCE_REQUESTS
                    }
                })

            with pytest.raises(YtResponseError):
                create_pod()

            yp_client.update_object("account", account_id, set_updates=[
                {"path": "/meta/acl/end", "value": {"action": "allow", "permissions": ["use"], "subjects": ["u"]}}
            ])

            create_pod()

    def test_null_account_id_yp_717(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            pod_set_id = yp_client1.create_object("pod_set")
            pod_id = yp_client1.create_object("pod", attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {
                    "resource_requests": ZERO_RESOURCE_REQUESTS,
                    "account_id": ""
                }
            })
