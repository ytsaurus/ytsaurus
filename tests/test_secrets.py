from .conftest import create_user

from yp.common import YtResponseError, YpAuthorizationError

from yt.yson import YsonEntity

import pytest


MY_SECRET = {"secret_id": "id", "secret_version": "version", "delegation_token": "token"}


@pytest.mark.usefixtures("yp_env")
class TestSecrets(object):
    def test_secrets_writable_by_root(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[{"path": "/spec/secrets", "value": {"my_secret": MY_SECRET}}],
        )

    def test_secrets_writable_unless_allowed(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            yp_env.sync_access_control()

            pod_set_id = yp_client.create_object("pod_set")
            pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

            with pytest.raises(YpAuthorizationError):
                yp_client1.update_object(
                    "pod", pod_id, set_updates=[{"path": "/spec/secrets", "value": {}}]
                )

            yp_client.update_object(
                "pod_set",
                pod_set_id,
                set_updates=[
                    {
                        "path": "/meta/acl",
                        "value": [{"action": "allow", "permissions": ["write"], "subjects": ["u"]}],
                    }
                ],
            )

            yp_client1.update_object(
                "pod", pod_id, set_updates=[{"path": "/spec/secrets", "value": {}}]
            )

    def test_secrets_readable_by_root(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        assert yp_client.get_object("pod", pod_id, selectors=["/spec/secrets"])[0] == {}

    def test_secrets_are_not_readable_unless_allowed(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            yp_env.sync_access_control()

            pod_set_id = yp_client.create_object("pod_set")
            pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

            with pytest.raises(YpAuthorizationError):
                yp_client1.get_object("pod", pod_id, selectors=["/spec/secrets"])

            yp_client.update_object(
                "pod_set",
                pod_set_id,
                set_updates=[
                    {
                        "path": "/meta/acl",
                        "value": [
                            {"action": "allow", "permissions": ["read_secrets"], "subjects": ["u"]}
                        ],
                    }
                ],
            )

            assert yp_client1.get_object("pod", pod_id, selectors=["/spec/secrets"])[0] == {}

    def test_secrets_not_allowed_in_filter(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.select_objects(
                "pod", selectors=["/meta/id"], filter='[/spec/secrets/0/secret_id] = "my_secret"'
            )

    def test_secrets_not_allowed_in_selector(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.select_objects("pod", selectors=["/spec/secrets"])

    def test_partial_secrets_update_requires_read(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            yp_env.sync_access_control()

            pod_set_id = yp_client.create_object("pod_set")
            pod_id = yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id}})

            with pytest.raises(YpAuthorizationError):
                yp_client1.update_object(
                    "pod",
                    pod_id,
                    set_updates=[{"path": "/spec/secrets/my_secret", "value": MY_SECRET}],
                )

            yp_client.update_object(
                "pod_set",
                pod_set_id,
                set_updates=[
                    {
                        "path": "/meta/acl",
                        "value": [{"action": "allow", "permissions": ["write"], "subjects": ["u"]}],
                    }
                ],
            )

            with pytest.raises(YpAuthorizationError):
                yp_client1.update_object(
                    "pod",
                    pod_id,
                    set_updates=[{"path": "/spec/secrets/my_secret", "value": MY_SECRET}],
                )

            yp_client.update_object(
                "pod_set",
                pod_set_id,
                set_updates=[
                    {
                        "path": "/meta/acl",
                        "value": [
                            {
                                "action": "allow",
                                "permissions": ["read_secrets", "write"],
                                "subjects": ["u"],
                            }
                        ],
                    }
                ],
            )

            yp_client1.update_object(
                "pod", pod_id, set_updates=[{"path": "/spec/secrets/my_secret", "value": MY_SECRET}]
            )

    def test_secrets_are_opaque(self, yp_env):
        yp_client = yp_env.yp_client

        user_id = create_user(yp_client, grant_create_permission_for_types=("pod_set", "pod"))
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as yp_client1:
            pod_set_id = yp_client1.create_object("pod_set")
            pod_id = yp_client1.create_object(
                "pod", attributes={"meta": {"pod_set_id": pod_set_id}}
            )
            yp_client1.update_object(
                "pod", pod_id, set_updates=[{"path": "/spec/secrets/my_secret", "value": MY_SECRET}]
            )

            assert (
                yp_client1.get_object("pod", pod_id, selectors=["/spec"])[0]["secrets"]
                == YsonEntity()
            )
            assert (
                yp_client1.select_objects("pod", selectors=["/spec"])[0][0]["secrets"]
                == YsonEntity()
            )
