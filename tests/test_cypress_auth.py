from .conftest import create_user

from yp.common import YtResponseError

import pytest


def _select_objects_succeeds(yp_client):
    assert yp_client.select_objects("pod", selectors=["/meta/id"]) == []


def _select_objects_fails(yp_client):
    with pytest.raises(YtResponseError):
        yp_client.select_objects("pod", selectors=["/meta/id"])


@pytest.mark.usefixtures("yp_env_auth")
class TestCypressAuth(object):
    YP_MASTER_CONFIG = {
        "authentication_manager": {"cypress_token_authenticator": {"root_path": "//yp/tokens"}},
    }

    def test_cypress_tokens(self, yp_env_auth):
        yt_client = yp_env_auth.yt_client
        yt_client.set("//yp/tokens/VALIDTOKEN", "u")

        yp_client = yp_env_auth.yp_client
        create_user(yp_client, "u")
        yp_env_auth.sync_access_control()

        # Overrides the default Oauth token.
        with yp_env_auth.create_client({"token": "VALIDTOKEN"}) as yp_client:
            _select_objects_succeeds(yp_client)

        with yp_env_auth.create_client({"token": "INVALIDTOKEN"}) as yp_client:
            _select_objects_fails(yp_client)

    def test_ban_user(self, yp_env_auth):
        yp_client = yp_env_auth.yp_client

        create_user(yp_client, "u")
        yp_env_auth.sync_access_control()

        yt_client = yp_env_auth.yt_client
        yt_client.set("//yp/tokens/VALIDTOKEN", "u")

        with yp_env_auth.create_client({"token": "VALIDTOKEN"}) as yp_client_u:
            assert yp_client_u.get_object("user", "u", selectors=["/meta/id"])[0] == "u"

            yp_client.update_object(
                "user", "u", set_updates=[{"path": "/spec/banned", "value": True}]
            )

            yp_env_auth.sync_access_control()

            with pytest.raises(YtResponseError):
                yp_client_u.get_object("user", "u", selectors=["/meta/id"])

            yp_client.update_object(
                "user", "u", set_updates=[{"path": "/spec/banned", "value": False}]
            )

            yp_env_auth.sync_access_control()

            assert yp_client_u.get_object("user", "u", selectors=["/meta/id"])[0] == "u"
