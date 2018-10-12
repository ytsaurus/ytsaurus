from yp.common import YtResponseError

from yt.environment.helpers import assert_items_equal

import pytest


@pytest.mark.usefixtures("yp_env_configurable")
class TestAuth(object):
    YP_MASTER_CONFIG = {
        "authentication_manager": {
            "cypress_token_authenticator": {
                "root_path": "//yp/tokens"
            }
        }
    }

    def test_cypress_tokens(self, yp_env_configurable):
        yt_client = yp_env_configurable.yt_client
        yt_client.set("//yp/tokens/VALIDTOKEN", "u")

        yp_client = yp_env_configurable.yp_client
        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        yp_client1 = yp_env_configurable.yp_instance.create_client(config={"token": "VALIDTOKEN"})
        yp_client2 = yp_env_configurable.yp_instance.create_client(config={"token": "INVALIDTOKEN"})

        yp_env_configurable.sync_access_control()

        assert yp_client1.select_objects("pod", selectors=["/meta/id"]) == []
        with pytest.raises(YtResponseError):
            yp_client2.select_objects("pod", selectors=["/meta/id"])

    def test_ban_user(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})

        yt_client = yp_env_configurable.yt_client
        yt_client.set("//yp/tokens/VALIDTOKEN", "u")

        yp_env_configurable.sync_access_control()

        yp_client_u = yp_env_configurable.yp_instance.create_client(config={"token": "VALIDTOKEN"})
        assert yp_client_u.get_object("user", "u", selectors=["/meta/id"])[0] == "u"

        yp_client.update_object("user", "u", set_updates=[{"path": "/spec/banned", "value": True}])

        yp_env_configurable.sync_access_control()

        with pytest.raises(YtResponseError):
            yp_client_u.get_object("user", "u", selectors=["/meta/id"])

        yp_client.update_object("user", "u", set_updates=[{"path": "/spec/banned", "value": False}])

        yp_env_configurable.sync_access_control()

        assert yp_client_u.get_object("user", "u", selectors=["/meta/id"])[0] == "u"
