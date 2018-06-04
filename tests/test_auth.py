import pytest

from yp.client import YpResponseError
from yt.environment.helpers import assert_items_equal

@pytest.mark.usefixtures("yp_env_configurable")
class TestAuth(object):
    YP_MASTER_CONFIG = {
        "access_control_manager": {
            "cypress_token_authenticator": {
                "root_path": "//yp/tokens"
            }
        }
    }
    
    def test_cypress_tokens(self, yp_env_configurable):
        yt_client = yp_env_configurable.yt_client
        yt_client.set("//yp/tokens/VALIDTOKEN", "u", recursive=True)
        
        yp_client1 = yp_env_configurable.yp_instance.create_client(config={"token": "VALIDTOKEN"})
        yp_client2 = yp_env_configurable.yp_instance.create_client(config={"token": "INVALIDTOKEN"})

        assert yp_client1.select_objects("pod", selectors=["/meta/id"]) == []
        with pytest.raises(YpResponseError):
            yp_client2.select_objects("pod", selectors=["/meta/id"])
        
