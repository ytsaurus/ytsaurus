import pytest

from yp.common import YtResponseError
from yt.environment.helpers import assert_items_equal

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
