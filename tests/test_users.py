import pytest

from yp.client import YpResponseError
from yt.environment.helpers import assert_items_equal

@pytest.mark.usefixtures("yp_env")
class TestUsers(object):
    def test_create_user(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(5):
            yp_client.create_object("user", attributes={"meta": {"id": "u"}})
            assert yp_client.get_object("user", "u", selectors=["/meta/id"]) == ["u"]
            yp_client.remove_object("user", "u")

    def test_create_group(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(5):
	        yp_client.create_object("group", attributes={"meta": {"id": "g"}})
	        assert yp_client.get_object("group", "g", selectors=["/meta/id"]) == ["g"]
	        yp_client.remove_object("group", "g")

    def test_subject_ids(self, yp_env):
    	yp_client = yp_env.yp_client

    	yp_client.create_object("user", attributes={"meta": {"id": "u"}})
    	assert yp_client.get_object("user", "u", selectors=["/meta/id"]) == ["u"]
    	with pytest.raises(YpResponseError): 
    	    yp_client.create_object("group", attributes={"meta": {"id": "u"}})
    	yp_client.remove_object("user", "u")
    	yp_client.create_object("group", attributes={"meta": {"id": "u"}})

    def test_builtin_users(self, yp_env):
        yp_client = yp_env.yp_client

        users = yp_client.select_objects("user", selectors=["/meta/id"])
        assert_items_equal([x[0] for x in users], ["root"])

    def test_builtin_groups(self, yp_env):
        yp_client = yp_env.yp_client

        print list(yp_env.yt_client.select_rows("* from [//yp/db/groups]"))

        groups = yp_client.select_objects("group", selectors=["/meta/id"])
        assert_items_equal([x[0] for x in groups], ["superusers"])

        assert_items_equal(yp_client.get_object("group", "superusers", selectors=["/spec/members"])[0], ["root"])

    def test_cannot_create_wellknown(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["user", "group"]:
            for id in ["owner", "everyone"]:
                with pytest.raises(YpResponseError): 
                    yp_client.create_object(type, attributes={"meta": {"id": id}})

