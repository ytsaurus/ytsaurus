from yt_env_setup import YTEnvSetup

from yt_commands import (authors, get, set, create, raises_yt_error, create_user, check_permission)

##################################################################


class TestRegisterQueueConsumerPermission(YTEnvSetup):
    @authors("max42")
    def test_register_queue_consumer_permission(self):
        create_user("u_vital")
        create_user("u_non_vital")
        create("map_node", "//tmp/t", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})

        with raises_yt_error("Permission \"register_queue_consumer\" requires vitality"):
            set("//tmp/t/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"],
                                     "subjects": ["u_vital"]})
        with raises_yt_error("ACE specifying vitality must contain a single \"register_queue_consumer\""):
            set("//tmp/t/@acl/end", {"action": "allow", "permissions": ["read"], "vital": True,
                                     "subjects": ["u_vital"]})
        with raises_yt_error("ACE specifying vitality must contain a single \"register_queue_consumer\""):
            set("//tmp/t/@acl/end", {"action": "allow", "permissions": ["read", "register_queue_consumer"],
                                     "vital": True, "subjects": ["u_vital"]})

        set("//tmp/t/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": True,
                                 "subjects": ["u_vital"]})
        set("//tmp/t/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": False,
                                 "subjects": ["u_non_vital"]})

        assert get("//tmp/t/@acl/0") == {"action": "allow", "permissions": ["register_queue_consumer"], "vital": True,
                                         "subjects": ["u_vital"], "inheritance_mode": "object_and_descendants"}
        assert get("//tmp/t/@acl/1") == {"action": "allow", "permissions": ["register_queue_consumer"], "vital": False,
                                         "subjects": ["u_non_vital"], "inheritance_mode": "object_and_descendants"}

        # Unspecified vitality is assumed to be vital=False.
        assert check_permission("u_non_vital", "register_queue_consumer", "//tmp/t")["action"] == "allow"
        assert check_permission("u_non_vital", "register_queue_consumer", "//tmp/t", vital=False)["action"] == "allow"
        assert check_permission("u_non_vital", "register_queue_consumer", "//tmp/t", vital=True)["action"] == "deny"

        assert check_permission("u_vital", "register_queue_consumer", "//tmp/t")["action"] == "allow"
        assert check_permission("u_vital", "register_queue_consumer", "//tmp/t", vital=False)["action"] == "allow"
        assert check_permission("u_vital", "register_queue_consumer", "//tmp/t", vital=True)["action"] == "allow"


class TestRegisterQueueConsumerPermissionRpcProxy(TestRegisterQueueConsumerPermission):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
