from yt.orm.library.common import YtResponseError

import pytest


class TestTransitiveReference:
    def test_create_object_specifying_transitive_key(self, example_env):
        nexus_id = example_env.client.create_object("nexus", request_meta_response=True)
        mother_ship = example_env.create_mother_ship(nexus_id)
        interceptor = example_env.client.create_object("interceptor", attributes={"meta": {"mother_ship_id": int(mother_ship), "nexus_id": int(nexus_id)}})
        interceptor_meta = example_env.client.get_object("interceptor", str(interceptor), ["/meta"])[0]
        assert interceptor_meta["nexus_id"] == int(nexus_id)

    def test_create_object(self, example_env):
        mother_ship = example_env.create_mother_ship()
        interceptor = example_env.client.create_object("interceptor", attributes={"meta": {"mother_ship_id": int(mother_ship)}})
        interceptor_meta = example_env.client.get_object("interceptor", str(interceptor), ["/meta"])[0]
        mother_ship_meta = example_env.client.get_object("mother_ship", str(mother_ship), ["/meta"])[0]
        assert interceptor_meta["nexus_id"] == mother_ship_meta["nexus_id"]

    def test_no_update(self, example_env):
        mother_ship = example_env.create_mother_ship()
        interceptor = example_env.client.create_object("interceptor", attributes={"meta": {"mother_ship_id": int(mother_ship)}})
        with pytest.raises(YtResponseError):
            example_env.client.update_object("interceptor", str(interceptor), set_updates=[{"path": "/meta/nexus_id", "value": 1122}])

    def test_no_remove(self, example_env):
        mother_ship = example_env.create_mother_ship()
        interceptor = example_env.client.create_object("interceptor", attributes={"meta": {"mother_ship_id": int(mother_ship)}})
        with pytest.raises(YtResponseError):
            example_env.client.update_object("interceptor", str(interceptor), remove_updates=[{"path": "/meta/nexus_id"}])
