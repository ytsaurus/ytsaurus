import pytest

from yp.client import YpResponseError
from yt.yson import YsonEntity, YsonUint64

@pytest.mark.usefixtures("yp_env")
class TestVirtualServices(object):
    def test_get_virtual_service(self, yp_env):
        yp_client = yp_env.yp_client

        virtual_service_id = yp_client.create_object(
            object_type="virtual_service",
            attributes={
                "meta": {"id": "VS_ID"},
                "spec": {
                    "ip4_addresses": ["1.1.1.1", "2.2.2.2"],
                    "ip6_addresses": [":::1", "1:1:1:1:1"],
                },
            })

        result = yp_client.get_object("virtual_service", virtual_service_id, selectors=["/meta", "/spec"])
        assert result[0]["id"] == virtual_service_id
        assert result[1]["ip4_addresses"][0] == "1.1.1.1"
        assert result[1]["ip4_addresses"][1] == "2.2.2.2"
        assert result[1]["ip6_addresses"][0] == ":::1"
        assert result[1]["ip6_addresses"][1] == "1:1:1:1:1"
