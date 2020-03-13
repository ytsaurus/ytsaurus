import pytest


@pytest.mark.usefixtures("yp_env")
class TestDnsRecordsSet(object):
    def test_dns_records_set(self, yp_env):
        yp_client = yp_env.yp_client

        dns_record_set_id = yp_client.create_object(
            object_type="dns_record_set",
            attributes={
                "meta": {"id": "u"},
                "spec": {
                    "records": [
                        {"ttl": 100, "class": "IN", "type": "PTR", "data": "some_data"},
                        {"ttl": 200, "class": "IN", "type": "AAAA", "data": "another_data"},
                        {"ttl": 300, "class": "IN", "type": "SRV", "data": "srv_data"},
                        {"ttl": 400, "class": "IN", "type": "CNAME", "data": "cname_data"},
                        {"ttl": 500, "class": "IN", "type": "TXT", "data": "txt_data"},
                    ],
                },
            },
        )

        result = yp_client.get_object(
            "dns_record_set", dns_record_set_id, selectors=["/meta", "/spec"]
        )
        assert result[0]["id"] == dns_record_set_id

        assert result[1]["records"][0]["ttl"] == 100
        assert result[1]["records"][0]["class"] == "IN"
        assert result[1]["records"][0]["type"] == "PTR"
        assert result[1]["records"][0]["data"] == "some_data"

        assert result[1]["records"][1]["ttl"] == 200
        assert result[1]["records"][1]["class"] == "IN"
        assert result[1]["records"][1]["type"] == "AAAA"
        assert result[1]["records"][1]["data"] == "another_data"

        assert result[1]["records"][2]["ttl"] == 300
        assert result[1]["records"][2]["class"] == "IN"
        assert result[1]["records"][2]["type"] == "SRV"
        assert result[1]["records"][2]["data"] == "srv_data"

        assert result[1]["records"][3]["ttl"] == 400
        assert result[1]["records"][3]["class"] == "IN"
        assert result[1]["records"][3]["type"] == "CNAME"
        assert result[1]["records"][3]["data"] == "cname_data"

        assert result[1]["records"][4]["ttl"] == 500
        assert result[1]["records"][4]["class"] == "IN"
        assert result[1]["records"][4]["type"] == "TXT"
        assert result[1]["records"][4]["data"] == "txt_data"
