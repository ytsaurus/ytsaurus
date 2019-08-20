from . import templates

import pytest


@pytest.mark.usefixtures("yp_env")
class TestIP4AddressPools(object):

    def test_update_oject(self, yp_env):
        yp_client = yp_env.yp_client
        pool_id = yp_client.create_object("ip4_address_pool", attributes={"meta": {"id": "mega_pool"}})
        yp_client.remove_object("ip4_address_pool", pool_id)

        templates.update_spec_test_template(
            yp_client=yp_client,
            object_type="ip4_address_pool",
            initial_spec={},
            update_path="/meta/acl",
            update_value=[
                {
                    "action": "allow",
                    "permissions": [
                        "read",
                        "write",
                    ],
                    "subjects": [
                        "root",
                    ]
                }
            ],
        )

    def test_create_new_address(self, yp_env):
        yp_client = yp_env.yp_client
        yt_client = yp_env.yt_client

        pool_id = yp_client.create_object("ip4_address_pool", attributes={"meta": {"id": "pool"}})
        assert pool_id == "pool"

        addr_id = yp_client.create_object("internet_address", attributes={
            "meta" : {
                "ip4_address_pool_id": pool_id
            },
            "spec": {
                "ip4_address": "1.3.5.7",
                "network_module_id": "VLA-100.500",
            }
        })
        parent_records = yt_client.select_rows(
            "* from [//yp/db/parents] where object_id=\"{}\" AND object_type=11 AND parent_id=\"pool\"".format(addr_id))
        assert len(list(parent_records)) == 1
