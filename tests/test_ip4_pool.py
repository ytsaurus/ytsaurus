from . import templates

import pytest


@pytest.mark.usefixtures("yp_env")
class TestIP4Pools(object):

    def test_update_oject(self, yp_env):
        yp_client = yp_env.yp_client
        pool_id = yp_client.create_object("ip4_pool", attributes={"meta": {"id": "mega_pool"}})
        yp_client.remove_object("ip4_pool", pool_id)

        templates.update_spec_test_template(
            yp_client=yp_client,
            object_type="ip4_pool",
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
