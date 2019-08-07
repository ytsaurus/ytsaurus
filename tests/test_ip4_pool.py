from . import templates

import pytest


@pytest.mark.usefixtures("yp_env")
class TestIP4Pools(object):

    def test_update_oject(self, yp_env):
        yp_client = yp_env.yp_client
        pool_id = yp_client.create_object("ip4_pool", attributes={"meta": {"id": "mega_pool"}})

        templates.update_spec_test_template(
            yp_client=yp_client,
            object_type="ip4_pool",
            initial_spec={"cdir": ["5.45.207.0/24", "5.45.225.48/30"]},
            update_path="/spec/cdir",
            update_value=["5.45.207.0/24"],
        )
