from . import templates

import pytest

@pytest.mark.usefixtures("yp_env")
class TestStages(object):
    def test_permissions(self, yp_env):
        templates.permissions_test_template(yp_env, "stage")

    def test_update_spec(self, yp_env):
        templates.update_spec_revision_test_template(yp_env.yp_client, "stage")

