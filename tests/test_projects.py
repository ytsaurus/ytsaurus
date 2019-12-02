from . import templates

from yp.common import YtResponseError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestProjects(object):
    def test_permissions(self, yp_env):
        templates.permissions_test_template(yp_env, "project")

    def test_project_validation_success(self, yp_env):
        yp_client = yp_env.yp_client
        yp_client.create_object("project", attributes={"meta": {"id": "project"}, "spec": {"account_id": "tmp"}})

    def test_project_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("project", attributes={"meta": {"id": "project*"}, "spec": {"account_id": "tmp"}})

    def test_account_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("project", attributes={"meta": {"id": "project"}, "spec": {"account_id": "tmp*"}})

    def test_account_id_not_updatable(self, yp_env):
        yp_client = yp_env.yp_client
        project_id = yp_client.create_object("project", attributes={"meta": {"id": "project"}, "spec": {"account_id": "tmp"}})

        yp_client.create_object("account", attributes={"meta": {"id": "a"}})
        with pytest.raises(YtResponseError):
            yp_client.update_object("project", project_id, set_updates=[{"path": "/spec/account_id", "value": "a"}])
