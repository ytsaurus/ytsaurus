from . import templates

from yp.common import YtResponseError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestStages(object):
    def test_permissions(self, yp_env):
        templates.permissions_test_template(yp_env, "stage")

    def test_update_spec(self, yp_env):
        templates.update_spec_revision_test_template(yp_env.yp_client, "stage")

    def test_stage_validation_success(self, yp_env):
        yp_client = yp_env.yp_client
        yp_client.create_object("stage", attributes={"meta": {"id": "val"}, "spec": {"account_id": "tmp"}})

    def test_stage_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("stage", attributes={"meta": {"id": "inv*"}, "spec": {"account_id": "tmp"}})

    def test_stage_spec_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("stage", attributes={"meta": {"id": "val"}, "spec": {"account_id": "tmp", "deploy_units": {"inv*": {}}}})

        stage_id = yp_client.create_object("stage", attributes={"meta": {"id": "val"}, "spec": {"account_id": "tmp", "deploy_units": {"correct_deploy_unit_id": {}}}})
        with pytest.raises(YtResponseError):
            yp_client.update_object("stage", stage_id, set_updates=[{"path": "/spec", "value": {"account_id": "tmp", "deploy_units": {"inv*": {}}}}])

    def test_update_project_id(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = yp_client.create_object("stage", attributes={"meta": {"id": "stage_id", "project_id": "project1"}, "spec": {"account_id": "tmp"}})
        yp_client.update_object("stage", stage_id, set_updates=[{"path": "/meta/project_id", "value": "project2"}])
