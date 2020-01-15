from . import templates

from yp.common import YtResponseError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestProjects(object):
    def test_permissions(self, yp_env):
        templates.permissions_test_template(yp_env, "project", True, meta_specific_fields={"owner_id": "new_owner"})

    def test_project_validation_success(self, yp_env):
        yp_client = yp_env.yp_client
        yp_client.create_object("project", attributes={"meta": {"id": "project", "owner_id": "new_owner"}, "spec": {"account_id": "tmp"}})

    def test_project_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("project", attributes={"meta": {"id": "project*", "owner_id": "new_owner"}, "spec": {"account_id": "tmp"}})

    def test_account_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("project", attributes={"meta": {"id": "project", "owner_id": "new_owner"}, "spec": {"account_id": "tmp*"}})

    def test_account_id_not_updatable(self, yp_env):
        yp_client = yp_env.yp_client
        project_id = yp_client.create_object("project", attributes={"meta": {"id": "project", "owner_id": "new_owner"}, "spec": {"account_id": "tmp"}})

        yp_client.create_object("account", attributes={"meta": {"id": "a"}})
        with pytest.raises(YtResponseError):
            yp_client.update_object("project", project_id, set_updates=[{"path": "/spec/account_id", "value": "a"}])

    def test_owner_updatable(self, yp_env):
        yp_client = yp_env.yp_client
        project_id = yp_client.create_object("project", attributes={"meta": {"id": "project", "owner_id": "new_owner"}, "spec": {"account_id": "tmp"}})

        yp_client.update_object("project", project_id, set_updates=[{"path": "/meta/owner_id", "value": "a"}])
        result = yp_client.get_object("project", project_id, selectors=["/meta/owner_id"])

        assert result[0] == "a"

    def test_update_empty_owner(self, yp_env):
        yp_client = yp_env.yp_client
        project_id = yp_client.create_object("project", attributes={"meta": {"id": "project", "owner_id": "new_owner"}, "spec": {"account_id": "tmp"}})

        with pytest.raises(YtResponseError):
            yp_client.update_object("project", project_id, set_updates=[{"path": "/meta/owner_id", "value": ""}])

    def test_create_without_account_id(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("project", attributes={"meta": {"id": "project", "owner_id": "new_owner"}})

    def test_create_without_owner(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("project", attributes={"meta": {"id": "project"}, "spec": {"account_id": "tmp"}})

    def test_create_with_user_specific_boxes(self, yp_env):
        yp_client = yp_env.yp_client
        yp_client.create_object("project", attributes={
            "meta": {"id": "project", "owner_id": "new_owner"},
            "spec": {
                "account_id": "tmp",
                "user_specific_box_types": ["my_box", "front_box"]
            }
        })

    def test_user_specific_boxes_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object("project", attributes={
                "meta": {"id": "project", "owner_id": "new_owner"},
                "spec": {
                    "account_id": "tmp",
                    "user_specific_box_types": ["my_box", "front*box"]
                }
            })

    def test_update_user_specific_boxes_id(self, yp_env):
        yp_client = yp_env.yp_client
        project_id = yp_client.create_object("project", attributes={
            "meta": {"id": "project", "owner_id": "new_owner"}, "spec": {"account_id": "tmp"}
        })

        yp_client.update_object("project", project_id, set_updates=[
            {"path": "/spec/user_specific_box_types", "value": ["new_box"]}
        ])

        result = yp_client.get_object("project", project_id, selectors=["/spec/user_specific_box_types"])

        assert result[0] == ["new_box"]

        with pytest.raises(YtResponseError):
            yp_client.update_object("project", project_id, set_updates=[
                {"path": "/spec/user_specific_box_types", "value": ["new.box"]}
            ])

        result = yp_client.get_object("project", project_id, selectors=["/spec/user_specific_box_types"])

        assert result[0] == ["new_box"]
