from . import templates

from yp.common import YtResponseError, YpAuthorizationError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestReleaseRules(object):
    def _create_stage(self, yp_env):
        return yp_env.yp_client.create_object(
            "stage",
            attributes={"meta": {"id": "stage"}, "spec": {"account_id": "tmp"}}
        )

    def test_update_spec(self, yp_env):
        stage_id = self._create_stage(yp_env)
        templates.update_spec_test_template(
            yp_env.yp_client,
            "release_rule",
            initial_spec={"stage_id": stage_id, "description": "desc"},
            update_path="/spec/description",
            update_value="desc1"
        )

    def test_release_rule_oneof_selector(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        invalid_oneof_spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "docker": {"image_name": "image"}
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "val"}, "spec": invalid_oneof_spec}
            )

    def test_release_rule_validation_success(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "val"}, "spec": {"stage_id": stage_id}}
        )

    def test_release_rule_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "inv*"}, "spec": {"stage_id": stage_id}}
            )

    def test_release_rule_selector_patch_validation(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        valid_spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "static_sandbox_resource": {"patches": [{"sandbox_resource_type": "RESOURCE"}]}
        }
        invalid_spec_sandbox_docker_mismatch = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "docker_image": {
                "patches": {
                    "box_refs": [
                        {"deploy_unit_id": "du_id", "box_id": "b_id"}
                    ]
                }
            }
        }

        invalid_spec_empty_sandbox_selector = {
            "stage_id": stage_id,
            "sandbox": {},
            "static_sandbox_resource": {"patches": [{"sandbox_resource_type": "RESOURCE"}]}
        }

        # Case 1: sandbox selector and docker patch cannot be used together on
        # create
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "val"}, "spec": invalid_spec_sandbox_docker_mismatch}
            )

        # Case 2: empty sandbox selector on create
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "val"}, "spec": invalid_spec_empty_sandbox_selector}
            )

        release_rule_id = yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "val"}, "spec": valid_spec}
        )

        # Case 3: sandbox selector and docker patch cannot be used together on
        # update
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "release_rule",
                release_rule_id,
                set_updates=[{"path": "/spec", "value": invalid_spec_sandbox_docker_mismatch}]
            )

        # Case 4: empty sandbox selector on update
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "release_rule",
                release_rule_id,
                set_updates=[{"path": "/spec", "value": invalid_spec_empty_sandbox_selector}]
            )

    def test_release_rule_stage_validation(self, yp_env):
        yp_client = yp_env.yp_client

        # Case 1: no stage_id in spec
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "val"}, "spec": {}}
            )

        stage_id = self._create_stage(yp_env)
        release_rule_id = yp_client.create_object(
            "release_rule",
            attributes={"spec": {"stage_id": stage_id}}
        )

        # Case 2: stage_id cannot be set to null
        with pytest.raises(YtResponseError) as exc:
            yp_client.update_object(
                "release_rule",
                release_rule_id,
                set_updates=[{"path": "/spec/stage_id", "value": ""}]
            )
            assert exc.contains_text("Cannot set null stage")

        # Case 3: user has no write permission to stage
        user_id = yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            with pytest.raises(YpAuthorizationError):
                client.create_object("release_rule", attributes={"spec": {"stage_id": stage_id}})

        # Case 4: all is ok
        upds = [{
            "path": "/meta/acl/end",
            "value": {"action": "allow", "permissions": ["write"], "subjects": [user_id]}}]
        yp_client.update_object("stage", stage_id, set_updates=upds)
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            client.create_object("release_rule", attributes={"spec": {"stage_id": stage_id}})
