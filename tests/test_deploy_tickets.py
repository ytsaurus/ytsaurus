from . import templates

from yp.common import YtResponseError, YpAuthorizationError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestDeployTickets(object):
    def _create_stage(self, yp_env):
        return yp_env.yp_client.create_object(
            "stage",
            attributes={"meta": {"id": "stage"}, "spec": {"account_id": "tmp"}}
        )

    def _create_release(self, yp_env):
        spec = {
            'docker': {
                'image_name': 'some_image_name',
                'image_tag': 'some_image_tag',
                'image_hash': 'some_image_hash',
                'release_type': 'stable'
            }
        }
        return yp_env.yp_client.create_object(
            "release",
            attributes={"meta": {"id": "release"}, "spec": spec}
        )

    def _create_release_rule(self, yp_env, stage_id):
        spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            }
        }
        return yp_env.yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "release-rule"}, "spec": spec}
        )

    def test_update_spec(self, yp_env):
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        templates.update_spec_test_template(
            yp_env.yp_client,
            "deploy_ticket",
            initial_spec={
                "stage_id": stage_id,
                "release_id": release_id,
                "release_rule_id": release_rule_id,
                "description": "desc",
                "patches": {
                    "my-patch": {
                        "sandbox": {"sandbox_resource_type": "RESOURCE"}
                    }
                }
            },
            update_path="/spec/description",
            update_value="desc1"
        )

    def test_deploy_ticket_validation_success(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "stage_id": stage_id,
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            }
        }
        yp_client.create_object(
            "deploy_ticket",
            attributes={"meta": {"id": "val"}, "spec": spec}
        )

    def test_deploy_ticket_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "stage_id": stage_id,
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            }
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "deploy_ticket",
                attributes={"meta": {"id": "inv*"}, "spec": spec}
            )

    def test_deploy_ticket_patch_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "stage_id": stage_id,
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {
                "inv*": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            }
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "deploy_ticket",
                attributes={"meta": {"id": "val"}, "spec": spec}
            )

    def test_deploy_ticket_stage_validation(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            }
        }

        # Case 1: no stage_id in spec
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "deploy_ticket",
                attributes={"meta": {"id": "val"}, "spec": spec}
            )

        spec["stage_id"] = stage_id
        deploy_ticket_id = yp_client.create_object(
            "deploy_ticket",
            attributes={"spec": spec}
        )

        # Case 2: stage_id cannot be set to null
        with pytest.raises(YtResponseError) as exc:
            yp_client.update_object(
                "deploy_ticket",
                deploy_ticket_id,
                set_updates=[{"path": "/spec/stage_id", "value": ""}]
            )
            assert exc.contains_text("Cannot set null stage")

        # Case 3: user has no write permission to stage
        user_id = yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        upds = [{
            "path": "/meta/acl/end",
            "value": {"action": "allow", "permissions": ["create"], "subjects": [user_id]}}]
        yp_client.update_object("schema", "deploy_ticket", set_updates=upds)
        yp_env.sync_access_control()
        spec["stage_id"] = stage_id

        def create_deploy_ticket():
            with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
                client.create_object("deploy_ticket", attributes={"spec": spec})

        with pytest.raises(YpAuthorizationError):
            create_deploy_ticket()

        # Case 4: all is ok
        upds = [{
            "path": "/meta/acl/end",
            "value": {"action": "allow", "permissions": ["write"], "subjects": [user_id]}}]
        yp_client.update_object("stage", stage_id, set_updates=upds)
        create_deploy_ticket()
