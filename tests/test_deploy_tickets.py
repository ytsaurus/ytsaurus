from . import templates
from .conftest import create_user

from yp.common import YtResponseError, YpAuthorizationError

import pytest


@pytest.mark.usefixtures("yp_env")
class TestDeployTickets(object):
    def _create_stage(self, yp_env):
        return yp_env.yp_client.create_object(
            "stage", attributes={"meta": {"id": "stage"}, "spec": {"account_id": "tmp"}}
        )

    def _create_release(self, yp_env):
        spec = {
            "docker": {
                "image_name": "some_image_name",
                "image_tag": "some_image_tag",
                "image_hash": "some_image_hash",
                "release_type": "stable",
            }
        }
        return yp_env.yp_client.create_object(
            "release", attributes={"meta": {"id": "release"}, "spec": spec}
        )

    def _create_release_rule(self, yp_env, stage_id):
        spec = {
            "sandbox": {"task_type": "TASK"},
            "patches": {"my-patch": {"sandbox": {"sandbox_resource_type": "RESOURCE"}}},
        }
        return yp_env.yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "release-rule", "stage_id": stage_id}, "spec": spec},
        )

    def test_update_spec(self, yp_env):
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        templates.update_spec_test_template(
            yp_env.yp_client,
            "deploy_ticket",
            initial_spec={
                "release_id": release_id,
                "release_rule_id": release_rule_id,
                "description": "desc",
                "patches": {"my-patch": {"sandbox": {"sandbox_resource_type": "RESOURCE"}}},
            },
            update_path="/spec/description",
            update_value="desc1",
            initial_meta={"stage_id": stage_id},
        )

    def test_deploy_ticket_validation_success(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {"my-patch": {"sandbox": {"sandbox_resource_type": "RESOURCE"}}},
        }
        yp_client.create_object(
            "deploy_ticket", attributes={"meta": {"id": "val", "stage_id": stage_id}, "spec": spec}
        )

    def test_deploy_ticket_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {"my-patch": {"sandbox": {"sandbox_resource_type": "RESOURCE"}}},
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "deploy_ticket",
                attributes={"meta": {"id": "inv*", "stage_id": stage_id}, "spec": spec},
            )

    def test_deploy_ticket_patch_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {"inv*": {"sandbox": {"sandbox_resource_type": "RESOURCE"}}},
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "deploy_ticket",
                attributes={"meta": {"id": "val", "stage_id": stage_id}, "spec": spec},
            )

    def test_deploy_ticket_stage_validation(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)

        spec = {
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {"my-patch": {"sandbox": {"sandbox_resource_type": "RESOURCE"}}},
        }
        meta = {}

        # Case 1: no stage_id in meta
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "deploy_ticket", attributes={"meta": {"id": "val"}, "spec": spec}
            )

        meta["stage_id"] = stage_id
        deploy_ticket_id = yp_client.create_object(
            "deploy_ticket", attributes={"meta": meta, "spec": spec}
        )

        # Case 2: stage_id not updatable
        with pytest.raises(YtResponseError) as exc:
            yp_client.update_object(
                "deploy_ticket",
                deploy_ticket_id,
                set_updates=[{"path": "/meta/stage_id", "value": "another"}],
            )

        # Case 3: user has no write permission to stage
        user_id = create_user(
            yp_client,
            grant_create_permission_for_types=("deploy_ticket",),
        )
        yp_env.sync_access_control()

        def create_deploy_ticket():
            with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
                client.create_object("deploy_ticket", attributes={"meta": meta, "spec": spec})

        with pytest.raises(YpAuthorizationError):
            create_deploy_ticket()

        # Case 4: all is ok
        upds = [
            {
                "path": "/meta/acl/end",
                "value": {"action": "allow", "permissions": ["write"], "subjects": [user_id]},
            }
        ]
        yp_client.update_object("stage", stage_id, set_updates=upds)
        create_deploy_ticket()

    def test_inherit_acl(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        release_id = self._create_release(yp_env)
        release_rule_id = self._create_release_rule(yp_env, stage_id)
        spec = {
            "release_id": release_id,
            "release_rule_id": release_rule_id,
            "patches": {"my-patch": {"sandbox": {"sandbox_resource_type": "RESOURCE"}}},
        }

        deploy_ticket_id = yp_client.create_object(
            "deploy_ticket", attributes={"meta": {"id": "val", "stage_id": stage_id}, "spec": spec}
        )

        user = create_user(yp_client)
        yp_client.update_object(
            "stage",
            stage_id,
            set_updates=[
                {
                    "path": "/meta/acl/end",
                    "value": {"action": "allow", "permissions": ["write"], "subjects": [user]},
                }
            ],
        )
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user}) as client:
            client.update_object(
                "deploy_ticket",
                deploy_ticket_id,
                set_updates=[
                    {
                        "path": "/spec/patches/my-patch/sandbox/sandbox_resource_type",
                        "value": "ANOTHER",
                    }
                ],
            )
