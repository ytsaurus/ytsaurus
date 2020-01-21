from . import templates

import yp.data_model
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
        spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }
        templates.update_spec_test_template(
            yp_env.yp_client,
            "release_rule",
            initial_spec=spec,
            update_path="/spec/description",
            update_value="desc1"
        )

    def test_release_rule_oneof_selector(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        sandbox_spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }
        docker_spec = {
            "stage_id": stage_id,
            "docker": {"image_name": "image"},
            "patches": {
                "my-patch": {
                    "docker": {
                        "docker_image_ref": {
                            "deploy_unit_id": "du_id",
                            "box_id": "b_id"
                        }
                    }
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }
        yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "val1"}, "spec": sandbox_spec}
        )
        yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "val2"}, "spec": docker_spec}
        )

    def test_release_rule_validation_success(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }
        yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "val"}, "spec": spec}
        )

    def test_release_rule_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "inv*"}, "spec": spec}
            )

    def test_release_rule_patch_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "inv*": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "val"}, "spec": spec}
            )

    def test_release_rule_selector_patches_validation(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = self._create_stage(yp_env)
        valid_spec = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }

        invalid_spec_sandbox_docker_mismatch = {
            "stage_id": stage_id,
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "docker": {
                        "docker_image_ref": {
                            "deploy_unit_id": "du_id",
                            "box_id": "b_id"
                        }
                    }
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }

        invalid_spec_no_selector = {
            "stage_id": stage_id,
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }

        # Case 1: sandbox selector and docker patches cannot be used together on
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
                attributes={"meta": {"id": "val"}, "spec": invalid_spec_no_selector}
            )

        release_rule_id = yp_client.create_object(
            "release_rule",
            attributes={"meta": {"id": "val"}, "spec": valid_spec}
        )

        # Case 3: sandbox selector and docker patches cannot be used together on
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
                set_updates=[{"path": "/spec", "value": invalid_spec_no_selector}]
            )

    def test_release_rule_stage_validation(self, yp_env):
        yp_client = yp_env.yp_client
        spec = {
            "sandbox": {"task_type": "TASK"},
            "patches": {
                "my-patch": {
                    "sandbox": {"sandbox_resource_type": "RESOURCE"}
                }
            },
            "selector_source": yp.data_model.TReleaseRuleSpec.ESelectorSource.CUSTOM
        }

        # Case 1: no stage_id in spec
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release_rule",
                attributes={"meta": {"id": "val"}, "spec": spec}
            )

        stage_id = self._create_stage(yp_env)
        spec["stage_id"] = stage_id
        release_rule_id = yp_client.create_object(
            "release_rule",
            attributes={"spec": spec}
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

        spec["stage_id"] = stage_id
        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            with pytest.raises(YpAuthorizationError):
                client.create_object("release_rule", attributes={"spec": spec})

        # Case 4: all is ok
        upds = [{
            "path": "/meta/acl/end",
            "value": {"action": "allow", "permissions": ["write"], "subjects": [user_id]}}]
        yp_client.update_object("stage", stage_id, set_updates=upds)
        yp_env.sync_access_control()

        spec["stage_id"] = stage_id
        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            client.create_object("release_rule", attributes={"spec": spec})
