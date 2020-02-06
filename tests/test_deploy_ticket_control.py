from yp.common import YtResponseError

import pytest


sandbox_task_type = "test-sandbox-task-type"
sandbox_task_id = "test-sandbox-task"
sandbox_resource_id1 = "test-sandbox-resource-1"
sandbox_resource_id2 = "test-sandbox-resource-1"

deploy_unit = "test-deploy-unit"
resource_type1 = "test-type-1"
resource_type2 = "test-type-2"
resource_ref1 = "test-resource-1"
resource_ref2 = "test-resource-2"

deploy_patch1 = "test-patch-1"
deploy_patch2 = "test-patch-2"
new_md5 = "12345"
empty_md5 = ""
new_skynet_id = "test-skynet-id"

new_resource_meta1 = dict(
    task_type=sandbox_task_type,
    task_id=sandbox_task_id,
    resource_type=resource_type1,
    resource_id=sandbox_resource_id1
)

new_resource_meta2 = dict(
    task_type=sandbox_task_type,
    task_id=sandbox_task_id,
    resource_type=resource_type2,
    resource_id=sandbox_resource_id2
)


def prepare_objects(yp_client):
    release_id = yp_client.create_object("release", attributes={
        "spec": {
            "sandbox": {
                "task_id": sandbox_task_id,
                "task_type": sandbox_task_type,
                "release_type": "test-release-type",
                "resources": [
                    {
                        "resource_id": sandbox_resource_id1,
                        "type": resource_type1,
                        "file_md5": new_md5,
                        "skynet_id": new_skynet_id
                    },
                    {
                        "resource_id": sandbox_resource_id2,
                        "type": resource_type2,
                        "file_md5": empty_md5,
                        "skynet_id": new_skynet_id
                    }
                ]
            }
        }
    })

    stage_id = yp_client.create_object("stage", attributes={
        "spec": {
            "deploy_units": {
                deploy_unit: {
                    "replica_set": {
                        "replica_set_template": {
                            "pod_template_spec": {
                                "spec": {
                                    "pod_agent_payload": {
                                        "spec": {
                                            "resources": {
                                                "static_resources": [
                                                    {
                                                        "id": resource_ref1,
                                                        "url": "default",
                                                        "verification": {
                                                            "checksum": "default"
                                                        },
                                                        "meta": {
                                                            "sandbox_resource": {}
                                                        }
                                                    },
                                                    {
                                                        "id": resource_ref2,
                                                        "url": "default",
                                                        "verification": {
                                                            "checksum": "default"
                                                        },
                                                        "meta": {
                                                            "sandbox_resource": {}
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })

    release_rule_id = yp_client.create_object("release_rule", attributes={
        "spec": {
            "stage_id": stage_id,
            "sandbox": {
                "task_type": sandbox_task_type
            },
            "patches": {
                deploy_patch1: {
                    "sandbox": {
                        "sandbox_resource_type": resource_type1,
                        "static": {
                            "deploy_unit_id": deploy_unit,
                            "static_resource_ref": resource_ref1
                        }
                    }
                },
                deploy_patch2: {
                    "sandbox": {
                        "sandbox_resource_type": resource_type2,
                        "static": {
                            "deploy_unit_id": deploy_unit,
                            "static_resource_ref": resource_ref2
                        }
                    }
                }
            }
        }
    })

    default_action = {
        "type": "on_hold",
        "reason": "ON_HOLD",
        "message": "hold"
    }

    ticket_id = yp_client.create_object("deploy_ticket", attributes={
        "spec": {
            "release_id": release_id,
            "stage_id": stage_id,
            "release_rule_id": release_rule_id,
            "patches": {
                deploy_patch1: {
                    "sandbox": {
                        "sandbox_resource_type": resource_type1,
                        "static": {
                            "deploy_unit_id": deploy_unit,
                            "static_resource_ref": resource_ref1
                        }
                    }
                },
                deploy_patch2: {
                    "sandbox": {
                        "sandbox_resource_type": resource_type2,
                        "static": {
                            "deploy_unit_id": deploy_unit,
                            "static_resource_ref": resource_ref2
                        }
                    }
                }
            }
        },
        "status": {
            "action": default_action,
            "patches": {
                deploy_patch1: {
                    "action": default_action
                },
                deploy_patch2: {
                    "action": default_action
                }
            }
        }
    })

    return stage_id, ticket_id


def check_action_state(action, type, reason, message):
    assert action["type"] == type
    assert action["reason"] == reason
    assert action["message"] == message


def check_static_resource_state(static_resource, url, md5, sandbox_meta):
    assert static_resource["url"] == url
    assert static_resource["verification"]["checksum"] == md5
    assert static_resource["meta"]["sandbox_resource"] == sandbox_meta


@pytest.mark.usefixtures("yp_env_configurable")
class TestCommitDeployTicket(object):
    def test_full_commit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]

        check_action_state(ticket_action_result, "commit", "COMMITTED", "new commit")

        static_resources = yp_client.get_object("stage", stage_id, selectors=[
            "/spec/deploy_units/test-deploy-unit/replica_set/replica_set_template/pod_template_spec/spec/pod_agent_payload/spec/resources/static_resources"
        ])[0]

        check_static_resource_state(static_resources[0], new_skynet_id, "MD5:" + new_md5, new_resource_meta1)
        check_static_resource_state(static_resources[1], new_skynet_id, "EMPTY:", new_resource_meta2)

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "commit", "COMMITTED", "Parent COMMITTED: new commit")
        check_action_state(patch_actions_result[1], "commit", "COMMITTED", "Parent COMMITTED: new commit")

    def test_partial_commit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="partial", patch_ids=[deploy_patch1], message="new commit", reason="COMMITTED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "on_hold", "ON_HOLD", "hold")

        static_resources = yp_client.get_object("stage", stage_id, selectors=[
            "/spec/deploy_units/test-deploy-unit/replica_set/replica_set_template/pod_template_spec/spec/pod_agent_payload/spec/resources/static_resources"
        ])[0]

        check_static_resource_state(static_resources[0], new_skynet_id, "MD5:" + new_md5, new_resource_meta1)
        check_static_resource_state(static_resources[1], "default", "default", {})

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "commit", "COMMITTED", "new commit")
        check_action_state(patch_actions_result[1], "on_hold", "ON_HOLD", "hold")

    def test_full_commit_when_some_patches_already_committed(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("deploy_ticket", ticket_id, set_updates=[{
            "path": "/status/patches/{}/action".format(deploy_patch1),
            "value": {
                "type": "commit",
                "message": "old commit",
                "reason": "COMMITTED"
            }
        }])

        yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "commit", "COMMITTED", "new commit")

        static_resources = yp_client.get_object("stage", stage_id, selectors=[
            "/spec/deploy_units/test-deploy-unit/replica_set/replica_set_template/pod_template_spec/spec/pod_agent_payload/spec/resources/static_resources"
        ])[0]

        check_static_resource_state(static_resources[0], "default", "default", {})
        check_static_resource_state(static_resources[1], new_skynet_id, "EMPTY:", new_resource_meta2)

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "commit", "COMMITTED", "old commit")
        check_action_state(patch_actions_result[1], "commit", "COMMITTED", "Parent COMMITTED: new commit")


    def test_commit_patch_which_already_committed(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("deploy_ticket", ticket_id, set_updates=[{
            "path": "/status/patches/{}/action".format(deploy_patch1),
            "value": {
                "type": "commit",
                "message": "old commit",
                "reason": "COMMITTED"
            }
        }])

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="partial", patch_ids=[deploy_patch1], message="new commit", reason="COMMITTED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "on_hold", "ON_HOLD", "hold")

        static_resources = yp_client.get_object("stage", stage_id, selectors=[
            "/spec/deploy_units/test-deploy-unit/replica_set/replica_set_template/pod_template_spec/spec/pod_agent_payload/spec/resources/static_resources"
        ])[0]


        check_static_resource_state(static_resources[0], "default", "default", {})
        check_static_resource_state(static_resources[1], "default", "default", {})

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "commit", "COMMITTED", "old commit")
        check_action_state(patch_actions_result[1], "on_hold", "ON_HOLD", "hold")

    def test_commit_ticket_which_already_committed(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("deploy_ticket", ticket_id, set_updates=[{
            "path": "/status/action",
            "value": {
                "type": "commit",
                "message": "old commit",
                "reason": "COMMITTED"
            }
        }])

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "commit", "COMMITTED", "old commit")

    def test_full_skip(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.skip_deploy_ticket(ticket_id=ticket_id, type="full", message="skip ticket", reason="SKIPPED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "skip", "SKIPPED", "skip ticket")

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "skip", "SKIPPED", "Parent SKIPPED: skip ticket")
        check_action_state(patch_actions_result[1], "skip", "SKIPPED", "Parent SKIPPED: skip ticket")

    def test_partial_skip(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.skip_deploy_ticket(ticket_id=ticket_id, type="partial", patch_ids=[deploy_patch1], message="skip patch", reason="SKIPPED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "on_hold", "ON_HOLD", "hold")

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "skip", "SKIPPED", "skip patch")
        check_action_state(patch_actions_result[1], "on_hold", "ON_HOLD", "hold")

    def test_full_skip_when_some_patches_already_skipped(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("deploy_ticket", ticket_id, set_updates=[{
            "path": "/status/patches/{}/action".format(deploy_patch1),
            "value": {
                "type": "skip",
                "message": "skip patch",
                "reason": "SKIPPED"
            }
        }])

        yp_client.skip_deploy_ticket(ticket_id=ticket_id, type="full", message="skip ticket", reason="SKIPPED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "skip", "SKIPPED", "skip ticket")

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "skip", "SKIPPED", "skip patch")
        check_action_state(patch_actions_result[1], "skip", "SKIPPED", "Parent SKIPPED: skip ticket")

    def test_skip_patch_which_already_has_terminal_state(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("deploy_ticket", ticket_id, set_updates=[{
            "path": "/status/patches/{}/action".format(deploy_patch1),
            "value": {
                "type": "commit",
                "message": "old commit",
                "reason": "COMMITTED"
            }
        }])

        with pytest.raises(YtResponseError):
            yp_client.skip_deploy_ticket(ticket_id=ticket_id, type="partial", patch_ids=[deploy_patch1], message="skip patch", reason="SKIPPED")

        patch_actions_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2)
        ])

        check_action_state(patch_actions_result[0], "commit", "COMMITTED", "old commit")
        check_action_state(patch_actions_result[1], "on_hold", "ON_HOLD", "hold")

    def test_skip_ticket_which_already_has_terminal_state(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("deploy_ticket", ticket_id, set_updates=[{
            "path": "/status/action",
            "value": {
                "type": "commit",
                "message": "old commit",
                "reason": "COMMITTED"
            }
        }])

        with pytest.raises(YtResponseError):
            yp_client.skip_deploy_ticket(ticket_id=ticket_id, type="full", message="skip ticket", reason="SKIPPED")

        ticket_action_result = yp_client.get_object("deploy_ticket", ticket_id, selectors=["/status/action"])[0]
        check_action_state(ticket_action_result, "commit", "COMMITTED", "old commit")

    def test_commit_when_deploy_unit_does_not_exist(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("stage", stage_id, set_updates=[{
            "path": "/spec/deploy_units",
            "value": {}
        }])

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="full", message="commit ticket", reason="COMMITTED")

    def test_commit_when_pod_deploy_primitive_does_not_set(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("stage", stage_id, set_updates=[{
            "path": "/spec/deploy_units/" + deploy_unit,
            "value": {}
        }])

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="full", message="commit ticket", reason="COMMITTED")

    def test_commit_when_resource_id_does_not_exist(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("stage", stage_id, set_updates=[{
            "path": "/spec/deploy_units/test-deploy-unit/replica_set/replica_set_template/pod_template_spec/spec/pod_agent_payload/spec/resources/static_resources",
            "value": [{
                "id": resource_ref1,
                "url": "default",
                "verification": {
                    "checksum": "default"
                },
                "meta": {
                    "sandbox_resource": {}
                }
            }]
        }])

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="full", message="commit ticket", reason="COMMITTED")

    def test_commit_when_resource_type_from_patch_does_not_exist_in_release(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_objects(yp_client)

        yp_client.update_object("deploy_ticket", ticket_id, set_updates=[{
            "path": "/spec/patches/{}/sandbox/sandbox_resource_type".format(deploy_patch2),
            "value": "unknown-type"
        }])

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(ticket_id=ticket_id, type="full", message="commit ticket", reason="COMMITTED")
