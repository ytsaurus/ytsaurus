from .conftest import create_user

from yp.common import YtResponseError

import pytest


sandbox_task_type = "test-sandbox-task-type"
sandbox_task_id = "test-sandbox-task"
sandbox_resource_id1 = "test-sandbox-resource-1"
sandbox_resource_id2 = "test-sandbox-resource-2"
sandbox_resource_id3 = "test-sandbox-resource-3"

deploy_unit = "test-deploy-unit"
box1 = "box-1"
box2 = "box-2"
resource_type1 = "test-type-1"
resource_type2 = "test-type-2"
resource_type3 = "test-type-3"
resource_ref1 = "test-resource-1"
resource_ref2 = "test-resource-2"
layer_ref1 = "test-layer-1"

deploy_patch1 = "test-patch-1"
deploy_patch2 = "test-patch-2"
deploy_patch3 = "test-patch-3"
new_md5 = "12345"
empty_md5 = ""
new_skynet_id = "test-skynet-id"

image_name = "test-image-name"
image_tag = "test-image-tag"

new_resource_meta1 = dict(
    task_type=sandbox_task_type,
    task_id=sandbox_task_id,
    resource_type=resource_type1,
    resource_id=sandbox_resource_id1,
)

new_resource_meta2 = dict(
    task_type=sandbox_task_type,
    task_id=sandbox_task_id,
    resource_type=resource_type2,
    resource_id=sandbox_resource_id2,
)

new_resource_meta3 = dict(
    task_type=sandbox_task_type,
    task_id=sandbox_task_id,
    resource_type=resource_type3,
    resource_id=sandbox_resource_id3,
)

default_action = {"type": "on_hold", "reason": "ON_HOLD", "message": "hold"}

default_docker_resource = {"name": "default", "tag": "default"}


def get_default_stage_spec():
    return {
        "revision": 1,
        "revision_info": {
            "description": "empty"
        },
        "deploy_units": {
            deploy_unit: {
                "replica_set": {
                    "replica_set_template": {
                        "pod_template_spec": {
                            "spec": {
                                "pod_agent_payload": {
                                    "spec": {
                                        "resources": {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    }


def create_deploy_ticket_object(yp_client, stage_id, release_id, release_rule_id, patches):
    return yp_client.create_object(
        "deploy_ticket",
        attributes={
            "meta": {"id": "deploy-ticket", "stage_id": stage_id},
            "spec": {
                "release_id": release_id,
                "release_rule_id": release_rule_id,
                "patches": patches,
            },
            "status": {
                "action": default_action,
                "patches": {
                    patch_id: {"action": default_action} for patch_id in patches.keys()
                },
            },
        },
    )


def create_release_static_resource(resource_id, resource_type, file_md5, skynet_id):
    return {
        "resource_id": resource_id,
        "type": resource_type,
        "file_md5": file_md5,
        "skynet_id": skynet_id,
    }


def create_sandbox_deploy_patch(resource_type, resource_key, resource_ref):
    return {
        "sandbox": {
            "sandbox_resource_type": resource_type,
            "static": {"deploy_unit_id": deploy_unit, resource_key: resource_ref},
        }
    }


def prepare_sandbox_resources_objects(yp_client):

    release_id = yp_client.create_object(
        "release",
        attributes={
            "spec": {
                "sandbox": {
                    "task_id": sandbox_task_id,
                    "task_type": sandbox_task_type,
                    "release_type": "test-release-type",
                    "resources": [
                        create_release_static_resource(
                            sandbox_resource_id1, resource_type1, new_md5, new_skynet_id
                        ),
                        create_release_static_resource(
                            sandbox_resource_id2, resource_type2, empty_md5, new_skynet_id
                        ),
                        create_release_static_resource(
                            sandbox_resource_id3, resource_type3, new_md5, new_skynet_id
                        ),
                    ],
                }
            }
        },
    )

    def create_static_resource_default_info(resource_ref):
        return {
            "id": resource_ref,
            "url": "default",
            "verification": {"checksum": "default"},
            "meta": {"sandbox_resource": {}},
        }

    def create_layer_resource_default_info(resource_ref):
        return {
            "id": resource_ref,
            "url": "default",
            "checksum": "default",
            "meta": {"sandbox_resource": {}},
        }

    spec_with_resources = get_default_stage_spec()
    spec_with_resources["deploy_units"][deploy_unit]["replica_set"]["replica_set_template"]["pod_template_spec"]["spec"]["pod_agent_payload"]["spec"]["resources"] = {
        "static_resources": [
            create_static_resource_default_info(
                resource_ref1
            ),
            create_static_resource_default_info(
                resource_ref2
            ),
        ],
        "layers": [
            create_layer_resource_default_info(
                layer_ref1
            )
        ],
    }

    stage_id = yp_client.create_object(
        "stage",
        attributes={
            "meta": {"id": "stage-id", "project_id": "project"},
            "spec": spec_with_resources,
        },
    )

    patches = {
        deploy_patch1: create_sandbox_deploy_patch(
            resource_type1, "static_resource_ref", resource_ref1
        ),
        deploy_patch2: create_sandbox_deploy_patch(
            resource_type2, "static_resource_ref", resource_ref2
        ),
        deploy_patch3: create_sandbox_deploy_patch(resource_type3, "layer_ref", layer_ref1),
    }

    release_rule_id = yp_client.create_object(
        "release_rule",
        attributes={
            "meta": {"id": "release-rule", "stage_id": stage_id},
            "spec": {"sandbox": {"task_type": sandbox_task_type}, "patches": patches},
        },
    )

    ticket_id = create_deploy_ticket_object(
        yp_client, stage_id, release_id, release_rule_id, patches
    )

    return stage_id, ticket_id


def prepare_docker_resources_objects(yp_client):
    release_id = yp_client.create_object(
        "release",
        attributes={
            "spec": {
                "docker": {
                    "image_name": image_name,
                    "image_tag": image_tag,
                    "image_hash": "hash",
                    "release_type": "test-release-type",
                }
            }
        },
    )

    stage_id = yp_client.create_object(
        "stage",
        attributes={
            "meta": {"id": "stage-id", "project_id": "project"},
            "spec": {
                "revision": 1,
                "revision_info": {
                    "description": "empty"
                },
                "deploy_units": {
                    deploy_unit: {
                        "images_for_boxes": {
                            box1: default_docker_resource,
                            box2: default_docker_resource,
                        },
                        "replica_set": {
                            "replica_set_template": {
                                "pod_template_spec": {
                                    "spec": {
                                        "pod_agent_payload": {
                                            "spec": {"boxes": [{"id": box1}, {"id": box2}]}
                                        }
                                    }
                                }
                            }
                        },
                    }
                },
            },
        },
    )

    def create_docker_patch(box_id):
        return {"docker": {"docker_image_ref": {"deploy_unit_id": deploy_unit, "box_id": box_id}}}

    patches = {deploy_patch1: create_docker_patch(box1), deploy_patch2: create_docker_patch(box2)}

    release_rule_id = yp_client.create_object(
        "release_rule",
        attributes={
            "meta": {"id": "release-rule", "stage_id": stage_id},
            "spec": {"docker": {"image_name": image_name}, "patches": patches},
        },
    )

    ticket_id = create_deploy_ticket_object(
        yp_client, stage_id, release_id, release_rule_id, patches
    )

    return stage_id, ticket_id


def check_static_resource_state(static_resource, need_static_resource_state):
    assert static_resource["url"] == need_static_resource_state["url"]
    assert (
        static_resource["meta"]["sandbox_resource"]
        == need_static_resource_state["meta"]["sandbox_resource"]
    )

    if "verification" in static_resource.keys():
        assert (
            static_resource["verification"]["checksum"]
            == need_static_resource_state["verification"]["checksum"]
        )
    else:
        assert static_resource["checksum"] == need_static_resource_state["checksum"]


def check_all_states_after_sandbox_release(
    yp_client,
    ticket_id,
    stage_id,
    ticket_action_state,
    static_resource_state1,
    static_resource_state2,
    layer_state1,
    patch_action_state1,
    patch_action_state2,
    patch_action_state3,
    stage_revision,
    commit_message
):

    ticket_action_result = yp_client.get_object(
        "deploy_ticket", ticket_id, selectors=["/status/action"]
    )[0]

    assert ticket_action_result == ticket_action_state

    resources_path = "/spec/deploy_units/test-deploy-unit/replica_set/replica_set_template/pod_template_spec/spec/pod_agent_payload/spec/resources/"
    static_resources = yp_client.get_object(
        "stage",
        stage_id,
        selectors=[resources_path + "static_resources", resources_path + "layers"],
    )

    check_static_resource_state(static_resources[0][0], static_resource_state1)
    check_static_resource_state(static_resources[0][1], static_resource_state2)
    check_static_resource_state(static_resources[1][0], layer_state1)

    patch_actions_result = yp_client.get_object(
        "deploy_ticket",
        ticket_id,
        selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2),
            "/status/patches/{}/action".format(deploy_patch3),
        ],
    )

    assert patch_actions_result[0] == patch_action_state1
    assert patch_actions_result[1] == patch_action_state2
    assert patch_actions_result[2] == patch_action_state3

    stage_revision_result = yp_client.get_object("stage", stage_id, selectors=["/spec/revision", "/spec/revision_info/description"])
    assert stage_revision_result[0] == stage_revision
    assert stage_revision_result[1] == commit_message


def construct_static_resource_state(url, file_md5, resource_meta):
    return {
        "url": url,
        "verification": {"checksum": file_md5},
        "meta": {"sandbox_resource": resource_meta},
    }


def construct_layer_state(url, file_md5, resource_meta):
    return {"url": url, "checksum": file_md5, "meta": {"sandbox_resource": resource_meta}}


def construct_commit_action_state(message):
    return {"type": "commit", "reason": "COMMITTED", "message": message}


def construct_skip_action_state(message):
    return {"type": "skip", "reason": "SKIPPED", "message": message}


def check_all_states_after_docker_release(
    yp_client,
    ticket_id,
    stage_id,
    ticket_action_state,
    docker_resource_state1,
    docker_resource_state2,
    patch_action_state1,
    patch_action_state2,
    stage_revision,
    commit_message,
):

    ticket_action_result = yp_client.get_object(
        "deploy_ticket", ticket_id, selectors=["/status/action"]
    )[0]

    assert ticket_action_result == ticket_action_state

    docker_resources = yp_client.get_object(
        "stage",
        stage_id,
        selectors=[
            "/spec/deploy_units/test-deploy-unit/images_for_boxes/" + box1,
            "/spec/deploy_units/test-deploy-unit/images_for_boxes/" + box2,
        ],
    )

    assert docker_resources[0] == docker_resource_state1
    assert docker_resources[1] == docker_resource_state2

    patch_actions_result = yp_client.get_object(
        "deploy_ticket",
        ticket_id,
        selectors=[
            "/status/patches/{}/action".format(deploy_patch1),
            "/status/patches/{}/action".format(deploy_patch2),
        ],
    )

    assert patch_actions_result[0] == patch_action_state1
    assert patch_actions_result[1] == patch_action_state2

    stage_revision_result = yp_client.get_object("stage", stage_id, selectors=["/spec/revision", "/spec/revision_info/description"])
    assert stage_revision_result[0] == stage_revision
    assert stage_revision_result[1] == commit_message


@pytest.mark.usefixtures("yp_env_configurable")
class TestCommitDeployTicket(object):
    def test_full_commit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.commit_deploy_ticket(
            ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
        )

        check_all_states_after_sandbox_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=construct_commit_action_state(message="new commit"),
            static_resource_state1=construct_static_resource_state(
                new_skynet_id, "MD5:" + new_md5, new_resource_meta1
            ),
            static_resource_state2=construct_static_resource_state(
                new_skynet_id, "EMPTY:", new_resource_meta2
            ),
            layer_state1=construct_layer_state(new_skynet_id, "MD5:" + new_md5, new_resource_meta3),
            patch_action_state1=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            patch_action_state2=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            patch_action_state3=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            stage_revision=2,
            commit_message="new commit",
        )

    def test_full_commit_with_inherit_acl(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)
        user = create_user(yp_client)

        with yp_env_configurable.yp_instance.create_client(config={"user": user}) as client:
            with pytest.raises(YtResponseError):
                client.commit_deploy_ticket(
                    ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
                )

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
        yp_env_configurable.sync_access_control()

        with yp_env_configurable.yp_instance.create_client(config={"user": user}) as client:
            client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
            )

        check_all_states_after_sandbox_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=construct_commit_action_state(message="new commit"),
            static_resource_state1=construct_static_resource_state(
                new_skynet_id, "MD5:" + new_md5, new_resource_meta1
            ),
            static_resource_state2=construct_static_resource_state(
                new_skynet_id, "EMPTY:", new_resource_meta2
            ),
            layer_state1=construct_layer_state(new_skynet_id, "MD5:" + new_md5, new_resource_meta3),
            patch_action_state1=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            patch_action_state2=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            patch_action_state3=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            stage_revision=2,
            commit_message="new commit",
        )

    def test_partial_commit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.commit_deploy_ticket(
            ticket_id=ticket_id,
            type="partial",
            patch_ids=[deploy_patch1],
            message="new commit",
            reason="COMMITTED",
        )

        check_all_states_after_sandbox_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=default_action,
            static_resource_state1=construct_static_resource_state(
                new_skynet_id, "MD5:" + new_md5, new_resource_meta1
            ),
            static_resource_state2=construct_static_resource_state("default", "default", {}),
            layer_state1=construct_layer_state("default", "default", {}),
            patch_action_state1=construct_commit_action_state(message="new commit"),
            patch_action_state2=default_action,
            patch_action_state3=default_action,
            stage_revision=2,
            commit_message="new commit",
        )

    def test_full_commit_when_some_patches_already_committed(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/status/patches/{}/action".format(deploy_patch1),
                    "value": construct_commit_action_state("old commit"),
                }
            ],
        )

        yp_client.commit_deploy_ticket(
            ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
        )

        check_all_states_after_sandbox_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=construct_commit_action_state(message="new commit"),
            static_resource_state1=construct_static_resource_state("default", "default", {}),
            static_resource_state2=construct_static_resource_state(
                new_skynet_id, "EMPTY:", new_resource_meta2
            ),
            layer_state1=construct_layer_state(new_skynet_id, "MD5:" + new_md5, new_resource_meta3),
            patch_action_state1=construct_commit_action_state(message="old commit"),
            patch_action_state2=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            patch_action_state3=construct_commit_action_state(
                message="Parent COMMITTED: new commit"
            ),
            stage_revision=2,
            commit_message="new commit",
        )

    def test_commit_patch_which_already_committed(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/status/patches/{}/action".format(deploy_patch1),
                    "value": construct_commit_action_state("old commit"),
                }
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id,
                type="partial",
                patch_ids=[deploy_patch1],
                message="new commit",
                reason="COMMITTED",
            )

        check_all_states_after_sandbox_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=default_action,
            static_resource_state1=construct_static_resource_state("default", "default", {}),
            static_resource_state2=construct_static_resource_state("default", "default", {}),
            layer_state1=construct_layer_state("default", "default", {}),
            patch_action_state1=construct_commit_action_state(message="old commit"),
            patch_action_state2=default_action,
            patch_action_state3=default_action,
            stage_revision=1,
            commit_message="empty",
        )

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1

    def test_commit_ticket_which_already_committed(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {"path": "/status/action", "value": construct_commit_action_state("old commit")}
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
            )

        check_all_states_after_sandbox_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=construct_commit_action_state("old commit"),
            static_resource_state1=construct_static_resource_state("default", "default", {}),
            static_resource_state2=construct_static_resource_state("default", "default", {}),
            layer_state1=construct_layer_state("default", "default", {}),
            patch_action_state1=default_action,
            patch_action_state2=default_action,
            patch_action_state3=default_action,
            stage_revision=1,
            commit_message="empty",
        )

    def test_partial_commit_when_patch_id_does_not_exist(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id,
                type="partial",
                patch_ids=["unknown"],
                message="new commit",
                reason="COMMITTED",
            )

        check_all_states_after_sandbox_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=default_action,
            static_resource_state1=construct_static_resource_state("default", "default", {}),
            static_resource_state2=construct_static_resource_state("default", "default", {}),
            layer_state1=construct_layer_state("default", "default", {}),
            patch_action_state1=default_action,
            patch_action_state2=default_action,
            patch_action_state3=default_action,
            stage_revision=1,
            commit_message="empty",
        )

    def test_full_skip(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.skip_deploy_ticket(
            ticket_id=ticket_id, type="full", message="skip ticket", reason="SKIPPED"
        )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == construct_skip_action_state("skip ticket")

        patch_actions_result = yp_client.get_object(
            "deploy_ticket",
            ticket_id,
            selectors=[
                "/status/patches/{}/action".format(deploy_patch1),
                "/status/patches/{}/action".format(deploy_patch2),
            ],
        )

        assert patch_actions_result[0] == construct_skip_action_state("Parent SKIPPED: skip ticket")
        assert patch_actions_result[1] == construct_skip_action_state("Parent SKIPPED: skip ticket")

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_partial_skip(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.skip_deploy_ticket(
            ticket_id=ticket_id,
            type="partial",
            patch_ids=[deploy_patch1],
            message="skip patch",
            reason="SKIPPED",
        )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == default_action

        patch_actions_result = yp_client.get_object(
            "deploy_ticket",
            ticket_id,
            selectors=[
                "/status/patches/{}/action".format(deploy_patch1),
                "/status/patches/{}/action".format(deploy_patch2),
            ],
        )

        assert patch_actions_result[0] == construct_skip_action_state("skip patch")
        assert patch_actions_result[1] == default_action

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_full_skip_when_some_patches_already_skipped(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/status/patches/{}/action".format(deploy_patch1),
                    "value": construct_skip_action_state("skip patch"),
                }
            ],
        )

        yp_client.skip_deploy_ticket(
            ticket_id=ticket_id, type="full", message="skip ticket", reason="SKIPPED"
        )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == construct_skip_action_state("skip ticket")

        patch_actions_result = yp_client.get_object(
            "deploy_ticket",
            ticket_id,
            selectors=[
                "/status/patches/{}/action".format(deploy_patch1),
                "/status/patches/{}/action".format(deploy_patch2),
            ],
        )

        assert patch_actions_result[0] == construct_skip_action_state("skip patch")
        assert patch_actions_result[1] == construct_skip_action_state("Parent SKIPPED: skip ticket")

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_skip_patch_which_already_has_terminal_state(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/status/patches/{}/action".format(deploy_patch1),
                    "value": construct_commit_action_state("old commit"),
                }
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.skip_deploy_ticket(
                ticket_id=ticket_id,
                type="partial",
                patch_ids=[deploy_patch1],
                message="skip patch",
                reason="SKIPPED",
            )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == default_action

        patch_actions_result = yp_client.get_object(
            "deploy_ticket",
            ticket_id,
            selectors=[
                "/status/patches/{}/action".format(deploy_patch1),
                "/status/patches/{}/action".format(deploy_patch2),
            ],
        )

        assert patch_actions_result[0] == construct_commit_action_state("old commit")
        assert patch_actions_result[1] == default_action

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_skip_ticket_which_already_has_terminal_state(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {"path": "/status/action", "value": construct_commit_action_state("old commit")}
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.skip_deploy_ticket(
                ticket_id=ticket_id, type="full", message="skip ticket", reason="SKIPPED"
            )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == construct_commit_action_state("old commit")

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_commit_when_deploy_unit_does_not_exist(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "stage", stage_id, set_updates=[{"path": "/spec/deploy_units", "value": {}}]
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="commit ticket", reason="COMMITTED"
            )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == default_action

        patch_actions_result = yp_client.get_object(
            "deploy_ticket",
            ticket_id,
            selectors=[
                "/status/patches/{}/action".format(deploy_patch1),
                "/status/patches/{}/action".format(deploy_patch2),
                "/status/patches/{}/action".format(deploy_patch3),
            ],
        )

        assert patch_actions_result[0] == default_action
        assert patch_actions_result[1] == default_action
        assert patch_actions_result[2] == default_action

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_commit_when_resource_id_does_not_exist(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "stage",
            stage_id,
            set_updates=[
                {
                    "path": "/spec/deploy_units/test-deploy-unit/replica_set/replica_set_template/pod_template_spec/spec/pod_agent_payload/spec/resources/static_resources",
                    "value": [
                        {
                            "id": resource_ref1,
                            "url": "default",
                            "verification": {"checksum": "default"},
                            "meta": {"sandbox_resource": {}},
                        }
                    ],
                }
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="commit ticket", reason="COMMITTED"
            )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == default_action

        patch_actions_result = yp_client.get_object(
            "deploy_ticket",
            ticket_id,
            selectors=[
                "/status/patches/{}/action".format(deploy_patch1),
                "/status/patches/{}/action".format(deploy_patch2),
                "/status/patches/{}/action".format(deploy_patch3),
            ],
        )

        assert patch_actions_result[0] == default_action
        assert patch_actions_result[1] == default_action
        assert patch_actions_result[2] == default_action

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_commit_when_resource_type_from_patch_does_not_exist_in_release(
        self, yp_env_configurable
    ):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_sandbox_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/spec/patches/{}/sandbox/sandbox_resource_type".format(deploy_patch2),
                    "value": "unknown-type",
                }
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="commit ticket", reason="COMMITTED"
            )

        ticket_action_result = yp_client.get_object(
            "deploy_ticket", ticket_id, selectors=["/status/action"]
        )[0]
        assert ticket_action_result == default_action

        patch_actions_result = yp_client.get_object(
            "deploy_ticket",
            ticket_id,
            selectors=[
                "/status/patches/{}/action".format(deploy_patch1),
                "/status/patches/{}/action".format(deploy_patch2),
                "/status/patches/{}/action".format(deploy_patch3),
            ],
        )

        assert patch_actions_result[0] == default_action
        assert patch_actions_result[1] == default_action
        assert patch_actions_result[2] == default_action

        revision = yp_client.get_object("stage", stage_id, selectors=["/spec/revision"])[0]
        assert revision == 1
        revision_message = yp_client.get_object("stage", stage_id, selectors=["/spec/revision_info/description"])[0]
        assert revision_message == "empty"

    def test_full_docker_resources_commit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_docker_resources_objects(yp_client)

        yp_client.commit_deploy_ticket(
            ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
        )

        check_all_states_after_docker_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=construct_commit_action_state("new commit"),
            docker_resource_state1={"name": image_name, "tag": image_tag},
            docker_resource_state2={"name": image_name, "tag": image_tag},
            patch_action_state1=construct_commit_action_state("Parent COMMITTED: new commit"),
            patch_action_state2=construct_commit_action_state("Parent COMMITTED: new commit"),
            stage_revision=2,
            commit_message="new commit",
        )

    def test_full_docker_resources_commit_when_some_patches_already_committed(
        self, yp_env_configurable
    ):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_docker_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/status/patches/{}/action".format(deploy_patch1),
                    "value": construct_commit_action_state("old commit"),
                }
            ],
        )

        yp_client.commit_deploy_ticket(
            ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
        )

        check_all_states_after_docker_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=construct_commit_action_state("new commit"),
            docker_resource_state1=default_docker_resource,
            docker_resource_state2={"name": image_name, "tag": image_tag},
            patch_action_state1=construct_commit_action_state("old commit"),
            patch_action_state2=construct_commit_action_state("Parent COMMITTED: new commit"),
            stage_revision=2,
            commit_message="new commit",
        )

    def test_full_docker_resources_commit_when_ticket_already_committed(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_docker_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {"path": "/status/action", "value": construct_commit_action_state("old commit")}
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
            )

        check_all_states_after_docker_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=construct_commit_action_state("old commit"),
            docker_resource_state1=default_docker_resource,
            docker_resource_state2=default_docker_resource,
            patch_action_state1=default_action,
            patch_action_state2=default_action,
            stage_revision=1,
            commit_message="empty",
        )

    def test_full_docker_resources_commit_when_deploy_unit_does_not_exist(
        self, yp_env_configurable
    ):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_docker_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/spec/patches/{}/docker/docker_image_ref/deploy_unit_id".format(
                        deploy_patch1
                    ),
                    "value": "unknown-deploy-unit",
                }
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
            )

        check_all_states_after_docker_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=default_action,
            docker_resource_state1=default_docker_resource,
            docker_resource_state2=default_docker_resource,
            patch_action_state1=default_action,
            patch_action_state2=default_action,
            stage_revision=1,
            commit_message="empty",
        )

    def test_full_docker_resources_commit_when_box_does_not_exist(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_docker_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/spec/patches/{}/docker/docker_image_ref/box_id".format(deploy_patch1),
                    "value": "unknown-box",
                }
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id, type="full", message="new commit", reason="COMMITTED"
            )

        check_all_states_after_docker_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=default_action,
            docker_resource_state1=default_docker_resource,
            docker_resource_state2=default_docker_resource,
            patch_action_state1=default_action,
            patch_action_state2=default_action,
            stage_revision=1,
            commit_message="empty",
        )

    def test_partial_docker_resources_commit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_docker_resources_objects(yp_client)

        yp_client.commit_deploy_ticket(
            ticket_id=ticket_id,
            type="partial",
            patch_ids=[deploy_patch1],
            message="new commit",
            reason="COMMITTED",
        )

        check_all_states_after_docker_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=default_action,
            docker_resource_state1={"name": image_name, "tag": image_tag},
            docker_resource_state2=default_docker_resource,
            patch_action_state1=construct_commit_action_state("new commit"),
            patch_action_state2=default_action,
            stage_revision=2,
            commit_message="new commit",
        )

    def test_partial_docker_resources_commit_patch_which_already_committed(
        self, yp_env_configurable
    ):
        yp_client = yp_env_configurable.yp_client
        stage_id, ticket_id = prepare_docker_resources_objects(yp_client)

        yp_client.update_object(
            "deploy_ticket",
            ticket_id,
            set_updates=[
                {
                    "path": "/status/patches/{}/action".format(deploy_patch1),
                    "value": construct_commit_action_state("old commit"),
                }
            ],
        )

        with pytest.raises(YtResponseError):
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id,
                type="partial",
                patch_ids=[deploy_patch1],
                message="commit ticket",
                reason="COMMITTED",
            )

        check_all_states_after_docker_release(
            yp_client,
            ticket_id,
            stage_id,
            ticket_action_state=default_action,
            docker_resource_state1=default_docker_resource,
            docker_resource_state2=default_docker_resource,
            patch_action_state1=construct_commit_action_state("old commit"),
            patch_action_state2=default_action,
            stage_revision=1,
            commit_message="empty",
        )

    def test_error_when_empty_resources_in_deploy_unit(
        self, yp_env_configurable
    ):
        yp_client = yp_env_configurable.yp_client

        release_id = yp_client.create_object(
            "release",
            attributes={
                "spec": {
                    "sandbox": {
                        "task_id": sandbox_task_id,
                        "task_type": sandbox_task_type,
                        "release_type": "test-release-type",
                        "resources": [
                            create_release_static_resource(
                                sandbox_resource_id1, resource_type1, new_md5, new_skynet_id
                            ),
                        ],
                    }
                }
            },
        )

        stage_id = yp_client.create_object(
            "stage",
            attributes={
                "meta": {"id": "stage-id", "project_id": "project"},
                "spec": get_default_stage_spec(),
            },
        )

        patches = {
            deploy_patch1: create_sandbox_deploy_patch(
                resource_type1, "static_resource_ref", resource_ref1
            ),
        }

        release_rule_id = yp_client.create_object(
            "release_rule",
            attributes={
                "meta": {"id": "release-rule", "stage_id": stage_id},
                "spec": {"sandbox": {"task_type": sandbox_task_type}, "patches": patches},
            },
        )

        ticket_id = create_deploy_ticket_object(
            yp_client, stage_id, release_id, release_rule_id, patches
        )

        with pytest.raises(YtResponseError) as e:
            yp_client.commit_deploy_ticket(
                ticket_id=ticket_id,
                type="full",
                message="commit ticket",
                reason="COMMITTED",
            )

            assert 'Static resource id "{}" does not exist in deploy unit "{}", stage "{}"'.format(
                resource_ref1, deploy_unit, stage_id
            ) in str(e)
