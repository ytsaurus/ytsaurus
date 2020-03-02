from . import templates
from .conftest import create_user

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
        yp_client.create_object(
            "stage", attributes={"meta": {"id": "val"}, "spec": {"account_id": "tmp"}}
        )

    def test_stage_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "stage", attributes={"meta": {"id": "inv*"}, "spec": {"account_id": "tmp"}}
            )

    def test_stage_spec_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "stage",
                attributes={
                    "meta": {"id": "val"},
                    "spec": {"account_id": "tmp", "deploy_units": {"inv*": {}}},
                },
            )

        stage_id = yp_client.create_object(
            "stage",
            attributes={
                "meta": {"id": "val"},
                "spec": {
                    "account_id": "tmp",
                    "deploy_units": {"correct_deploy_unit_id": {"replica_set": {}}},
                },
            },
        )
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "stage",
                stage_id,
                set_updates=[
                    {"path": "/spec", "value": {"account_id": "tmp", "deploy_units": {"inv*": {}}}}
                ],
            )
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "stage",
                stage_id,
                set_updates=[
                    {
                        "path": "/spec",
                        "value": {
                            "account_id": "tmp",
                            "deploy_units": {"correct_deploy_unit_id": {}},
                        },
                    }
                ],
            )

        with pytest.raises(YtResponseError):
            yp_client.update_object("stage", stage_id, set_updates=[{
                "path": "/spec",
                "value": {
                    "account_id": "tmp",
                    "deploy_units": {
                        "replica_set": {},
                        "correct_deploy_unit_id": {
                            "images_for_boxes": {
                                "unknown-box": {}
                            }
                        }
                    }
                }
            }])

    def test_update_project_id(self, yp_env):
        yp_client = yp_env.yp_client
        stage_id = yp_client.create_object(
            "stage",
            attributes={
                "meta": {"id": "stage_id", "project_id": "project1"},
                "spec": {"account_id": "tmp"},
            },
        )
        yp_client.update_object(
            "stage", stage_id, set_updates=[{"path": "/meta/project_id", "value": "project2"}]
        )

    def test_default_network_project_permissions(self, yp_env):
        project_id = "project_id"

        spec = {
            "account_id": "tmp",
            "deploy_units": {
                "Unit": {
                    "replica_set": {"replica_set_template": {"pod_template_spec": {}}},
                    "network_defaults": {"network_id": project_id},
                }
            },
        }

        user_id = create_user(yp_env.yp_client, grant_create_permission_for_types=("stage",))
        yp_env.sync_access_control()

        templates.network_project_permissions_test_template(
            yp_env, "stage", project_id, spec, {}, user_id
        )

    def test_template_network_project_permissions(self, yp_env):
        project_id = "project_id"

        spec = {
            "account_id": "tmp",
            "deploy_units": {
                "Unit": {
                    "replica_set": {
                        "replica_set_template": {
                            "pod_template_spec": {
                                "spec": {
                                    "ip6_address_requests": [
                                        {"network_id": project_id, "vlan_id": "backbone"}
                                    ]
                                }
                            }
                        }
                    }
                }
            },
        }

        user_id = create_user(yp_env.yp_client, grant_create_permission_for_types=("stage",))
        yp_env.sync_access_control()

        templates.network_project_permissions_test_template(
            yp_env, "stage", project_id, spec, {}, user_id
        )

    def test_check_virtual_service_existence(self, yp_env):
        project_id = "project_id"
        virtual_service_id = "virtual_service"

        spec = {
            "account_id": "tmp",
            "deploy_units": {
                "Unit": {
                    "replica_set": {
                        "replica_set_template": {
                            "pod_template_spec": {
                                "spec": {
                                    "ip6_address_requests": [
                                        {
                                            "network_id": project_id,
                                            "vlan_id": "backbone",
                                            "virtual_service_ids": [virtual_service_id],
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
        }

        yp_client = yp_env.yp_client

        yp_client.create_object(
            "network_project", attributes={"spec": {"project_id": 1234}, "meta": {"id": project_id}}
        )

        user_id = create_user(yp_client, grant_create_permission_for_types=("stage",))
        yp_client.update_object(
            "network_project",
            project_id,
            set_updates=[
                {
                    "path": "/meta/acl/end",
                    "value": {"action": "allow", "permissions": ["use"], "subjects": [user_id]},
                }
            ],
        )
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            with pytest.raises(YtResponseError):
                client.create_object("stage", attributes={"spec": spec})

        yp_client.create_object(
            "virtual_service", attributes={"spec": {}, "meta": {"id": virtual_service_id}}
        )

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            stage_id = client.create_object("stage", attributes={"spec": spec})

        # check that update is possible after service has been removed
        yp_client.remove_object("virtual_service", virtual_service_id)

        with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
            client.update_object("stage", stage_id, set_updates=[{"path": "/spec", "value": spec}])
