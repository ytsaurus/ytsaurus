from .conftest import create_user

from yp.common import YtResponseError, YpAuthorizationError

import pytest


def permissions_test_template(
    yp_env, object_type, account_is_mandatory=False, meta_specific_fields={}
):
    yp_client = yp_env.yp_client

    account_id = yp_client.create_object("account")
    object_spec = {}
    if account_is_mandatory:
        with pytest.raises(YtResponseError):
            yp_client.create_object(object_type)
        object_spec = {"account_id": account_id}

    object_id = yp_client.create_object(
        object_type, attributes={"meta": meta_specific_fields, "spec": object_spec}
    )

    with pytest.raises(YtResponseError) as exc:
        yp_client.update_object(
            object_type, object_id, set_updates=[{"path": "/spec/account_id", "value": ""}]
        )
        assert exc.contains_text("Cannot set null account")

    user_id = create_user(yp_client, "u", grant_create_permission_for_types=(object_type,))
    yp_env.sync_access_control()

    with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
        if not account_is_mandatory:
            client.create_object(object_type, attributes={"meta": meta_specific_fields})
        with pytest.raises(YpAuthorizationError):
            client.create_object(
                object_type,
                attributes={"meta": meta_specific_fields, "spec": {"account_id": account_id}},
            )

    yp_client.update_object(
        "account",
        account_id,
        set_updates=[
            {
                "path": "/meta/acl/end",
                "value": {"action": "allow", "permissions": ["use"], "subjects": [user_id]},
            }
        ],
    )
    yp_env.sync_access_control()

    with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
        client.create_object(
            object_type,
            attributes={"meta": meta_specific_fields, "spec": {"account_id": account_id}},
        )


def update_spec_pod_disks_validation_test_template(yp_client, object_type, object_id):
    def update(path, value):
        yp_client.update_object(object_type, object_id, set_updates=[dict(path=path, value=value)])

    def update_spec(attributes):
        update(
            path="/spec",
            value=dict(pod_template_spec=dict(spec=dict(disk_volume_requests=[attributes],),),),
        )

    def update_spec_pod_template_spec(attributes):
        update(
            path="/spec/pod_template_spec",
            value=dict(spec=dict(disk_volume_requests=[attributes],),),
        )

    attributes = dict(id="123", storage_class="hdd")
    with pytest.raises(YtResponseError):
        update_spec(attributes)
    with pytest.raises(YtResponseError):
        update_spec_pod_template_spec(attributes)

    attributes["quota_policy"] = dict(capacity=123123)
    update_spec(attributes)
    update_spec_pod_template_spec(attributes)


def update_spec_test_template(
    yp_client, object_type, initial_spec, update_path, update_value, initial_meta={}
):
    object_id = yp_client.create_object(
        object_type=object_type, attributes={"meta": initial_meta, "spec": initial_spec}
    )

    yp_client.update_object(
        object_type, object_id, set_updates=[{"path": update_path, "value": update_value},]
    )

    assert yp_client.get_object(object_type, object_id, selectors=[update_path])[0] == update_value


def update_spec_revision_test_template(yp_client, object_type):
    update_spec_test_template(yp_client, object_type, {"revision": 1}, "/spec/revision", 2)


def replica_set_network_project_permissions_test_template(yp_env, replica_set_object_type):
    network_project = "project"

    spec = {
        "account_id": "tmp",
        "pod_template_spec": {
            "spec": {
                "ip6_address_requests": [{"network_id": network_project, "vlan_id": "backbone"}]
            }
        },
    }

    user_id = create_user(yp_env.yp_client, "u", grant_create_permission_for_types=(replica_set_object_type,))
    yp_env.sync_access_control()

    network_project_permissions_test_template(
        yp_env, replica_set_object_type, network_project, spec, {}, user_id
    )


def network_project_permissions_test_template(
    yp_env, object_type, network_project, spec, meta, user_id
):
    yp_client = yp_env.yp_client

    yp_client.create_object(
        "network_project",
        attributes={"spec": {"project_id": 1234}, "meta": {"id": network_project}},
    )

    # creation fails because user does not have access to the project
    with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
        with pytest.raises(YpAuthorizationError):
            client.create_object(object_type, attributes={"spec": spec, "meta": meta})

    yp_client.update_object(
        "network_project",
        network_project,
        set_updates=[
            {
                "path": "/meta/acl/end",
                "value": {"action": "allow", "permissions": ["use"], "subjects": [user_id]},
            }
        ],
    )
    yp_env.sync_access_control()

    with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
        object_id = client.create_object(object_type, attributes={"spec": spec, "meta": meta})

    yp_client.remove_object("network_project", network_project)

    # spec is now invalid as it is, but update succeeds because it does not change network project
    with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
        client.update_object(object_type, object_id, set_updates=[{"path": "/spec", "value": spec}])
