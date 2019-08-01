from yp.common import YtResponseError, YpAuthorizationError

import pytest


def permissions_test_template(yp_env, object_type, account_is_mandatory=False):
    yp_client = yp_env.yp_client

    account_id = yp_client.create_object("account")
    object_spec = {}
    if account_is_mandatory:
        with pytest.raises(YtResponseError):
            yp_client.create_object(object_type)
        object_spec = {"account_id": account_id}

    object_id = yp_client.create_object(object_type, attributes={"spec": object_spec})

    with pytest.raises(YtResponseError) as exc:
        yp_client.update_object(object_type, object_id, set_updates=[{"path": "/spec/account_id", "value": ""}])
        assert exc.contains_text("Cannot set null account")

    user_id = yp_client.create_object("user", attributes={"meta": {"id": "u"}})
    yp_env.sync_access_control()

    with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
        if not account_is_mandatory:
            client.create_object(object_type)
        with pytest.raises(YpAuthorizationError):
            client.create_object(object_type, attributes={"spec": {"account_id": account_id}})

    yp_client.update_object("account", account_id, set_updates=[
        {"path": "/meta/acl/end", "value": {"action": "allow", "permissions": ["use"], "subjects": [user_id]}}
    ])
    yp_env.sync_access_control()

    with yp_env.yp_instance.create_client(config={"user": user_id}) as client:
        client.create_object(object_type, attributes={"spec": {"account_id": account_id}})


def update_spec_test_template(yp_client, object_type, initial_spec, update_path, update_value):
    object_id = yp_client.create_object(
        object_type=object_type,
        attributes={
            "spec": initial_spec
        })

    yp_client.update_object(object_type, object_id, set_updates=[
        {"path": update_path, "value": update_value},
    ])

    assert yp_client.get_object(object_type, object_id, selectors=[update_path])[0] == update_value


def update_spec_revision_test_template(yp_client, object_type):
    update_spec_test_template(yp_client, object_type, {"revision": 1}, "/spec/revision", 2)
