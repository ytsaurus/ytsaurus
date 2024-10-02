from .conftest import sync_access_control

from yt.orm.library.common import AuthorizationError

import pytest


def test_access_control_parent(example_env, regular_user1, regular_user2):
    client = example_env.client
    client.update_object(
        "schema",
        "publisher",
        set_updates=[
            {
                "path": "/meta/acl/end",
                "value": {
                    "subjects": [regular_user1.id],
                    "permissions": ["create"],
                    "action": "allow",
                },
            }
        ],
    )
    client.update_object(
        "schema",
        "illustrator",
        set_updates=[
            {
                "path": "/meta/acl/end",
                "value": {
                    "subjects": [regular_user1.id],
                    "permissions": ["create"],
                    "action": "allow",
                },
            }
        ],
    )
    sync_access_control()

    publisher = regular_user1.client.create_object("publisher")
    part_time_job = regular_user1.client.create_object("publisher")
    illustrator = regular_user1.client.create_object(
        "illustrator",
        {"meta": {"publisher_id": int(publisher), "part_time_job": int(part_time_job)}},
    )
    with pytest.raises(AuthorizationError):
        regular_user2.client.update_object(
            "illustrator", illustrator, set_updates=[{"path": "/spec/name", "value": "Skipper"}]
        )

    client.update_object(
        "publisher",
        publisher,
        set_updates=[
            {
                "path": "/meta/acl/end",
                "value": {
                    "subjects": [regular_user2.id],
                    "permissions": ["write"],
                    "action": "allow",
                },
            }
        ],
    )
    sync_access_control()

    with pytest.raises(AuthorizationError):
        regular_user2.client.update_object(
            "illustrator", illustrator, set_updates=[{"path": "/spec/name", "value": "Skipper"}]
        )

    client.update_object(
        "publisher",
        part_time_job,
        set_updates=[
            {
                "path": "/meta/acl/end",
                "value": {
                    "subjects": [regular_user2.id],
                    "permissions": ["write"],
                    "action": "allow",
                },
            }
        ],
    )
    sync_access_control()

    regular_user2.client.update_object(
        "illustrator", illustrator, set_updates=[{"path": "/spec/name", "value": "Skipper"}]
    )

    illustrator_name = client.get_object("illustrator", illustrator, selectors=["/spec/name"])[0]
    assert "Skipper" == illustrator_name
