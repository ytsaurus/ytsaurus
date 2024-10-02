import pytest

from yt.orm.library.common import AuthorizationError

from .conftest import sync_access_control


class TestAttributeUpdatePolicy:
    def _grant_write_permission(self, example_env, object_type, object_key, user_id, attribute):
        example_env.client.update_object(
            object_type,
            object_key,
            set_updates=[
                dict(
                    path="/meta/acl/end",
                    value=dict(
                        subjects=[user_id],
                        permissions=["write"],
                        attributes=[attribute],
                        action="allow",
                    ),
                ),
            ],
        )
        sync_access_control()

    @pytest.mark.parametrize(
        "attr_path,update_path,update_value,superuser,attribute_updated,auth_error",
        [
            # OPAQUE_READ_ONLY
            ("/spec/last_sleep_duration", "/spec/last_sleep_duration", 7, False, False, True),
            ("/spec/last_sleep_duration", "/spec/last_sleep_duration", 7, True, True, False),
            ("/spec/last_sleep_duration", "/spec", {"favourite_toy": "springs", "last_sleep_duration": 7}, False, False, False),
            ("/spec/last_sleep_duration", "/spec", {"favourite_toy": "springs", "last_sleep_duration": 7}, True, False, False),
            ("/spec/last_sleep_duration", "/spec", {"favourite_toy": "springs"}, True, False, False),
            ("/spec/last_sleep_duration", "/spec", {"favourite_toy": "springs"}, False, False, False),
            ("/status/opaque_ro_nested/inner_third", "/status/opaque_ro_nested/inner_third", 3, False, False, True),
            ("/status/opaque_ro_nested/inner_third", "/status/opaque_ro_nested/inner_third", 3, True, True, False),
            # OPAQUE_UPDATABLE
            ("/spec/favourite_food", "/spec/favourite_food", "chips", False, True, False),
            ("/spec/favourite_food", "/spec/favourite_food", "chips", True, True, False),
            ("/spec/favourite_food", "/spec", {"favourite_toy": "springs", "favourite_food": "ice cream"}, False, False, False),
            ("/spec/favourite_food", "/spec", {"favourite_toy": "springs", "favourite_food": "ice cream"}, True, False, False),
            ("/spec/favourite_food", "/spec", {"favourite_toy": "springs"}, True, False, False),
            ("/spec/favourite_food", "/spec", {"favourite_toy": "springs"}, False, False, False),
            ("/status/opaque_nested/inner_third", "/status/opaque_nested/inner_third", 3, False, True, False),
            ("/status/opaque_nested/inner_third", "/status/opaque_nested/inner_third", 3, True, True, False),
            # READ_ONLY
            ("/meta/breed", "/meta/breed", "sphinx", False, False, True),
            ("/meta/breed", "/meta/breed", "sphinx", True, True, False),
            ("/status/read_only_nested/inner_second", "/status/read_only_nested/inner_second", 3, False, True, False),
            ("/status/read_only_nested/inner_second", "/status/read_only_nested/inner_second", 3, True, True, False),
            ("/status/read_only_nested/inner_third", "/status/read_only_nested/inner_third", 3, False, False, True),
            ("/status/read_only_nested/inner_third", "/status/read_only_nested/inner_third", 3, True, True, False),
            # UPDATABLE
            ("/spec/favourite_toy", "/spec/favourite_toy", "angry", False, True, False),
            ("/spec/favourite_toy", "/spec/favourite_toy", "angry", True, True, False),
            ("/spec/favourite_toy", "/spec", {"favourite_toy": "springs"}, False, True, False),
            ("/spec/favourite_toy", "/spec", {"favourite_toy": "springs"}, True, True, False),
            ("/status/updatable_nested/inner_first", "/status/updatable_nested/inner_first", 3, True, True, False),
            ("/status/updatable_nested/inner_first", "/status/updatable_nested/inner_first", 3, False, False, True),
        ],
    )
    def test_updates(
        self,
        attr_path,
        update_path,
        update_value,
        superuser,
        attribute_updated,
        auth_error,
        example_env,
        regular_user1,
    ):
        cat_creation = example_env.client.start_transaction()
        cat_id = example_env.create_cat(spec={"name": "Fluffy", "favourite_toy": "paper roll"}, transaction_id=cat_creation)
        example_env.client.commit_transaction(cat_creation)
        attr_before_update = example_env.client.get_object("cat", str(cat_id), selectors=[attr_path])[0]

        client = example_env.client
        if not superuser:
            client = regular_user1.client
            self._grant_write_permission(example_env, "cat", cat_id, regular_user1.id, "/spec")
            self._grant_write_permission(example_env, "cat", cat_id, regular_user1.id, "/status")

        cat_update = client.start_transaction()
        if auth_error:
            with pytest.raises(AuthorizationError):
                client.update_object(
                    "cat",
                    str(cat_id),
                    transaction_id=cat_update,
                    set_updates=[
                        {"path": update_path, "value": update_value},
                    ],
                )
            return

        client.update_object(
            "cat",
            str(cat_id),
            transaction_id=cat_update,
            set_updates=[
                {"path": update_path, "value": update_value},
            ],
        )
        client.commit_transaction(cat_update)
        attr_after_update = example_env.client.get_object("cat", str(cat_id), selectors=[attr_path])[0]
        if attribute_updated:
            assert attr_before_update != attr_after_update
        else:
            assert attr_before_update == attr_after_update
