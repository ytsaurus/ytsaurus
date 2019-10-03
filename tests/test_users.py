from yp.common import YpInvalidObjectIdError, YpDuplicateObjectIdError, YtResponseError

from yt.environment.helpers import assert_items_equal

from yt.packages.six.moves import xrange

import pytest


@pytest.mark.usefixtures("yp_env")
class TestUsers(object):
    def test_create_user(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(5):
            yp_client.create_object("user", attributes={"meta": {"id": "u"}})
            assert yp_client.get_object("user", "u", selectors=["/meta/id"]) == ["u"]
            yp_client.remove_object("user", "u")

    def test_create_group(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(5):
            yp_client.create_object("group", attributes={"meta": {"id": "g"}})
            assert yp_client.get_object("group", "g", selectors=["/meta/id"]) == ["g"]
            yp_client.remove_object("group", "g")

    def test_subject_ids(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        assert yp_client.get_object("user", "u", selectors=["/meta/id"]) == ["u"]
        with pytest.raises(YpDuplicateObjectIdError):
            yp_client.create_object("group", attributes={"meta": {"id": "u"}})
        yp_client.remove_object("user", "u")
        yp_client.create_object("group", attributes={"meta": {"id": "u"}})

    def test_builtin_users(self, yp_env):
        yp_client = yp_env.yp_client

        users = [x[0] for x in yp_client.select_objects("user", selectors=["/meta/id"])]
        assert_items_equal(users, ["root"])

        for user in users:
            with pytest.raises(YtResponseError):
                yp_client.remove_object("user", user)

    def test_builtin_groups(self, yp_env):
        yp_client = yp_env.yp_client

        groups = [x[0] for x in yp_client.select_objects("group", selectors=["/meta/id"])]
        assert_items_equal(groups, ["superusers"])

        assert_items_equal(yp_client.get_object("group", "superusers", selectors=["/spec/members"])[0], ["root"])

        for group in groups:
            with pytest.raises(YtResponseError):
                yp_client.remove_object("group", group)

    def test_cannot_create_wellknown(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["user", "group"]:
            for id in ["everyone"]:
                with pytest.raises(YpInvalidObjectIdError):
                    yp_client.create_object(type, attributes={"meta": {"id": id}})

    # Subject creation / removal internally affects not only corresponding object table,
    # but also technical tables (for example, subject_to_type table).
    # So we provide additional tests of removal and creation in transaction.
    def test_create_remove_in_transaction(self, yp_env):
        yp_client = yp_env.yp_client

        subject_id = "subject1"

        transaction_id = yp_client.start_transaction()
        yp_client.create_object(
            "user",
            attributes=dict(meta=dict(id=subject_id)),
            transaction_id=transaction_id,
        )
        yp_client.remove_object("user", subject_id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        yp_client.create_object("group", attributes=dict(meta=dict(id=subject_id)))
        assert subject_id == yp_client.get_object("group", subject_id, selectors=["/meta/id"])[0]
        with pytest.raises(YtResponseError):
            yp_client.get_object("user", subject_id, selectors=["/meta/id"])
