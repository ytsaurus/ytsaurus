import pytest

from yt.orm.library.common import AuthorizationError, YtResponseError

from .conftest import sync_access_control


@pytest.fixture
def create_executor(example_env):
    def _inner(executor_id, transaction_id=None):
        return example_env.client.create_object(
            "executor",
            attributes={"meta": {"id": executor_id}},
            request_meta_response=True,
            enable_structured_response=True,
            transaction_id=transaction_id,
        )["meta"]["id"]

    return _inner


class TestAttributeRevision:
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

    def test_revision_created(self, example_env, create_executor):
        creation_transaction = example_env.client.start_transaction(enable_structured_response=True)
        executor_id = create_executor(executor_id=5, transaction_id=creation_transaction["transaction_id"])
        mother_ship_id = example_env.create_mother_ship(
            spec={"executor_id": executor_id}, transaction_id=creation_transaction["transaction_id"]
        )
        cat_id = example_env.create_cat(
            spec={"name": "Fluffy", "favourite_food": "whiskas"}, transaction_id=creation_transaction["transaction_id"]
        )
        example_env.client.commit_transaction(creation_transaction["transaction_id"])
        response = example_env.client.get_object(
            "mother_ship", str(mother_ship_id), selectors=["/spec/revision", "/meta/revision", "/spec"]
        )
        assert len(response) == 3
        assert response[0] == creation_transaction["start_timestamp"]
        assert response[1] == creation_transaction["start_timestamp"]
        assert response[2]["revision"] == creation_transaction["start_timestamp"]

        response = example_env.client.get_object("cat", str(cat_id), selectors=["/spec/revision"])
        assert len(response) == 1
        assert response[0] == creation_transaction["start_timestamp"]

    @pytest.mark.parametrize(
        "path,value,revision_updated",
        [
            ("/spec/name", "Leo", True),
            ("/spec/favourite_food", "marshmallow", True),
            ("/spec/favourite_toy", "Jerry", False),
            ("/spec", {"favourite_toy": "mouse"}, True),
            ("/spec", {"name": "Leo"}, True),
            ("/spec", {"name": "Leo", "favourite_food": "cookies"}, True),
        ],
    )
    def test_revision_several_tracked_paths(self, path, value, revision_updated, example_env):
        client = example_env.client
        creation_transaction = example_env.client.start_transaction(enable_structured_response=True)

        cat_id = example_env.create_cat(
            spec={"name": "Fluffy", "favourite_food": "whiskas"}, transaction_id=creation_transaction["transaction_id"]
        )
        client.commit_transaction(creation_transaction["transaction_id"])

        response = example_env.client.get_object("cat", str(cat_id), selectors=["/spec/revision"])
        assert len(response) == 1
        assert response[0] == creation_transaction["start_timestamp"]

        update_transaction = client.start_transaction(enable_structured_response=True)

        client.update_object(
            "cat",
            str(cat_id),
            transaction_id=update_transaction["transaction_id"],
            set_updates=[
                {"path": path, "value": value},
            ],
        )
        client.commit_transaction(update_transaction["transaction_id"])
        response = client.get_object("cat", str(cat_id), selectors=["/spec/revision"])
        if revision_updated:
            assert response[0] == update_transaction["start_timestamp"]
        else:
            assert response[0] == creation_transaction["start_timestamp"]

    @pytest.mark.parametrize(
        "revision_path,path,value,revision_updated",
        [
            ("/spec/revision", "/spec/executor_id", 5, False),
            ("/spec/revision", "/spec/executor_id", 7, True),
            ("/spec/revision", "/spec", {"executor_id": 5}, False),
            ("/spec/revision", "/spec", {"executor_id": 7}, True),
            ("/spec/revision", "/spec/capacity", 100500, False),  # Field is in excluded paths.
            ("/meta/revision", "/meta/release_year", 2009, False),  # Field is in excluded paths.
            ("/spec/revision", "/spec/sector_names", ["A", "B"], False),  # Field is in excluded paths.
            # Update changes /meta/price in type handler.
            ("/spec/revision", "/meta/release_year", 1777, True),
            # Update changes /meta/price in type handler.
            ("/spec/revision", "/meta/release_year", 1888, True),
        ],
    )
    def test_object_update(self, revision_path, path, value, revision_updated, example_env, create_executor):
        client = example_env.client
        mother_ship_creation = client.start_transaction(enable_structured_response=True)
        executor_id = create_executor(executor_id=5, transaction_id=mother_ship_creation["transaction_id"])
        create_executor(executor_id=7, transaction_id=mother_ship_creation["transaction_id"])

        mother_ship_id = example_env.create_mother_ship(
            spec={"executor_id": executor_id}, transaction_id=mother_ship_creation["transaction_id"]
        )
        client.commit_transaction(mother_ship_creation["transaction_id"])

        mother_ship_update = client.start_transaction(enable_structured_response=True)
        client.update_object(
            "mother_ship",
            str(mother_ship_id),
            transaction_id=mother_ship_update["transaction_id"],
            set_updates=[
                {"path": path, "value": value},
            ],
        )
        client.commit_transaction(mother_ship_update["transaction_id"])
        response = client.get_object("mother_ship", str(mother_ship_id), selectors=[revision_path])
        if revision_updated:
            assert response[0] == mother_ship_update["start_timestamp"]
        else:
            assert response[0] == mother_ship_creation["start_timestamp"]

    def test_double_update(self, example_env, create_executor):
        client = example_env.client
        mother_ship_creation = client.start_transaction(enable_structured_response=True)
        executor_id = create_executor(executor_id=5, transaction_id=mother_ship_creation["transaction_id"])
        create_executor(executor_id=7, transaction_id=mother_ship_creation["transaction_id"])

        mother_ship_id = example_env.create_mother_ship(
            spec={"executor_id": executor_id}, transaction_id=mother_ship_creation["transaction_id"]
        )
        client.commit_transaction(mother_ship_creation["transaction_id"])

        mother_ship_update = client.start_transaction(enable_structured_response=True)
        client.update_object(
            "mother_ship",
            str(mother_ship_id),
            transaction_id=mother_ship_update["transaction_id"],
            set_updates=[
                {"path": "/spec/executor_id", "value": 7},
            ],
        )
        client.update_object(
            "mother_ship",
            str(mother_ship_id),
            transaction_id=mother_ship_update["transaction_id"],
            set_updates=[
                {"path": "/spec/executor_id", "value": 5},
            ],
        )
        client.commit_transaction(mother_ship_update["transaction_id"])
        response = client.get_object("mother_ship", str(mother_ship_id), selectors=["/spec/revision"])

        assert response[0] == mother_ship_creation["start_timestamp"]

    @pytest.mark.parametrize(
        "path,superuser,error",
        [
            ("/spec/revision", False, True),
            ("/spec/revision", True, False),
            ("/spec", False, False),
            ("/spec", True, False),
        ],
    )
    def test_revision_removes(self, path, superuser, error, example_env, create_executor, regular_user1):
        client = example_env.client
        executor_id = create_executor(executor_id=5)
        mother_ship_creation = client.start_transaction(enable_structured_response=True)
        mother_ship_id = example_env.create_mother_ship(
            spec={"executor_id": executor_id}, transaction_id=mother_ship_creation["transaction_id"]
        )
        client.commit_transaction(mother_ship_creation["transaction_id"])

        if not superuser:
            client = regular_user1.client
            self._grant_write_permission(example_env, "mother_ship", mother_ship_id, regular_user1.id, "/spec")

        if error:
            with pytest.raises(AuthorizationError):
                client.update_object(
                    "mother_ship",
                    str(mother_ship_id),
                    remove_updates=[
                        {"path": path},
                    ],
                )
            return

        client.update_object(
            "mother_ship",
            str(mother_ship_id),
            remove_updates=[
                {"path": path},
            ],
        )

    def test_revision_lock_group_filter(self, example_env):
        book_creation = example_env.client.start_transaction(enable_structured_response=True)
        publisher = example_env.create_publisher(transaction_id=book_creation["transaction_id"])
        book_id = example_env.create_book(publisher, transaction_id=book_creation["transaction_id"])
        example_env.client.commit_transaction(book_creation["transaction_id"])

        book_update_web_lock_group = example_env.client.start_transaction()
        example_env.client.update_object(
            "book",
            str(book_id),
            set_updates=[{"path": "/spec/name", "value": "New name"}],
            transaction_id=book_update_web_lock_group,
        )
        example_env.client.commit_transaction(book_update_web_lock_group)

        response = example_env.client.get_object("book", str(book_id), selectors=["/spec/api_revision"])
        assert response[0] == book_creation["start_timestamp"]

        book_update_api_lock_group = example_env.client.start_transaction(enable_structured_response=True)
        example_env.client.update_object(
            "book",
            str(book_id),
            set_updates=[{"path": "/spec/year", "value": 1999}],
            transaction_id=book_update_api_lock_group["transaction_id"],
        )
        example_env.client.commit_transaction(book_update_api_lock_group["transaction_id"])
        response = example_env.client.get_object("book", str(book_id), selectors=["/spec/api_revision"])
        assert response[0] == book_update_api_lock_group["start_timestamp"]

    def test_touch_revision(self, example_env, create_executor):
        mother_ship_creation = example_env.client.start_transaction(enable_structured_response=True)
        executor_id = create_executor(executor_id=5, transaction_id=mother_ship_creation["transaction_id"])
        create_executor(executor_id=7, transaction_id=mother_ship_creation["transaction_id"])

        mother_ship_id = example_env.create_mother_ship(
            spec={"executor_id": executor_id}, transaction_id=mother_ship_creation["transaction_id"]
        )
        example_env.client.commit_transaction(mother_ship_creation["transaction_id"])
        revision = example_env.client.get_object("mother_ship", str(mother_ship_id), selectors=["/spec/revision"])[0]
        assert revision == mother_ship_creation["start_timestamp"]

        touch_revision_call = example_env.client.start_transaction(enable_structured_response=True)
        example_env.client.update_object(
            "mother_ship",
            mother_ship_id,
            set_updates=[dict(path="/control/touch_revision", value=dict(path="/spec/revision"))],
            transaction_id=touch_revision_call["transaction_id"],
        )
        example_env.client.commit_transaction(touch_revision_call["transaction_id"])
        bumped_revision = example_env.client.get_object(
            "mother_ship", str(mother_ship_id), selectors=["/spec/revision"]
        )[0]
        assert bumped_revision == touch_revision_call["start_timestamp"]

    def test_touch_revision_incorrect_path(self, example_env, create_executor):
        mother_ship_creation = example_env.client.start_transaction()
        executor_id = create_executor(executor_id=5, transaction_id=mother_ship_creation)
        create_executor(executor_id=7, transaction_id=mother_ship_creation)

        mother_ship_id = example_env.create_mother_ship(
            spec={"executor_id": executor_id}, transaction_id=mother_ship_creation
        )
        example_env.client.commit_transaction(mother_ship_creation)

        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "mother_ship",
                mother_ship_id,
                set_updates=[dict(path="/control/touch_revision", value=dict(path="/spec/executor_id"))],
            )

        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "mother_ship",
                mother_ship_id,
                set_updates=[dict(path="/control/touch_revision")],
            )

        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "mother_ship",
                mother_ship_id,
                set_updates=[dict(path="/control/touch_revision", value=dict(path=""))],
            )

    @pytest.mark.parametrize(
        "skip,superuser,has_permission,skipped",
        [
            (True, True, False, True),
            (True, False, True, True),
            (True, False, False, False),
            (False, True, True, False),
        ],
    )
    def test_skip_revision_bump_option(
        self,
        skip,
        superuser,
        has_permission,
        skipped,
        example_env,
        regular_user1,
    ):
        book_creation = example_env.client.start_transaction(enable_structured_response=True)
        publisher = example_env.create_publisher(transaction_id=book_creation["transaction_id"])
        book_id = example_env.create_book(publisher, transaction_id=book_creation["transaction_id"])
        example_env.client.commit_transaction(book_creation["transaction_id"])

        client = example_env.client
        if not superuser:
            client = regular_user1.client
            if has_permission:
                example_env.grant_permission(
                    "schema", "book", "use", regular_user1.id, "/access/event_generation_skip_allowed"
                )
            self._grant_write_permission(example_env, "book", book_id, regular_user1.id, "/spec")

        book_update = client.start_transaction(enable_structured_response=True, skip_revision_bump=skip)
        client.update_object(
            "book",
            str(book_id),
            transaction_id=book_update["transaction_id"],
            set_updates=[{"path": "/spec/year", "value": 1999}],
        )
        client.commit_transaction(book_update["transaction_id"])
        response = example_env.client.get_object("book", str(book_id), selectors=["/spec/api_revision"])
        if skipped:
            assert response[0] == book_creation["start_timestamp"]
        else:
            assert response[0] == book_update["start_timestamp"]

    def test_revision_set_zero(self, example_env, create_executor):
        executor_id = create_executor(executor_id=5)
        mother_ship_creation = example_env.client.start_transaction(enable_structured_response=True)
        mother_ship_id = example_env.create_mother_ship(
            spec={"executor_id": executor_id}, transaction_id=mother_ship_creation["transaction_id"]
        )
        example_env.client.commit_transaction(mother_ship_creation["transaction_id"])

        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "mother_ship",
                str(mother_ship_id),
                set_updates=[{"path": "/spec/revision", "value": 0}],
            )
