from .conftest import ExampleTestEnvironment

from yt.orm.tests.base_object_test import BaseObjectTest

import pytest


class TestPublisher(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "publisher"

    def object_watchable(self):
        return True

    @pytest.fixture
    def object_spec(self, example_env):
        editor_id = example_env.client.create_object("editor", {"spec": {"name": "Greg"}})
        return {
            "name": "O'REILLY",
            "editor_in_chief": editor_id,
            "illustrator_in_chief": 0,
            "publisher_group": 0,
            "featured_illustrators": [],
        }

    @pytest.fixture
    def set_updates(self):
        return [{"path": "/spec/name", "value": "O'Reilly"}]


class TestTransacionPollution:
    def test_remove_then_create(self, example_env):
        publisher = example_env.create_publisher()
        tx = example_env.client.start_transaction()
        example_env.client.remove_object(
            "publisher",
            str(publisher),
            transaction_id=tx,
        )
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher}},
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        example_env.client.get_object("publisher", str(publisher), selectors=["/meta", "/spec"])

    def test_remove_then_upsert(self, example_env):
        publisher = example_env.create_publisher()
        tx = example_env.client.start_transaction()
        example_env.client.remove_object(
            "publisher",
            str(publisher),
            transaction_id=tx,
        )
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher}},
            update_if_existing={
                "set_updates": [
                    {"path": "/spec/year_of_fondation", "value": 1991},
                ]
            },
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        example_env.client.get_object("publisher", str(publisher), selectors=["/meta", "/spec"])

    def test_create_then_remove(self, example_env):
        publisher = example_env.client.generate_timestamp()
        tx = example_env.client.start_transaction()
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher}},
            transaction_id=tx,
        )
        example_env.client.remove_object(
            "publisher",
            str(publisher),
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        result = example_env.client.get_objects(
            "publisher",
            str(publisher),
            selectors=["/meta", "/spec"],
            options={"skip_nonexistent": True},
        )
        assert len(result) == 0

    def test_create_then_upsert(self, example_env):
        publisher = example_env.client.generate_timestamp()
        tx = example_env.client.start_transaction()
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher}},
            transaction_id=tx,
        )
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher}},
            update_if_existing={
                "set_updates": [
                    {"path": "/spec/year_of_fondation", "value": 1991},
                ]
            },
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        example_env.client.get_object("publisher", str(publisher), selectors=["/meta", "/spec"])

    @pytest.mark.parametrize(("options"), [{"skip_nonexistent": True}, {"ignore_nonexistent": True}])
    def test_get_nonexistent(self, options: dict, example_env: ExampleTestEnvironment):
        client = example_env.client
        publisher = example_env.create_publisher()

        client.remove_object("publisher", str(publisher))
        result = client.get_object("publisher", str(publisher), ["/meta"], options=options)
        assert result is None

    def test_update_ignore_nonexistent_then_create(self, example_env):
        publisher_id = example_env.client.generate_timestamp()
        tx = example_env.client.start_transaction()
        example_env.client.update_object(
            "publisher",
            str(publisher_id),
            set_updates=[{"path": "/spec/name", "value": "Питер"}],
            ignore_nonexistent=True,
            transaction_id=tx,
        )
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher_id}},
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)

    def test_update_ignore_nonexistent_then_upsert(self, example_env):
        publisher_id = example_env.client.generate_timestamp()
        tx = example_env.client.start_transaction()
        example_env.client.update_object(
            "publisher",
            str(publisher_id),
            set_updates=[{"path": "/spec/name", "value": "Питер"}],
            ignore_nonexistent=True,
            transaction_id=tx,
        )
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher_id}},
            update_if_existing={
                "set_updates": [
                    {"path": "/spec/year_of_fondation", "value": 1991},
                ]
            },
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)

    def test_remove_then_create_then_remove(self, example_env):
        publisher_id = example_env.create_publisher()
        tx = example_env.client.start_transaction()
        example_env.client.remove_object(
            "publisher",
            str(publisher_id),
            transaction_id=tx,
        )
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher_id}},
            transaction_id=tx,
        )
        example_env.client.remove_object(
            "publisher",
            str(publisher_id),
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)

    def test_remove_nonexistent_then_create(self, example_env):
        publisher = example_env.client.generate_timestamp()
        tx = example_env.client.start_transaction()
        example_env.client.remove_object(
            "publisher",
            str(publisher),
            ignore_nonexistent=True,
            transaction_id=tx,
        )
        example_env.client.create_object(
            "publisher",
            {"meta": {"id": publisher}},
            update_if_existing={
                "set_updates": [
                    {"path": "/spec/year_of_fondation", "value": 1991},
                ]
            },
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        example_env.client.get_object("publisher", str(publisher), selectors=["/meta", "/spec"])
