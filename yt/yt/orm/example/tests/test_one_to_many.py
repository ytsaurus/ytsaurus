from .test_incomplete_base import IncompleteBaseTest

from yt.orm.library.common import RemovalForbiddenError

import pytest
import random


class TestOneToMany:
    ILLISTRATOR_TO_PUBLISHERS_TABLE = "//home/example/db/illustrator_to_publishers"
    PUBLISHER_TO_PUBLISHERS_TABLE = "//home/example/db/publisher_to_publishers"
    PUBLISHER_TO_ILLUSTRATORS_TABLE = "//home/example/db/publisher_to_illustrators"

    @pytest.fixture(autouse=True)
    def clear_one_to_many_before_test(self, example_env):
        for table in (
            self.ILLISTRATOR_TO_PUBLISHERS_TABLE,
            self.PUBLISHER_TO_PUBLISHERS_TABLE,
            self.PUBLISHER_TO_ILLUSTRATORS_TABLE,
        ):
            example_env.clear_dummy_content_table(table)

    @pytest.fixture
    def greg_uid(self, example_env):
        return example_env.create_illustrator(name="Greg")

    @pytest.fixture
    def alan_uid(self, example_env):
        return example_env.create_illustrator(name="Alan")

    def test_create_filled(self, example_env, greg_uid):
        publisher_id = example_env.create_publisher(illustrator_in_chief=greg_uid)
        assert [
            {
                "illustrator_uid": greg_uid,
                "publisher_id": publisher_id,
                "dummy": None,
            }
        ] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_create_empty(self, example_env):
        example_env.create_publisher()
        assert [] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_update_to_other_value(
        self, greg_uid, alan_uid, example_env
    ):
        publisher_id = example_env.create_publisher(illustrator_in_chief=greg_uid)
        example_env.client.update_object(
            "publisher",
            {"id": publisher_id},
            set_updates=[{"path": "/spec/illustrator_in_chief", "value": alan_uid}],
        )
        assert [
            {
                "illustrator_uid": alan_uid,
                "publisher_id": publisher_id,
                "dummy": None,
            }
        ] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_update_to_empty_value(
        self, greg_uid, example_env
    ):
        publisher_id = example_env.create_publisher(illustrator_in_chief=greg_uid)
        example_env.client.update_object(
            "publisher",
            {"id": publisher_id},
            set_updates=[{"path": "/spec/illustrator_in_chief", "value": 0}],
        )
        assert [] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_update_to_default_value(self, greg_uid, example_env):
        publisher_id = example_env.create_publisher(illustrator_in_chief=greg_uid)
        example_env.client.update_object(
            "publisher",
            {"id": publisher_id},
            set_updates=[{"path": "/spec", "value": {}}],
        )
        assert [] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_remove_many_side(self, greg_uid, example_env):
        publisher_id = example_env.create_publisher(illustrator_in_chief=greg_uid)
        example_env.client.remove_object("publisher", {"id": publisher_id})
        assert [] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_remove_one_side(self, greg_uid, example_env):
        publisher_id = example_env.create_publisher(illustrator_in_chief=greg_uid)
        example_env.client.remove_object("illustrator", {"uid": greg_uid})
        assert [] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)
        actual = example_env.client.get_object(
            "publisher", {"id": publisher_id}, selectors=["/spec/illustrator_in_chief"]
        )[0]
        assert 0 == actual

    def test_self_reference(self, example_env):
        publisher_id = random.randint(1, 1000000)
        example_env.create_publisher(publisher_group=publisher_id, publisher_id=publisher_id)
        assert [
            {
                "target_publisher_id": publisher_id,
                "source_publisher_id": publisher_id,
                "dummy": None,
            }
        ] == example_env.get_table_rows(self.PUBLISHER_TO_PUBLISHERS_TABLE)

        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object("publisher", {"id": publisher_id})

        example_env.client.remove_object(
            "publisher", {"id": publisher_id}, allow_removal_with_non_empty_reference=True
        )
        assert [] == example_env.get_table_rows(self.PUBLISHER_TO_PUBLISHERS_TABLE)

    def test_parent_reference(self, example_env):
        parent_publisher_id = example_env.create_publisher()
        part_time_job_publisher_id = example_env.create_publisher()
        illustrator_uid = example_env.client.create_object(
            "illustrator",
            attributes={
                "meta": {
                    "parent_key": str(parent_publisher_id),
                    "part_time_job": part_time_job_publisher_id,
                },
            },
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["uid"]
        assert [
            {
                "source_illustrator_uid": illustrator_uid,
                "source_publisher_id": parent_publisher_id,
                "target_publisher_id": part_time_job_publisher_id,
                "dummy": None,
            }
        ] == example_env.get_table_rows(self.PUBLISHER_TO_ILLUSTRATORS_TABLE)

        example_env.client.remove_object("publisher", {"id": part_time_job_publisher_id})
        assert [] == example_env.get_table_rows(self.PUBLISHER_TO_ILLUSTRATORS_TABLE)

    def test_history(self, greg_uid, alan_uid, example_env):
        client = example_env.client
        publisher_identity = client.create_object(
            "publisher",
            {"spec": {"illustrator_in_chief": greg_uid}},
            request_meta_response=True,
        )
        client.update_object(
            "publisher",
            publisher_identity,
            set_updates=[{"path": "/spec/illustrator_in_chief", "value": alan_uid}],
        )
        actual = client.select_object_history(
            "publisher", publisher_identity, ["/spec/illustrator_in_chief"]
        )
        assert 2 == len(actual["events"])
        assert (
            client.get_object_service_pb2().ET_OBJECT_CREATED
            == actual["events"][0]["event_type"]
        )
        assert greg_uid == actual["events"][0]["results"][0]["value"]
        assert (
            client.get_object_service_pb2().ET_OBJECT_UPDATED
            == actual["events"][1]["event_type"]
        )
        assert alan_uid == actual["events"][1]["results"][0]["value"]

    def test_transaction_remove_create(self, greg_uid, example_env):
        publisher_id = example_env.create_publisher(illustrator_in_chief=greg_uid)

        transaction_id = example_env.client.start_transaction()
        example_env.client.remove_object(
            "publisher", {"id": publisher_id}, transaction_id=transaction_id
        )
        example_env.create_publisher(
            publisher_id=publisher_id,
            illustrator_in_chief=greg_uid,
            transaction_id=transaction_id,
        )
        example_env.client.commit_transaction(transaction_id)
        assert [
            {
                "illustrator_uid": greg_uid,
                "publisher_id": publisher_id,
                "dummy": None,
            }
        ] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_transaction_create_remove(self, greg_uid, example_env):
        transaction_id = example_env.client.start_transaction()
        publisher_id = example_env.create_publisher(
            illustrator_in_chief=greg_uid, transaction_id=transaction_id
        )
        example_env.client.remove_object(
            "publisher", {"id": publisher_id}, transaction_id=transaction_id
        )
        example_env.client.commit_transaction(transaction_id)
        assert [] == example_env.get_table_rows(self.ILLISTRATOR_TO_PUBLISHERS_TABLE)

    def test_forbid_non_empty_removal(self, example_env):
        illustrator_id = example_env.create_illustrator()
        publisher_uid = example_env.create_publisher()
        book = example_env.client.create_object(
            "book",
            {
                "meta": {
                    "isbn": "978-1449355739",
                    "parent_key": str(publisher_uid),
                },
                "spec": {"illustrator_id": illustrator_id},
            },
        )

        # Reference `illustrator_id` is marked with `forbid_removal_with_non_empty_references`.
        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object("book", book)
        example_env.client.remove_object("illustrator", str(illustrator_id))

    class TestOneToManyIncomplete(IncompleteBaseTest):
        EXAMPLE_MASTER_CONFIG = {
            "transaction_manager": {
                "scalar_attribute_loads_null_as_typed": True,
                "any_to_one_attribute_value_getter_returns_null": False,
            },
        }

        @pytest.fixture
        def object_type(self):
            return "publisher"

        @pytest.fixture
        def object_id(self, example_env):
            return example_env.create_publisher(illustrator_in_chief=0)

        @pytest.fixture
        def incomplete_path_items(self):
            return ["spec", "illustrator_in_chief"]

        @pytest.fixture
        def old_value(self):
            return 0

        @pytest.fixture
        def new_value(self, example_env):
            return example_env.create_illustrator(name="Alan")
