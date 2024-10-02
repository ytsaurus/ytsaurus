from .test_incomplete_base import IncompleteBaseTest

from yt.orm.tests.base_object_test import BaseObjectTest

from datetime import timedelta
import pytest


class TestAuthor(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "author"

    def object_watchable(self):
        return True

    @pytest.fixture
    def object_spec(self):
        return {"name": "Mark Lutz", "age": 31}

    @pytest.fixture
    def set_updates(self):
        return [{"path": "/spec/name", "value": "The world leader in Python training"}]


class TestAuthorWatchMeta:
    def test(self, example_env):
        start_timestamp = example_env.client.generate_timestamp()

        author_key = example_env.client.create_object("author", request_meta_response=True)
        assert author_key

        timestamp = example_env.client.generate_timestamp()

        events = example_env.client.watch_objects(
            "author",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=30),
            request_meta_response=True,
        )
        assert 1 == len(events)
        assert author_key == events[0]["meta"]["id"]
        assert author_key == events[0]["meta"]["key"]
        assert author_key in events[0]["meta"]["fqid"]
        assert events[0]["meta"]["uuid"]
        assert events[0]["meta"]["uuid"] in events[0]["meta"]["fqid"]


class TestScalarIncomplete(IncompleteBaseTest):
    EXAMPLE_MASTER_CONFIG = {
        "transaction_manager": {
            "scalar_attribute_loads_null_as_typed": True,
        },
    }

    @pytest.fixture
    def object_type(self):
        return "author"

    @pytest.fixture
    def old_value(self):
        return 27

    @pytest.fixture
    def new_value(self):
        return 28

    @pytest.fixture
    def object_id(self, example_env, old_value):
        return example_env.client.create_object(
            "author",
            attributes={"spec": {"age": old_value}},
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["id"]

    @pytest.fixture
    def incomplete_path_items(self):
        return ["spec", "age"]

    @pytest.fixture
    def select_query(self, incomplete_id):
        return '[/meta/id]="{}"'.format(incomplete_id)
