from yt.orm.tests.base_object_test import BaseObjectTest

from yt.orm.library.common import YtResponseError, InvalidContinuationTokenError

from yt.yson import YsonEntity

import pytest


class TestWatchLogConsumer(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "watch_log_consumer"

    @pytest.fixture
    def object_spec(self):
        return {"object_type": "author"}

    @pytest.fixture
    def object_type_name(self, object_type):
        assert object_type
        return object_type.capitalize()

    @pytest.fixture
    def object_id(self, example_env, object_type, object_spec, object_status):
        return example_env.client.create_object(
            object_type, {"spec": object_spec, "status": object_status}
        )

    @pytest.fixture
    def set_updates(self):
        return [{"path": "/spec/object_type", "value": "author"}]

    @pytest.fixture
    def object_status(self):
        return {}

    def test_invalid_create(self, example_env, object_type):
        with pytest.raises(YtResponseError):
            example_env.client.create_object(object_type, {"spec": {"object_type": "xxx"}})
        # Nonwatchable object
        with pytest.raises(YtResponseError):
            example_env.client.create_object(object_type, {"spec": {"object_type": "editor"}})
        with pytest.raises(InvalidContinuationTokenError):
            example_env.client.create_object(
                object_type,
                {
                    "spec": {"object_type": "author"},
                    "status": {"continuation_token": "xxx"},
                },
            )

    def test_invalid_update(self, example_env, object_type):
        object_id = example_env.client.create_object(object_type, {"spec": {"object_type": "author"}})
        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                object_type,
                object_id,
                set_updates=[{"path": "/spec/object_type", "value": "publisher"}],
            )

    def test_read_status(self, example_env, object_type, object_id, object_status):
        meta, status = example_env.client.get_object(object_type, object_id, ["/meta", "/status"])
        assert object_type == meta["type"]
        assert object_id == meta["id"]
        assert object_status == status

    def test_upsert(self, example_env, object_type, object_spec, set_updates):
        object_id = "non-existent-id"
        example_env.client.create_object(
            object_type,
            {"meta": {"id": object_id}, "spec": object_spec},
            update_if_existing={"set_updates": [{"path": "/spec", "value": YsonEntity()}]},
        )
        for set_update in set_updates:
            (actual,) = example_env.client.get_object(object_type, object_id, [set_update["path"]])
            assert set_update["value"] == actual
