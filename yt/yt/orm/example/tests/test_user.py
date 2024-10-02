from yt.orm.tests.base_object_test import BaseObjectTest

import pytest


class TestUser(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "user"

    def object_watchable(self):
        return True

    @pytest.fixture
    def object_spec(self):
        return {"banned": False}

    @pytest.fixture
    def set_updates(self):
        return [{"path": "/spec/banned", "value": True}]

    def test_batch_create(self, example_env):
        response = example_env.client.create_objects(
            (
                ("user", dict())
                for _ in range(100)
            ),
            common_options=dict(fetch_performance_statistics=True),
            enable_structured_response=True,
        )
        assert 2 == response["performance_statistics"]["read_phase_count"]
