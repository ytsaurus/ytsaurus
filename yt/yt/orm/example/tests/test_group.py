from yt.orm.tests.base_object_test import BaseObjectTest

import pytest


class TestGroup(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "group"

    def object_watchable(self):
        return True

    @pytest.fixture
    def object_spec(self, example_env):
        return {
            "members": [
                example_env.client.create_object("user", {"spec": {"banned": True}}),
                example_env.client.create_object("user", {"spec": {"banned": False}}),
            ]
        }

    @pytest.fixture
    def set_updates(self, example_env):
        return [
            {
                "path": "/spec/members",
                "value": [
                    example_env.client.create_object("user", {"spec": {"banned": False}}),
                ],
            }
        ]
