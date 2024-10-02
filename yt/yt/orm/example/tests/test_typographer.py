from yt.orm.tests.base_object_test import BaseObjectTest

from yt.orm.library.common import InvalidRequestArgumentsError

import pytest


class TestTypographer(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "typographer"

    @pytest.fixture
    def object_meta(self, example_env):
        return {
            "login": "printer",
            "inn": "1234",
            "parent_key": example_env.client.create_object(
                "publisher",
                {"spec": {"name": "O'REILLY"}},
                request_meta_response=True,
            ),
        }

    @pytest.fixture
    def object_spec(self, example_env):
        return {"test_mandatory_etc_field": "first", "test_mandatory_column_field": "second"}

    def test_mandatory(self, example_env, object_meta, object_spec):
        del object_meta["inn"]
        with pytest.raises(InvalidRequestArgumentsError):
            example_env.client.create_object("typographer", {"meta": object_meta, "spec": object_spec})

        del object_spec["test_mandatory_etc_field"]
        object_meta["inn"] = "1234"
        with pytest.raises(InvalidRequestArgumentsError):
            example_env.client.create_object("typographer", {"meta": object_meta, "spec": object_spec})
