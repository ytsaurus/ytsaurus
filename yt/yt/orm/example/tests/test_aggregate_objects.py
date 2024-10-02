from .conftest import ExampleTestEnvironment

from yt.orm.library.common import InvalidRequestArgumentsError

import pytest


class TestAggregateObjects:
    def test_any_forbidden(self, example_env: ExampleTestEnvironment):
        example_env.create_editor(labels={"cat_name": "Small Cat"})

        with pytest.raises(InvalidRequestArgumentsError):
            example_env.client.aggregate_objects("editor", group_by=["[/labels/cat_name]"], aggregators=["sum(1)"])
