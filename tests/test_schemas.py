from __future__ import print_function

from yp.local import OBJECT_TYPES

import pytest


@pytest.mark.usefixtures("yp_env")
class TestSchemas(object):
    def test_schemas(self, yp_env):
        schemas = map(
            lambda x: x[0], yp_env.yp_client.select_objects("schema", selectors=["/meta/id"])
        )
        assert frozenset(schemas) == frozenset(filter(lambda x: x != "schema", OBJECT_TYPES))
