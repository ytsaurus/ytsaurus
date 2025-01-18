from typing import Any

import pytest

from yt.yt_sync.core.constants import PRODUCER_ATTRS
from yt.yt_sync.core.constants import PRODUCER_SCHEMA
from yt.yt_sync.core.spec.details import ProducerSpec
from yt.yt_sync.core.spec.details import SchemaSpec


class TestProducerSpecification:
    def test_parse_empty(self):
        with pytest.raises(AssertionError):
            ProducerSpec.parse({})

    @pytest.mark.parametrize("value", [1, 1.0, "1", True, []])
    def test_parse_non_dict(self, value: Any):
        with pytest.raises(AssertionError):
            ProducerSpec.parse(value)

    def test_no_treat_as_producer_attribute(self, table_path: str):
        with pytest.raises(AssertionError):
            ProducerSpec.parse(
                {
                    "table": {
                        "schema": PRODUCER_SCHEMA,
                        "clusters": {"primary": {"path": table_path, "attributes": {"dynamic": True}}},
                    },
                }
            )

    def test_parse_consumer_minimal(self, table_path: str):
        spec: ProducerSpec = ProducerSpec.parse(
            {
                "table": {
                    "schema": PRODUCER_SCHEMA,
                    "clusters": {"primary": {"path": table_path, "attributes": PRODUCER_ATTRS}},
                },
            }
        )
        assert spec.table
        assert spec.table.main_cluster_spec.path == table_path
        assert spec.table.schema == SchemaSpec.parse(PRODUCER_SCHEMA)
