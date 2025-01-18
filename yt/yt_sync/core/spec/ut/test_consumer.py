from typing import Any

import pytest

from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.spec.details import ConsumerSpec
from yt.yt_sync.core.spec.details import SchemaSpec


class TestConsumerSpecification:
    def test_parse_empty(self):
        with pytest.raises(AssertionError):
            ConsumerSpec.parse({})

    @pytest.mark.parametrize("value", [1, 1.0, "1", True, []])
    def test_parse_non_dict(self, value: Any):
        with pytest.raises(AssertionError):
            ConsumerSpec.parse(value)

    def test_parse_no_table(self, table_path: str):
        with pytest.raises(AssertionError):
            ConsumerSpec.parse(
                {
                    "queues": [{"cluster": "primary", "path": table_path}],
                }
            )

    def test_no_treat_as_consumer_attribute(self, table_path: str):
        with pytest.raises(AssertionError):
            ConsumerSpec.parse(
                {
                    "table": {
                        "schema": CONSUMER_SCHEMA,
                        "clusters": {"primary": {"path": table_path, "attributes": {"dynamic": True}}},
                    },
                    "queues": [{"cluster": "primary", "path": table_path}],
                }
            )

    def test_parse_no_queues(self, table_path: str):
        with pytest.raises(AssertionError):
            ConsumerSpec.parse(
                {
                    "table": {
                        "schema": CONSUMER_SCHEMA,
                        "clusters": {"primary": {"path": table_path, "attributes": CONSUMER_ATTRS}},
                    },
                }
            )

    def test_no_cluster_in_queue(self, table_path: str):
        with pytest.raises(AssertionError):
            ConsumerSpec.parse(
                {
                    "table": {
                        "schema": CONSUMER_SCHEMA,
                        "clusters": {"primary": {"path": table_path, "attributes": CONSUMER_ATTRS}},
                    },
                    "queues": [{"path": table_path}],
                }
            )

    def test_no_path_in_queue(self, table_path: str):
        with pytest.raises(AssertionError):
            ConsumerSpec.parse(
                {
                    "table": {
                        "schema": CONSUMER_SCHEMA,
                        "clusters": {"primary": {"path": table_path, "attributes": CONSUMER_ATTRS}},
                    },
                    "queues": [{"cluster": "primary"}],
                }
            )

    def test_parse_consumer_minimal(self, table_path: str):
        spec: ConsumerSpec = ConsumerSpec.parse(
            {
                "table": {
                    "schema": CONSUMER_SCHEMA,
                    "clusters": {"primary": {"path": table_path, "attributes": CONSUMER_ATTRS}},
                },
                "queues": [{"cluster": "primary", "path": table_path}],
            }
        )
        assert spec.table
        assert spec.table.main_cluster_spec.path == table_path
        assert spec.table.schema == SchemaSpec.parse(CONSUMER_SCHEMA)
        assert spec.queues
        assert len(spec.queues) == 1
        queue_spec = spec.queues[0]
        assert queue_spec.cluster == "primary"
        assert queue_spec.path == table_path
        assert queue_spec.vital is False
        assert queue_spec.partitions is None

    def test_parse_consumer_full(self, table_path: str):
        spec: ConsumerSpec = ConsumerSpec.parse(
            {
                "table": {
                    "schema": CONSUMER_SCHEMA,
                    "clusters": {"primary": {"path": table_path, "attributes": CONSUMER_ATTRS}},
                },
                "queues": [{"cluster": "primary", "path": table_path, "vital": True, "partitions": [1]}],
            }
        )
        assert spec.table
        assert spec.table.main_cluster_spec.path == table_path
        assert spec.table.schema == SchemaSpec.parse(CONSUMER_SCHEMA)
        assert spec.queues
        assert len(spec.queues) == 1
        queue_spec = spec.queues[0]
        assert queue_spec.cluster == "primary"
        assert queue_spec.path == table_path
        assert queue_spec.vital is True
        assert queue_spec.partitions == [1]
