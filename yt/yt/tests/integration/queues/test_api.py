from yt_queue_agent_test_base import TestQueueAgentBase, GenericObjectPath

from yt_commands import (
    authors,
    get,
    insert_rows,
    ls,
    pull_consumer,
    advance_consumer,
    register_queue_consumer,
    select_rows,
    print_debug,
    raises_yt_error,
)

from yt_helpers import profiler_factory
from yt.test_helpers.profiler import Profiler

from yt_type_helpers import normalize_schema, make_schema

import yt.environment.init_queue_agent_state as init_queue_agent_state

import yt_error_codes

import pytest

from collections import defaultdict


##################################################################


TIME_METRIC_NON_FLAP_MULTIPLIER = 1.5
NONE_TAG = "none"


def get_profiler() -> Profiler:
    original_host = ls("//sys/queue_agents/instances")[0]
    return profiler_factory().at_queue_agent(original_host)


def advance_consumer_partitions(
    queue: GenericObjectPath,
    consumer: GenericObjectPath,
    partition_index_to_offset: dict[str, int],
) -> None:
    for partition_index, offset in partition_index_to_offset.items():
        advance_consumer(str(consumer), str(queue), partition_index=partition_index, old_offset=None, new_offset=offset)


def validate_consumer_table(consumer_path: str, expected_offsets: dict[str, dict[int, int]]):
    offset_rows = list(select_rows(
        f"queue_consumer_name, partition_index, `offset` from [{consumer_path}]"))
    print_debug(f"{offset_rows=}")
    offsets_by_name = defaultdict(dict)
    for row in offset_rows:
        offsets_by_name[row["queue_consumer_name"]][row["partition_index"]] = row["offset"]
    assert offsets_by_name == expected_offsets


##################################################################


class TestQueueApi(TestQueueAgentBase):
    @pytest.mark.parametrize("partition_to_insert", [
        0,
        1,
        2,
    ])
    @authors("panesher")
    def test_multi_consumer_api(self, partition_to_insert):
        consumer_path = "//tmp/consumer"
        consumer_name = "my_1"
        other_consumer_name = "other_1"

        self._create_consumer(consumer_path, multi_consumer=True)

        assert get(f"{consumer_path}/@type") == "table"
        assert get(f"{consumer_path}/@treat_as_queue_consumer")
        expected_schema = make_schema(
            init_queue_agent_state.MULTI_CONSUMER_OBJECT_TABLE_SCHEMA,
            strict=True,
            unique_keys=True,
        )
        assert normalize_schema(get(f"{consumer_path}/@schema")) == expected_schema

        queue_path = "//tmp/queue"
        queue_ref = GenericObjectPath(queue_path, "primary")
        consumer_ref = GenericObjectPath(consumer_path, "primary", consumer_name)
        other_consumer_ref = GenericObjectPath(consumer_path, "primary", other_consumer_name)
        partition_count = 3
        self._create_queue(queue_path, partition_count=partition_count)
        insert_rows(queue_path, [{
            "$tablet_index": partition_to_insert,
            "data": f"hello world {insert_index}",
        } for insert_index in range(3)])

        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_consumer(str(consumer_ref), str(queue_ref), partition_index=partition_to_insert)
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_consumer(str(other_consumer_ref), str(queue_ref), partition_index=partition_to_insert)

        register_queue_consumer(queue_ref, consumer_ref, vital=False)

        assert len(pull_consumer(str(consumer_ref), str(queue_ref), partition_index=partition_to_insert)) == 3
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_consumer(str(other_consumer_ref), str(queue_ref), partition_index=partition_to_insert)

        partition_index_to_offset = {i: 0 for i in range(partition_count)}
        partition_index_to_offset[partition_to_insert] = 1
        advance_consumer_partitions(queue_ref, consumer_ref, partition_index_to_offset)
        assert len(pull_consumer(str(consumer_ref), str(queue_ref), partition_index=partition_to_insert)) == 2

        partition_index_to_offset[partition_to_insert] = 3
        advance_consumer_partitions(queue_ref, consumer_ref, partition_index_to_offset)
        assert len(pull_consumer(str(consumer_ref), str(queue_ref), partition_index=partition_to_insert)) == 0

        # Check when both consumers are registered
        register_queue_consumer(queue_ref, other_consumer_ref, vital=False)

        assert len(pull_consumer(str(other_consumer_ref), str(queue_ref), partition_index=partition_to_insert)) == 3
        assert len(pull_consumer(str(consumer_ref), str(queue_ref), partition_index=partition_to_insert)) == 0

        other_partition_index_to_offset = {i: 0 for i in range(partition_count)}
        other_partition_index_to_offset[partition_to_insert] = 1
        advance_consumer_partitions(queue_ref, other_consumer_ref, other_partition_index_to_offset)
        assert len(pull_consumer(str(other_consumer_ref), str(queue_ref), partition_index=partition_to_insert)) == 2
        assert len(pull_consumer(str(consumer_ref), str(queue_ref), partition_index=partition_to_insert)) == 0

        validate_consumer_table(consumer_path, {
            consumer_name: partition_index_to_offset,
            other_consumer_name: other_partition_index_to_offset,
        })

    @pytest.mark.parametrize("partition_order_to_insert", [
        [("my_1", 0, 1), ("other_1", 0, 1), ("my_1", 1, 2), ("other_1", 1, 2), ("my_1", 2, 3), ("other_1", 2, 3)],
        [("my_1", 2, 1), ("other_1", 1, 1), ("my_1", 0, 2), ("other_1", 0, 2), ("my_1", 1, 3), ("other_1", 1, 3)],
        [("other_1", 0, 1), ("my_1", 2, 1), ("other_1", 1, 2), ("my_1", 1, 2), ("other_1", 2, 3), ("my_1", 2, 3)],
    ])
    @authors("panesher")
    def test_multi_consumer_api_with_offsets(self, partition_order_to_insert: list[tuple[str, int, int]]):
        consumer_path = "//tmp/consumer"
        consumer_name = "my_1"
        other_consumer_name = "other_1"

        self._create_consumer(consumer_path, multi_consumer=True)
        queue_path = "//tmp/queue"
        queue_ref = GenericObjectPath(queue_path, "primary")
        consumer_ref = GenericObjectPath(consumer_path, "primary", consumer_name)
        other_consumer_ref = GenericObjectPath(consumer_path, "primary", other_consumer_name)
        partition_count = 3
        self._create_queue(queue_path, partition_count=partition_count)

        data_by_partition = defaultdict(list)
        data = []
        for insert_index in range(10):
            entry = {"$tablet_index": insert_index % partition_count, "data": f"hello world {insert_index}"}
            data.append(entry)
            data_by_partition[insert_index % partition_count].append(entry)

        insert_rows(queue_path, data)

        register_queue_consumer(queue_ref, consumer_ref, vital=False)
        register_queue_consumer(queue_ref, other_consumer_ref, vital=False)

        # Initialize offsets to 0
        offsets_by_name = {
            consumer_name: {i: 0 for i in range(partition_count)},
            other_consumer_name: {i: 0 for i in range(partition_count)},
        }
        for name in offsets_by_name.keys():
            ref = GenericObjectPath(consumer_path, "primary", name)
            advance_consumer_partitions(queue_ref, ref, offsets_by_name[name])

        def validate_pull_consumer():
            for name, offsets in offsets_by_name.items():
                ref = GenericObjectPath(consumer_path, "primary", name)
                for partition_index, expected_data in data_by_partition.items():
                    pulled_data = pull_consumer(str(ref), str(queue_ref), partition_index=partition_index)
                    assert len(pulled_data) == len(expected_data) - offsets[partition_index]
                    for i, expected_entry in enumerate(expected_data[offsets[partition_index]:]):
                        for key, value in expected_entry.items():
                            assert pulled_data[i][key] == value, (
                                f"For {i} element expected {key} to be {value} but got {pulled_data[i][key]}"
                            )

        # Validate initial state
        validate_consumer_table(consumer_path, offsets_by_name)
        validate_pull_consumer()

        # Advance offsets and validate
        for name, partition_index, offset in partition_order_to_insert:
            cur_ref = GenericObjectPath(consumer_path, "primary", name)
            offsets_by_name[name][partition_index] = offset
            advance_consumer_partitions(queue_ref, cur_ref, offsets_by_name[name])
            validate_consumer_table(consumer_path, offsets_by_name)
            validate_pull_consumer()

    @authors("panesher")
    def test_invalid_api_usage(self):
        multi_consumer_path = "//tmp/consumer-multi"
        signle_consumer_path = "//tmp/consumer-single"

        self._create_consumer(multi_consumer_path, multi_consumer=True)
        self._create_consumer(signle_consumer_path)
        queue_path = "//tmp/queue"
        queue_ref = GenericObjectPath(queue_path, "primary")
        multi_consumer_ref = GenericObjectPath(multi_consumer_path, "primary")
        single_consumer_ref_with_name = GenericObjectPath(signle_consumer_path, "primary", "some_name")
        self._create_queue(queue_path, partition_count=3)
        insert_rows(queue_path, [{
            "$tablet_index": 0,
            "data": f"hello world {insert_index}",
        } for insert_index in range(3)])

        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_consumer(multi_consumer_ref, str(queue_ref), partition_index=0)
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_consumer(single_consumer_ref_with_name, str(queue_ref), partition_index=0)

        register_queue_consumer(queue_ref, multi_consumer_ref, vital=False)
        register_queue_consumer(queue_ref, single_consumer_ref_with_name, vital=False)

        with raises_yt_error("Queue consumer name is required for multi-consumer schema"):
            pull_consumer(multi_consumer_ref, str(queue_ref), partition_index=0)
        with raises_yt_error("Queue consumer name is not supported for consumer schema"):
            pull_consumer(single_consumer_ref_with_name, str(queue_ref), partition_index=0)

        with raises_yt_error("Queue consumer name is required for multi-consumer schema"):
            advance_consumer_partitions(queue_ref, multi_consumer_ref, {0: 1})
        with raises_yt_error("Queue consumer name is not supported for consumer schema"):
            advance_consumer_partitions(queue_ref, single_consumer_ref_with_name, {0: 1})


class TestQueueApiOldRegistrationImpl(TestQueueApi):
    # There are component restarts.
    ENABLE_MULTIDAEMON = False

    DELTA_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG = {
        "implementation": "legacy",
        "bypass_caching": True,
    }
