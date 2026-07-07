from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, start_shuffle, write_shuffle_data, read_shuffle_data, start_transaction,
    abort_transaction, commit_transaction, wait, raises_yt_error, ls, create_user,
    create_account, create_domestic_medium, get_account_disk_space_limit,
    set_account_disk_space_limit, get_account_disk_space, get_chunks, get,
    run_test_vanilla, with_breakpoint, wait_breakpoint, release_breakpoint, set
)

from yt_type_helpers import make_schema

from yt_driver_rpc_bindings import Driver
from yt.test_helpers import assert_items_equal
from yt_helpers import profiler_factory
from yt.yson import parser

from copy import deepcopy
from hashlib import sha1
from random import shuffle, choice

import builtins
import os
import pytest
import string


##################################################################


def _maybe_schema(use_push_based_shuffle, columns):
    # Push-based shuffle is schemaful: the schema is required and is the single
    # source of the column name-to-id mapping. Pull-based shuffle stays
    # schemaless, so no schema is passed for it.
    if not use_push_based_shuffle:
        return {}
    return {
        "schema": make_schema(
            [{"name": name, "type": type_} for name, type_ in columns],
            strict=True,
            unique_keys=False,
        )
    }


##################################################################

@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
class TestShuffleService(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_DRIVER_CONFIG = {
        "enable_retries": True,
    }

    DELTA_RPC_PROXY_CONFIG = {
        "enable_shuffle_service": True,
        "signature_components": {
            "generation": {
                "generator": {},
                "cypress_key_writer": {},
                "key_rotator": {},
            },
            "validation": {
                "cypress_key_reader": {},
            },
        },
    }

    STORE_LOCATION_COUNT = 2

    NON_DEFAULT_MEDIUM = "hdd1"

    @staticmethod
    def _make_random_string(size) -> str:
        return ''.join(choice(string.ascii_letters) for _ in range(size))

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 2

        config["data_node"]["store_locations"][0]["medium_name"] = "default"
        config["data_node"]["store_locations"][1]["medium_name"] = cls.NON_DEFAULT_MEDIUM

    @classmethod
    def setup_class(cls):
        super(TestShuffleService, cls).setup_class()
        create_domestic_medium(cls.NON_DEFAULT_MEDIUM)

    @authors("apollo1321")
    @pytest.mark.parametrize("abort", [False, True])
    def test_simple(self, use_push_based_shuffle, abort):
        proxy = ls("//sys/rpc_proxies")[0]
        active_shuffle_count_sensor = profiler_factory().at_rpc_proxy(proxy).gauge("shuffle_manager/active")

        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=11,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("key", "int64"), ("value", "int64")]))

        for i in range(10):
            rows = [{"key": i, "value": i * 10 + j} for j in range(10)]
            write_shuffle_data(shuffle_handle, "key", rows)

        for i in range(10):
            rows = [{"key": i, "value": i * 10 + j} for j in range(10)]
            assert read_shuffle_data(shuffle_handle, i) == rows

        assert read_shuffle_data(shuffle_handle, 10) == []

        # Check that active_shuffle_count_sensor actually works.
        wait(lambda: active_shuffle_count_sensor.get() == 1, iter=300, sleep_backoff=0.1)

        if abort:
            abort_transaction(parent_transaction)
        else:
            commit_transaction(parent_transaction)

        wait(lambda: active_shuffle_count_sensor.get() == 0, iter=300, sleep_backoff=0.1)

        with raises_yt_error("Shuffle with id .* does not exist"):
            read_shuffle_data(shuffle_handle, 1)

    @authors("apollo1321")
    def test_different_partition_columns(self, use_push_based_shuffle):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=11,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("key1", "int64"), ("key2", "int64")]))

        # Check that a negative partition index is not accepted.
        with raises_yt_error("Received negative partition index .*"):
            write_shuffle_data(shuffle_handle, "key1", [{"key1": -1}])

        rows = [{"key1": 0, "key2": -1}, {"key1": -1, "key2": 0}]
        write_shuffle_data(shuffle_handle, "key1", rows[0])
        write_shuffle_data(shuffle_handle, "key2", rows[1])

        assert read_shuffle_data(shuffle_handle, 0) == rows

    @authors("apollo1321")
    def test_write_unsorted_rows(self, use_push_based_shuffle):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=11,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("key1", "int64"), ("key2", "int64")]))

        # Verify that the writer does not enforce any specific sort order on the rows.
        rows = [
            {"key1": 5, "key2": -1},
            {"key1": 0, "key2": -10},
            {'key1': 4, "key2": -42},
            {'key1': 4, "key2": 10},
        ]
        write_shuffle_data(shuffle_handle, "key1", rows)

        assert read_shuffle_data(shuffle_handle, 0) == [rows[1]]
        assert read_shuffle_data(shuffle_handle, 5) == [rows[0]]
        assert read_shuffle_data(shuffle_handle, 4) == [rows[2], rows[3]]

    @authors("apollo1321")
    def test_invalid_arguments(self, use_push_based_shuffle):
        # A schema is supplied so push-based passes the schema gate and the
        # transaction/account errors below are what actually fire.
        schema_kwargs = _maybe_schema(use_push_based_shuffle, [("key", "int64")])

        with raises_yt_error('Parent transaction id is null'):
            start_shuffle(
                "root",
                partition_count=2,
                parent_transaction_id="0-0-0-0",
                use_push_based_shuffle=use_push_based_shuffle,
                **schema_kwargs)

        with raises_yt_error('Unknown master cell tag'):
            start_shuffle(
                "root",
                partition_count=2,
                parent_transaction_id="1-2-3-4",
                use_push_based_shuffle=use_push_based_shuffle,
                **schema_kwargs)

        parent_transaction = start_transaction(timeout=60000)

        with raises_yt_error('Node has no child with key "a"'):
            start_shuffle(
                "a",
                partition_count=2,
                parent_transaction_id=parent_transaction,
                use_push_based_shuffle=use_push_based_shuffle,
                **schema_kwargs)

        create_user("u")
        create_account("a")

        with raises_yt_error('User "u" has been denied "use" access to account "a"'):
            start_shuffle(
                "a",
                partition_count=2,
                parent_transaction_id=parent_transaction,
                authenticated_user="u",
                use_push_based_shuffle=use_push_based_shuffle,
                **schema_kwargs)

    @authors("apollo1321")
    def test_store_options(self, use_push_based_shuffle):
        disk_space_limit = get_account_disk_space_limit("intermediate", "default")
        set_account_disk_space_limit("intermediate", disk_space_limit, self.NON_DEFAULT_MEDIUM)

        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=11,
            parent_transaction_id=parent_transaction,
            replication_factor=5,
            medium=self.NON_DEFAULT_MEDIUM,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("key1", "int64"), ("key2", "int64")]))

        rows = [{"key1": 0, "key2": -1}, {"key1": 1, "key2": 0}]
        write_shuffle_data(shuffle_handle, "key1", rows[1])
        write_shuffle_data(shuffle_handle, "key1", rows[0])

        assert read_shuffle_data(shuffle_handle, 0) == [rows[0]]
        assert read_shuffle_data(shuffle_handle, 1) == [rows[1]]

        # Push-based shuffle finalizes chunks lazily (via the seal-on-read hint);
        # the master's per-account disk-space metric updates a brief moment later.
        # Wait briefly so the assertion is stable for both modes.
        wait(lambda: get_account_disk_space("intermediate", self.NON_DEFAULT_MEDIUM) > 0)
        assert get_account_disk_space("intermediate", "default") == 0

        chunks = get_chunks()
        assert len(chunks) > 0
        for chunk in chunks:
            media = get(f"#{chunk}/@media")
            assert len(media) == 1
            assert media[self.NON_DEFAULT_MEDIUM]["replication_factor"] == 5

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_read_from_writer_index_range(self, use_push_based_shuffle):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=4,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "string")]))

        writer_indices = [0, 1, 5, 10, 11]
        partitions = [0, 1, 3]

        rows_data = {}

        shuffle(writer_indices)
        for writer_index in writer_indices:
            shuffle(partitions)
            for partition_id in partitions:
                data = self._make_random_string(10)
                rows_data[(writer_index, partition_id)] = data
                write_shuffle_data(shuffle_handle, "partition_id", {"partition_id": partition_id, "data": data}, writer_index=writer_index)

        shuffle(writer_indices)
        for writer_index in writer_indices:
            shuffle(partitions)
            for partition_id in partitions:
                expected = [{"partition_id": partition_id, "data": rows_data[(writer_index, partition_id)]}]
                assert read_shuffle_data(shuffle_handle, partition_id, writer_index_begin=writer_index, writer_index_end=writer_index + 1) == expected

        for partition_id in partitions:
            expected = [{"partition_id": partition_id, "data": rows_data[(writer_index, partition_id)]} for writer_index in writer_indices]
            # Check that chunk fetching is effective.
            actual = read_shuffle_data(shuffle_handle, partition_id, writer_index_begin=0, writer_index_end=2**30)
            assert_items_equal(actual, expected)

        for partition_id in partitions:
            for writer_from, writer_to in [(0, 0), (1, 4), (1, 5), (1, 10), (1, 11), (11, 11)]:
                expected = [
                    {"partition_id": partition_id, "data": rows_data[(writer_index, partition_id)]}
                    for writer_index in range(writer_from, writer_to)
                    if rows_data.get((writer_index, partition_id))
                ]
                actual = read_shuffle_data(shuffle_handle, partition_id, writer_index_begin=writer_from, writer_index_end=writer_to)
                assert_items_equal(actual, expected)

    @authors("apollo1321")
    def test_read_from_writer_index_range_invalid_arguments(self, use_push_based_shuffle):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=4,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("id", "int64")]))

        with raises_yt_error("Lower limit of mappers range .* cannot be greater than upper limit .*"):
            read_shuffle_data(shuffle_handle, 0, writer_index_begin=10, writer_index_end=5)

        with raises_yt_error("Expected >= .*, found .*"):
            read_shuffle_data(shuffle_handle, 0, writer_index_begin=-5, writer_index_end=5)

        with raises_yt_error("Request has only one writer range limit"):
            read_shuffle_data(shuffle_handle, 0, writer_index_begin=0)

        with raises_yt_error("Expected >= .*, found .*"):
            write_shuffle_data(shuffle_handle, "id", {"id": 0}, writer_index=-42)

    @authors("apollo1321")
    def test_overwrite_existing_writer_data(self, use_push_based_shuffle):
        def _checkpoint(writes, expected_reads):
            parent_transaction = start_transaction(timeout=60000)
            shuffle_handle = start_shuffle(
                "intermediate",
                partition_count=4,
                parent_transaction_id=parent_transaction,
                use_push_based_shuffle=use_push_based_shuffle,
                **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "int64")]))
            for write_op in writes:
                write_op(shuffle_handle)
            for partition, w_range, expected in expected_reads:
                kwargs = {}
                if w_range:
                    kwargs["writer_index_begin"], kwargs["writer_index_end"] = w_range
                assert read_shuffle_data(shuffle_handle, partition, **kwargs) == expected
            commit_transaction(parent_transaction)

        permanent_rows = [
            {"partition_id": 3, "data": -1},
            {"partition_id": 1, "data": -1},
        ]
        rows = [
            {"partition_id": 0, "data": 0},
            {"partition_id": 1, "data": 1},
        ]
        extra_no_overwrite = {"partition_id": 0, "data": 2}
        new_writer_row = {"partition_id": 0, "data": 3}
        overwrite_again_row = {"partition_id": 0, "data": 4}

        # Checkpoint A: permanent + K=1 (W=0, overwrite=True).
        _checkpoint(
            writes=[
                lambda h: write_shuffle_data(h, "partition_id", permanent_rows),
                lambda h: write_shuffle_data(h, "partition_id", rows, writer_index=0, overwrite_existing_writer_data=True),
            ],
            expected_reads=[
                (0, None, [rows[0]]),
                (1, None, [permanent_rows[1], rows[1]]),
                (3, None, [permanent_rows[0]]),
            ],
        )

        # Checkpoint B: + K=2 (W=0, overwrite=False).
        _checkpoint(
            writes=[
                lambda h: write_shuffle_data(h, "partition_id", permanent_rows),
                lambda h: write_shuffle_data(h, "partition_id", rows, writer_index=0, overwrite_existing_writer_data=True),
                lambda h: write_shuffle_data(h, "partition_id", extra_no_overwrite, writer_index=0, overwrite_existing_writer_data=False),
            ],
            expected_reads=[
                (0, None, [rows[0], extra_no_overwrite]),
                (1, None, [permanent_rows[1], rows[1]]),
                (3, None, [permanent_rows[0]]),
            ],
        )

        # Checkpoint C: + K=3 (W=2, overwrite=True).
        _checkpoint(
            writes=[
                lambda h: write_shuffle_data(h, "partition_id", permanent_rows),
                lambda h: write_shuffle_data(h, "partition_id", rows, writer_index=0, overwrite_existing_writer_data=True),
                lambda h: write_shuffle_data(h, "partition_id", extra_no_overwrite, writer_index=0, overwrite_existing_writer_data=False),
                lambda h: write_shuffle_data(h, "partition_id", new_writer_row, writer_index=2, overwrite_existing_writer_data=True),
            ],
            expected_reads=[
                (0, None, [rows[0], extra_no_overwrite, new_writer_row]),
                (1, None, [permanent_rows[1], rows[1]]),
                (0, (0, 1), [rows[0], extra_no_overwrite]),
                (1, (0, 1), [rows[1]]),
                (0, (1, 11), [new_writer_row]),
                (1, (1, 11), []),
                (3, (1, 11), []),
                (3, None, [permanent_rows[0]]),
            ],
        )

        # Checkpoint D: + K=4 (W=0, overwrite=True → invalidates K=1 and K=2).
        _checkpoint(
            writes=[
                lambda h: write_shuffle_data(h, "partition_id", permanent_rows),
                lambda h: write_shuffle_data(h, "partition_id", rows, writer_index=0, overwrite_existing_writer_data=True),
                lambda h: write_shuffle_data(h, "partition_id", extra_no_overwrite, writer_index=0, overwrite_existing_writer_data=False),
                lambda h: write_shuffle_data(h, "partition_id", new_writer_row, writer_index=2, overwrite_existing_writer_data=True),
                lambda h: write_shuffle_data(h, "partition_id", overwrite_again_row, writer_index=0, overwrite_existing_writer_data=True),
            ],
            expected_reads=[
                (0, None, [new_writer_row, overwrite_again_row]),
                (0, (0, 2), [overwrite_again_row]),
                (0, (2, 3), [new_writer_row]),
                (1, None, [permanent_rows[1]]),
                (1, (0, 2), []),
                (1, (2, 3), []),
                (3, (0, 1000), []),
                (3, None, [permanent_rows[0]]),
            ],
        )

    @authors("apollo1321")
    def test_overwrite_requires_writer_index(self, use_push_based_shuffle):
        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=4,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "int64")]))

        # overwrite_existing_writer_data requires a writer_index to identify the
        # data being overwritten.
        with raises_yt_error("Writer index must be set"):
            write_shuffle_data(
                shuffle_handle, "partition_id",
                {"partition_id": 0, "data": 0},
                overwrite_existing_writer_data=True)

        commit_transaction(parent_transaction)

    @authors("pavook")
    def test_shuffle_with_modified_handle(self, use_push_based_shuffle):
        account = "intermediate"
        modified_account = "pwnedmediate"  # same length

        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            account,
            partition_count=1,
            parent_transaction_id=parent_transaction,
            parse_yson=False,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("key", "int64"), ("value", "int64")]))

        assert account in shuffle_handle["payload"]
        modified_handle = deepcopy(shuffle_handle)
        modified_handle["payload"] = shuffle_handle["payload"].replace(account, modified_account)

        rows = [{"key": 0, "value": 1}]
        write_shuffle_data(shuffle_handle, "key", rows)

        with raises_yt_error("Signature validation failed"):
            write_shuffle_data(modified_handle, "key", rows)

        assert read_shuffle_data(shuffle_handle, 0) == rows
        with raises_yt_error("Signature validation failed"):
            read_shuffle_data(modified_handle, 0)

    @authors("apollo1321")
    def test_job_proxy_shuffle_service_without_api_service(self, use_push_based_shuffle):
        with raises_yt_error("Option .* cannot be enabled when .* is disabled"):
            run_test_vanilla(
                "exit 0",
                task_patch={
                    "enable_rpc_proxy_in_job_proxy": False,
                    "enable_shuffle_service_in_job_proxy": True,
                },
            )

    @authors("apollo1321")
    def test_push_based_write_after_read_fails(self, use_push_based_shuffle):
        if not use_push_based_shuffle:
            pytest.skip("push-based-only test")
        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=4,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=True,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "string")]))

        rows = [{"partition_id": 0, "data": "x"}]
        write_shuffle_data(shuffle_handle, "partition_id", rows, writer_index=0, overwrite_existing_writer_data=True)

        assert read_shuffle_data(shuffle_handle, 0) == rows

        with raises_yt_error("Shuffle read phase has started"):
            write_shuffle_data(
                shuffle_handle, "partition_id",
                {"partition_id": 0, "data": "y"},
                writer_index=1,
                overwrite_existing_writer_data=True)

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_concurrent_writers_same_partition(self, use_push_based_shuffle):
        from concurrent.futures import ThreadPoolExecutor

        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=2,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "string")]))

        writer_count = 8
        rows_per_writer = 10

        def _do_write(writer_index):
            rows = [
                {"partition_id": 0, "data": f"w{writer_index}_r{i}"}
                for i in range(rows_per_writer)
            ]
            write_shuffle_data(
                shuffle_handle, "partition_id", rows,
                writer_index=writer_index,
                overwrite_existing_writer_data=True)

        with ThreadPoolExecutor(max_workers=writer_count) as pool:
            list(pool.map(_do_write, range(writer_count)))

        result = read_shuffle_data(shuffle_handle, 0)
        # Expect writer_count * rows_per_writer rows, no duplicates.
        assert len(result) == writer_count * rows_per_writer
        seen = builtins.set(row["data"] for row in result)
        assert len(seen) == writer_count * rows_per_writer

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_concurrent_reads_same_partition(self, use_push_based_shuffle):
        from concurrent.futures import ThreadPoolExecutor

        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=2,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "int64")]))

        rows = [{"partition_id": 0, "data": i} for i in range(50)]
        write_shuffle_data(
            shuffle_handle, "partition_id", rows,
            writer_index=0,
            overwrite_existing_writer_data=True)

        def _do_read(_):
            return read_shuffle_data(shuffle_handle, 0)

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(_do_read, range(4)))

        for r in results:
            assert r == rows

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_empty_partition(self, use_push_based_shuffle):
        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=5,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "string")]))

        write_shuffle_data(
            shuffle_handle, "partition_id",
            [{"partition_id": 2, "data": "only-here"}],
            writer_index=0,
            overwrite_existing_writer_data=True)

        for partition in range(5):
            actual = read_shuffle_data(shuffle_handle, partition)
            if partition == 2:
                assert actual == [{"partition_id": 2, "data": "only-here"}]
            else:
                assert actual == []

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_push_based_requires_schema(self, use_push_based_shuffle):
        if not use_push_based_shuffle:
            pytest.skip("push-based-only test")
        parent_transaction = start_transaction(timeout=60000)

        with raises_yt_error("Push-based shuffle requires a schema"):
            start_shuffle(
                "intermediate",
                partition_count=4,
                parent_transaction_id=parent_transaction,
                use_push_based_shuffle=True)

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_push_based_requires_strict_schema(self, use_push_based_shuffle):
        if not use_push_based_shuffle:
            pytest.skip("push-based-only test")
        parent_transaction = start_transaction(timeout=60000)

        # Push-based readers reconstruct column names solely from the schema, so a
        # non-strict schema (which would permit columns outside it) is rejected.
        nonstrict_schema = make_schema(
            [{"name": "partition_id", "type": "int64"}, {"name": "data", "type": "string"}],
            strict=False,
            unique_keys=False)

        with raises_yt_error("Push-based shuffle requires a strict schema"):
            start_shuffle(
                "intermediate",
                partition_count=4,
                parent_transaction_id=parent_transaction,
                use_push_based_shuffle=True,
                schema=nonstrict_schema)

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_push_based_column_order_independent(self, use_push_based_shuffle):
        if not use_push_based_shuffle:
            pytest.skip("push-based-only test")
        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=2,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=True,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "int64")]))

        # The column order within a written row need not match the schema order;
        # validation and partitioning key off the schema-derived column ids, not
        # value position.
        write_shuffle_data(
            shuffle_handle, "partition_id",
            [{"data": 10, "partition_id": 0}, {"partition_id": 1, "data": 11}],
            writer_index=0,
            overwrite_existing_writer_data=True)
        assert read_shuffle_data(shuffle_handle, 0) == [{"partition_id": 0, "data": 10}]
        assert read_shuffle_data(shuffle_handle, 1) == [{"partition_id": 1, "data": 11}]

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_push_based_strict_schema_validation(self, use_push_based_shuffle):
        if not use_push_based_shuffle:
            pytest.skip("push-based-only test")
        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=4,
            parent_transaction_id=parent_transaction,
            use_push_based_shuffle=True,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "string")]))

        # A column outside the schema is rejected; the schema is the single
        # source of the column name-to-id mapping, so unknown columns cannot be
        # written. Type mismatches are rejected the same way by the writer.
        with raises_yt_error("Unexpected column"):
            write_shuffle_data(
                shuffle_handle, "partition_id",
                {"partition_id": 0, "unexpected_column": 1},
                writer_index=0,
                overwrite_existing_writer_data=True)

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_push_based_quorum_safe(self, use_push_based_shuffle):
        if not use_push_based_shuffle:
            pytest.skip("push-based-only test")
        parent_transaction = start_transaction(timeout=60000)
        replication_factor = 5
        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=4,
            parent_transaction_id=parent_transaction,
            replication_factor=replication_factor,
            use_push_based_shuffle=True,
            **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("data", "int64")]))

        write_shuffle_data(
            shuffle_handle, "partition_id",
            [{"partition_id": 0, "data": 1}],
            writer_index=0,
            overwrite_existing_writer_data=True)
        assert read_shuffle_data(shuffle_handle, 0) == [{"partition_id": 0, "data": 1}]

        # The journal chunks must use read/write quorums derived from the
        # replication factor, so that the read and write quorums always
        # intersect: read_quorum + write_quorum > replication_factor.
        chunks = get_chunks()
        assert len(chunks) > 0
        for chunk in chunks:
            read_quorum = get(f"#{chunk}/@read_quorum")
            write_quorum = get(f"#{chunk}/@write_quorum")
            assert read_quorum + write_quorum > replication_factor

        commit_transaction(parent_transaction)


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
class TestShuffleServiceInJobProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 5

    DRIVER_BACKEND = "native"

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_authentication_manager": {
                    "require_authentication": True,
                    "cypress_token_authenticator": {
                        "secure": True,
                    },
                },
            },
            "signature_components": {
                "generation": {
                    "generator": {},
                    "cypress_key_writer": {},
                    "key_rotator": {},
                },
                "validation": {
                    "cypress_key_reader": {},
                },
            },
        },
    }

    def setup_method(self, method):
        super(TestShuffleServiceInJobProxy, self).setup_method(method)
        self.work_dir = os.getcwd()
        create_user("tester_name")
        set("//sys/tokens/" + sha1(b"tester_token").hexdigest(), "tester_name")

    def teardown_method(self, method):
        os.chdir(self.work_dir)
        super(TestShuffleServiceInJobProxy, self).teardown_method(method)

    def create_driver_from_uds(self, socket_file):
        return Driver(config={
            "connection_type": "rpc",
            "proxy_unix_domain_socket": socket_file,
            "api_version": 4,
        })

    @authors("apollo1321")
    @pytest.mark.parametrize("port_count", [0, 1])
    def test_shuffle_service_in_job_proxy(self, use_push_based_shuffle, port_count):
        """
        This test verifies that the Shuffle Service is accessible to all jobs
        within a single vanilla operation, ensuring cross-job communication for
        shuffle methods via IPv6 while maintaining user job API interactions
        through Unix domain sockets (UDS).

        Container Structure (simplified for 2 jobs):
        +---------------------------------+ +---------------------------------+
        |       Exec Node 1, slot K       | |        Exec Node 2, slot Q      |
        |     /tmp/unix_domain_socket1    | |     /tmp/unix_domain_socket2    |
        | +-----------------------------+ | | +-----------------------------+ |
        | |         Job Proxy 1         | | | |         Job Proxy 2         | |
        | | ApiService -> /mounted_uds  | | | | ApiService -> /mounted_uds  | |
        | | ShuffleService -> [::]:P1   | | | | ShuffleService -> [::]:P2   | |
        | +-----------------------------+ | | +-----------------------------+ |
        | +-----------------------------+ | | +-----------------------------+ |
        | |         User Job 1          | | | |         User Job 2          | |
        | |  YtClient -> /mounted_uds   | | | |  YtClient -> /mounted_uds   | |
        | +-----------------------------+ | | +-----------------------------+ |
        +---------------------------------+ +---------------------------------+

        When a user job enables shuffle service in job proxy:
        - During initialization, the API service is configured with the shuffle
          service address running on the job proxy.
        - User job uses the mounted Unix domain socket to send requests to the
          API service.
        - Unlike the API service (which uses a Unix domain socket), the shuffle
          service registers on an RPC server bound to IPv6 port. This allows
          other jobs to access the current job's shuffle service.
        """

        job_count = 5

        op = run_test_vanilla(
            with_breakpoint(
                "echo $YT_JOB_PROXY_SOCKET_PATH >&2; BREAKPOINT; "
                f"if {'!' if port_count > 0 else ''} [[ -v YT_PORT_0 ]]; then exit 1; fi; "
                "if [[ -v YT_PORT_1 ]]; then exit 2; fi; "
            ),
            task_patch={
                "enable_rpc_proxy_in_job_proxy": True,
                "enable_shuffle_service_in_job_proxy": True,
                "port_count": port_count,
            },
            job_count=job_count,
        )

        # NB(apollo1321): Change current directory to make the Unix domain
        # socket path relative, thus shorter. The maximum length for the socket
        # path string is ~108 characters.
        os.chdir(self.path_to_test)

        job_ids = wait_breakpoint(job_count=job_count)
        for job_id in job_ids:
            wait(lambda: len(op.read_stderr(job_id)) > 0)

        socket_files = [os.path.relpath(op.read_stderr(job_id).decode("ascii").strip()) for job_id in job_ids]

        drivers = [self.create_driver_from_uds(socket_file) for socket_file in socket_files]

        coordinator_addresses = builtins.set()
        job_id_to_coordinator_port = {}
        for index in range(len(job_ids)):
            parent_transaction = start_transaction(
                timeout=60000,
                driver=drivers[index],
                token="tester_token")

            shuffle_handle = start_shuffle(
                "intermediate",
                partition_count=job_count + 1,
                parent_transaction_id=parent_transaction,
                driver=drivers[index],
                token="tester_token",
                use_push_based_shuffle=use_push_based_shuffle,
                **_maybe_schema(use_push_based_shuffle, [("partition_id", "int64"), ("job_id", "int64")]))

            coordinator_address = parser.loads(shuffle_handle["payload"].encode())["coordinator_address"]
            assert coordinator_address not in coordinator_addresses
            coordinator_addresses.add(coordinator_address)
            coordinator_port = int(coordinator_address.split(":")[1])
            assert coordinator_port > 0
            job_id_to_coordinator_port[job_ids[index]] = coordinator_port

            for job_id in range(job_count):
                write_shuffle_data(
                    shuffle_handle,
                    "partition_id",
                    [
                        {"partition_id": partition_id, "job_id": job_id} for partition_id in range(job_count)
                    ],
                    driver=drivers[job_id],
                    token="tester_token")

            for job_id in range(job_count):
                for partition_id in range(job_count):
                    assert read_shuffle_data(shuffle_handle, partition_id, driver=drivers[job_id], token="tester_token") == [
                        {"partition_id": partition_id, "job_id": job_id} for job_id in range(job_count)
                    ]

            abort_transaction(parent_transaction)

        exec_nodes = ls("//sys/exec_nodes")
        for node_id in exec_nodes:
            allocations_path = f"//sys/cluster_nodes/{node_id}/orchid/exec_node/job_controller/allocations"
            wait(lambda: len(ls(allocations_path)) > 0)
            allocations = ls(allocations_path)
            assert len(allocations) == 1
            allocation_info = get(f"{allocations_path}/{allocations[0]}")
            job_info = get(f"{allocations_path}/{allocations[0]}/job")
            assert job_info["job_proxy_rpc_server_port"] == job_id_to_coordinator_port[allocation_info["last_job_id"]]

        release_breakpoint()
        op.track()

    @authors("pavook")
    def test_shuffle_with_modified_handle(self, use_push_based_shuffle):
        account = "intermediate"
        modified_account = "pwnedmediate"
        assert len(account) == len(modified_account)

        op = run_test_vanilla(
            with_breakpoint("echo $YT_JOB_PROXY_SOCKET_PATH >&2; BREAKPOINT;"),
            task_patch={
                "enable_rpc_proxy_in_job_proxy": True,
                "enable_shuffle_service_in_job_proxy": True,
            },
            job_count=1)

        os.chdir(self.path_to_test)

        job_ids = wait_breakpoint()
        socket_file = os.path.relpath(op.read_stderr(job_ids[0]).decode("ascii").strip())
        driver = self.create_driver_from_uds(socket_file)

        parent_transaction = start_transaction(timeout=60000, driver=driver, token="tester_token")
        shuffle_handle = start_shuffle(
            account,
            partition_count=1,
            parent_transaction_id=parent_transaction,
            driver=driver,
            parse_yson=False,
            token="tester_token",
            use_push_based_shuffle=use_push_based_shuffle,
            **_maybe_schema(use_push_based_shuffle, [("key", "int64"), ("value", "int64")]))

        assert account in shuffle_handle["payload"]
        modified_handle = deepcopy(shuffle_handle)
        modified_handle["payload"] = shuffle_handle["payload"].replace(account, modified_account)

        rows = [{"key": 0, "value": 1}]
        write_shuffle_data(shuffle_handle, "key", rows, driver=driver, token="tester_token")

        with raises_yt_error("Signature validation failed"):
            write_shuffle_data(modified_handle, "key", rows, driver=driver, token="tester_token")

        assert read_shuffle_data(shuffle_handle, 0, driver=driver, token="tester_token") == rows
        with raises_yt_error("Signature validation failed"):
            read_shuffle_data(modified_handle, 0, driver=driver, token="tester_token")

        release_breakpoint()
        op.track()
