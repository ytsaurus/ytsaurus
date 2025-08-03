from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, start_shuffle, write_shuffle_data, read_shuffle_data, start_transaction,
    abort_transaction, commit_transaction, wait, raises_yt_error, ls, create_user,
    create_account, create_domestic_medium, get_account_disk_space_limit,
    set_account_disk_space_limit, get_account_disk_space, get_chunks, get
)

from yt.test_helpers import assert_items_equal
from yt_helpers import profiler_factory

from copy import deepcopy
from random import shuffle, choice

import pytest
import string


##################################################################

@pytest.mark.enabled_multidaemon
class TestShuffleService(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "enable_shuffle_service": True,
        "signature_generation": {
            "generator": {},
            "cypress_key_writer": {
                "owner_id": "test"
            },
            "key_rotator": {},
        },
        "signature_validation": {
            "validator": {},
            "cypress_key_reader": {},
        }
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
    def test_simple(self, abort):
        proxy = ls("//sys/rpc_proxies")[0]
        active_shuffle_count_sensor = profiler_factory().at_rpc_proxy(proxy).gauge("shuffle_manager/active")

        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=11, parent_transaction_id=parent_transaction)

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

        with raises_yt_error("does not exist"):
            read_shuffle_data(shuffle_handle, 1)

    @authors("apollo1321")
    def test_different_partition_columns(self):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=11, parent_transaction_id=parent_transaction)

        # Check that a negative partition index is not accepted.
        with raises_yt_error("Received negative partition index -1"):
            write_shuffle_data(shuffle_handle, "key1", [{"key1": -1}])

        rows = [{"key1": 0, "key2": -1}, {"key1": -1, "key2": 0}]
        write_shuffle_data(shuffle_handle, "key1", rows[0])
        write_shuffle_data(shuffle_handle, "key2", rows[1])

        assert read_shuffle_data(shuffle_handle, 0) == rows

    @authors("apollo1321")
    def test_write_unsorted_rows(self):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=11, parent_transaction_id=parent_transaction)

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
    def test_invalid_arguments(self):
        with raises_yt_error('Parent transaction id is null'):
            start_shuffle("root", partition_count=2, parent_transaction_id="0-0-0-0")

        with raises_yt_error('Unknown master cell tag'):
            start_shuffle("root", partition_count=2, parent_transaction_id="1-2-3-4")

        parent_transaction = start_transaction(timeout=60000)

        with raises_yt_error('Node has no child with key "a"'):
            start_shuffle("a", partition_count=2, parent_transaction_id=parent_transaction)

        create_user("u")
        create_account("a")

        with raises_yt_error('User "u" has been denied "use" access to account "a"'):
            start_shuffle("a", partition_count=2, parent_transaction_id=parent_transaction, authenticated_user="u")

    @authors("apollo1321")
    def test_store_options(self):
        disk_space_limit = get_account_disk_space_limit("intermediate", "default")
        set_account_disk_space_limit("intermediate", disk_space_limit, self.NON_DEFAULT_MEDIUM)

        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=11,
            parent_transaction_id=parent_transaction,
            replication_factor=5,
            medium=self.NON_DEFAULT_MEDIUM)

        rows = [{"key1": 0, "key2": -1}, {"key1": 1, "key2": 0}]
        write_shuffle_data(shuffle_handle, "key1", rows[1])
        write_shuffle_data(shuffle_handle, "key1", rows[0])

        assert read_shuffle_data(shuffle_handle, 0) == [rows[0]]
        assert read_shuffle_data(shuffle_handle, 1) == [rows[1]]

        assert get_account_disk_space("intermediate", "default") == 0
        assert get_account_disk_space("intermediate", self.NON_DEFAULT_MEDIUM) > 0

        chunks = get_chunks()
        assert len(chunks) > 0
        for chunk in chunks:
            media = get(f"#{chunk}/@media")
            assert len(media) == 1
            assert media[self.NON_DEFAULT_MEDIUM]["replication_factor"] == 5

        commit_transaction(parent_transaction)

    @authors("apollo1321")
    def test_read_from_writer_index_range(self):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=4, parent_transaction_id=parent_transaction)

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
    def test_read_from_writer_index_range_invalid_arguments(self):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=4, parent_transaction_id=parent_transaction)

        with raises_yt_error("Lower limit of mappers range 10 cannot be greater than upper limit 5"):
            read_shuffle_data(shuffle_handle, 0, writer_index_begin=10, writer_index_end=5)

        with raises_yt_error("Expected >= 0, found -5"):
            read_shuffle_data(shuffle_handle, 0, writer_index_begin=-5, writer_index_end=5)

        with raises_yt_error("Request has only one writer range limit"):
            read_shuffle_data(shuffle_handle, 0, writer_index_begin=0)

        with raises_yt_error("Expected >= 0, found -42"):
            write_shuffle_data(shuffle_handle, "id", {"id": 0}, writer_index=-42)

    @authors("apollo1321")
    def test_overwrite_existing_writer_data(self):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=4, parent_transaction_id=parent_transaction)

        permanent_rows = [
            {"partition_id": 3, "data": -1},
            {"partition_id": 1, "data": -1},
        ]

        write_shuffle_data(shuffle_handle, "partition_id", permanent_rows)

        rows = [
            {"partition_id": 0, "data": 0},
            {"partition_id": 1, "data": 1},
        ]
        write_shuffle_data(shuffle_handle, "partition_id", rows, writer_index=0, overwrite_existing_writer_data=True)
        assert read_shuffle_data(shuffle_handle, 0) == [rows[0]]
        assert read_shuffle_data(shuffle_handle, 1) == [permanent_rows[1], rows[1]]
        assert read_shuffle_data(shuffle_handle, 3) == [permanent_rows[0]]

        rows.append({"partition_id": 0, "data": 2})
        write_shuffle_data(shuffle_handle, "partition_id", rows[2], writer_index=0, overwrite_existing_writer_data=False)

        assert read_shuffle_data(shuffle_handle, 0) == [rows[0], rows[2]]
        assert read_shuffle_data(shuffle_handle, 1) == [permanent_rows[1], rows[1]]
        assert read_shuffle_data(shuffle_handle, 3) == [permanent_rows[0]]

        rows.append({"partition_id": 0, "data": 3})
        write_shuffle_data(shuffle_handle, "partition_id", rows[3], writer_index=2, overwrite_existing_writer_data=True)
        assert read_shuffle_data(shuffle_handle, 0) == [rows[0], rows[2], rows[3]]
        assert read_shuffle_data(shuffle_handle, 1) == [permanent_rows[1], rows[1]]

        assert read_shuffle_data(shuffle_handle, 0, writer_index_begin=0, writer_index_end=1) == [rows[0], rows[2]]
        assert read_shuffle_data(shuffle_handle, 1, writer_index_begin=0, writer_index_end=1) == [rows[1]]

        assert read_shuffle_data(shuffle_handle, 0, writer_index_begin=1, writer_index_end=11) == [rows[3]]
        assert read_shuffle_data(shuffle_handle, 1, writer_index_begin=1, writer_index_end=11) == []

        assert read_shuffle_data(shuffle_handle, 3, writer_index_begin=1, writer_index_end=11) == []
        assert read_shuffle_data(shuffle_handle, 3) == [permanent_rows[0]]

        rows.append({"partition_id": 0, "data": 4})
        write_shuffle_data(shuffle_handle, "partition_id", rows[4], writer_index=0, overwrite_existing_writer_data=True)

        assert read_shuffle_data(shuffle_handle, 0) == [rows[3], rows[4]]
        assert read_shuffle_data(shuffle_handle, 0, writer_index_begin=0, writer_index_end=2) == [rows[4]]
        assert read_shuffle_data(shuffle_handle, 0, writer_index_begin=2, writer_index_end=3) == [rows[3]]

        assert read_shuffle_data(shuffle_handle, 1) == [permanent_rows[1]]
        assert read_shuffle_data(shuffle_handle, 1, writer_index_begin=0, writer_index_end=2) == []
        assert read_shuffle_data(shuffle_handle, 1, writer_index_begin=2, writer_index_end=3) == []

        assert read_shuffle_data(shuffle_handle, 3, writer_index_begin=0, writer_index_end=1000) == []
        assert read_shuffle_data(shuffle_handle, 3) == [permanent_rows[0]]

        with raises_yt_error("Writer index must be set"):
            write_shuffle_data(shuffle_handle, "partition_id", rows[0], overwrite_existing_writer_data=True)

    @authors("pavook")
    def test_shuffle_read_with_modified_handle(self):
        account = "intermediate"
        modified_account = "pwnedmediate"  # same length

        parent_transaction = start_transaction(timeout=60000)
        shuffle_handle = start_shuffle(account, partition_count=1, parent_transaction_id=parent_transaction, parse_yson=False)

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
