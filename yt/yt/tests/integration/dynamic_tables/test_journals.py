from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, MASTERS_SERVICE, is_asan_build

from yt_commands import (
    authors, wait, create, get, set, ls, copy, move, remove,
    exists, create_domestic_medium, raises_yt_error,
    abort_transaction, commit_transaction, build_master_snapshots, update_nodes_dynamic_config,
    read_journal, write_journal, truncate_journal, wait_until_sealed, get_singular_chunk_id,
    set_node_banned, set_nodes_banned, start_transaction,
    get_account_disk_space, get_account_committed_disk_space, get_chunk_owner_disk_space,
    externalize)

from yt_helpers import (
    get_chunk_owner_master_cell_counters, get_chunk_owner_master_cell_gauges,
    master_exit_read_only_sync)

import yt.yson as yson
from yt.common import YtError

import pytest

import random
import string
import sys
from io import TextIOBase
from time import sleep

##################################################################

PAYLOAD = [
    {
        "payload": "payload-"
        + str(i)
        + "-"
        + "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(i * i + random.randrange(10))
        )
    }
    for i in range(0, 10)
]

ERASURE_JOURNAL_ATTRIBUTES = {
    "none": {
        "erasure_codec": "none",
        "replication_factor": 3,
        "read_quorum": 2,
        "write_quorum": 2,
    },
    "isa_lrc_12_2_2": {
        "erasure_codec": "isa_lrc_12_2_2",
        "replication_factor": 1,
        "read_quorum": 14,
        "write_quorum": 15,
    },
    "isa_reed_solomon_3_3": {
        "erasure_codec": "isa_reed_solomon_3_3",
        "replication_factor": 1,
        "read_quorum": 4,
        "write_quorum": 5,
    },
    "isa_reed_solomon_6_3": {
        "erasure_codec": "isa_reed_solomon_6_3",
        "replication_factor": 1,
        "read_quorum": 7,
        "write_quorum": 8,
    }
}

##################################################################


class TestJournalsBase(YTEnvSetup):
    def _write_and_wait_until_sealed(self, path, *args, **kwargs):
        write_journal(path, *args, **kwargs)
        wait_until_sealed(path)

    def _write_slowly(self, path, rows, *args, **kwargs):
        class SlowStream(TextIOBase):
            def __init__(self, data):
                self._data = data
                self._position = 0

            def read(self, size):
                if size < 0:
                    raise ValueError()

                size = min(size, 1000)
                size = min(size, len(self._data) - self._position)

                sys.stderr.write("Reading {} bytes at position {}\n".format(size, self._position))
                sleep(0.1)

                result = self._data[self._position:self._position + size]
                self._position = min(self._position + size, len(self._data))
                return result

        yson_rows = yson.dumps(rows, yson_type="list_fragment")

        write_journal(path, input_stream=SlowStream(yson_rows), *args, **kwargs)

    def _wait_until_last_chunk_sealed(self, path):
        def check():
            try:
                chunk_ids = get(path + "/@chunk_ids")
                chunk_id = chunk_ids[-1]
                return all(r.attributes["state"] == "sealed" for r in get("#{}/@stored_replicas".format(chunk_id)))
            except YtError:
                return False
        wait(check)

    def _truncate_and_check(self, path, row_count, prerequisite_transaction_ids=[]):
        rows = read_journal(path)
        original_row_count = len(rows)

        assert get(path + "/@quorum_row_count") == original_row_count
        assert get(path + "/@sealed")

        truncate_journal(path, row_count, prerequisite_transaction_ids=prerequisite_transaction_ids)

        assert get(path + "/@sealed")

        if row_count < original_row_count:
            assert get(path + "/@quorum_row_count") == row_count
            assert read_journal(path) == rows[:row_count]
        else:
            assert get(path + "/@quorum_row_count") == original_row_count
            assert read_journal(path) == rows

    def _get_chunk_replica_length(self, chunk_id):
        result = []
        for replica in get("#{}/@stored_replicas".format(chunk_id)):
            orchid = get("//sys/cluster_nodes/{}/orchid/data_node/stored_chunks/{}".format(replica, chunk_id))
            result.append(orchid["flushed_row_count"])
        return result

    def _find_replicas_with_length(self, chunk_id, length):
        result = []
        for replica in get("#{}/@last_seen_replicas".format(chunk_id)):
            orchid = get("//sys/cluster_nodes/{}/orchid/data_node/stored_chunks/{}".format(replica, chunk_id))
            if orchid["flushed_row_count"] == length:
                result.append(replica)
        return result

    def _check_journal_copy_attribute_correct(self, source_path, copy_path):
        attributes = [
            "erasure_codec",
            "replication_factor",
            "read_quorum",
            "write_quorum",
        ]
        for attr in attributes:
            assert get(f"{source_path}/@{attr}") == get(f"{copy_path}/@{attr}")


##################################################################


class TestJournals(TestJournalsBase):
    NUM_TEST_PARTITIONS = 10
    NUM_MASTERS = 3
    NUM_NODES = 6

    PAYLOAD = [
        {
            "payload": "payload-"
            + str(i)
            + "-"
            + "".join(
                random.choice(string.ascii_uppercase + string.digits) for _ in range(i * i + random.randrange(10))
            )
        }
        for i in range(0, 10)
    ]

    @authors("babenko")
    def test_explicit_compression_codec_forbidden(self):
        with pytest.raises(YtError):
            create("journal", "//tmp/j", attributes={"compression_codec": "lz4"})

    @authors("babenko")
    def test_inherited_compression_codec_allowed(self):
        create("map_node", "//tmp/m", attributes={"compression_codec": "lz4"})
        create("journal", "//tmp/m/j")
        assert get("//tmp/m/j/@compression_codec") == "none"

    @authors("ignat")
    def test_create_regular_success(self):
        create("journal", "//tmp/j")
        assert get("//tmp/j/@erasure_codec") == "none"
        assert get("//tmp/j/@replication_factor") == 3
        assert get("//tmp/j/@read_quorum") == 2
        assert get("//tmp/j/@write_quorum") == 2
        assert get_chunk_owner_disk_space("//tmp/j") == 0
        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 0
        assert get("//tmp/j/@chunk_ids") == []

    @authors("babenko")
    def test_create_erasure_success(self):
        create(
            "journal",
            "//tmp/j",
            attributes={
                "erasure_codec": "isa_lrc_12_2_2",
                "replication_factor": 1,
                "read_quorum": 14,
                "write_quorum": 15,
            },
        )
        assert get("//tmp/j/@erasure_codec") == "isa_lrc_12_2_2"
        assert get("//tmp/j/@replication_factor") == 1
        assert get("//tmp/j/@read_quorum") == 14
        assert get("//tmp/j/@write_quorum") == 15
        assert get_chunk_owner_disk_space("//tmp/j") == 0
        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 0
        assert get("//tmp/j/@chunk_ids") == []

    @authors("babenko", "ignat")
    def test_create_regular_failure(self):
        BAD_ATTRIBUTES = [
            {"replication_factor": 1},
            {"read_quorum": 4},
            {"write_quorum": 4},
            {"replication_factor": 4},
        ]
        for attributes in BAD_ATTRIBUTES:
            with pytest.raises(YtError):
                create("journal", "//tmp/j", attributes=attributes)

    @authors("babenko")
    def test_create_erasure_failure(self):
        BAD_ATTRIBUTES = [
            {"erasure_codec": "isa_lrc_12_2_2", "replication_factor": 2},
            {"erasure_codec": "isa_lrc_12_2_2", "read_quorum": 17},
            {"erasure_codec": "isa_lrc_12_2_2", "write_quorum": 17},
            {"erasure_codec": "isa_lrc_12_2_2", "read_quorum": 14, "write_quorum": 14},
        ]
        for attributes in BAD_ATTRIBUTES:
            with pytest.raises(YtError):
                create("journal", "//tmp/j", attributes=attributes)

    @authors("babenko")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_journal_quorum_row_count(self, enable_chunk_preallocation):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=enable_chunk_preallocation,
            journal_writer={
                "max_batch_row_count": 4,
                "max_flush_row_count": 4,
                "max_chunk_row_count": 4,
            }
        )
        assert get("//tmp/j/@quorum_row_count") == 10

        write_journal(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=enable_chunk_preallocation,
            journal_writer={"dont_close": True},
        )
        assert get("//tmp/j/@quorum_row_count") == 20

    @authors("babenko", "ignat")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_read_write(self, enable_chunk_preallocation):
        create("journal", "//tmp/j")
        for i in range(0, 10):
            self._write_and_wait_until_sealed(
                "//tmp/j",
                PAYLOAD,
                enable_chunk_preallocation=enable_chunk_preallocation,
                journal_writer={
                    "max_batch_row_count": 4,
                    "max_chunk_row_count": 4,
                    "max_flush_row_count": 4,
                },
            )

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 100

        chunk_count = get("//tmp/j/@chunk_count")
        assert chunk_count >= 10 * 3
        assert chunk_count <= 10 * (3 + (1 if enable_chunk_preallocation else 0))

        for i in range(0, 10):
            assert read_journal("//tmp/j[#" + str(i * 10) + ":]") == PAYLOAD * (10 - i)

        for i in range(0, 9):
            assert read_journal("//tmp/j[#" + str(i * 10 + 5) + ":]") == (PAYLOAD * (10 - i))[5:]

        assert read_journal("//tmp/j[#200:]") == []

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_truncate1(self, enable_chunk_preallocation):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=enable_chunk_preallocation,
        )

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 10

        self._truncate_and_check("//tmp/j", 3)
        self._truncate_and_check("//tmp/j", 10)

        with pytest.raises(YtError):
            truncate_journal("//tmp/j", -2)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_truncate2(self, enable_chunk_preallocation):
        create("journal", "//tmp/j")
        for i in range(0, 10):
            self._write_and_wait_until_sealed(
                "//tmp/j",
                PAYLOAD,
                enable_chunk_preallocation=enable_chunk_preallocation,
            )

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 100

        self._truncate_and_check("//tmp/j", 73)
        self._truncate_and_check("//tmp/j", 2)
        self._truncate_and_check("//tmp/j", 0)

        for i in range(0, 10):
            self._write_and_wait_until_sealed(
                "//tmp/j",
                PAYLOAD,
                enable_chunk_preallocation=enable_chunk_preallocation,
            )
            self._truncate_and_check("//tmp/j", (i + 1) * 3)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 30

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_truncate_restart(self, enable_chunk_preallocation):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=enable_chunk_preallocation,
        )

        row_count = 7
        self._truncate_and_check("//tmp/j", row_count)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get("//tmp/j/@quorum_row_count") == row_count

        chunk_ids = get("//tmp/j/@chunk_ids")
        assert get("#{}/@row_count".format(chunk_ids[0])) == row_count

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_truncate_prerequisites(self, enable_chunk_preallocation):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=enable_chunk_preallocation,
        )

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 10

        tx = start_transaction()
        self._truncate_and_check("//tmp/j", 7, prerequisite_transaction_ids=[tx])

        commit_transaction(tx)
        with pytest.raises(YtError):
            self._truncate_and_check("//tmp/j", 5, prerequisite_transaction_ids=[tx])

        self._truncate_and_check("//tmp/j", 5)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_simulated_failures_truncate(self, enable_chunk_preallocation):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", True)
        set("//sys/@config/chunk_manager/chunk_refresh_period", 50)

        create("journal", "//tmp/j")

        rows = PAYLOAD * 100

        self._write_slowly(
            "//tmp/j",
            rows,
            enable_chunk_preallocation=enable_chunk_preallocation,
            journal_writer={
                "dont_seal": True,
                "max_batch_row_count": 9,
                "max_flush_row_count": 9,
                "max_chunk_row_count": 49,
                "replica_failure_probability": 0.1,
                "open_session_backoff_time": 100,
            },
        )

        self._wait_until_last_chunk_sealed("//tmp/j")
        assert get("//tmp/j/@quorum_row_count") == len(rows)
        assert read_journal("//tmp/j") == rows

        self._truncate_and_check("//tmp/j", 73)
        self._truncate_and_check("//tmp/j", 42)
        self._truncate_and_check("//tmp/j", 13)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_cannot_truncate_unsealed(self, enable_chunk_preallocation):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)

        create("journal", "//tmp/j")
        write_journal(
            "//tmp/j", PAYLOAD,
            enable_chunk_preallocation=enable_chunk_preallocation,
            journal_writer={"dont_close": True}
        )

        with pytest.raises(YtError):
            truncate_journal("//tmp/j", 1)

    @authors("babenko")
    def test_resource_usage(self):
        wait(lambda: get_account_committed_disk_space("tmp") == 0)

        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", PAYLOAD)

        chunk_id = get_singular_chunk_id("//tmp/j")

        wait(lambda: get("#" + chunk_id + "/@sealed"))

        get("#" + chunk_id + "/@owning_nodes")
        disk_space_delta = get_chunk_owner_disk_space("//tmp/j")
        assert disk_space_delta > 0

        get("//sys/accounts/tmp/@")

        wait(
            lambda: get_account_committed_disk_space("tmp") == disk_space_delta
            and get_account_disk_space("tmp") == disk_space_delta
        )

        remove("//tmp/j")

        wait(lambda: get_account_committed_disk_space("tmp") == 0 and get_account_disk_space("tmp") == 0)

    @authors("babenko")
    def test_move(self):
        create("journal", "//tmp/j1")
        self._write_and_wait_until_sealed("//tmp/j1", PAYLOAD)

        move("//tmp/j1", "//tmp/j2")
        assert read_journal("//tmp/j2") == PAYLOAD

    @authors("babenko")
    def test_no_storage_change_after_creation(self):
        create(
            "journal",
            "//tmp/j",
            attributes={"replication_factor": 5, "read_quorum": 3, "write_quorum": 3},
        )
        with pytest.raises(YtError):
            set("//tmp/j/@replication_factor", 6)
        with pytest.raises(YtError):
            set("//tmp/j/@vital", False)
        with pytest.raises(YtError):
            set("//tmp/j/@primary_medium", "default")

    @authors("babenko")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    def test_read_write_unsealed(self, enable_chunk_preallocation):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)

        create("journal", "//tmp/j")
        write_journal(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=enable_chunk_preallocation,
            journal_writer={
                "dont_close": True,
                "max_batch_row_count": 4,
                "max_flush_row_count": 4,
                "max_chunk_row_count": 4,
            },
        )

        assert read_journal("//tmp/j") == PAYLOAD

    @authors("babenko")
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    @pytest.mark.parametrize("seal_mode", ["client-side", "master-side"])
    def test_simulated_failures(self, enable_chunk_preallocation, seal_mode):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", seal_mode == "master-side")
        set("//sys/@config/chunk_manager/chunk_refresh_period", 50)

        create("journal", "//tmp/j")

        rows = PAYLOAD * 100

        self._write_slowly(
            "//tmp/j",
            rows,
            enable_chunk_preallocation=enable_chunk_preallocation,
            journal_writer={
                "dont_close": seal_mode == "client-side",
                "dont_seal": seal_mode == "master-side",
                "max_batch_row_count": 9,
                "max_flush_row_count": 9,
                "max_chunk_row_count": 49,
                "replica_failure_probability": 0.1,
                "open_session_backoff_time": 100,
            },
        )

        assert get("//tmp/j/@quorum_row_count") == len(rows)
        assert read_journal("//tmp/j") == rows

        # If master-side chunk seal is disabled, some chunks can remain unsealed after write finish.
        if seal_mode == "master-side":
            self._wait_until_last_chunk_sealed("//tmp/j")
            assert read_journal("//tmp/j") == rows

    @authors("ifsmirnov")
    def test_data_node_orchid(self):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", PAYLOAD)
        chunk_id = get("//tmp/j/@chunk_ids/0")
        replica = get("#{}/@last_seen_replicas/0".format(chunk_id))
        orchid = get("//sys/cluster_nodes/{}/orchid/data_node/stored_chunks/{}".format(replica, chunk_id))
        assert "location" in orchid
        assert "disk_space" in orchid

    @authors("gritukan")
    def test_replica_lag_limit(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)
        set("//sys/@config/chunk_manager/chunk_refresh_period", 50)

        def _check(row_count,
                   max_batch_row_count,
                   max_flush_row_count,
                   replica_lag_limit,
                   replica_row_limits,
                   expected_replica_length):
            remove("//tmp/j", force=True)

            rf = len(replica_row_limits)
            rq = 2
            wq = rf - 1
            create("journal", "//tmp/j", attributes={
                "erasure_codec": "none",
                "replication_factor": rf,
                "read_quorum": rq,
                "write_quorum": wq,
            })

            rows = []
            for i in range(row_count):
                rows.append(PAYLOAD[i % len(PAYLOAD)])

            self._write_slowly(
                "//tmp/j",
                rows,
                enable_chunk_preallocation=True,
                replica_lag_limit=replica_lag_limit,
                journal_writer={
                    "dont_close": False,
                    "dont_seal": True,
                    "node_ban_timeout": 0,
                    "max_batch_row_count": max_batch_row_count,
                    "max_flush_row_count": max_flush_row_count,
                    "replica_row_limits": replica_row_limits,
                    "replica_fake_timeout_delay": 500,
                },
            )

            chunk_id = get("//tmp/j/@chunk_ids/0")
            replica_length = self._get_chunk_replica_length(chunk_id)
            # Discount header row.
            for i in range(len(replica_length)):
                replica_length[i] -= 1
            if expected_replica_length:
                assert sorted(replica_length) == sorted(expected_replica_length)

            set("//sys/@config/chunk_manager/enable_chunk_sealer", True)
            wait(lambda: get("#{}/@sealed".format(chunk_id)))
            set("//sys/@config/chunk_manager/enable_chunk_sealer", False)

        _check(
            row_count=10,
            max_batch_row_count=1,
            max_flush_row_count=1,
            replica_lag_limit=3,
            replica_row_limits=[1, 2, 3, 4, 5, 6],
            expected_replica_length=[1, 2, 3, 4, 4, 4],
        )
        _check(
            row_count=10,
            max_batch_row_count=1,
            max_flush_row_count=1,
            replica_lag_limit=100500,
            replica_row_limits=[1, 2, 3, 4, 5, 6],
            expected_replica_length=[1, 2, 3, 4, 5, 6],
        )
        _check(
            row_count=3,
            max_batch_row_count=1,
            max_flush_row_count=1,
            replica_lag_limit=1,
            replica_row_limits=[1, 2, 3],
            expected_replica_length=[1, 2, 2],
        )
        _check(
            row_count=12,
            max_batch_row_count=3,
            max_flush_row_count=3,
            replica_lag_limit=4,
            replica_row_limits=[10, 7, 4],
            expected_replica_length=[3, 6, 6],
        )
        _check(
            row_count=12,
            max_batch_row_count=2,
            max_flush_row_count=4,
            replica_lag_limit=4,
            replica_row_limits=[10, 7, 4],
            expected_replica_length=None,
        )

    @authors("gritukan")
    def test_replica_lag_limit_attribute(self):
        if self.Env.get_component_version("ytserver-master").abi <= (20, 3):
            pytest.skip("Replica lag limit is available in 21.1+ versions")

        create("journal", "//tmp/j")
        self._write_slowly(
            "//tmp/j",
            PAYLOAD,
            replica_lag_limit=123456,
        )

        chunk_id = get("//tmp/j/@chunk_ids/0")
        # Replica lag limit is rounded up to the nearest
        # power of two at master.
        assert get("#{}/@replica_lag_limit".format(chunk_id)) == 131072

    @authors("gritukan")
    def test_copy_journal(self):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", PAYLOAD)
        copy("//tmp/j", "//tmp/j2")
        self._check_journal_copy_attribute_correct("//tmp/j", "//tmp/j2")
        assert read_journal("//tmp/j2") == PAYLOAD

    @authors("gritukan")
    def test_unsealed_journal_copy_forbidden(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)

        create("journal", "//tmp/j")
        self._write_slowly(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=True,
            journal_writer={
                "dont_close": False,
                "dont_seal": True,
            })

        assert not get("//tmp/j/@sealed")
        with pytest.raises(YtError, match="Journal is not sealed"):
            copy("//tmp/j", "//tmp/j2")

    @authors("kvk1920")
    def test_data_weight_for_journals_absent(self):
        create("journal", "//tmp/journal_without_data_weight")
        assert not exists("//tmp/journal_without_data_weight/@data_weight")
        with raises_yt_error("Attribute \"data_weight\" is not found"):
            get("//tmp/journal_without_data_weight/@data_weight")


class TestJournalsMulticell(TestJournals):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestJournalsPortal(TestJournalsMulticell):
    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    @authors("gritukan", "danilalexeev")
    def test_copy_sealed_journal_cross_shard(self):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", PAYLOAD)
        copy("//tmp/j", "//portals/j")
        self._check_journal_copy_attribute_correct("//tmp/j", "//portals/j")
        assert read_journal("//portals/j") == PAYLOAD
        externalize("//portals", 11)
        assert read_journal("//portals/j") == PAYLOAD


class TestJournalsRpcProxy(TestJournals):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


##################################################################


class TestJournalsChangeMedia(TestJournalsBase):
    NUM_MASTERS = 3
    NUM_NODES = 5

    PAYLOAD = [{"payload": "payload" + str(i)} for i in range(0, 10)]

    @authors("ilpauzner", "shakurov")
    def test_journal_replica_changes_medium_yt_8669(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)

        create("journal", "//tmp/j1")
        write_journal("//tmp/j1", PAYLOAD, journal_writer={"dont_close": True})

        assert not get("//tmp/j1/@sealed")
        chunk_id = get_singular_chunk_id("//tmp/j1")
        assert not get("#{0}/@sealed".format(chunk_id))
        node_to_patch = str(get("#{0}/@stored_replicas".format(chunk_id))[0])

        with Restarter(self.Env, NODES_SERVICE):
            create_domestic_medium("ssd")

            patched_node_config = False
            for i in range(0, len(self.Env.configs["node"])):
                config = self.Env.configs["node"][i]

                node_address = "{0}:{1}".format(config["address_resolver"]["localhost_fqdn"], config["rpc_port"])

                if node_address == node_to_patch:
                    location = config["data_node"]["store_locations"][0]

                    assert "medium_name" not in location or location["medium_name"] == "default"
                    location["medium_name"] = "ssd"

                    config_path = self.Env.config_paths["node"][i]
                    with open(config_path, "wb") as fout:
                        yson.dump(config, fout)
                    patched_node_config = True
                    break

            assert patched_node_config

        set("//sys/@config/chunk_manager/enable_chunk_sealer", True)

        wait_until_sealed("//tmp/j1")

        def replicator_has_done_well():
            try:
                stored_replicas = get("#{0}/@stored_replicas".format(chunk_id))
                if len(stored_replicas) != 3:
                    return False

                for replica in stored_replicas:
                    if replica.attributes["medium"] != "default":
                        return False

                return True
            except YtError:
                return False

        wait(replicator_has_done_well)


##################################################################


class TestErasureJournals(TestJournalsBase):
    NUM_TEST_PARTITIONS = 12
    NUM_MASTERS = 3
    NUM_NODES = 20

    JOURNAL_ATTRIBUTES = {
        "none": {
            "erasure_codec": "none",
            "replication_factor": 3,
            "read_quorum": 2,
            "write_quorum": 2,
        },
        "isa_lrc_12_2_2": {
            "erasure_codec": "isa_lrc_12_2_2",
            "replication_factor": 1,
            "read_quorum": 14,
            "write_quorum": 15,
        },
        "isa_reed_solomon_3_3": {
            "erasure_codec": "isa_reed_solomon_3_3",
            "replication_factor": 1,
            "read_quorum": 4,
            "write_quorum": 5,
        },
        "isa_reed_solomon_6_3": {
            "erasure_codec": "isa_reed_solomon_6_3",
            "replication_factor": 1,
            "read_quorum": 7,
            "write_quorum": 8,
        }
    }

    def _check_repair_jobs(self, path, rows):
        chunk_ids = get("{}/@chunk_ids".format(path))
        replica_count = len(get("#{}/@stored_replicas".format(chunk_ids[0])))

        def _check_all_replicas_ok():
            for chunk_id in chunk_ids:
                replicas = get("#{}/@stored_replicas".format(chunk_id))
                if len(replicas) != replica_count:
                    return False
                if not all(r.attributes["state"] == "sealed" for r in replicas):
                    return False
            return True

        for i in range(3):
            wait(_check_all_replicas_ok)

            chunk_id = random.choice(chunk_ids)
            replicas = get("#{}/@stored_replicas".format(chunk_id))
            random.shuffle(replicas)
            nodes_to_ban = [str(x) for x in replicas[:3]]

            set_nodes_banned(nodes_to_ban, True)

            wait(_check_all_replicas_ok)

            assert read_journal("//tmp/j") == rows

            set_nodes_banned(nodes_to_ban, False)

    @pytest.mark.parametrize(
        "erasure_codec",
        ["none", "isa_lrc_12_2_2", "isa_reed_solomon_3_3", "isa_reed_solomon_6_3"])
    @authors("babenko", "ignat")
    def test_seal_abruptly_closed_journal(self, erasure_codec):
        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES[erasure_codec])
        N = 3
        for i in range(N):
            self._write_and_wait_until_sealed("//tmp/j", PAYLOAD, journal_writer={"dont_close": True})
            self._wait_until_last_chunk_sealed("//tmp/j")

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == len(PAYLOAD) * N
        assert get("//tmp/j/@chunk_count") == N
        assert read_journal("//tmp/j") == PAYLOAD * N

    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "isa_reed_solomon_3_3", "isa_reed_solomon_6_3"])
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    @pytest.mark.timeout(180)
    @authors("babenko")
    def test_repair_jobs(self, erasure_codec, enable_chunk_preallocation):
        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES[erasure_codec])
        write_journal("//tmp/j", PAYLOAD, enable_chunk_preallocation=enable_chunk_preallocation)

        self._check_repair_jobs("//tmp/j", PAYLOAD)

    def _test_critical_erasure_state(self, state, n):
        set("//sys/@config/chunk_manager/enable_chunk_replicator", False)
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)

        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES["isa_lrc_12_2_2"])
        write_journal("//tmp/j", PAYLOAD, journal_writer={"dont_close": True})

        chunk_ids = get("//tmp/j/@chunk_ids")
        chunk_id = chunk_ids[-1]
        check_path = "//sys/{}_chunks/{}".format(state, chunk_id)

        assert not exists(check_path)
        replicas = get("#{}/@stored_replicas".format(chunk_id))
        set_nodes_banned(replicas[:n], True)
        wait(lambda: exists(check_path))
        set_nodes_banned(replicas[:n], False)
        wait(lambda: not exists(check_path))

    @authors("babenko")
    def test_erasure_quorum_missing(self):
        self._test_critical_erasure_state("quorum_missing", 3)

    @authors("babenko")
    def test_erasure_lost(self):
        self._test_critical_erasure_state("lost_vital", 5)

    @pytest.mark.parametrize(
        "erasure_codec",
        ["none", "isa_lrc_12_2_2", "isa_reed_solomon_3_3", "isa_reed_solomon_6_3"])
    @pytest.mark.parametrize("enable_chunk_preallocation", [False, True])
    @pytest.mark.timeout(300)
    @pytest.mark.skipif(is_asan_build(), reason="Test is too slow to fit into timeout")
    @authors("babenko", "ignat")
    def test_read_with_repair(self, erasure_codec, enable_chunk_preallocation):
        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES[erasure_codec])
        self._write_and_wait_until_sealed("//tmp/j", PAYLOAD, enable_chunk_preallocation=enable_chunk_preallocation)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == len(PAYLOAD)
        if not enable_chunk_preallocation:
            assert get("//tmp/j/@chunk_count") == 1

        def check():
            for i in range(0, len(PAYLOAD)):
                assert read_journal("//tmp/j[#" + str(i) + ":#" + str(i + 1) + "]") == [PAYLOAD[i]]
            for i in range(0, len(PAYLOAD)):
                assert read_journal("//tmp/j[#" + str(i) + ":]") == PAYLOAD[i:]
            for i in range(0, len(PAYLOAD)):
                assert read_journal("//tmp/j[:#" + str(i) + "]") == PAYLOAD[:i]

        check()

        chunk_ids = get("//tmp/j/@chunk_ids")
        chunk_id = chunk_ids[-1]
        replicas = get("#{}/@stored_replicas".format(chunk_id))
        for replica in replicas:
            set_node_banned(replica, True)
            check()
            set_node_banned(replica, False)

    @authors("gritukan", "babenko")
    @pytest.mark.timeout(180)
    def test_repair_overlayed_chunk(self):
        set("//sys/@config/chunk_manager/chunk_refresh_period", 50)

        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES["isa_reed_solomon_3_3"])

        rows = PAYLOAD * 20

        self._write_slowly(
            "//tmp/j",
            rows,
            enable_chunk_preallocation=True,
            journal_writer={
                "dont_seal": True,
                "max_batch_row_count": 10,
                "max_flush_row_count": 10,
                "max_chunk_row_count": 50,
                "replica_failure_probability": 0.1,
                "open_session_backoff_time": 100,
            },
        )
        self._wait_until_last_chunk_sealed("//tmp/j")

        self._check_repair_jobs("//tmp/j", rows)


class TestErasureJournalsRpcProxy(TestErasureJournals):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

##################################################################


class TestChunkAutotomizer(TestJournalsBase):
    NUM_TEST_PARTITIONS = 3
    NUM_MASTERS = 1
    NUM_NODES = 20

    def _write_simple_journal(self):
        write_journal(
            "//tmp/j",
            PAYLOAD,
            enable_chunk_preallocation=True,
            replica_lag_limit=4,
            journal_writer={
                "dont_close": True,
                "dont_seal": True,
                "dont_preallocate": True,
                "max_batch_row_count": 4,
            },
        )

    def _check_simple_journal(self):
        chunk_ids = get("//tmp/j/@chunk_ids")
        if len(chunk_ids) == 1:
            return False

        for chunk_id in get("//tmp/j/@chunk_ids"):
            if not get("#{}/@sealed".format(chunk_id)):
                return False

        assert len(chunk_ids) == 2
        assert get("#{}/@row_count".format(chunk_ids[0])) == 6
        assert get("#{}/@row_count".format(chunk_ids[1])) == 4

        assert read_journal("//tmp/j") == PAYLOAD
        return True

    def _set_fail_jobs(self, fail_jobs):
        update_nodes_dynamic_config({
            "data_node": {
                "autotomize_chunk_job": {
                    "fail_jobs": fail_jobs,
                },
            }
        })

    def _set_sleep_in_jobs(self, sleep_in_jobs):
        update_nodes_dynamic_config({
            "data_node": {
                "autotomize_chunk_job": {
                    "sleep_in_jobs": sleep_in_jobs,
                },
            }
        })

    @authors("gritukan")
    def test_simple(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)
        set("//sys/@config/chunk_manager/chunk_refresh_period", 50)

        create("journal", "//tmp/j", attributes={
            "erasure_codec": "none",
            "replication_factor": 5,
            "read_quorum": 3,
            "write_quorum": 3,
        })

        success_counters = get_chunk_owner_master_cell_counters("//tmp/j", "chunk_server/chunk_autotomizer/successful_autotomies")

        rows = PAYLOAD

        write_journal(
            "//tmp/j",
            rows,
            enable_chunk_preallocation=True,
            replica_lag_limit=8,
            journal_writer={
                "dont_close": False,
                "dont_seal": True,
                "dont_preallocate": True,
                "node_ban_timeout": 0,
                "max_batch_row_count": 1,
                "max_flush_row_count": 1,
                "replica_row_limits": [6, 7, 8, 9, 10],
                "replica_fake_timeout_delay": 500,
            },
        )

        body_chunk_id = get("//tmp/j/@chunk_ids/0")
        replica = self._find_replicas_with_length(body_chunk_id, 7)[0]

        set_node_banned(replica, True)

        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/enable_chunk_sealer", True)

        def check():
            for chunk_id in get("//tmp/j/@chunk_ids"):
                if not get("#{}/@sealed".format(chunk_id)):
                    return False
            return True

        wait(lambda: check())
        wait(lambda: sum(counter.get_delta() for counter in success_counters) == 1)
        assert 3 <= len(get("//tmp/j/@chunk_ids")) <= 4
        self._wait_until_last_chunk_sealed("//tmp/j")
        assert read_journal("//tmp/j") == rows

    @pytest.mark.parametrize("erasure_codec", ["none", "isa_lrc_12_2_2", "isa_reed_solomon_3_3", "isa_reed_solomon_6_3"])
    @authors("gritukan")
    def test_erasure(self, erasure_codec):
        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", True)

        create("journal", "//tmp/j", attributes=ERASURE_JOURNAL_ATTRIBUTES[erasure_codec])

        rows = PAYLOAD
        write_journal(
            "//tmp/j",
            rows,
            enable_chunk_preallocation=True,
            replica_lag_limit=4,
            journal_writer={
                "dont_close": True,
                "dont_seal": True,
                "dont_preallocate": True,
                "max_batch_row_count": 4,
            },
        )

        wait(lambda: self._check_simple_journal())

    @authors("gritukan")
    def test_job_failure(self):
        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", True)

        create("journal", "//tmp/j")

        fail_counters = get_chunk_owner_master_cell_counters("//tmp/j", "chunk_server/jobs_failed", tags={"job_type": "autotomize_chunk"})

        self._set_fail_jobs(True)
        self._write_simple_journal()

        wait(lambda: sum(counter.get_delta() for counter in fail_counters) > 5)

        registered_gauges = get_chunk_owner_master_cell_gauges("//tmp/j", "chunk_server/chunk_autotomizer/registered_chunks")
        assert sum(gauge.get() for gauge in registered_gauges) == 1

        assert len(get("//tmp/j/@chunk_ids")) == 1

        self._set_fail_jobs(False)
        wait(lambda: self._check_simple_journal())

    @authors("gritukan")
    @pytest.mark.skipif("True", reason="Flaky")
    def test_job_speculation(self):
        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", True)
        set("//sys/@config/chunk_manager/chunk_autotomizer/job_timeout", 5000)

        create("journal", "//tmp/j")

        win_counters = get_chunk_owner_master_cell_counters("//tmp/j", "chunk_server/chunk_autotomizer/speculative_job_wins")
        loss_counters = get_chunk_owner_master_cell_counters("//tmp/j", "chunk_server/chunk_autotomizer/speculative_job_wins")
        speculative_counters = win_counters + loss_counters

        self._set_sleep_in_jobs(True)
        self._write_simple_journal()

        sleep(5)
        assert len(get("//tmp/j/@chunk_ids")) == 1
        self._set_sleep_in_jobs(False)

        wait(lambda: self._check_simple_journal())
        assert sum(counter.get_delta() for counter in speculative_counters) > 0

    @authors("gritukan")
    @pytest.mark.parametrize("build_snapshot", [False, True])
    def test_master_restart(self, build_snapshot):
        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", True)

        create("journal", "//tmp/j")

        self._set_fail_jobs(True)
        self._write_simple_journal()

        if build_snapshot:
            build_master_snapshots(set_read_only=True)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        master_exit_read_only_sync()

        self._set_fail_jobs(False)
        wait(lambda: self._check_simple_journal(), ignore_exceptions=True)

    @authors("gritukan")
    def test_abandon_autotomy(self):
        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", True)

        create("journal", "//tmp/j")

        unsuccess_counters = get_chunk_owner_master_cell_counters("//tmp/j", "chunk_server/chunk_autotomizer/unsuccessful_autotomies")

        self._set_fail_jobs(True)
        self._write_simple_journal()

        sleep(10)
        assert len(get("//tmp/j/@chunk_ids")) == 1
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", False)

        chunk_id = get_singular_chunk_id("//tmp/j")
        wait(lambda: get("#{}/@sealed".format(chunk_id)))
        assert read_journal("//tmp/j") == PAYLOAD
        wait(lambda: sum(counter.get_delta() for counter in unsuccess_counters) == 1)

    @authors("gritukan")
    def test_remove_autotomizable_chunk(self):
        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", True)

        create("journal", "//tmp/j")

        registered_gauges = get_chunk_owner_master_cell_gauges("//tmp/j", "chunk_server/chunk_autotomizer/registered_chunks")

        self._set_fail_jobs(True)
        self._write_simple_journal()

        sleep(5)
        assert len(get("//tmp/j/@chunk_ids")) == 1

        remove("//tmp/j")
        wait(lambda: sum(gauge.get() for gauge in registered_gauges) == 0)

    @authors("gritukan")
    @pytest.mark.parametrize("action", ["abort", "commit"])
    def test_finish_autotomizer_transactions(self, action):
        def finish_txs():
            for tx in ls("//sys/transactions", attributes=["title"]):
                title = tx.attributes.get("title", "")
                if title == "Chunk autotomizer transaction":
                    if action == "abort":
                        abort_transaction(tx)
                    else:
                        commit_transaction(tx)

        set("//sys/@config/chunk_manager/enable_chunk_autotomizer", True)
        set("//sys/@config/chunk_manager/testing/force_unreliable_seal", True)

        create("journal", "//tmp/j")

        self._set_fail_jobs(True)
        self._write_simple_journal()

        for _ in range(10):
            finish_txs()

        self._set_fail_jobs(False)

        for _ in range(10):
            finish_txs()

        wait(lambda: self._check_simple_journal())


class TestChunkAutotomizerMulticell(TestChunkAutotomizer):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestChunkAutotomizerPortal(TestChunkAutotomizerMulticell):
    ENABLE_TMP_PORTAL = True
