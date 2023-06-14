from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_commands import (
    authors, create, remove, get, ls, set, wait, write_table, print_debug,
    switch_leader, is_active_primary_master_leader, is_active_primary_master_follower,
    get_active_primary_master_leader_address, get_active_primary_master_follower_address,
    raises_yt_error)

from yt_helpers import profiler_factory

from yt.common import YtError

import pytest

from datetime import datetime, timedelta
from time import sleep


##################################################################


def parse_time(t):
    return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000


##################################################################


class TestMasterCellChunkStatisticsCollector(YTEnvSetup):
    NUM_MASTERS = 5
    NUM_NODES = 3

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "master_cell_chunk_statistics_collector": {
                "chunk_scan_period": 250,
                "max_skipped_chunks_per_scan": 10,
                "max_visited_chunk_lists_per_scan": 500,
            }
        },
        "cell_master": {
            "mutation_time_commit_period": 150,
        }
    }

    def _switch_leader(self):
        while True:
            try:
                old_leader_rpc_address = get_active_primary_master_leader_address(self)
                new_leader_rpc_address = get_active_primary_master_follower_address(self)
                cell_id = get("//sys/@cell_id")
                switch_leader(cell_id, new_leader_rpc_address)
                wait(lambda: is_active_primary_master_leader(new_leader_rpc_address))
                wait(lambda: is_active_primary_master_follower(old_leader_rpc_address))
                return
            except YtError as ex:
                if ex.contains_text("Peer is not leading"):
                    continue
                raise

    def _check_histogram(self):
        bounds = sorted(map(parse_time, get("//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/creation_time_histogram_bucket_bounds")))
        chunks = ls("//sys/chunks", attributes=["estimated_creation_time"])

        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)
        histogram = [
            bin["count"]
            for bin in profiler.histogram("chunk_server/histograms/chunk_creation_time_histogram").get_bins()
        ]

        true_histogram = [0] * (len(bounds) + 1)
        verbose_true_histogram = [[] for _ in range(len(bounds) + 1)]
        for chunk in chunks:
            creation_time = parse_time(chunk.attributes["estimated_creation_time"]["min"])
            bin_index = 0
            while bin_index < len(bounds) and creation_time >= bounds[bin_index]:
                bin_index += 1
            true_histogram[bin_index] += 1
            verbose_true_histogram[bin_index].append(str(chunk))

        if histogram != true_histogram:
            print_debug(f"actual:   {histogram}")
            print_debug(f"expected: {true_histogram}")
            print_debug(f"verbose:  {verbose_true_histogram}")

        return histogram == true_histogram

    @authors("kvk1920")
    def test_empty_histogram_bounds(self):
        path = "//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/creation_time_histogram_bucket_bounds"
        set(path, [])
        assert len(get(path)) == 1

    @authors("kvk1920")
    @pytest.mark.parametrize("inject_leader_switch", [False, True])
    def test_chunk_creation_time_histogram(self, inject_leader_switch):
        create("table", "//tmp/t")
        create("table", "//tmp/t2")

        for _ in range(10):
            write_table("<append=%true>//tmp/t", {"a": "b"})
            sleep(1)

        if inject_leader_switch:
            self._switch_leader()

        write_table("//tmp/t2", {"a": "b"})

        now = datetime.utcnow()
        set(
            "//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/creation_time_histogram_bucket_bounds",
            [str(now - timedelta(seconds=.5 * i)) for i in range(20)])

        write_table("<append=%true>//tmp/t", {"a": "b"})
        remove("//tmp/t2")

        if inject_leader_switch:
            self._switch_leader()

        wait(self._check_histogram)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        wait(self._check_histogram)

    @authors("kvk1920")
    def test_empty_bounds(self):
        with raises_yt_error("cannot be empty"):
            set(
                "//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/creation_time_histogram_bucket_bounds",
                [])
