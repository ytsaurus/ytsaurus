from yt_env_setup import NODES_SERVICE, Restarter, YTEnvSetup

from yt_commands import (
    authors,
    create,
    get,
    print_debug,
    update_nodes_dynamic_config,
    wait,
    write_table,
)

import pytest
import errno
import os
import tempfile
import json


def format_bytes(value, per_second=False):
    for divisor, suffix in ((1024 * 1024, "mb"), (1024, "kb"), (1, "b")):
        if abs(value) >= divisor or divisor == 1:
            formatted_value = f"{value / divisor:.2f}".rstrip("0").rstrip(".")
            rate_suffix = "/s" if per_second else ""
            return f"{formatted_value}{suffix}{rate_suffix}"


def try_direct_io(env):
    if not hasattr(os, "O_DIRECT"):
        return False

    for node_config in env.configs["node"]:
        for location in node_config["data_node"]["store_locations"]:
            with tempfile.NamedTemporaryFile(dir=location["path"]) as probe_file:
                try:
                    probe_fd = os.open(probe_file.name, os.O_WRONLY | os.O_DIRECT)
                except OSError as error:
                    if error.errno in (errno.EINVAL, errno.EOPNOTSUPP):
                        return False
                    raise
                else:
                    os.close(probe_fd)
    return True


@authors("depression")
class TestDataNodeIOThroughputMeter(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 0

    @authors("depression")
    @pytest.mark.timeout(240)
    @pytest.mark.parametrize("io_stats_update", [1, 1000])
    def test_meter_with_empty_workload(self, io_stats_update):
        use_direct_io = try_direct_io(self.Env)
        use_device_stats = use_direct_io
        capacity_keys = ("disk_read_capacity", "disk_write_capacity")
        reasonable_min_capacity = 400 * 1024  # even in test env, we should expect at least 400 kb/s
        meter_config = {
            "enabled": False,
            "time_between_tests": 1000,
            "estimate_time_limit": 10000,
            "max_estimate_congestions": 0,
            "use_device_io_stats": use_device_stats,
            "media": [{
                "medium_name": "default",
                "enabled": True,
                "packet_size": 4096,
                "default_read_to_write_ratio": 75,
                "reader_count": 2,
                "writer_count": 2,
                "max_write_file_size": 32 * 1024 * 1024,  # 32 mb
                "wait_after_congested": 100,
                "segment_size": 5,
                "window_period": 100,
                "use_direct_io": use_direct_io,
                "verification_window_period": 100,
                "congestion_detector": {"probes_interval": 50},
            }],
        }

        update_nodes_dynamic_config({
            "data_node": {
                "io_statistics_update_timeout": io_stats_update,
                "store_location_config_per_medium": {
                    "default": {
                        "io_config": {
                            "enable_sync": False,
                            "use_direct_io_for_reads": "always" if use_direct_io else "never",
                            "use_direct_io_for_writes": "always" if use_direct_io else "never",
                        },
                    },
                },
                "io_throughput_meter": meter_config,
            },
        })
        # Restart the node to reset the previously measured capacity.
        with Restarter(self.Env, NODES_SERVICE):
            pass
        wait(lambda: all(get("//sys/cluster_nodes/@io_statistics").get(key) == 0 for key in capacity_keys))

        # Seed chunks that the meter can read, then leave the node idle while
        # the meter runs without any concurrent workload.
        def create_chunk(id):
            create(
                "table",
                f"//tmp/meter_empty_workload_source{id}",
                attributes={"compression_codec": "none", "replication_factor": 1},
            )
            write_table(
                f"//tmp/meter_empty_workload_source{id}",
                [{"payload": "x" * 4096}],
                table_writer={"use_direct_io": use_direct_io, "sync_on_close": True},
            )
        for i in range(3):
            create_chunk(i)

        meter_config["enabled"] = True
        update_nodes_dynamic_config({
            "data_node": {
                "io_throughput_meter": meter_config,
            },
        })

        io_statistics = {}

        def has_positive_capacity():
            io_statistics.update(get("//sys/cluster_nodes/@io_statistics"))
            return all(io_statistics[key] > 0 for key in capacity_keys)

        wait(
            has_positive_capacity,
            timeout=60,
        )

        print_debug("I/O throughput meter result with no concurrent workload:")
        print_debug(json.dumps({
            "io_stats_update": io_stats_update,
            "use_device_stats": use_device_stats,
            "io_statistics": {
                key: format_bytes(value, per_second=True)
                for key, value in io_statistics.items()
            },
        }, indent=2, sort_keys=True))
        assert all(io_statistics[key] > reasonable_min_capacity for key in capacity_keys)
