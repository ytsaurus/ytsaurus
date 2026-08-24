from yt.admin.bundle_controller import (
    CpuLimits,
    MemoryLimits,
    guess_default_config,
    pretty_bytes,
    run_bundle_controller_set_resource_limits,
)

import pytest


ZONE_RESOURCE_GUARANTEE_PATH = \
    "//sys/bundle_controller/controller/zones/zone_default/@tablet_node_sizes/regular/resource_guarantee"


class TestPrettyBytes:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (0, "0"),
            (1023, "1023"),
            (2**10, "1 KB"),
            (1536, "1.5 KB"),
            (2**20, "1 MB"),
            (2**30, "1 GB"),
            (3 * 2**30, "3 GB"),
        ],
    )
    def test_pretty_bytes(self, value, expected):
        assert pretty_bytes(value) == expected


class TestGuessDefaultConfig:
    @pytest.mark.parametrize(
        "cpu, expected_cpu_limits",
        [
            (1, CpuLimits(1, 1, 1)),
            (4, CpuLimits(1, 1, 1)),
            (5, CpuLimits(5, 2, 2)),
            (10, CpuLimits(5, 2, 2)),
            (11, CpuLimits(10, 6, 6)),
            (64, CpuLimits(10, 6, 6)),
        ],
    )
    def test_cpu_thresholds(self, cpu, expected_cpu_limits):
        cpu_limits, _ = guess_default_config(cpu, 64 * 2**30)
        assert cpu_limits == expected_cpu_limits

    def test_memory_limits_are_consistent(self):
        memory = 64 * 2**30
        _, memory_limits = guess_default_config(16, memory)
        assert isinstance(memory_limits, MemoryLimits)
        assert memory_limits.tablet_static >= 0
        total = (
            memory_limits.tablet_dynamic
            + memory_limits.tablet_static
            + memory_limits.compressed_block_cache
            + memory_limits.uncompressed_block_cache
            + memory_limits.versioned_chunk_meta
            + memory_limits.lookup_row_cache
            + memory_limits.reserved
        )
        assert total == memory


class _RecordingClient:
    def __init__(self, values):
        self._values = values
        self.mutations = []

    def get(self, path, *args, **kwargs):
        return self._values[path]

    def set(self, path, value, *args, **kwargs):
        self.mutations.append(("set", path, value))

    def remove(self, path, *args, **kwargs):
        self.mutations.append(("remove", path))

    def create(self, type, path=None, *args, **kwargs):
        self.mutations.append(("create", type, path))


class TestDryRun:
    def test_set_resource_limits_dry_run_does_not_mutate(self):
        client = _RecordingClient({
            ZONE_RESOURCE_GUARANTEE_PATH: {"vcpu": 8000, "memory": 2**30, "net_bytes": 0},
        })

        run_bundle_controller_set_resource_limits(
            bundle_name="sys",
            node_count=3,
            dry_run=True,
            yes=True,
            client=client,
        )

        assert client.mutations == []

    def test_set_resource_limits_applies_expected_values(self):
        client = _RecordingClient({
            ZONE_RESOURCE_GUARANTEE_PATH: {"vcpu": 8000, "memory": 2**30, "net_bytes": 0},
        })

        run_bundle_controller_set_resource_limits(
            bundle_name="sys",
            node_count=3,
            dry_run=False,
            yes=True,
            client=client,
        )

        assert client.mutations == [
            ("set", "//sys/tablet_cell_bundles/sys/@resource_limits/cpu", 3 * 8),
            ("set", "//sys/tablet_cell_bundles/sys/@resource_limits/memory", 3 * 2**30),
        ]
