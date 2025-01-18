from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any
from typing import ClassVar

from library.python import confmerge
from yt.yt_sync.core.constants import KB
from yt.yt_sync.core.constants import MB


@dataclass
class Settings:
    REPLICATED_DB: ClassVar[str] = "replicated"
    CHAOS_DB: ClassVar[str] = "chaos"
    DEFAULT_MAP_SPEC: ClassVar[dict[str, Any]] = {
        "weight": 999,
    }
    DEFAULT_SORT_SPEC: ClassVar[dict[str, Any]] = {
        "weight": 999,
        "merge_job_io": {"table_writer": {"block_size": 256 * KB, "desired_chunk_size": 100 * MB}},  # YTADMINREQ-25182
    }
    DEFAULT_REMOTE_COPY_SPEC: ClassVar[dict[str, Any]] = {
        "scheduling_options_per_pool_tree": {
            "physical": {
                "resource_limits": {
                    "user_slots": 1000,
                },
            },
        },
    }

    db_type: str
    """Type of DB: chaos or replicated (standalone=replicated)."""

    min_sync_clusters: int = 1
    """
    Minimal count for sync replicas.
    Used when switching replica clusters mode in ensure scenario.
    When DB has only async replicas - set value to 0.
    """

    always_async: set[str] = dataclass_field(default_factory=set)
    """Clusters that never should be switched to sync mode (aka always async replicas)."""

    switch_replicas_mode_always: bool = False
    """Switch replicas to async mode before unmount tables in ensure/reshard scenario."""

    switch_replicas_mode_for_data_modification: bool = True
    """Switch replicas to async mode before unmount tables tables for heavy modifications only in ensure scenario."""

    wait_in_sync_replicas: bool = True
    """Wait replicas in sync when switching cluster mode before other actions."""

    ensure_folders: bool = False
    """Create YT folders before running scenario. Works for ensure and clean scenarios."""

    managed_roots: dict[str, list[str]] = dataclass_field(default_factory=dict)
    """
    Lists of managed roots on clusters. Is used in dump_diff and drop_unmanaged scenarios.
    Roots are acquired by cluster name or from 'default' if cluster name is not present.
    """

    parallel_processing_for_unmounted: bool = False
    """
    Process tables to be unmounted simultaneously on cluster.
    Use with care, because all tables will be unmounted/processed at the same time - it can lead to massive downtime.
    Works for ensure/reshard scenario.
    """

    batch_size_for_parallel: dict[str, int] = dataclass_field(default_factory=dict)
    """
    Set batch size for parallel table processing for each scenario (only for parallel_processing_for_unmounted=True).
    If no explicit scenario settings given then value is taken from "default" scenario if given or 0.
    batch_size=0 means unlimited batch size.
    batch_size=1 means sequential execution (aka parallel_processing_for_unmounted=False).
    """

    use_fake_actual_state_builder: bool = False
    """
    Skip building actual DB state.
    Actual DB is copy of desired with all tables treated as non-existent.
    Use in tests only.
    """

    operations_spec: dict[str, dict] = dataclass_field(default_factory=dict)
    """
    Operation spec per cluster like map/sort/remote_copy.
    Supported operations are 'map', 'sort', 'remote_copy'.
    Operation spec should be retrieved via get_operation_spec() method.

    If no explicit cluster/operation spec is given then 'default' fallback is used to search suitable spec in order:
        1. operations_spec[cluster][operation]
        2. operations_spec["default"][operation]
        3. operations_spec[cluster]["default"]
        4. operations_spec["default"]["default"]
    'remote_copy' operation is exception though: if no explicit spec for given or 'default' cluster is given then
    DEFAULT_REMOTE_COPY_SPEC is used.

    Spec example:
    operations_spec = {
        "default": { "default": { ... }, "remote_copy": { ... }}  # default specs
        "hahn: { "default": { .. }, "map": { ... } }  # hahn operations spec
        "seneca-sas": { "sort": { ... } },  # seneca-sas operations spec
    }
    """

    ensure_collocation: bool = False
    """Ensure collocation at the end of clean/ensure/reshard scenarios."""

    collocation_name: str | None = None
    """Replication collocation name, must be not empty if ensure_collocation is True."""

    chaos_collocation_allow_batched: bool = False
    """Use batch operations for collocation over chaos tables. Use in tests only."""

    max_operation_action_duration: int = 7200  # 2hrs
    """
    Max duration of long actions (like TransformTableSchemaAction or RemoteCopyAction).
    Limit for all operations duration in action, including map and sort.
    Duration is in seconds.
    """

    wait_between_replica_switch_delay: int = 0
    """Wait before next replica cluster processing in ensure scenarios (seconds)."""

    wait_in_memory_preload: bool = True
    """Wait until in-memory tables preloaded before processing next cluster in ensure/reshard scenarios."""

    allow_table_full_downtime: bool = True
    """
    Allows schema changes leading to full table downtime (like simultaneous add and remove columns to table).
    """

    ensure_tables_mounted: bool = True
    """
    Move all unmounted/freezed tables to mounted state before running ensure scenario.
    """

    use_deprecated_spec_format: bool = True
    """Accept yt_sync v1 compatible specs in YtSync.add_desired* methods."""

    ensure_rtt_settings: bool = True
    """Check RTT settings integrity for DB. Temporary option."""

    always_log_full_diff: bool = False
    """Log full diff for every scenario, not only dump_diff."""

    fix_implicit_replicated_queue_attrs: bool = True
    """Implicitly add `preserve_table_index = true` and `commit_ordering = strong` to replicated queues."""

    max_actual_state_builder_workers: int = 8
    """To build actual state in parallel for clusters use value more than 1."""

    @classmethod
    def make(cls, cluster_settings: dict[str, Any]) -> Settings:
        """Cluster settings are in GrUT format, see grut/tools/admin/settings/clusters.py for example."""
        is_chaos = cluster_settings.get("use_chaos", False)
        rtt_enabled = cluster_settings.get("enable_replicated_table_tracker", True)
        always_async: set[str] = set()
        for async_replica in cluster_settings.get("async_replicas", list()):
            replica_settings = confmerge.get_section(cluster_settings.get("attributes", {}), async_replica)
            if "enable_replicated_table_tracker" in replica_settings:
                if rtt_enabled and not replica_settings["enable_replicated_table_tracker"]:
                    always_async.add(async_replica)
        return cls(db_type=cls.CHAOS_DB if is_chaos else cls.REPLICATED_DB, always_async=always_async)

    @property
    def is_chaos(self) -> bool:
        return self.CHAOS_DB == self.db_type

    def is_ok(self):
        if self.ensure_collocation:
            assert self.collocation_name, "Collocation name can't be empty with ensure_collocation=True"

    def get_operation_spec(self, cluster: str, operation: str) -> dict[str, Any] | None:
        assert operation in ("map", "sort", "remote_copy"), f"Unknown operation {operation}"
        default_cluster_spec = self.operations_spec.get("default", dict())
        cluster_spec: dict[str, Any] = self.operations_spec.get(cluster, dict())

        if operation in cluster_spec:
            return cluster_spec[operation]
        if operation in default_cluster_spec:
            return default_cluster_spec[operation]
        if operation == "map":
            return self.DEFAULT_MAP_SPEC
        elif operation == "sort":
            return self.DEFAULT_SORT_SPEC
        elif operation == "remote_copy":
            return self.DEFAULT_REMOTE_COPY_SPEC

    def get_batch_size_for_parallel(self, scenario: str) -> int:
        return self.batch_size_for_parallel.get(scenario, self.batch_size_for_parallel.get("default", 0))

    def get_managed_roots_for(self, cluster_name: str) -> list[str]:
        return self.managed_roots.get(cluster_name, self.managed_roots.get("default", list()))
