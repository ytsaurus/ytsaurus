from dataclasses import dataclass
import logging

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.action import WaitInMemoryPreloadAction
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

LOG = logging.getLogger("yt_sync")


@dataclass
class CheckDiffResult:
    has_diff: bool
    is_valid: bool


def should_switch_replicas(settings: Settings, is_unmount_required: bool, is_data_modification_required: bool) -> bool:
    if settings.is_chaos:
        return is_unmount_required or is_data_modification_required

    return (is_unmount_required and settings.switch_replicas_mode_always) or (
        is_data_modification_required and settings.switch_replicas_mode_for_data_modification
    )


def generate_wait_until_in_memory_preloaded(
    settings: Settings, cluster: YtCluster, unmounted_tables: set[str]
) -> list[ActionBatch]:
    if not settings.wait_in_memory_preload or not unmounted_tables:
        return []
    action_collector = TableActionCollector(cluster.name)
    for table_key in unmounted_tables:
        action_collector.add(table_key, WaitInMemoryPreloadAction(cluster.tables[table_key]))
    return action_collector.dump()


def check_and_log_db_diff(
    settings: Settings,
    db: YtDatabase,
    diff: DbDiff,
    diff_types: set[int] | None = None,
    add_prompt: bool = False,
) -> CheckDiffResult:
    if settings.always_log_full_diff:
        diff_types = None
        add_prompt = True
    result = CheckDiffResult(has_diff=False, is_valid=True)

    for cluster in db.clusters.values():
        for node_path in sorted(cluster.nodes):
            node_diffs = diff.nodes_diff.get((cluster.name, node_path))
            if not node_diffs:
                LOG.debug("No diff for node %s:%s", cluster.name, node_path)
                continue

            first_iteration = True
            for diff_item in node_diffs:
                if diff_types and diff_item.diff_type not in diff_types:
                    continue

                if first_iteration:
                    LOG.warning("Has diff for node %s:%s, see details below:", cluster.name, node_path)
                    first_iteration = False

                result.is_valid &= diff_item.check_and_log(LOG)
                result.has_diff = True

    main_cluster: YtCluster = db.main
    mountable_cluster_names: list[str] = [c.name for c in db.mountable_clusters]
    unmount_required: bool = False
    reshard_required: bool = False
    sync_replicas_required: bool = False
    for table_key in sorted(main_cluster.tables):
        table_diff = diff.tables_diff.get(table_key)
        if not table_diff:
            LOG.info("No diff for table %s", main_cluster.tables[table_key].path)
            continue

        first_iteration = True
        clusters_to_unmount: set[str] = set()
        clusters_to_remount: set[str] = set()
        for diff_item in table_diff:
            if diff_types and diff_item.diff_type not in diff_types:
                continue
            if first_iteration:
                LOG.warning(
                    "Has diff for table %s, see details below:",
                    main_cluster.tables[table_key].path,
                )
                first_iteration = False
            result.is_valid &= diff_item.check_and_log(LOG)
            if diff_item.diff_type == TableDiffType.TABLET_COUNT_CHANGE:
                reshard_required = True
            elif diff_item.diff_type == TableDiffType.REPLICAS_CHANGE:
                sync_replicas_required = True
            else:
                result.has_diff = True
                for cluster in mountable_cluster_names:
                    if diff_item.is_unmount_required(cluster):
                        clusters_to_unmount.add(cluster)
                        unmount_required = True
                    elif diff_item.is_remount_required(cluster):
                        clusters_to_remount.add(cluster)
        if clusters_to_unmount:
            LOG.warning("  will be UNMOUNTED and MOUNTED on %s", sorted(clusters_to_unmount))
        clusters_to_remount.difference_update(clusters_to_unmount)
        if clusters_to_remount:
            LOG.warning("  will be REMOUNTED on %s", sorted(clusters_to_remount))

    if result.has_diff or reshard_required or sync_replicas_required:
        if add_prompt and result.is_valid:
            if result.has_diff:
                data_modification_required = diff.is_data_modification_required()
                suggested_scenario = "ensure_heavy" if data_modification_required else "ensure"
                LOG.warning("Consider running scenario '%s' to fix diff.", suggested_scenario)
                if should_switch_replicas(settings, unmount_required, data_modification_required):
                    LOG.warning("Replica will be switched while running '%s' scenario!", suggested_scenario)
            if reshard_required:
                LOG.warning("Consider running scenario 'reshard' to fix diff.")
            if sync_replicas_required:
                LOG.warning("Consider running scenario 'sync_replicas' to fix diff.")
    else:
        LOG.info("No difference between desired and actual state.")
    return result
