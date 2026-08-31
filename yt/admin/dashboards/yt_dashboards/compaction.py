# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

try:
    from .constants import COMPACTION_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    from .yandex_constants import COMPACTION_DASHBOARD_DEFAULT_CLUSTER

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import (
    MonitoringCustomDashboardParameter,
    MonitoringLabelDashboardParameter,
    MonitoringQueryDashboardParameter,
)
from yt_dashboard_generator.backends.monitoring.sensors import (
    MonitoringExpr, DownsamplingAggregation)


DIGEST_COMPACTION_REASONS = [
    "apply_deletions",
    "remove_duplicates",
    "ttl_cleanup_expected",
    "too_many_timestamps",
    "aggregate_ttl_cleanup_expected",
    "aggregate_delete_too_many_timestamps",
]

DIGEST_COMPACTION_REASON_SELECTOR = "|".join(DIGEST_COMPACTION_REASONS)
TOP_SERIES_LIMIT = 20


def _anomaly_view(expression):
    return (expression
        .top_max(TOP_SERIES_LIMIT)
        .downsampling_aggregation(DownsamplingAggregation.Max))


def generate1():
    write_dw = MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate"))

    lsm_dw = MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_data_weight.rate")
        .value("activity", "compaction|partitioning")
        .aggr("eden", "reason"))
    comp_dw = lsm_dw.value("activity", "compaction")
    part_dw = lsm_dw.value("activity", "partitioning")

    lsm_dw_hunks = MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.hunks.out_data_weight.rate")
        .value("activity", "compaction|partitioning")
        .aggr("eden", "reason"))
    comp_dw_hunks = lsm_dw_hunks.value("activity", "compaction")
    part_dw_hunks = lsm_dw_hunks.value("activity", "partitioning")

    return [
        (Rowset()
            .aggr("host")
            .row()
                .cell("Write data weight rate", write_dw)
                .cell(
                    "Overlapping store count (average maximum over nodes)",
                    MonitoringExpr(NodeTablet("yt.tablet_node.tablet.overlapping_store_count.max"))
                    .all("host")
                    .series_avg("cluster"))
            .row()
                .cell("LSM data weight rate", lsm_dw)
                .cell("LSM data weight rate (hunks only)", lsm_dw_hunks)
            .row()
                .cell("LSM write amplification", (comp_dw + part_dw).moving_avg("10m") / write_dw.moving_avg("10m"))
                .cell("LSM write amplification (hunks only)", (comp_dw_hunks + part_dw_hunks).moving_avg("10m") / write_dw.moving_avg("10m"))
            .row()
                .cell("LSM out store count rate", NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate")
                    .aggr("eden").all("reason", "activity"))
                .cell("LSM in hunk chunk count rate", NodeTablet("yt.tablet_node.store_compactor.hunks.in_hunk_chunk_count.rate")
                    .aggr("activity", "eden").all("reason", "hunk_compaction_reason"))
            .row()
                .aggr("eden")
                .all("reason")
                .value("activity", "compaction")
                .stack()
                .cell("Compaction reasons (by in data weight)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.in_data_weight.rate"))
                    .alias("{{cluster}}, {{reason}}"))
                .cell("Compaction reasons (by out store count)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate"))
                    .alias("{{cluster}}, {{reason}}"))
            .row()
                .aggr("eden")
                .all("reason")
                .value("activity", "partitioning")
                .stack()
                .cell("Partitioning reasons (by in data weight)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.in_data_weight.rate"))
                    .alias("{{cluster}}, {{reason}}"))
                .cell("Partitioning reasons (by out store count)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate"))
                    .alias("{{cluster}}, {{reason}}"))
            #  .row()
            #      .cell("Compaction in Eden / partitions (by data weight)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_data_weight.rate"))

        ).owner,
    ]

def generate15():
    lookup_dw = MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate"))
    lookup_unmerged_dw = MonitoringExpr(NodeTablet("yt.tablet_node.lookup.unmerged_data_weight.rate"))

    def _lookup_percentile(p):
        return (MonitoringExpr(NodeTablet("yt.tablet_node.multiread.request_duration.max"))
            .moving_avg("30s")
            .group_by_labels("cluster", f"v -> series_percentile({p}, v)")
            .alias("{{cluster}} p" + str(p))
            .all("host"))

    return [
        Rowset().row(height=2).cell("", Title("Lookup timings", size="TITLE_SIZE_L")).owner,
        (Rowset()
            .row()
                .cell("Lookup data weight rate", lookup_dw)
                .cell("Lookup unmerged data weight rate", lookup_unmerged_dw)
            .row()
                .cell("Lookup data weight: unmerged : merged", lookup_unmerged_dw.moving_avg("5m") / lookup_dw.moving_avg("5m"))
                .cell("multiread.request_duration.max percentiles", MultiSensor(
                    _lookup_percentile(99), _lookup_percentile(90), _lookup_percentile(50)))
        ).owner,
    ]

def generate2():
    return [
        Rowset().row(height=2).cell("", Title("Rotation, split/merge", size="TITLE_SIZE_L")).owner,
        (Rowset()
            .aggr("host")
            .row()
                .cell("Rotation reasons", NodeTablet("yt.tablet_node.store_rotator.rotation_count.rate")
                    .all("reason")
                    .stack())
                .cell(
                    "Flushed store memory size (p50)",
                    MonitoringExpr(NodeTablet("yt.tablet_node.store_rotator.rotated_memory_usage.max"))
                        .aggr("reason")
                        .all("host")
                        .drop_below(1)
                        .group_by_labels("cluster", "v -> series_percentile(50, v)")
                        .alias("{{cluster}} p50")
                        .downsampling_aggregation(DownsamplingAggregation.Last))
                .cell("Partition splits/merges", NodeTablet("yt.tablet_node.partition_balancer.partition_*.rate"))
        ).owner,
    ]


def generate3():
    return [
        Rowset().row(height=2).cell("", Title("Table size", size="TITLE_SIZE_L")).owner,
        (Rowset()
            .aggr("host")
            .row()
                .cell("Data weight", NodeTablet("yt.tablet_node.tablet.data_weight"))
                .cell("Chunk count", NodeTablet("yt.tablet_node.tablet.chunk_count"))
            .row()
                .cell("Tablet count", NodeTablet("yt.tablet_node.tablet.tablet_count"))
                .cell("Hunk chunk count", NodeTablet("yt.tablet_node.tablet.hunk_chunk_count"))
        ).owner,
    ]


def _build_digest_compaction_rowset():
    def data_weight_sensor(direction):
        return (NodeTablet(f"yt.tablet_node.store_compactor.{direction}_data_weight.rate")
            .aggr("eden")
            .value("activity", "compaction")
            .value("reason", TemplateTag("reason"))
            .unit("UNIT_BYTES_SI_PER_SECOND"))

    def store_count_sensor(direction):
        return MonitoringExpr(
            NodeTablet(f"yt.tablet_node.store_compactor.{direction}_store_count.rate")
                .aggr("eden")
                .value("activity", "compaction")
                .value("reason", TemplateTag("reason")))

    in_data_weight = MonitoringExpr(data_weight_sensor("in"))
    out_data_weight = MonitoringExpr(data_weight_sensor("out"))
    in_store_count = store_count_sensor("in")
    out_store_count = store_count_sensor("out")
    write_data_weight = MonitoringExpr(
        NodeTablet("yt.tablet_node.write.data_weight.rate")
            .unit("UNIT_BYTES_SI_PER_SECOND"))

    return (Rowset()
        .aggr("host", "table_tag", "user")
        .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
        .value("table_path", TemplateTag("table_path"))
        .row()
            .cell("Compacted data weight",
                MultiSensor(
                    _anomaly_view(in_data_weight),
                    _anomaly_view(out_data_weight))
                    .unit("UNIT_BYTES_SI_PER_SECOND"))
            .cell("Output / input data weight",
                _anomaly_view(out_data_weight / in_data_weight.drop_below(1e-6))
                    .unit("UNIT_NONE"))
        .row()
            .cell("Digest input / table write data weight",
                _anomaly_view(in_data_weight / write_data_weight.drop_below(1e-6))
                    .unit("UNIT_NONE"))
            .cell("Input / output store rate",
                MultiSensor(
                    _anomaly_view(in_store_count),
                    _anomaly_view(out_store_count))
                    .unit("UNIT_COUNTS_PER_SECOND"))
    ).owner


def _build_digest_fetching_rowset():
    def fetch_sensor(digest_kind, metric):
        return (NodeTablet(f"yt.tablet_node.compaction_hints.{digest_kind}.{metric}.rate")
            .aggr("host", "cell_id")
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle")))

    def digest_sensors(metric):
        return MultiSensor(
            _anomaly_view(MonitoringExpr(fetch_sensor("row_digest", metric))),
            _anomaly_view(MonitoringExpr(fetch_sensor("min_hash_digest", metric))),
        )

    def finished_to_requested(digest_kind):
        finished = MonitoringExpr(fetch_sensor(digest_kind, "finished_request_count"))
        requested = MonitoringExpr(fetch_sensor(digest_kind, "request_count"))
        return (_anomaly_view(finished / requested.drop_below(1e-6))
            .unit("UNIT_NONE"))

    return (Rowset()
        .row()
            .cell("Fetch requests", digest_sensors("request_count")
                .unit("UNIT_REQUESTS_PER_SECOND"))
            .cell("Finished fetches", digest_sensors("finished_request_count")
                .unit("UNIT_REQUESTS_PER_SECOND"))
        .row()
            .cell("Failed fetches", digest_sensors("failed_request_count")
                .unit("UNIT_REQUESTS_PER_SECOND"))
            .cell("Throttled fetch cycles", digest_sensors("throttled_request_count")
                .unit("UNIT_COUNTS_PER_SECOND"))
        .row()
            .cell("Digest parse CPU", digest_sensors("parse_cumulative_time"))
            .cell("Finished / requested fetches", MultiSensor(
                finished_to_requested("row_digest"),
                finished_to_requested("min_hash_digest")))
    ).owner


def _build_all_compactions_rowset():
    def data_weight_sensor(direction, reason):
        return _anomaly_view(MonitoringExpr(
            NodeTablet(f"yt.tablet_node.store_compactor.{direction}_data_weight.rate")
                .value("reason", reason)))

    return (Rowset()
        .aggr("host", "eden", "table_tag", "user")
        .value("activity", "compaction")
        .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
        .value("table_path", TemplateTag("table_path"))
        .row()
            .cell("Input data weight by reason",
                _anomaly_view(MonitoringExpr(
                    NodeTablet("yt.tablet_node.store_compactor.in_data_weight.rate")
                        .all("reason")))
                    .unit("UNIT_BYTES_SI_PER_SECOND")
                    .stack())
            .cell("Output store count by reason",
                _anomaly_view(MonitoringExpr(
                    NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate")
                        .all("reason")))
                    .unit("UNIT_COUNTS_PER_SECOND")
                    .stack())
        .row()
            .cell("Input data weight — all digest reasons",
                data_weight_sensor("in", DIGEST_COMPACTION_REASON_SELECTOR)
                    .unit("UNIT_BYTES_SI_PER_SECOND")
                    .stack())
            .cell("Periodic compaction data weight",
                MultiSensor(
                    data_weight_sensor("in", "periodic"),
                    data_weight_sensor("out", "periodic"))
                    .unit("UNIT_BYTES_SI_PER_SECOND"))
    ).owner


def build_compaction_digest():
    dashboard = Dashboard()
    dashboard.set_title("Compaction digest")
    dashboard.set_description(
        "Compaction efficiency and fetch pipeline health for row and min-hash digests.")

    dashboard.add(Rowset().row(height=2).cell(
        "", Title("Digest compactions", size="TITLE_SIZE_L")).owner)
    dashboard.add(_build_digest_compaction_rowset())
    dashboard.add(Rowset().row(height=2).cell(
        "", Title("All compactions", size="TITLE_SIZE_L")).owner)
    dashboard.add(_build_all_compactions_rowset())
    dashboard.add(Rowset().row(height=2).cell(
        "", Title("Digest fetching", size="TITLE_SIZE_L")).owner)
    dashboard.add(_build_digest_fetching_rowset())

    dashboard.add_parameter("cluster", "Cluster",
        MonitoringQueryDashboardParameter(
            "yt",
            "cluster",
            COMPACTION_DASHBOARD_DEFAULT_CLUSTER,
            selectors='{service="node_tablet"}',
            custom_items=[("All", "*")],
            multiselectable=True))
    dashboard.add_parameter("tablet_cell_bundle", "Tablet cell bundle",
        MonitoringQueryDashboardParameter(
            "yt",
            "tablet_cell_bundle",
            "-",
            selectors='{service="node_tablet", cluster="{{cluster}}"}',
            custom_items=[("All", "*")],
            multiselectable=True))
    dashboard.add_parameter("table_path", "Table path",
        MonitoringQueryDashboardParameter(
            "yt",
            "table_path",
            "*",
            selectors=(
                '{service="node_tablet", cluster="{{cluster}}", '
                'tablet_cell_bundle="{{tablet_cell_bundle}}"}'),
            custom_items=[("All", "*")],
            multiselectable=True))
    dashboard.add_parameter("reason", "Digest reason",
        MonitoringCustomDashboardParameter(
            values=DIGEST_COMPACTION_REASONS,
            default_value="apply_deletions",
            multiselectable=True))

    return dashboard.value("cluster", TemplateTag("cluster"))


def build_per_table_compaction():
    def with_table_path_tag(rowsets: list[Rowset]):
        return [
            (rowset
                .value("table_path", TemplateTag("table_path"))
                .all("tablet_cell_bundle"))
            for rowset in rowsets
        ]

    def with_common_tags(rowsets: list[Rowset]):
        return [
            rowset.aggr("user")
            for rowset in with_table_path_tag(rowsets)
        ]

    d = Dashboard()
    d.set_title("Per-table compaction statistics")
    d.add_parameter("cluster_", "cluster", MonitoringLabelDashboardParameter("yt", "cluster", COMPACTION_DASHBOARD_DEFAULT_CLUSTER))
    d.add_parameter("table_path", "table_path", MonitoringLabelDashboardParameter("yt", "table_path", "*"))

    rowsets = []
    rowsets += with_common_tags(generate1())
    rowsets += with_common_tags(generate2())
    rowsets += with_table_path_tag(generate3())
    rowsets += with_common_tags(generate15())

    for rowset in rowsets:
        d.add(rowset)

    d = d.value("cluster", TemplateTag("cluster_"))
    return d
