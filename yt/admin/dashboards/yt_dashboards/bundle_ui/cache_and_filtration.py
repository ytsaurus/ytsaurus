# flake8: noqa

from ..common.sensors import NodeTablet, TabNode

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell, Title

def build_bundle_lookup_rowset():
    lookup_row_count = NodeTablet("yt.tablet_node.lookup.{}_count.rate")

    return (Rowset()
        .min(0)
            .row()
            .cell("Lookup row count", MultiSensor(
                MonitoringExpr(lookup_row_count("row")).alias("row count"),
                MonitoringExpr(lookup_row_count("unmerged_row")).alias("unmerged row count"),
                MonitoringExpr(lookup_row_count("missing_row")).alias("missing row count"),
                MonitoringExpr(lookup_row_count("unmerged_missing_row")).alias("unmerged missing row count"),
            ))
            .cell("Lookup missing row count to row count", MultiSensor(
                (MonitoringExpr(lookup_row_count("missing_row")) / MonitoringExpr(lookup_row_count("row")))
                    .alias("missing : row count"),
                (MonitoringExpr(lookup_row_count("unmerged_missing_row")) / MonitoringExpr(lookup_row_count("row")))
                    .alias("unmerged missing : row count"),
            ))
        ).owner

def build_bundle_key_filter_cache_and_filtration_rowsets():
    lookup_key_filter = NodeTablet("yt.tablet_node.lookup.key_filter.{}_key_count.rate")
    filter_cache = TabNode("yt.data_node.block_cache.xor_filter.{}")

    return [
        Rowset().row(height=2).cell("", Title("Key Filter", size="TITLE_SIZE_M")).owner,
        (Rowset()
            .min(0)
            .row()
                .cell("Filtration performance", MultiSensor(
                    MonitoringExpr(lookup_key_filter("input")).alias("input key count"),
                    MonitoringExpr(lookup_key_filter("filtered_out")).alias("filtered out key count"),
                    MonitoringExpr(lookup_key_filter("false_positive")).alias("false positive key count"),
                ))
                .cell("Key filter block cache usage", filter_cache("weight").aggr("segment"))
            .row()
                .cell("Key filter cache hit count rate", MultiSensor(
                    filter_cache("hit_count.rate").aggr("hit_type"),
                    filter_cache("missed_count.rate"),
                ))
                .cell("Key filter cache hit weight rate", MultiSensor(
                    filter_cache("hit_weight.rate").aggr("hit_type"),
                    filter_cache("missed_weight.rate"),
                ))
            ).owner
    ]

def build_bundle_lookup_cache_rowsets():
    lookup_cache_rate = NodeTablet("yt.tablet_node.lookup.cache_{}.rate")

    return [
        Rowset().row(height=2).cell("", Title("Lookup Cache", size="TITLE_SIZE_M")).owner,
        (Rowset()
            .min(0)
            .row()
                .cell("Cache performance", MultiSensor(
                    MonitoringExpr(lookup_cache_rate("hits")).alias("hits"),
                    MonitoringExpr(lookup_cache_rate("misses")).alias("misses"),
                ))
                .cell("Keys distribution", MultiSensor(
                    MonitoringExpr(lookup_cache_rate("inserts")).alias("inserts"),
                    MonitoringExpr(lookup_cache_rate("outdated")).alias("outdated"),
                ))
        ).owner
    ]
