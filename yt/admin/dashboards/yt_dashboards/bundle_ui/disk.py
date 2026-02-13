# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from ..common.sensors import *

##################################################################


def build_user_disk():
    reader_stats = MultiSensor(
        NodeTablet("yt.tablet_node.{}.chunk_reader_statistics.{}.rate").host_container_legend_format(),
        NodeTablet("yt.tablet_node.{}.hunks.chunk_reader_statistics.{}.rate").host_container_legend_format("hunks"))

    return (Rowset()
            .aggr("table_tag", "table_path", "user", "medium")
            .top()
            .min(0)
            .stack(False)
            .row()
                .cell("Table lookup data bytes read from disk", reader_stats("lookup", "data_bytes_read_from_disk"))
                .cell("Table select data bytes read from disk", reader_stats("select", "data_bytes_read_from_disk"))
            .row()
                .cell("Table lookup chunk meta bytes read from disk", reader_stats("lookup", "meta_bytes_read_from_disk"))
                .cell("Table select chunk meta bytes read from disk", reader_stats("select", "meta_bytes_read_from_disk"))
            .row()
                .cell(
                    "Table lookup data bytes transmitted from data node",
                    reader_stats("lookup", "data_bytes_transmitted"),
                    description="Data nodes are cluster storage layer. If data is not found in tablet static memory or tablet node caches it is read via network from data nodes")
                .cell(
                    "Table select data bytes transmitted from data node",
                    reader_stats("select", "data_bytes_transmitted"),
                    description="Data nodes are cluster storage layer. If data is not found in tablet static memory or tablet node caches it is read via network from data nodes")
            .row()
                .cell("Table lookup chunk meta bytes transmitted from data node", reader_stats("lookup", "meta_bytes_transmitted"))
                .cell("Table select chunk meta bytes transmitted from data node", reader_stats("select", "meta_bytes_transmitted"))
            .row()
                .cell("Table lookup data wait time", reader_stats("lookup", "data_wait_time"))
                .cell("Table select data wait time", reader_stats("select", "data_wait_time"))
            .row()
                .cell("Table lookup meta wait time", reader_stats("lookup", "meta_wait_time"))
                .cell("Table select meta wait time", reader_stats("select", "meta_wait_time"))
            .row()
                .cell("Table lookup pick peer time", reader_stats("lookup", "pick_peer_wait_time"))
                .cell("Table select pick peer time", reader_stats("select", "pick_peer_wait_time"))
            .row()
                .cell("Table lookup meta disk read time", reader_stats("lookup", "meta_read_from_disk_time"))
                .cell("Table select meta disk read time", reader_stats("select", "meta_read_from_disk_time"))
            ).owner


def build_user_background_disk():
    top_disk = NodeTablet("yt.tablet_node.{}.{}.rate").host_container_legend_format("{{account}} {{method}}")

    return (Rowset()
            .all("#AB", "method")
            .aggr("table_tag", "table_path", "medium")
            .top()
            .stack()
            .row()
                .cell("Tablet background data bytes read from disk", top_disk("chunk_reader_statistics", "data_bytes_read_from_disk"))
                .cell("Tablet background chunk meta bytes read from disk", top_disk("chunk_reader_statistics", "meta_bytes_read_from_disk"))
            .row()
                .cell("Tablet background disk bytes written (with replication)", top_disk("chunk_writer", "disk_space"))
                .cell("Tablet background data weight written (without replication)", top_disk("chunk_writer", "data_weight"))
            ).owner


def build_user_caches():
    usage = TabNode("{}.hit_weight.rate")
    misses = TabNode("yt.{}_node.{}.missed_weight.rate").host_container_legend_format()
    return (Rowset()
            .stack(False)
            .top()
            .min(0)
            .row()
                .cell(
                    "Versioned chunk meta cache hit weight rate",
                    NodeTablet("yt.tablet_node.versioned_chunk_meta_cache.hit_weight.rate").aggr("hit_type").host_container_legend_format())
                .cell(
                    "Versioned chunk meta cache miss weight rate",
                    NodeTablet("yt.tablet_node.versioned_chunk_meta_cache.missed_weight.rate").host_container_legend_format())
            .row()
                .cell(
                    "Block cache hit weight rate",
                    MultiSensor(
                        usage("yt.data_node.block_cache.compressed_data").aggr("hit_type").host_container_legend_format("compressed"),
                        usage("yt.data_node.block_cache.uncompressed_data").aggr("hit_type").host_container_legend_format("uncompressed")),
                    )
                .cell("Block cache miss weight rate",
                    MultiSensor(
                        misses("data", "block_cache.compressed_data").host_container_legend_format("compressed"),
                        misses("data", "block_cache.uncompressed_data").host_container_legend_format("uncompressed"))
                    )
            .row()
                .cell("Block cache memory", TabNode("yt.cluster_node.memory_usage.used")
                    .value("category", "block_cache"))
                .cell("Cached versioned chunk meta memory", TabNode("yt.cluster_node.memory_usage.used")
                    .value("category", "versioned_chunk_meta"))
    ).owner


def build_block_cache_planning():
    def miss_weight_rate(name):
        return MultiSensor(
            MonitoringExpr(TabNode("yt.data_node.block_cache.{}_data.missed_weight.rate".format(name))).alias("current missed weight rate"),
            MonitoringExpr(TabNode("yt.data_node.block_cache.{}_data.large_ghost_cache.missed_weight.rate".format(name))).alias("x2 larger cache missed weight rate"),
            MonitoringExpr(TabNode("yt.data_node.block_cache.{}_data.small_ghost_cache.missed_weight.rate".format(name))).alias("x/2 smaller cache missed weight rate"))

    def meta_miss_weight_rate():
        return MultiSensor(
            MonitoringExpr(NodeTablet("yt.tablet_node.versioned_chunk_meta_cache.missed_weight.rate")).alias("current missed weight rate"),
            MonitoringExpr(NodeTablet("yt.tablet_node.versioned_chunk_meta_cache.large_ghost_cache.missed_weight.rate")).alias("x2 larger cache missed weight rate"),
            MonitoringExpr(NodeTablet("yt.tablet_node.versioned_chunk_meta_cache.small_ghost_cache.missed_weight.rate")).alias("x/2 smaller cache missed weight rate"))

    return (Rowset()
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
            .stack(False)
            .top(1)
            .min(0)
            .row()
                .cell("Compressed block cache size planning", miss_weight_rate("compressed"))
                .cell("Uncompressed block cache size planning", miss_weight_rate("uncompressed"))
            .row()
                .cell("Versioned chunk meta cache size planning", meta_miss_weight_rate())
                .cell("", EmptyCell())
    ).owner
