# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

try:
    from .yandex_constants import CACHE_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    CACHE_DASHBOARD_DEFAULT_CLUSTER = ""

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import (
    MonitoringLabelDashboardParameter,
    MonitoringCustomDashboardParameter
)

from yt_dashboard_generator.sensor import MultiSensor


CACHES = [
    "yt.clickhouse_proxy.discovery_cache",
    "yt.cluster_node.object_service_cache",
    "yt.codegen_cache",
    "yt.connection.block_cache.compressed_data",
    "yt.connection.block_cache.uncompressed_data",
    "yt.connection.chunk_meta_cache",
    "yt.data_node.block_cache.chunk_fragments_data",
    "yt.data_node.block_cache.compressed_data",
    "yt.data_node.block_cache.hash_table_chunk_index",
    "yt.data_node.block_cache.min_hash_digest",
    "yt.data_node.block_cache.uncompressed_data",
    "yt.data_node.block_cache.xor_filter",
    "yt.data_node.blocks_ext_cache",
    "yt.data_node.changelog_reader_cache",
    "yt.data_node.chunk_meta_cache",
    "yt.exec_node.artifact_cache",
    "yt.exec_node.job_input_cache.block_cache.compressed_data",
    "yt.exec_node.job_input_cache.meta_cache",
    "yt.exec_node.layer_block_cache.compressed_data",
    "yt.exec_node.layer_cache",
    "yt.exec_node.ronbd_volume_cache",
    "yt.exec_node.squashfs_volume_cache",
    "yt.flow.worker.state_cache",
    "yt.master_cache.object_service_cache",
    "yt.object_server.object_service_cache",
    "yt.tablet_node.compression_dictionary_cache",
    "yt.tablet_node.versioned_chunk_meta_cache"
]

SELECTOR = '{project="yt"}'


def build_rowset1():
    return (Rowset()
        .min(0)
        .aggr("hit_type")
        .row(height=10)
            .cell("Hits/misses (weight)",
                MultiSensor(
                    Sensor("{{cache}}.hit_weight.rate").legend_format("hits"),
                    Sensor("{{cache}}.missed_weight.rate").legend_format("misses"))
                .stack(True)
                .unit("UNIT_BYTES_SI"),
                display_legend=True)
            .cell("Hits/misses (count)",
                MultiSensor(
                    Sensor("{{cache}}.hit_count.rate").legend_format("hits"),
                    Sensor("{{cache}}.missed_count.rate").legend_format("misses"))
                .stack(True),
                display_legend=True)
        .row(height=10)
            .aggr("segment")
            .cell("Cache weight",
                Sensor("{{cache}}.weight")
                .stack(True)
                .unit("UNIT_BYTES_SI"),
                display_legend=False)
            .cell("Cache size",
                Sensor("{{cache}}.size")
                .stack(True),
                display_legend=False)
    ).owner


def build_rowset2():
    return (Rowset()
        .min(0)
        .aggr("hit_type")
        .row(height=10)
            .cell("Hit weight",
                MultiSensor(
                    Sensor("{{cache}}.large_ghost_cache.hit_weight.rate").legend_format("large_ghost"),
                    Sensor("{{cache}}.hit_weight.rate").legend_format("now"),
                    Sensor("{{cache}}.small_ghost_cache.hit_weight.rate").legend_format("small_ghost"))
                .unit("UNIT_BYTES_SI"),
                display_legend=True)
            .cell("Hit count",
                MultiSensor(
                    Sensor("{{cache}}.large_ghost_cache.hit_count.rate").legend_format("large_ghost"),
                    Sensor("{{cache}}.hit_count.rate").legend_format("now"),
                    Sensor("{{cache}}.small_ghost_cache.hit_count.rate").legend_format("small_ghost")),
                display_legend=True)
        .row(height=10)
            .cell("Missed weight",
                MultiSensor(
                    Sensor("{{cache}}.large_ghost_cache.missed_weight.rate").legend_format("large_ghost"),
                    Sensor("{{cache}}.missed_weight.rate").legend_format("now"),
                    Sensor("{{cache}}.small_ghost_cache.missed_weight.rate").legend_format("small_ghost"))
                .unit("UNIT_BYTES_SI"),
                display_legend=True)
            .cell("Missed count",
                MultiSensor(
                    Sensor("{{cache}}.large_ghost_cache.missed_count.rate").legend_format("large_ghost"),
                    Sensor("{{cache}}.missed_count.rate").legend_format("now"),
                    Sensor("{{cache}}.small_ghost_cache.missed_count.rate").legend_format("small_ghost")),
                display_legend=True)
    ).owner


def build_cache_with_ghosts():
    d = Dashboard()
    d.add(build_rowset1())
    d.add(build_rowset2())

    d.value("service", TemplateTag("service"))
    d.value("host", TemplateTag("host"))

    d.set_title("Cache and all its ghosts")
    d.set_description("Everything you'd like to know about cache hits&misses")
    d.add_parameter("cluster", "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", CACHE_DASHBOARD_DEFAULT_CLUSTER, selectors=SELECTOR))
    d.add_parameter("service", "Service",
        MonitoringLabelDashboardParameter("yt", "service", "dat_node", selectors=SELECTOR))
    d.add_parameter("cache", "Cache path",
        MonitoringCustomDashboardParameter(values=CACHES, default_value="yt.data_node.block_cache.compressed_data"))
    d.add_parameter("container", "Container",
        MonitoringLabelDashboardParameter("yt", "container", "-", selectors=SELECTOR))
    d.add_parameter("host", "Host",
        MonitoringLabelDashboardParameter("yt", "host", "Aggr", selectors=SELECTOR))
    d.add_parameter("tablet_cell_bundle", "Tablet cell bundle",
        MonitoringLabelDashboardParameter("yt", "tablet_cell_bundle", "-", selectors=SELECTOR))
    return d
