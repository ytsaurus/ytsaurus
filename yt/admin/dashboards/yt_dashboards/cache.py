# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

try:
    from .constants import CACHE_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    CACHE_DASHBOARD_DEFAULT_CLUSTER = ""

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, Text
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter


def build_rowset1():
    return (Rowset()
        .min(0)
        .row()
            .aggr("hit_type")
            .cell(
                "Hit Weight",
                Sensor("{{cache}}.hit_weight.rate"))
            .cell(
                "Small Ghost Hit Weight",
                Sensor("{{cache}}.small_ghost_cache.hit_weight.rate"))
            .cell(
                "Large Ghost Hit Weight",
                Sensor("{{cache}}.large_ghost_cache.hit_weight.rate"))
        .row()
            .aggr("hit_type")
            .cell(
                "Hit Count",
                Sensor("{{cache}}.hit_count.rate"))
            .cell(
                "Small Ghost Hit Count",
                Sensor("{{cache}}.small_ghost_cache.hit_count.rate"))
            .cell(
                "Large Ghost Hit Count",
                Sensor("{{cache}}.large_ghost_cache.hit_count.rate"))
        .row()
            .cell(
                "Missed Count",
                Sensor("{{cache}}.missed_count.rate"))
            .cell(
                "Small Ghost Missed Count",
                Sensor("{{cache}}.small_ghost_cache.missed_count.rate"))
            .cell(
                "Large Ghost Missed Count",
                Sensor("{{cache}}.large_ghost_cache.missed_count.rate"))
        .row()
            .cell(
                "Missed Weight",
                Sensor("{{cache}}.missed_weight.rate"))
            .cell(
                "Small Ghost Missed Weight",
                Sensor("{{cache}}.small_ghost_cache.missed_weight.rate"))
            .cell(
                "Large Ghost Missed Weight",
                Sensor("{{cache}}.large_ghost_cache.missed_weight.rate"))
    ).owner

def build_rowset2():
    def all_ghosts(suffix):
        return Sensor(
            "{c}.{s}.rate|{c}.small_ghost_cache.{s}.rate|{c}.large_ghost_cache.{s}.rate".format(
                c="{{cache}}",
                s=suffix))

    return (Rowset()
        .min(0)
        .row()
            .aggr("hit_type")
            .cell("Hit Weight (all caches)", all_ghosts("hit_weight"))
            .cell("Hit Count (all caches)", all_ghosts("hit_count"))
        .row()
            .cell("Missed Weight (all caches)", all_ghosts("missed_weight"))
            .cell("Missed Count (all caches)", all_ghosts("missed_count"))
        .row()
            .aggr("segment")
            .cell(
                "Cache Weight",
                Sensor("{{cache}}.weight"))
            .cell(
                "Cache Size (item count)",
                Sensor("{{cache}}.size"))
    ).owner

def build_cache_with_ghosts():
    d = Dashboard()
    d.add(build_rowset2())
    d.add(build_rowset1())

    d.value("service", TemplateTag("service"))
    d.value("container", TemplateTag("container"))
    d.value("host", TemplateTag("host"))
    d.value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))

    d.set_title("Cache and all its ghosts ")
    d.set_description("Everything you'd like to know about cache hits&misses")
    d.add_parameter("cluster", "YT cluster", MonitoringLabelDashboardParameter("yt", "cluster", CACHE_DASHBOARD_DEFAULT_CLUSTER))
    d.add_parameter("service", "Service", MonitoringLabelDashboardParameter("yt", "service", "dat_node"))
    d.add_parameter("cache", "Cache path",
        MonitoringLabelDashboardParameter("yt", "cache", "yt.data_node.block_cache.compressed_data"))
    d.add_parameter("container", "Container", MonitoringLabelDashboardParameter("yt", "container", "-"))
    d.add_parameter("host", "Host", MonitoringLabelDashboardParameter("yt", "host", "Aggr"))
    d.add_parameter("tablet_cell_bundle", "Tablet cell bundle", MonitoringLabelDashboardParameter("yt", "tablet_cell_bundle", "*"))
    return d
