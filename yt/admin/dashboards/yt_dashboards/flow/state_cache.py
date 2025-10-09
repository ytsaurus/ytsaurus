# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import build_versions, add_common_dashboard_parameters

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell
from yt_dashboard_generator.taggable import NotEquals


def build_job_cache():
    # due to state cache nature, insert rate equals to sum of extract_hit and extract_miss
    extract_hit = MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.extract_hit.rate"))
    insert = MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.insert.rate"))

    extract_hit_bytes = MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.extract_hit_bytes.rate"))
    insert_bytes = MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.insert_bytes.rate"))

    return (Rowset()
        .all("computation_id")
        .all("name")
        .aggr("host")
        .row()
            .cell(
                "Cache size per computation",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.size"))
                    .alias("{{computation_id}} - {{name}}")
                    .unit("UNIT_BYTES_SI")
                    .stack(True))
            .cell(
                "Cache count per computation",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.count"))
                    .alias("{{computation_id}} - {{name}}")
                    .unit("UNIT_COUNT")
                    .stack(True))
            .cell(
                "Cache hit per computation",
                MonitoringExpr(extract_hit / insert)
                    .alias("{{computation_id}} - {{name}}")
                    .stack(False)
                    .unit("UNIT_PERCENT_UNIT"))
            .cell(
                "Cache hit size per computation",
                MonitoringExpr(extract_hit_bytes / insert_bytes)
                    .alias("{{computation_id}} - {{name}}")
                    .stack(False)
                    .unit("UNIT_PERCENT_UNIT"))
        .row()
            .cell(
                "Cache time to compress per computation",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.time_to_compress.max"))
                    .all("host")
                    .query_transformation('group_lines("median", ["computation_id", "name"], {query})')
                    .alias("{{computation_id}} - {{name}}")
                    .unit("UNIT_SECONDS")
                    .stack(False)
                    .precision(1))
            .cell(
                "Cache time to expire per computation",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.job_state_cache.time_to_expire.max"))
                    .all("host")
                    .query_transformation('group_lines("median", ["computation_id", "name"], {query})')
                    .alias("{{computation_id}} - {{name}}")
                    .unit("UNIT_SECONDS")
                    .stack(False)
                    .precision(1))
            .cell("", EmptyCell())
            .cell("", EmptyCell())
    )


def build_global_cache():
    return (Rowset()
        .all("cache")
        .aggr("host")
        .row()
            .cell(
                "Global cache size",
                MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.weight"))
                    .alias("{{cache}}")
                    .unit("UNIT_BYTES_SI")
                    .value("segment", "-")
                    .stack(False)
                    .precision(1))
            .cell(
                "Global cache count",
                MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.size"))
                    .alias("{{cache}}")
                    .unit("UNIT_COUNT")
                    .value("segment", "-")
                    .stack(False)
                    .precision(1))
            .cell(
                "Global uncompressed cache requests",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.hit_count.rate"))
                        .value("cache", "uncompressed")
                        .value("hit_type", "-")
                        .alias("hit requests"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.small_ghost_cache.hit_count.rate"))
                        .value("cache", "uncompressed")
                        .value("hit_type", "-")
                        .alias("ghost hit requests (cache size x0.5)"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.large_ghost_cache.hit_count.rate"))
                        .value("cache", "uncompressed")
                        .value("hit_type", "-")
                        .alias("ghost hit requests (cache size x2)"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.missed_count.rate"))
                        .value("cache", "uncompressed")
                        .value("hit_type", "-")
                        .alias("missed requests"),
                )
                    .unit("UNIT_COUNT")
                    .stack(False)
                    .precision(1))
            .cell(
                "Global compressed cache requests",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.hit_count.rate"))
                        .value("cache", "compressed")
                        .value("hit_type", "-")
                        .alias("hit requests"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.small_ghost_cache.hit_count.rate"))
                        .value("cache", "compressed")
                        .value("hit_type", "-")
                        .alias("ghost hit requests (cache size x0.5)"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.large_ghost_cache.hit_count.rate"))
                        .value("cache", "compressed")
                        .value("hit_type", "-")
                        .alias("ghost hit requests (cache size x2)"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.missed_count.rate"))
                        .value("cache", "compressed")
                        .value("hit_type", "-")
                        .alias("missed requests"),
                )
                    .unit("UNIT_COUNT")
                    .stack(False)
                    .precision(1))
        .row()
            .cell(
                "Global cache item time to expire",
                MonitoringExpr(FlowWorker("yt.flow.worker.state_cache.time_to_expire.max"))
                    .all("host")
                    .query_transformation('group_lines("median", ["cache"], {query})')
                    .alias("{{cache}}")
                    .unit("UNIT_SECONDS")
                    .stack(False)
                    .precision(1))
            .cell("", EmptyCell())
            .cell("", EmptyCell())
            .cell("", EmptyCell())


    )


def build_flow_state_cache():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_job_cache())
    d.add(build_global_cache())

    d.set_title("[YT Flow] State cache")
    add_common_dashboard_parameters(d)

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
