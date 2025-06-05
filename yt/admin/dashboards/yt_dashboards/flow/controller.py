# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import (
    build_versions,
    build_resource_usage,
    build_extra_cpu,
    add_common_dashboard_parameters,
    add_partitions_by_current_job_status_cell,
    build_yt_rpc,
)

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell


def build_flow_layout():
    return (Rowset()
        .stack(False)
        .row()
            .cell("Registered workers count", FlowController("yt.flow.controller.worker_count").unit("UNIT_COUNT"))
            .cell("Computations count", FlowController("yt.flow.controller.computation_count").unit("UNIT_COUNT"))
            .cell(
                "Active partitions count",
                FlowController("yt.flow.controller.partition_count")
                    .all("computation_id")
                    .value("state", "*ing")
                    .unit("UNIT_COUNT"))
            .cell(
                "Finished partitions rate",
                FlowController("yt.flow.controller.finished_partitions.rate")
                    .unit("UNIT_COUNTS_PER_SECOND"))
    ).owner


def build_flow_layout_mutations():
    return (Rowset()
        .stack(False)
        .row()
            .apply_func(add_partitions_by_current_job_status_cell)
            .cell("Layout mutations", FlowController("yt.flow.controller.mutations.*.rate").unit("UNIT_COUNTS_PER_SECOND"))
            .cell("Job manage mutations", FlowController("yt.flow.controller.job_manager.*.rate").unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Controller iteration duration",
                MonitoringExpr(FlowController("yt.flow.controller.*iteration_time.max"))
                    .unit("UNIT_SECONDS"))
    ).owner


def build_heartbeats():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "Handshake/heartbeats requests",
                MultiSensor(
                    MonitoringExpr(FlowController("yt.rpc.server.request_count.rate").value("method", "Heartbeat")),
                    MonitoringExpr(FlowController("yt.rpc.server.request_count.rate").value("method", "Handshake"))
                )
                    .unit("UNIT_REQUESTS_PER_SECOND")
                    .aggr("host"))
            .cell(
                "Worker heartbeat prepare time",
                FlowWorker("yt.flow.worker.controller_connector.heartbeat.prepare_request_time.max")
                    .unit("UNIT_SECONDS"))
            .cell(
                "Worker heartbeat wait response time",
                FlowWorker("yt.flow.worker.controller_connector.heartbeat.wait_response_time.max")
                    .unit("UNIT_SECONDS"))
            .cell(
                "Worker heartbeat process response time",
                FlowWorker("yt.flow.worker.controller_connector.heartbeat.process_response_time.max")
                    .unit("UNIT_SECONDS"))
    )


def build_watermark_heuristics():
    description = (
        "[Documentation about idle/unavailable]"
        "(https://yt.yandex-team.ru/docs/flow/concepts/spec#watermark-generator)"
    )

    confirmed_unavailable_description = description + "\n" + (
        "Availability group is considered as unavailable if all its partitions are unavailable. "
        "Confirmed unavailable partitions are partitions from unavailable availability groups. "
        "So only they are really ignored in watermark evaluation."
    )

    def build_availability_partitions(suffix):
        return (
            FlowController(f"yt.flow.controller.computation.{suffix}")
                .all("computation_id")
                .all("availability_group")
                .unit("UNIT_COUNT")
        )

    return (Rowset()
        .stack(False)
        .row()
            .cell(
                "Idle partitions",
                FlowController("yt.flow.controller.computation.idle_partitions")
                    .all("computation_id")
                    .unit("UNIT_COUNT"),
                description=description)
            .cell(
                "Unavailable partitions",
                build_availability_partitions("unavailable_partitions"),
                description=description)
            .cell(
                "Unavailable idle partitions",
                build_availability_partitions("unavailable_idle_partitions"),
                description=description)
            .cell(
                "Confirmed unavailable partitions",
                build_availability_partitions("confirmed_unavailable_partitions"),
                description=confirmed_unavailable_description)
    )


def build_flow_controller():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_resource_usage("controller", add_component_to_title=False))
    d.add(build_flow_layout())
    d.add(build_flow_layout_mutations())
    d.add(build_heartbeats())
    d.add(build_watermark_heuristics())
    d.add(build_extra_cpu("controller"))
    d.add(build_yt_rpc("controller"))

    d.set_title("[YT Flow] Pipeline controller")
    add_common_dashboard_parameters(d)

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
