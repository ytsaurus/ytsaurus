# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import (
    create_dashboard,
    build_resource_usage,
    build_extra_cpu,
    build_yt_rpc,
)

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell

from textwrap import dedent


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


def add_partitions_by_current_job_status_cell(row):

    def job_status(status, alias):
        return (FlowController(f"yt.flow.controller.job_status.{status}")
            .aggr("computation_id")
            .aggr("previous_job_finish_reason")
            .query_transformation(f'alias({{query}}, "{alias}")'))

    description = dedent("""\
        Statuses of jobs of not finished partitions.
        If partition has no current job, consider it as `Unknown` job.
        So sum of lines on graph must be equal to total partition count.

        **Unknown/Recovering/Has_retryable_errors** are bad statuses.
        If you constantly have jobs in these statuses, your pipeline degrades significantly.

        **Warming up** is semi-good status. These jobs are working, but not for a long time.

        **Working** is good status. If all jobs are `Working` pipeline has no problems with job deaths.

        **Stopped** means that pipeline is pausing/paused/stopped and partitions have no jobs due to this.
    """)

    return (row
            .cell("Partitions by current job status",
                MultiSensor(
                    job_status("ok", "Working"),
                    job_status("working_old", "Stably working (≥ 5 min after recovering)"),
                    job_status("working_young", "Warming up (working ≤ 5 min after recovering)"),
                    job_status("working_with_retryable_error", "Has retryable errors"),
                    job_status("preparing", "Recovering (new job is preparing)"),
                    job_status("unknown", "Unknown"),
                    job_status("stopped", "Stopped"))
                    .min(0.8)
                    .unit("UNIT_COUNT")
                    .axis_type("YAXIS_TYPE_LOGARITHMIC"),
                colors={
                    "Working": "#00b200",
                    "Stably working (≥ 5 min after recovering)": "#00b200",
                    "Warming up (working ≤ 5 min after recovering)": "#b7e500",
                    "Has retryable errors": "#cc0000",
                    "Recovering (new job is preparing)": "#ffa500",
                    "Unknown": "#11114e",
                    "Stopped": "#84c1ff",
                },
                description=description)
    )


def build_flow_layout_mutations():
    return (Rowset()
        .stack(False)
        .row()
            .apply_func(add_partitions_by_current_job_status_cell)
            .cell("Layout mutations", FlowController("yt.flow.controller.mutations.*.rate").unit("UNIT_COUNTS_PER_SECOND"))
            .cell("Job manage mutations", FlowController("yt.flow.controller.job_manager.*.rate").unit("UNIT_COUNTS_PER_SECOND"))
            .cell("", EmptyCell())
    ).owner


def add_controller_failed_iterations_cell(row):
    description = dedent("""\
        **Expect to see zero values on this panel if the pipeline is stable.**

        **schedule** - the most critical component; it starts and stops jobs.
        **collect_feedback** - the component that collects worker heartbeat data and updates job statuses.
        **update_metrics** - the component that updates most controller metrics.
        **build_cache** - the component that updates cached flow view (flow view is large, and its serialization is time-consuming).
    """)

    return (row
            .cell(
                "Controller failed iterations",
                FlowController("yt.flow.controller.*.iterations_failed.rate")
                    .unit("UNIT_COUNTS_PER_SECOND"),
                description=description)
    )


def build_controller_iterations():
    return (Rowset()
        .stack(False)
        .row()
            .cell(
                "Controller total iterations",
                FlowController("yt.flow.controller.*.iterations_total.rate")
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .apply_func(add_controller_failed_iterations_cell)
            .cell(
                "Controller iteration duration",
                MonitoringExpr(FlowController("yt.flow.controller.*iteration_time.max"))
                    .unit("UNIT_SECONDS"))
            .cell("", EmptyCell())
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

    def build_computation_partitions(suffix):
        return (
            FlowController(f"yt.flow.controller.computation.{suffix}")
                .all("computation_id")
                .unit("UNIT_COUNT")
        )

    return (Rowset()
        .stack(False)
        .row()
            .cell(
                "Idle partitions detected",
                build_computation_partitions("idle_partitions_detected"),
                description=description)
            .cell(
                "Idle partitions ignored",
                build_computation_partitions("idle_partitions_ignored"),
                description=description)
            .cell(
                "Late data partitions detected",
                build_computation_partitions("late_data_partitions_detected"),
                description=description)
            .cell(
                "Late data partitions ignored",
                build_computation_partitions("late_data_partitions_ignored"),
                description=description)
        .row()
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
            .cell("", EmptyCell())
    )


def build_flow_controller():
    def fill(d):
        d.add(build_resource_usage("controller", add_component_to_title=False))
        d.add(build_flow_layout())
        d.add(build_flow_layout_mutations())
        d.add(build_controller_iterations())
        d.add(build_heartbeats())
        d.add(build_watermark_heuristics())
        d.add(build_extra_cpu("controller"))
        d.add(build_yt_rpc("controller"))

    return create_dashboard("controller", fill)
