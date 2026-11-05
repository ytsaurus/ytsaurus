# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import (
    ExeNode, ExeNodeCpu, ExeNodeMemory, ExeNodePorto,
)

try:
    from .constants import EXE_NODES_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    from .yandex_constants import EXE_NODES_DASHBOARD_DEFAULT_CLUSTER

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.taggable import NotEquals

from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr


def _build_versions(d):
    d.add(Rowset()
        .stack(True)
        .aggr("container")
        .row()
            .cell("Versions", ExeNode("yt.build.version"))
            .cell("Kernel Versions", ExeNode("yt.host.kernel_version")
                .all("kernel_version").legend_format("{{kernel_version}}")
                .unit("UNIT_COUNT"),
                yaxis_label="Exe nodes", display_legend=True)
    )


def _build_cpu(d):
    def thread_cpu(sensor):
        return MultiSensor(*(
            (MonitoringExpr(ExeNodeCpu(sensor).value("thread", thread)) / 100).alias(thread)
            for thread in ("Job", "Control", "JobEnvironment")
        ))

    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .unit("UNIT_COUNT")
        .min(0)
        .row()
            .cell("CPU Usage by Thread", thread_cpu("yt.resource_tracker.total_cpu"),
                yaxis_label="CPU cores", display_legend=True)
            .cell("CPU Wait by Thread", thread_cpu("yt.resource_tracker.cpu_wait"),
                yaxis_label="CPU cores", display_legend=True)
    )


def _build_memory(d):
    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .unit("UNIT_BYTES_SI")
        .min(0)
        .row()
            .cell("Node Process Memory", MultiSensor(
                ExeNodeMemory("yt.memory.generic.bytes_in_use_by_app").legend_format("allocator usage"),
                ExeNodeMemory("yt.resource_tracker.memory_usage.rss").legend_format("RSS")),
                display_legend=True)
            .cell("Daemon Container Memory", MultiSensor(
                ExeNodePorto("yt.porto.memory.memory_usage").legend_format("usage"),
                ExeNodePorto("yt.porto.memory.memory_limit").legend_format("limit"))
                .value("container_category", "daemon"),
                display_legend=True)
    )


def _build_resources(d):
    occupied_slots = MonitoringExpr(ExeNode("yt.job_controller.resource_usage.user_slots")
        .value("state", "acquired|releasing")).series_sum("cluster", "host")

    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .min(0)
        .row()
            .cell("User Slots", MultiSensor(
                ExeNode("yt.job_controller.resource_limits.user_slots").legend_format("limit"),
                occupied_slots.alias("occupied (acquired + releasing)")),
                yaxis_label="Slots", display_legend=True)
            .cell("User Job Memory Limit", ExeNode("yt.job_controller.resource_limits.user_memory")
                .unit("UNIT_BYTES_SI"))
        .row()
            .cell("Pending User Slots", ExeNode("yt.job_controller.resource_usage.user_slots")
                .value("state", "pending").unit("UNIT_COUNT"),
                yaxis_label="Slots",
                description="Slots requested by allocations waiting for resource acquisition. "
                    "Pending slots are not included in occupied slots.")
    )


def _build_jobs(d, backend):
    def node_jobs(finished_state):
        return (ExeNode("yt.job_controller.job_final_state.rate")
            .value("origin", "scheduler").value("state", finished_state))

    total_finished = (MonitoringExpr(node_jobs("completed|failed|aborted"))
        .series_sum("cluster", "host").moving_avg("5m"))

    def finished_fraction(state):
        finished = MonitoringExpr(node_jobs(state)).series_sum("cluster", "host").moving_avg("5m")
        # Final-state counters are registered lazily; a missing numerator is zero
        # only where the denominator provides evidence of finished jobs.
        if backend == "monitoring":
            finished = finished.flatten(total_finished * 0)
        else:
            finished = finished | (total_finished * 0)
        finished = finished.series_sum("cluster", "host")
        return (100 * finished / total_finished.drop_below(1e-6)).alias(state)

    fractions = MultiSensor(*(
        finished_fraction(state)
        for state in ("failed", "aborted")
    )).unit("UNIT_PERCENT").range(0, 100)

    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .min(0)
        .row()
            .cell("Node Completed Jobs", node_jobs("completed"))
            .cell("Node Failed Jobs", node_jobs("failed"))
        .row()
            .cell("Node Aborted Jobs", node_jobs("aborted"))
            .cell("Finished Job Fractions (5m Moving Average)", fractions, display_legend=True,
                description="Fractions of completed, failed, and aborted jobs over the last 5 minutes. "
                    "Aborted jobs include normal cancellations and preemption.")
        .row()
            .cell("Active and Running Jobs", MultiSensor(
                ExeNode("yt.job_controller.active_job_count").legend_format("active"),
                ExeNode("yt.job_controller.running_job_count").legend_format("running state"))
                .value("origin", "scheduler"),
                display_legend=True,
                description="The Running state includes job preparation. Active jobs also include finished jobs "
                    "awaiting removal; the difference does not measure preparation backlog.")
            .cell("Allocations Waiting for Resources", ExeNode("yt.job_controller.waiting_allocation_count")
                .value("origin", "scheduler"))
        .row()
            .cell("Job Proxy Signal Exits", ExeNode("yt.job_controller.job_proxy_process_exit.count.rate")
                .all("terminated_by_signal").aggr("non_zero_exit_code")
                .legend_format("{{terminated_by_signal}}"),
                display_legend=True,
                description="SIGKILL may accompany normal job abortion and does not by itself indicate a node failure.")
            .cell("Job Proxy Nonzero Exit Codes", ExeNode("yt.job_controller.job_proxy_process_exit.count.rate")
                .all("non_zero_exit_code").aggr("terminated_by_signal")
                .legend_format("exit {{non_zero_exit_code}}"),
                display_legend=True)
    )


def _build_slow_jobs(d):
    def slow_fraction(sensor, threshold):
        histogram = MonitoringExpr(ExeNode(sensor).all("bin")).moving_avg("5m")
        samples = MonitoringExpr.func("histogram_count", '"bin"', histogram)
        fast_fraction = MonitoringExpr.func("histogram_cdfp",
            MonitoringExpr.func("as_vector", 0),
            MonitoringExpr.func("as_vector", threshold),
            '"bin"', histogram)
        return ((100 - fast_fraction) * (samples / samples.drop_below(1e-6))).alias(f"> {threshold}s")

    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .unit("UNIT_PERCENT")
        .range(0, 100)
        .row()
            .cell("Slow SettleJob Fraction (5m Moving Average)",
                slow_fraction("yt.job_controller.allocations.settle_job_duration", 1),
                display_legend=True,
                description="Fraction of SettleJob requests taking over 1 second over the last 5 minutes. "
                    "This measures job settlement, not the full job preparation time.")
            .cell("Slow Cleanup Fractions (5m Moving Average)", MultiSensor(
                slow_fraction("yt.job_controller.job_cleanup_duration", 5),
                slow_fraction("yt.job_controller.job_cleanup_duration", 30)),
                display_legend=True,
                description="Fractions of job cleanups taking over 5 and 30 seconds over the last 5 minutes. "
                    "Intervals without recorded events are omitted.")
    )


def _build_network(d):
    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .value("container_category", "pod")
        .row()
            .cell("Nodes Network RX", ExeNodePorto("yt.porto.network.rx_bytes"))
            .cell("Nodes Network TX", ExeNodePorto("yt.porto.network.tx_bytes"))
    )


def _build_alerts(d):
    d.add(Rowset()
        .stack(True)
        .aggr("container")
        .row()
            .cell("Node Alerts", ExeNode("yt.cluster_node.alerts").all("error_code"))
    )


def _build_porto_info(d):
    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .row()
            .cell("Volume Surplus", ExeNode("yt.exec_node.*orto.volume_surplus").aggr("location_id"))
            .cell("Layer Surplus", ExeNode("yt.exec_node.*porto.layer_surplus").aggr("location_id"))
        .row()
            .cell("Porto Volume Count", ExeNodePorto("yt.porto.volume.count")
                .value("container_category", "daemon")
                .all("backend"))
        .row()
            # COMPAT(pogorelov): Remove "job_envir onment" after 24.1.
            .cell("Porto Commands",
                ExeNode("yt.exec_node.job_envir onment.porto.command*.rate|yt.exec_node.job_environment.porto.command*.rate")
                .value("command", NotEquals("-"))
            )
    )


def _build_volume_errors(d):
    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .row()
            .cell("Volume Creation Errors", ExeNode("yt.volumes.create_errors.rate"))
            .cell("Volume Removal Errors", ExeNode("yt.volumes.remove_errors.rate"))
    )


def _build_cache_miss_info(d):
    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .row()
            .cell("Cache Miss Artifacts Size", ExeNode("yt.job_controller.chunk_cache.cache_miss_artifacts_size.rate"))
    )

def _build_page_faults(d):
    d.add(Rowset()
        .stack(False)
        .aggr("container")
        .row()
            .cell("Page Faults", ExeNodePorto("yt.porto.memory.major_page_faults").all("container_category"))
    )


def build_exe_nodes(backend="monitoring"):
    d = Dashboard()
    d.set_cell_per_row(2)

    _build_versions(d)
    _build_cpu(d)
    _build_memory(d)
    _build_resources(d)
    _build_jobs(d, backend)
    if backend == "monitoring":
        _build_slow_jobs(d)
    _build_network(d)
    _build_alerts(d)

    _build_porto_info(d)

    _build_volume_errors(d)

    _build_cache_miss_info(d)

    _build_page_faults(d)

    d.set_monitoring_serializer_options(dict(default_row_height=8))

    d.set_title("Exe Nodes [AUTOGENERATED]")

    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter(
            "yt",
            "cluster",
            EXE_NODES_DASHBOARD_DEFAULT_CLUSTER,
            selectors='{project="yt", service="exe_node", cluster!="-"}'),
        backends=["monitoring"],
    )
    d.add_parameter(
        "cluster", "Cluster",
        GrafanaTextboxDashboardParameter(EXE_NODES_DASHBOARD_DEFAULT_CLUSTER),
        backends=["grafana"],
    )

    d.add_parameter(
        "host", "Host",
        MonitoringLabelDashboardParameter(
            "yt", "host", "Aggr",
            selectors='{project="yt", cluster="{{cluster}}", service="exe_node"}'),
        backends=["monitoring"],
    )
    d.add_parameter(
        "host", "Host",
        GrafanaTextboxDashboardParameter(".*"),
        backends=["grafana"],
    )
    d.value("host", TemplateTag("host"))

    return d
