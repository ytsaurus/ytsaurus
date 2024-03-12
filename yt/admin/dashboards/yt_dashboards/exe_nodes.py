# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import (
    ExeNode, ExeNodeCpu, ExeNodeMemory, ExeNodePorto,
    CA, NodeMonitor,
    yt_host,
)

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor
from yt_dashboard_generator.taggable import NotEquals

from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr

def _build_versions(d):
    d.add(Rowset()
        .stack(True)
        .aggr(yt_host)
        .row()
            .cell("Versions", ExeNode("yt.build.version"))
    )

def _build_cpu(d):
    def exe_node_thread_cpu(thread):
        return ExeNodeCpu("yt.resource_tracker.user_cpu").value("thread", thread)

    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .min(0)
        .row()
            .cell("Job CPU Wait", exe_node_thread_cpu("Job"))
            .cell("Control", exe_node_thread_cpu("Control"))
        .row()
            .cell("JobEnvironment CPU Wait", exe_node_thread_cpu("JobEnvironment"))
    )

def _build_memory(d):
    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .row()
            .cell("Nodes Used Memory", ExeNodeMemory("yt.memory.generic.bytes_in_use_by_app"))
            .cell("Total Nodes User Job Memory Limit", ExeNode("yt.job_controller.resource_limits.user_memory"))
    )

def _build_jobs(d):
    def CA_jobs(finished_state):
        return CA("yt.controller_agent.jobs.{}_job_count.rate".format(finished_state)).aggr("job_type").aggr("tree")

    def node_jobs(finished_state):
        return ExeNode("yt.job_controller.job_final_state.rate").value("state", finished_state)

    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .row()
            .cell(
                "Controller Started And Completed Jobs",
                CA("yt.controller_agent.jobs.started_job_count.rate|yt.controller_agent.jobs.completed_job_count.rate").aggr("interruption_reason"))
            .cell(
                "Node Completed Jobs",
                node_jobs("completed").aggr("origin"))
        .row()
            .cell("Controller Failed Jobs", CA_jobs("failed"))
            .cell("Node Failed Jobs", node_jobs("failed").aggr("origin"))
        .row()
            .cell("Controller Aborted Jobs", CA_jobs("aborted").aggr("abort_reason"))
            .cell("Node Aborted Jobs", node_jobs("aborted").aggr("origin"))
        .row()
            .cell("Active Job Count", ExeNode("yt.job_controller.active_job_count").aggr("origin"))
    )

def _build_network(d):
    rowset = Rowset().stack(False)
    for metric_name, service_name in (
        (f"yt_{{{{cluster}}}}_*exe_nodes|yt_{{{{cluster}}}}_cloud_nodes|yt_{{{{cluster}}}}_*gpu_nodes", "Nodes"), (f"yt_{{{{cluster}}}}_controller_agents", "Controller Agent")
    ):
        row = rowset.row()
        for sensor, sensor_name_suffix in (("RxBytes", "RX"), ("TxBytes", "TX")):
            row.cell(
                f"{service_name} Network {sensor_name_suffix}",
                Sensor(f"/Porto/Containers/Ifs/{sensor}", sensor_tag_name="path")
                    .value("service", "porto_iss")
                    .value("intf", "veth")
                    .value("host", "cluster")
                    .value("name", metric_name)
            )
    d.add(rowset)

def _build_heartbeat_info(d):
    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .row()
            .cell("Controller Agent Heartbeats Throttled", CA("yt.controller_agent.job_tracker.node_heartbeat.throttled_heartbeat_count.rate"))
    )

def _build_alerts_and_slots(d):
    d.add(Rowset()
        .stack(True)
        .row()
            .cell("Node Alerts", ExeNode("yt.cluster_node.alerts").aggr(yt_host).all("error_code"))
            .cell("Node Disabled Slots", MonitoringExpr(NodeMonitor("node.resources.exec.cpu"))
                .value("presented_in_yp", "true")
                .value("host", "none")
                .value("flavor", NotEquals("*tablet*|*dat*|*gpu*|*journal*"))
                .value("state", "online")
                .value("disabled_slots_reason", NotEquals("none"))
                .series_sum("disabled_slots_reason")
                .replace_nan(0))
    )

def _build_porto_info(d):
    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .row()
            .cell("Volume Surplus", ExeNode("yt.exec_node.*orto.volume_surplus").aggr("location_id"))
            .cell("Layer Surplus", ExeNode("yt.exec_node.*porto.layer_surplus").aggr("location_id"))
        .row()
            .cell("Porto Volume Count", ExeNodePorto("yt.porto.volume.count")
                .value("container_category", "daemon")
                .all("backend"))
        .row()
            .cell("Porto Commands", ExeNode("yt.exec_node.job_envir onment.porto.command*.rate"))
    )

def _build_volume_errors(d):
    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .row()
            .cell("Volume Creation Errors", ExeNode("yt.volumes.create_errors.rate"))
            .cell("Volume Removal Errors", ExeNode("yt.volumes.remove_errors.rate"))
    )

def _build_cache_miss_info(d):
    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .row()
            .cell("Cache Miss Artifacts Size", ExeNode("yt.job_controller.chunk_cache.cache_miss_artifacts_size.rate"))
    )

def _build_page_faults(d):
    d.add(Rowset()
        .stack(False)
        .aggr(yt_host)
        .row()
            .cell("Page Faults", ExeNodePorto("yt.porto.memory.major_page_faults").all("container_category"))
    )

def build_exe_nodes():
    d = Dashboard()

    _build_versions(d)
    _build_cpu(d)
    _build_memory(d)
    _build_jobs(d)
    _build_network(d)
    _build_heartbeat_info(d)
    _build_alerts_and_slots(d)

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
            "-")
    )

    return d
