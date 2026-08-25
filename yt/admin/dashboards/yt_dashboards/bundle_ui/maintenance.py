# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor, Text, EmptyCell
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

from ..common.sensors import *

##################################################################


drills_mode_hint = """\
When bundle enters drills mode, all tablet cells become failed.
"""


def build_cell_health():
    return (Rowset()
        .row()
            .cell(
                "Tablet cell health",
                MonitoringExpr(Master("yt.tablet_server.tablet_cell_count"))
                    .all("container", "health"))
            .cell(
                "Drills mode",
                BundleController("yt.bundle_controller.scan_bundles_alarms_count.rate")
                    .all("host")
                    .value("alarm_id", "bundle_has_drills_mode_enabled")
                    .aggr("data_center"),
                description=drills_mode_hint)
    ).owner


def build_user_hydra():
    return (Rowset()
        .aggr("cell_id")
        .row()
            .cell("Hydra restart rate", NodeTablet("yt.hydra.restart_count.rate")
                .aggr(MonitoringTag("host"))
                .all("#B", "reason"))
            .cell(
                "Cell move rate",
                MonitoringExpr(Master("yt.tablet_server.tablet_tracker.tablet_cell_moves.rate"))
                    .all("container")
                    .series_sum())
    ).owner


def build_tablet_balancer():
    tb = Master("yt.tablet_server.tablet_balancer.{}.rate")
    stb = TabletBalancer("yt.tablet_balancer.tablet_balancer.{}.rate")
    return (Rowset()
        .stack(False)
        .row()
            .cell("Tablet balancer moves", MultiSensor(
                    MonitoringExpr(tb("in_memory_moves").aggr("#H")).alias("in-memory tablet moves"),
                    MonitoringExpr(tb("ext_memory_moves").aggr("#H")).alias("ordinary tablet moves"),
                    MonitoringExpr(stb("in_memory_moves").all("container").aggr("group", "table_path"))
                        .alias("in-memory tablet moves {{container}}"),
                    MonitoringExpr(stb("ordinary_moves").all("container").aggr("group", "table_path"))
                        .alias("ordinary tablet moves {{container}}")
                ))
            .cell("Tablet balancer reshards", MultiSensor(
                MonitoringExpr(tb("tablet_merges").aggr("#H")).alias("tablet merges"),
                MonitoringExpr(stb("non_trivial_reshards").all("container").aggr("group", "table_path"))
                    .alias("non-trivial reshards {{container}}"),
                MonitoringExpr(stb("tablet_merges").all("container").aggr("group", "table_path"))
                    .alias("tablet merges {{container}}"),
                MonitoringExpr(stb("tablet_splits").all("container").aggr("group", "table_path"))
                    .alias("tablet splits {{container}}")
            ))
        ).owner


def build_bundle_controller():
    bc = BundleController("yt.bundle_controller.resource.{}")
    return (Rowset()
        .aggr(MonitoringTag("host"))
        .row()
            .stack()
            .all(MonitoringTag("host"))
            .cell("Node restarts", MonitoringExpr(TabNode("yt.server.restarted")
                .top()
                .value("window", "5min")).alias("{{container}}"))
            .cell("Node OOMs", MultiSensor(
                    MonitoringExpr(TabNodePorto("yt.porto.memory.oom_kills"))
                        .value("container_category", "pod")
                        .diff()
                        .top_max(10)
                        .alias("porto oom kills {{container}}"),
                    (MonitoringExpr(TabNodeYtcfgen("yt.error_watcher.ooms"))
                        .diff()
                        .drop_below(0)
                        + MonitoringExpr.constant_line(0))
                        .top_max(10)
                        .alias("memory limit kills {{container}}")
                ))
        .row()
            .stack()
            .cell("Overload Controller", MonitoringExpr(NodeTablet("yt.tablet_node.overload_controller.overloaded.rate")
                .all("tracker")).alias("{{tracker}}"))
            .cell("Bundle Controller Alerts", MonitoringExpr(BundleController("yt.bundle_controller.scan_bundles_alarms_count.rate")
                .all("alarm_id", "data_center")).alias("{{alarm_id}}"))
        .row()
            .stack()
            .cell("Target tablet node count", MonitoringExpr(bc("target_tablet_node_count")
                .all("instance_size")).alias("target node count of size '{{instance_size}}'"))
            .cell("Alive tablet node count", MultiSensor(
                    MonitoringExpr(bc("alive_tablet_node_count")
                        .all("instance_size")).alias("alive bundle nodes of size '{{instance_size}}'"),
                    MonitoringExpr(bc("using_spare_node_count")).alias("assigned spare nodes")
                ))
        .row()
            .stack()
            .cell("Tablet node assignments", MultiSensor(
                    MonitoringExpr(bc("assigning_spare_nodes")).alias("assigning spare nodes"),
                    MonitoringExpr(bc("releasing_spare_nodes")).alias("releasing spare nodes"),
                    MonitoringExpr(bc("assigning_tablet_nodes")).alias("assigning new bundle nodes")
                ))
            .cell("Special tablet node states", MultiSensor(
                    MonitoringExpr(bc("maintenance_requested_node_count")).alias("maintenance requested nodes"),
                    MonitoringExpr(bc("decommissioned_node_count")).alias("decommissioned nodes"),
                    MonitoringExpr(bc("offline_node_count")).alias("offline nodes")
                ))
        .row()
            .cell("Inflight tablet node request count", MultiSensor(
                    MonitoringExpr(bc("inflight_node_allocations_count")).alias("inflight node allocations"),
                    MonitoringExpr(bc("inflight_node_deallocations_count")).alias("inflight node deallocations"),
                    MonitoringExpr(bc("inflight_cell_removal_count")).alias("inflight cell removal")
                ).stack(True))
            .cell("Inflight tablet node request age", MultiSensor(
                    MonitoringExpr(bc("node_allocation_request_age")).alias("node allocation age max"),
                    MonitoringExpr(bc("node_deallocation_request_age")).alias("node deallocation age max"),
                    MonitoringExpr(bc("removing_cells_age")).alias("cell removal age max")
                ).stack(False))
        .row()
            .stack()
            .cell("Target rpc proxy count", MonitoringExpr(bc("target_rpc_proxy_count")
                .all("instance_size")).alias("target proxy count of size '{{instance_size}}'"))
            .cell("Alive rpc proxy count", MultiSensor(
                    MonitoringExpr(bc("alive_rpc_proxy_count")
                        .all("instance_size")).alias("alive bundle proxies of size '{{instance_size}}'"),
                    MonitoringExpr(bc("using_spare_proxy_count")).alias("assigned spare proxies")
                ))
        .row()
            .cell("Inflight rpc proxy request count", MultiSensor(
                    MonitoringExpr(bc("inflight_proxy_allocation_counter")).alias("inflight proxy allocations"),
                    MonitoringExpr(bc("inflight_proxy_deallocation_counter")).alias("inflight proxy deallocations"),
                ).stack(True))
            .cell("Inflight rpc proxy request age", MultiSensor(
                    MonitoringExpr(bc("proxy_allocation_request_age")).alias("proxy allocation age max"),
                    MonitoringExpr(bc("proxy_deallocation_request_age")).alias("proxy deallocation age max"),
                ).stack(False))
        ).owner


LOGGING_PROFILING_GREETING = """\
Logging and profiling overloads do not directly affect user requests, \
but they make diagnostics more difficult. If the charts below show \
overloads, take appropriate action and contact support.
"""


def build_logging_profiling():
    return (Rowset()
        .row(height=2)
            .cell("", Text(LOGGING_PROFILING_GREETING))
            .cell("", EmptyCell())
            .cell("", EmptyCell())
        .row()
            .top()
            .all("host")
            .cell(
                "Tablet node dropped logs",
                TabNodeInternal("yt.logging.dropped_events.rate"))
            .cell(
                "Tablet node Logging/LogCompress utilization",
                TabNodeCpu("yt.resource_tracker.utilization")
                    .value("thread", "Logging|LogCompress"))
            .cell(
                "Tablet node profiling threads utilization",
                TabNodeCpu("yt.resource_tracker.utilization")
                    .value("thread", "Prof*"))
        .row()
    )
