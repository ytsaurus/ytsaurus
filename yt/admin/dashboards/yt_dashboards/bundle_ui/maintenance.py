# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

from ..common.sensors import *

##################################################################


def build_user_hydra():
    return (Rowset()
        .aggr("cell_id")
        .row()
            .cell("Hydra restart rate", NodeTablet("yt.hydra.restart_count.rate")
                .aggr(MonitoringTag("host"))
                .all("#B", "reason"))
            .cell("Cell move rate", Master("yt.tablet_server.tablet_tracker.tablet_cell_moves.rate"))
    ).owner


def build_tablet_balancer():
    tb = Master("yt.tablet_server.tablet_balancer.{}.rate")
    stb = TabletBalancer("yt.tablet_balancer.tablet_balancer.{}.rate")
    return (Rowset()
        .all("#HB", "container")
        .stack(False)
        .row()
            .cell("Tablet balancer moves", MultiSensor(
                    tb("*_memory_moves"),
                    stb("*moves").aggr("group", "table_path")
                ))
            .cell("Tablet balancer reshards", MultiSensor(
                tb("tablet_merges"),
                stb("non_trivial_reshards").aggr("group", "table_path"),
                stb("tablet_merges").aggr("group", "table_path"),
                stb("tablet_splits").aggr("group", "table_path")
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
            .cell("Node OOMs", MonitoringExpr(TabNodePorto("yt.porto.memory.oom_kills"))
                .value("container_category", "pod")
                .diff()
                .top_max(10)
                .alias("{{container}}"))
        .row()
            .stack()
            .cell("Overload Controller", MonitoringExpr(NodeTablet("yt.tablet_node.overload_controller.overloaded.rate")
                .all("tracker")).alias("{{tracker}}"))
            .cell("Bundle Controller Alerts", MonitoringExpr(BundleController("yt.bundle_controller.scan_bundles_alarms_count.rate")
                .all("alarm_id")).alias("{{alarm_id}}"))
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
