# flake8: noqa

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell
from .common.sensors import *
from yt_dashboard_generator.specific_sensors.monitoring import MonitoringExpr as Expr
from yt_dashboard_generator.backends.monitoring import MonitoringTag, MonitoringLabelDashboardParameter
from yt_dashboard_generator.specific_tags.tags import TemplateTag

percentiles = [0.1, 5, 10, 20, 50, 80, 90, 95, 100]

def make_row(row, expr, name):
    expr = Expr(expr).all("container").series_sum("container")
    return (row
        .cell(f"{name} (percentiles)", expr.series_percentile(percentiles))
        .cell(
            f"{name} (max/avg)",
            MultiSensor(
                (expr.series_max() / expr.series_avg()).alias("max/avg")
            ))
    )

def build1():
    def moving_avg(x):
        return Expr(x).moving_avg("10m")

    r = Rowset().stack(False)
    sensors = [
        [moving_avg(NodeTablet("yt.tablet_node.write.data_weight.rate").aggr("user")), "Write data weight"],
        [
            moving_avg(
                NodeTablet("yt.tablet_node.lookup.data_weight.rate|yt.tablet_node.lookup.hunks.data_weight.rate")
                    .aggr("user")),
            "Lookup data weight"
        ],
        [moving_avg(NodeTablet("yt.tablet_node.lookup.cpu_time.rate").aggr("user")), "Lookup CPU time"],
        [
            moving_avg(
                NodeTablet("yt.tablet_node.*data_bytes_transmitted.rate").aggr("user").all("medium")),
             "Bytes received"
        ],
        [NodeTablet("yt.tablet_node.tablet.compressed_data_size"), "Compressed size"],
        [NodeTablet("yt.tablet_node.tablet.tablet_count"), "Tablet count"],
    ]

    for expr, name in sensors:
        r = make_row(r.row(), expr, name)

    r = (r.row()
        .cell(
            "Tablet balancer moves",
            TabletBalancer("yt.tablet_balancer.tablet_balancer.parameterized_moves.rate").all("container", "group"))
        .cell(
            "Tablet balancer reshards",
            TabletBalancer("yt.tablet_balancer.tablet_balancer.*reshard*.rate").all("container", "group"))
    )

    return r.owner

def build():
    rowsets = [
        build1()
    ]

    d = Dashboard()
    for r in rowsets:
        d.add(r)
    d.value(MonitoringTag("tablet_cell_bundle"), TemplateTag("tablet_cell_bundle"))
    d.value(MonitoringTag("table_path"), TemplateTag("table_path"))
    d.add_parameter("cluster", "YT cluster", MonitoringLabelDashboardParameter("yt", "cluster", "CLUSTER"))
    d.add_parameter(
        "tablet_cell_bundle",
        "Bundle",
        MonitoringLabelDashboardParameter("yt", "tablet_cell_bundle", "default"))
    d.add_parameter(
        "table_path",
        "Table path",
        MonitoringLabelDashboardParameter("yt", "table_path", "-"))
    return d
