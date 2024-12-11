# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.specific_sensors.monitoring import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor

from .common.sensors import *

def add_queue_parameters(dashboard: Dashboard, default_queue_cluster="", default_queue_path=""):
    dashboard.add_parameter(
        "queue_cluster",
        "Queue cluster",
        MonitoringLabelDashboardParameter("yt", "queue_cluster", default_queue_cluster),
        backends=["monitoring"])

    dashboard.add_parameter(
        "queue_path",
        "Queue path",
        MonitoringLabelDashboardParameter("yt", "queue_path", default_queue_path),
        backends=["monitoring"])

    dashboard.add_parameter(
        "queue_cluster",
        "Queue cluster",
        GrafanaTextboxDashboardParameter(default_queue_cluster),
        backends=["grafana"])

    dashboard.add_parameter(
        "queue_path",
        "Queue path",
        GrafanaTextboxDashboardParameter(default_queue_path),
        backends=["grafana"])

    dashboard.value("queue_cluster", TemplateTag("queue_cluster"))
    dashboard.value("queue_path", TemplateTag("queue_path"))


def add_consumer_parameters(dashboard: Dashboard):
    dashboard.add_parameter(
        "consumer_cluster",
        "Consumer cluster",
        MonitoringLabelDashboardParameter("yt", "consumer_cluster", ""),
        backends=["monitoring"])

    dashboard.add_parameter(
        "consumer_path",
        "Consumer path",
        MonitoringLabelDashboardParameter("yt", "consumer_path", ""),
        backends=["monitoring"])

    dashboard.add_parameter(
        "consumer_cluster",
        "Consumer cluster",
        GrafanaTextboxDashboardParameter(""),
        backends=["grafana"])

    dashboard.add_parameter(
        "consumer_path",
        "Consumer path",
        GrafanaTextboxDashboardParameter(""),
        backends=["grafana"])

    dashboard.value("consumer_cluster", TemplateTag("consumer_cluster"))
    dashboard.value("consumer_path", TemplateTag("consumer_path"))


# NB(apachee): Followed a pattern from other dashboards to return a list of rowsets instead of one.
# We do not need that now, but it might be helpful later, and doesn't really affect anything.
def build_queue_metric_rowsets():
    rows_written_and_trimmed = (MultiSensor(
        QueueAgent("yt.queue_agent.queue_partition.rows_written.rate")
            .aggr("partition_index")
            .legend_format("Rows written"),
        QueueAgent("yt.queue_agent.queue_partition.rows_trimmed.rate")
            .aggr("partition_index")
            .legend_format("Row trimmed")
        ).unit("UNIT_COUNTS_PER_SECOND"))

    data_weight_written = (QueueAgent("yt.queue_agent.queue_partition.data_weight_written.rate")
        .aggr("partition_index")
        .unit("UNIT_BYTES_SI_PER_SECOND"))

    partition_count = (QueueAgent("yt.queue_agent.queue.partitions")
        .unit("UNIT_COUNT"))

    consumer_count = (MultiSensor(
        QueueAgent("yt.queue_agent.queue.consumers")
            .value("vital", "true")
            .legend_format("Vital"),
        QueueAgent("yt.queue_agent.queue.consumers")
            .value("vital", "false")
            .legend_format("Non-vital"))
        ).unit("UNIT_COUNT")

    queue_row_count = (QueueAgent("yt.queue_agent.queue_partition.row_count")
        .aggr("partition_index")
        .legend_format("Row count")
        .unit("UNIT_COUNT"))

    queue_data_weight = (QueueAgent("yt.queue_agent.queue_partition.data_weight")
        .aggr("partition_index")
        .legend_format("Data weight")
        .unit("UNIT_BYTES_SI"))

    return [
        Rowset()
            .row()
                .cell("Rows written & trimmed", rows_written_and_trimmed)
                .cell("Data weight written", data_weight_written)
            .row()
                .cell("Partition count", partition_count)
                .cell("Consumer count", consumer_count)
            .row()
                .cell("Queue stored row count", queue_row_count)
                .cell("Queue stored data weight", queue_data_weight)
    ]

def build_queue_metrics(backend: str):
    rowsets = build_queue_metric_rowsets()

    dashboard = Dashboard()
    for rowset in rowsets:
        dashboard.add(rowset)

    dashboard.set_title("Queue metrics dashboard for UI")
    dashboard.set_description("")
    add_queue_parameters(dashboard)

    if backend == "monitoring":
        dashboard.all("cluster")

    return dashboard

def build_queue_consumer_metric_rowsets(backend: str):
    rows_consumed = (QueueAgent("yt.queue_agent.consumer_partition.rows_consumed.rate")
        .aggr("partition_index")
        .legend_format("Rows consumed")
        .unit("UNIT_COUNT"))

    data_weight_consumed = (QueueAgent("yt.queue_agent.consumer_partition.data_weight_consumed.rate")
        .aggr("partition_index")
        .legend_format("Data weight consumed")
        .unit("UNIT_BYTES_SI_PER_SECOND"))

    total_row_lag = (QueueAgent("yt.queue_agent.consumer_partition.lag_rows.sum")
        .aggr("partition_index")
        .legend_format("Row lag")
        .unit("UNIT_COUNT"))

    max_time_lag = (QueueAgent("yt.queue_agent.consumer_partition.lag_time.max")
        .aggr("partition_index")
        .legend_format("Time lag")
        .unit("UNIT_SECONDS"))

    lag_time_histogram = (QueueAgent("yt.queue_agent.consumer_partition.lag_time_histogram")
        .aggr("partition_index")
        .legend_format(TemplateTag("bin"))
        .unit("UNIT_COUNT")
        .stack(True))

    # TODO(apachee): Implement for grafana
    monitoring_top50_row_lag = (MonitoringExpr(QueueAgent("yt.queue_agent.consumer_partition.lag_rows.max"))
        .all("partition_index")
        .top_last(50)
        .alias("Partition row lag [{{partition_index}}]")
        .stack(False))

    # TODO(apachee): Implement for grafana
    monitoring_top50_time_lag = (MonitoringExpr(QueueAgent("yt.queue_agent.consumer_partition.lag_time.max"))
        .all("partition_index")
        .top_last(50)
        .alias("Partition time lag [{{partition_index}}]")
        .stack(False))

    rowset = Rowset()
    (rowset
        .row()
            .cell("Rows consumed", rows_consumed)
            .cell("Data weight consumed", data_weight_consumed)
        .row()
            .cell("Total row lag", total_row_lag)
            .cell("Max time lag", max_time_lag))

    if backend == "monitoring":
        # XXX(apachee): Not sure how to configure histogram metric in grafana, so
        # it is disabled for grafana backend currently, but might be fixed in the future.
        (rowset
            .row()
                .cell("Row lag (top 50)", monitoring_top50_row_lag)
                .cell("Time lag (top 50)", monitoring_top50_time_lag)
            .row()
                .cell("Lag time histogram", lag_time_histogram))

    return [rowset]


def build_queue_consumer_metrics(backend: str):
    rowsets = build_queue_consumer_metric_rowsets(backend)

    dashboard = Dashboard()
    for rowset in rowsets:
        dashboard.add(rowset)

    dashboard.set_title("Consumer metrics dashboard for UI")
    dashboard.set_description("")
    add_consumer_parameters(dashboard)
    add_queue_parameters(dashboard, default_queue_cluster="-", default_queue_path="-")

    if backend == "monitoring":
        dashboard.all("cluster")

    return dashboard
