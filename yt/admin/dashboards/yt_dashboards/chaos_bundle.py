from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import EmptyCell, MultiSensor
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.taggable import NotEquals

from .common.sensors import Master, Node, NodeCpu, NodeTablet, TabNodeCpu


MAX_MEANINGFUL_REPLICATION_LAG_SECONDS = 10 * 365 * 24 * 60 * 60
DEFAULT_CHAOS_CLUSTERS = "poincare-alpha|poincare-beta"
DEFAULT_REPLICA_CLUSTER = "seneca-klg"

# Bundle UI contract: the chaos bundle and its replica tablet bundle have the same name.


def _replication_card_lag():
    lag = (
        NodeTablet("yt.chaos_node.replication_card.lag_time")
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("chaos_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", NotEquals("Aggr"))
        .value("chaos_replicated_table_path", NotEquals("-"))
        .value("replica_cluster_name", NotEquals("-"))
        .value("replica_id", NotEquals("-"))
        .value("replica_path", NotEquals("-"))
        .value("replication_card_id", NotEquals("-"))
    )

    return (
        MonitoringExpr(lag)
        .drop_above(MAX_MEANINGFUL_REPLICATION_LAG_SECONDS)
        .series_max("chaos_replicated_table_path", "replica_cluster_name")
        .top_max(20)
        .alias("{{replica_cluster_name}}; {{chaos_replicated_table_path}}")
        .unit("UNIT_SECONDS")
    )


def _table_puller_sensor(sensor):
    return MonitoringExpr(
        NodeTablet(sensor)
        .value("cluster", TemplateTag("cluster"))
        .value("tablet_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", "Aggr")
        .all("table_path"))


def _table_puller_lag():
    lag = (
        NodeTablet("yt.tablet_node.table_puller.lag_time.max")
        .value("cluster", TemplateTag("cluster"))
        .value("tablet_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", NotEquals("Aggr"))
        .value("table_path", NotEquals("-"))
    )
    return (
        MonitoringExpr(lag)
        .series_max("cluster", "table_path")
        .top_max(10)
        .alias("{{cluster}}; {{table_path}}")
        .unit("UNIT_SECONDS")
    )


def _table_puller_time(sensor, stage):
    expression = (
        NodeTablet(sensor)
        .value("cluster", TemplateTag("cluster"))
        .value("tablet_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", NotEquals("Aggr"))
        .value("table_path", NotEquals("-"))
    )
    return (
        MonitoringExpr(expression)
        .series_max("cluster", "table_path")
        .top_max(10)
        .alias("{{cluster}}; " + stage + "; {{table_path}}")
        .unit("UNIT_SECONDS")
    )


def _table_puller_throttle_time():
    return _table_puller_time("yt.tablet_node.table_puller.throttle_time.max", "throttle")


def _replicator_utilization():
    expression = (
        TabNodeCpu("yt.resource_tracker.utilization")
        .value("cluster", TemplateTag("cluster"))
        .value("tablet_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", NotEquals("Aggr"))
        .value("thread", "Replicator")
    )
    return (
        MonitoringExpr(expression)
        .series_max("cluster", "host")
        .top_max(10)
        .alias("{{cluster}}; {{host}}")
    )


def _table_puller_errors():
    return MultiSensor(
        _table_puller_sensor("yt.tablet_node.table_puller.error_count.rate")
        .top_max(10)
        .alias("{{cluster}}; errors; {{table_path}}"),
        _table_puller_sensor("yt.tablet_node.table_puller.fatal_error_count.rate")
        .top_max(10)
        .alias("{{cluster}}; fatal; {{table_path}}"),
    )


def _replica_mode_switches():
    return (
        MonitoringExpr(
            NodeTablet("yt.chaos_node.rtt.replica_mode_switch_count.rate")
            .value("cluster", TemplateTag("chaos_cluster"))
            .value("chaos_cell_bundle", TemplateTag("chaos_cell_bundle"))
            .value("host", "Aggr")
            .all("table_path", "replica_cluster", "tablet_cell_bundle"))
        .top_max(20)
        .alias("{{replica_cluster}}; {{table_path}}")
    )


def _table_puller_cluster_total(sensor, unit=None):
    expression = _table_puller_sensor(sensor).value("table_path", "-").alias("{{cluster}}")
    if unit is not None:
        expression = expression.unit(unit)
    return expression


def _build_chaos_bundle_dashboard(title, rowset, with_replica_cluster=False):
    dashboard = Dashboard()
    dashboard.set_title(title)
    dashboard.add(rowset)

    dashboard.add_parameter(
        "chaos_cluster",
        "Chaos clusters",
        MonitoringLabelDashboardParameter("yt", "cluster", DEFAULT_CHAOS_CLUSTERS),
    )
    dashboard.add_parameter(
        "chaos_cell_bundle",
        "Chaos cell bundle (required)",
        MonitoringLabelDashboardParameter("yt", "chaos_cell_bundle", "grut-stable"),
    )
    if with_replica_cluster:
        dashboard.add_parameter(
            "cluster",
            "YT cluster",
            MonitoringLabelDashboardParameter("yt", "cluster", DEFAULT_REPLICA_CLUSTER),
        )
    return dashboard


def build_chaos_bundle_overview():
    rowset = (
        Rowset()
        .stack(False)
        .min(0)
        .row()
        .cell(
            "Replicated data rate",
            _table_puller_cluster_total(
                "yt.tablet_node.table_puller.data_weight.rate",
                "UNIT_BYTES_SI_PER_SECOND",
            ),
            description="Total data copied by table pullers on the selected replica cluster.",
        )
        .cell(
            "Replicated row rate",
            _table_puller_cluster_total("yt.tablet_node.table_puller.row_count.rate"),
            description="Total rows copied by table pullers on the selected replica cluster.",
        )
        .row()
        .cell(
            "Replication card lag",
            _replication_card_lag(),
            description="Lag of a replica relative to the maximum synchronous replication progress.",
        )
        .cell(
            "Replica mode switches",
            _replica_mode_switches(),
            description="Commands issued by replicated table tracker; this does not confirm command success.",
        )
        .row()
        .cell(
            "Table puller lag",
            _table_puller_lag(),
            description="Maximum wall-clock pull replication lag on the selected replica cluster.",
        )
        .cell(
            "Table puller errors",
            _table_puller_errors(),
            description="Pull iteration errors and fatal timestamp or row-index validation errors.",
        )
        .row()
        .cell(
            "Table puller PullRows time",
            _table_puller_time("yt.tablet_node.table_puller.pull_rows_time.max", "pull"),
            description="Maximum source PullRows duration by physical replica table.",
        )
        .cell(
            "Table puller write time",
            _table_puller_time("yt.tablet_node.table_puller.write_time.max", "write"),
            description="Maximum destination write duration by physical replica table.",
        )
        .row()
        .cell(
            "Table puller throttle time",
            _table_puller_throttle_time(),
            description="Maximum time spent waiting for the replication throttler by physical replica table.",
        )
        .cell(
            "Replicator utilization",
            _replicator_utilization(),
            description="Maximum Replicator thread-pool utilization by tablet node.",
        )
        .owner
    )
    dashboard = _build_chaos_bundle_dashboard(
        "Chaos bundle overview [Autogenerated]",
        rowset,
        with_replica_cluster=True,
    )

    return dashboard


def _thread_cpu(sensor, thread, alias):
    expression = (
        NodeCpu(sensor)
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("chaos_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", "Aggr")
        .value("thread", thread)
    )
    return (MonitoringExpr(expression).series_sum("cluster") / 100).alias("{{cluster}}; " + alias)


def _action_queue_time():
    expression = (
        NodeCpu("yt.action_queue.time.cumulative.rate")
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("chaos_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", "Aggr")
        .value("thread", "ChaosSlot*")
        .value("queue", NotEquals("-"))
        .value("bucket", NotEquals("-"))
    )
    return MonitoringExpr(expression).series_sum("cluster", "queue").alias("{{cluster}}; {{queue}}")


def _changelog_write_rate():
    expression = (
        NodeTablet("yt.chaos_node.media_written_bytes_counter.rate")
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("chaos_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", "Aggr")
    )
    return MonitoringExpr(expression).alias("{{cluster}}; media written").unit("UNIT_BYTES_SI_PER_SECOND")


def _remote_changelog_lag(sensor, stage):
    expression = (
        NodeTablet(sensor)
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("chaos_cell_bundle", TemplateTag("chaos_cell_bundle"))
        .value("host", NotEquals("Aggr"))
        .value("cell_id", NotEquals("-"))
    )
    return (
        MonitoringExpr(expression)
        .series_max("cluster", "cell_id")
        .top_max(10)
        .alias("{{cluster}}; " + stage + "; {{cell_id}}")
        .unit("UNIT_SECONDS")
    )


def _remote_changelog_lags():
    return MultiSensor(
        _remote_changelog_lag(
            "yt.chaos_node.remote_changelog.write_quorum_lag.max",
            "quorum",
        ),
        _remote_changelog_lag(
            "yt.chaos_node.remote_changelog.max_replica_lag.max",
            "replica",
        ),
    )


def _chaos_cell_moves_and_node_restarts():
    node_restarts = (
        Node("yt.server.restarted")
        .value(MonitoringTag("service"), "node")
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("host", "Aggr")
        .value("window", "1min")
    )
    cell_moves = (
        Master("yt.tablet_server.tablet_tracker.tablet_cell_moves.rate")
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("host", "Aggr")
        .value("tablet_cell_bundle", TemplateTag("chaos_cell_bundle"))
    )
    return MultiSensor(
        MonitoringExpr(node_restarts).series_sum("cluster").alias("{{cluster}}; node restarts"),
        MonitoringExpr(cell_moves).series_sum("cluster").alias("{{cluster}}; cell moves"),
    )


def _process_thread_cpu(thread, sensor):
    expression = (
        NodeCpu(sensor)
        .value("cluster", TemplateTag("chaos_cluster"))
        .value("host", NotEquals("Aggr"))
        .value("thread", thread)
    )
    return (
        (MonitoringExpr(expression).series_sum("cluster", "host") / 100)
        .top_max(10)
        .alias("{{cluster}}; {{host}}")
    )


def build_chaos_bundle_runtime():
    rowset = (
        Rowset()
        .stack(False)
        .min(0)
        .row()
        .cell(
            "Chaos slot CPU",
            MultiSensor(
                _thread_cpu("yt.resource_tracker.user_cpu", "ChaosSlot*", "user"),
                _thread_cpu("yt.resource_tracker.system_cpu", "ChaosSlot*", "system"),
                _thread_cpu("yt.resource_tracker.cpu_wait", "ChaosSlot*", "wait"),
            ),
            description="CPU consumed by chaos automaton threads for the selected bundle.",
        )
        .cell(
            "Chaos action queue time",
            _action_queue_time(),
            description="Cumulative execution-time rate grouped by chaos automaton queue.",
        )
        .row()
        .cell(
            "BusXfer CPU",
            _process_thread_cpu("BusXfer*", "yt.resource_tracker.total_cpu"),
            description="Process-wide BusXfer CPU by host; this graph is not scoped to the selected bundle.",
        )
        .cell(
            "RpcHeavy CPU",
            _process_thread_cpu("RpcHeavy*", "yt.resource_tracker.total_cpu"),
            description="Process-wide RpcHeavy CPU by host; this graph is not scoped to the selected bundle.",
        )
        .row()
        .cell(
            "BusXfer CPU wait",
            _process_thread_cpu("BusXfer*", "yt.resource_tracker.cpu_wait"),
            description="Process-wide BusXfer CPU wait by host; this graph is not scoped to the selected bundle.",
        )
        .cell(
            "RpcHeavy CPU wait",
            _process_thread_cpu("RpcHeavy*", "yt.resource_tracker.cpu_wait"),
            description="Process-wide RpcHeavy CPU wait by host; this graph is not scoped to the selected bundle.",
        )
        .row()
        .cell(
            "Chaos cell moves and node restarts",
            _chaos_cell_moves_and_node_restarts(),
            description="Chaos-cell moves for the selected bundle and cluster-wide node restarts.",
        )
        .cell("", EmptyCell())
        .row()
        .cell(
            "Changelog write rate",
            _changelog_write_rate(),
            description="Physical media bytes written to chaos-cell changelogs.",
        )
        .cell(
            "Changelog replication lag",
            _remote_changelog_lags(),
            description="Maximum changelog write-quorum wait and lag of the slowest changelog replica by chaos cell.",
        )
        .owner
    )

    return _build_chaos_bundle_dashboard(
        "Chaos bundle runtime and persistence [Autogenerated]",
        rowset,
    )
