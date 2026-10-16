from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringTag
from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor, Title
from yt_dashboard_generator.specific_sensors.monitoring import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.taggable import NotEquals

from .common.sensors import Master, TabNode, TabNodeCpu


CHUNK_TRANSACTION_TYPES = (
    "incremental_heartbeat",
    "full_heartbeat",
    "finalize_heartbeat_session",
    "chunk_location_disposal",
    "chunk_confirmation",
    "dead_chunk_replica_removal",
    "global_refresh",
    "location_refresh",
)


def build_replica_modifications():
    sensor = Master("yt.chunk_server.sequoia_replica_modification.{}")
    rowset = Rowset().all("sequoia_transaction_type").aggr(MonitoringTag("container"))
    statuses = (
        ("Started", "started"),
        ("Successful", "finished_successfully"),
        ("Failed", "finished_with_error"),
    )
    row = rowset.row()
    for title, suffix in (
        ("Replica modifications count", "count"),
        ("Replica modifications replica count", "replica_count"),
    ):
        row.cell(
            title,
            MultiSensor(*(
                sensor(f"{metric}_{suffix}.rate")
                .legend_format(f"{status} {{{{cell_tag}}}} {{{{sequoia_transaction_type}}}}")
                for status, metric in statuses
            )),
            display_legend=True,
        )
    rowset.row().cell(
        "Modifications waiting for semaphore", sensor("semaphore_waiting_count")
    ).cell(
        "Replicas waiting for semaphore", sensor("semaphore_waiting_replica_count")
    )
    phase_row = (
        rowset.row().aggr("phase").all(MonitoringTag("container"))
        .value("sequoia_transaction_type", "|".join(CHUNK_TRANSACTION_TYPES))
        .unit("UNIT_SECONDS")
    )
    phase_row.cell(
        "Replica modification phase time (max)", sensor("time_seconds.max"), display_legend=True
    ).cell(
        "Replica modification phase time (avg)", sensor("time_seconds.avg"), display_legend=True
    )
    return rowset


def build_heartbeats():
    sensor = Master("yt.node_tracker.{}")
    rowset = Rowset()
    row = rowset.row()
    for heartbeat in ("full", "incremental"):
        row.cell(
            f"Sequoia {heartbeat} heartbeat replica count",
            MultiSensor(
                sensor(f"replica_count_in_received_sequoia_{heartbeat}_heartbeats.rate").legend_format("Received"),
                sensor(f"replica_count_in_sequoia_failed_{heartbeat}_heartbeats.rate").legend_format("Failed"),
            ),
            display_legend=True,
        )
    rowset.row().cell(
        "Locations with ongoing full heartbeat", sensor("locations_with_ongoing_full_heartbeat")
    ).cell(
        "Nodes with failed previous incremental heartbeat", sensor("nodes_with_failed_previous_incremental_heartbeat")
    )
    return rowset


def build_backlogs():
    sensor = Master("yt.chunk_server.{}")
    rowset = Rowset()
    rowset.row().cell(
        "Sequoia chunks awaiting confirmation", sensor("sequoia_chunks_awaiting_confirm")
    ).cell(
        "Sequoia chunk purgatory", sensor("sequoia_chunk_purgatory_size")
    )
    rowset.row().cell(
        "Waiting Sequoia incremental heartbeat count", sensor("sequoia_waiting_incremental_heartbeats_count")
    ).cell(
        "Waiting Sequoia incremental heartbeat replica count",
        sensor("sequoia_waiting_incremental_heartbeats_replica_count"),
    )
    return rowset


def build_nodes():
    sensor = Master("yt.node_tracker.{}")
    rowset = Rowset().min(0).stack(False).value("cluster", TemplateTag("cluster"))
    rowset.row().cell(
        "Locations awaiting disposal",
        sensor("chunk_locations_awaiting_disposal")
        .value("host", NotEquals("Aggr"))
        .all("cell_tag", MonitoringTag("container")),
    ).cell(
        "Online node count",
        sensor("online_node_count")
        .value("flavor", "data")
        .all("cell_tag", MonitoringTag("container")),
    )
    rowset.row().cell(
        "Nodes being disposed", sensor("node_count").value("state", "being_disposed"),
    )
    return rowset


def build_refresh():
    sensor = Master("yt.chunk_server.sequoia_{}")
    rowset = Rowset()
    rowset.row().cell(
        "Global refresh shards",
        MultiSensor(
            sensor("global_refresh_active_shards").legend_format("Active"),
            sensor("global_refresh_refreshed_shards").legend_format("Refreshed"),
            sensor("global_refresh_unrefreshed_shards").legend_format("Unrefreshed"),
        ),
        display_legend=True,
    ).cell(
        "Locations awaiting refresh", sensor("location_refresh_awaiting_refresh_locations")
    )
    # These are progress gauges for the current refresh, not monotonic counters.
    rowset.row().cell(
        "Global refresh chunks processed", sensor("global_refresh_total_chunks_processed")
    ).cell(
        "Global refresh chunks processed by shard", sensor("global_refresh_chunks_processed").all("shard_index"),
        display_legend=True,
    )
    return rowset


def build_tablet_static(backend):
    sensor = (
        TabNode("yt.cluster_node.memory_usage.{}")
        .value("cluster", "${cluster}-gnd" if backend == "grafana" else "{{cluster}}-gnd")
        .value("host", TemplateTag("host"))
        .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
        .value("category", "tablet_static")
        .top(10, "max")
    )
    return MultiSensor(
        sensor("used").host_container_legend_format(),
        MonitoringExpr(sensor("limit")).series_max().alias("Limit"),
    ).unit("UNIT_BYTES_SI")


def build_tablet_slot(backend):
    sensor = (
        TabNodeCpu("yt.resource_tracker.{}")
        .value("cluster", "${cluster}-gnd" if backend == "grafana" else "{{cluster}}-gnd")
        .value("host", TemplateTag("host"))
        .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
        .value("thread", "TabletSlot*")
        .top(10, "max")
    )
    return MultiSensor(
        MonitoringExpr(sensor("thread_count")).top_max(1).alias("Limit"),
        MonitoringExpr(sensor("total_cpu")) / 100,
    )


def build_section_title(title):
    return Rowset().row(height=2).cell("", Title(title, size="TITLE_SIZE_L"))


def build_sequoia_replicas(backend="monitoring"):
    dashboard = Dashboard()
    dashboard.add(
        Rowset().min(0).stack(False).row().cell(
            "Sequoia transaction commits",
            Master("yt.sequoia_client.transaction_commits_*")
            .value("cluster", TemplateTag("cluster"))
            .value("type", "|".join(CHUNK_TRANSACTION_TYPES))
            .aggr(MonitoringTag("container")),
            display_legend=True,
        )
    )
    for rowset in (
        build_backlogs().aggr(MonitoringTag("container")),
        build_replica_modifications(),
        build_heartbeats().aggr(MonitoringTag("container")),
    ):
        dashboard.add(
            rowset.min(0).stack(False)
            .value("cluster", TemplateTag("cluster"))
            .value("cell_tag", TemplateTag("cell_tag"))
        )

    dashboard.add(build_section_title("Nodes"))
    dashboard.add(build_nodes())

    dashboard.add(build_section_title("Refresh"))
    dashboard.add(
        build_refresh().min(0).stack(False).aggr(MonitoringTag("container"))
        .value("cluster", TemplateTag("cluster"))
        .value("cell_tag", TemplateTag("cell_tag"))
    )

    dashboard.add(build_section_title("Ground load"))
    dashboard.add(
        Rowset().min(0).stack(False).row().cell(
            "Sequoia chunks tablet static", build_tablet_static(backend), display_legend=True,
        ).cell(
            "Tablet Slot", build_tablet_slot(backend), display_legend=True,
        )
    )

    dashboard.set_title("Sequoia Replicas [Autogenerated]")
    dashboard.set_monitoring_serializer_options({"timeline": {"period": "1h"}})
    for name, title, default in (
        ("cluster", "YT cluster", "kolmogorov"),
        ("cell_tag", "Cell tag", "-"),
        ("host", "Host", "*"),
        ("tablet_cell_bundle", "Tablet cell bundle", "sequoia-chunks"),
    ):
        dashboard.add_parameter(
            name, title,
            MonitoringLabelDashboardParameter("yt", name, default),
            backends=["monitoring"],
        )
        dashboard.add_parameter(
            name, title,
            GrafanaTextboxDashboardParameter(".*" if default == "*" else default),
            backends=["grafana"],
        )

    return dashboard
