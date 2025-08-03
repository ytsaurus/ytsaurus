# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

try:
    from .constants import (
        MASTER_LOCAL_DASHBOARD_DEFAULT_CLUSTER,
        MASTER_LOCAL_DASHBOARD_DEFAULT_CONTAINER,
        MASTER_GLOBAL_DASHBOARD_DEFAULT_CLUSTER,
        MASTER_MERGE_JOBS_DASHBOARD_DEFAULT_CLUSTER,
    )
except ImportError:
    MASTER_LOCAL_DASHBOARD_DEFAULT_CLUSTER = ""
    MASTER_LOCAL_DASHBOARD_DEFAULT_CONTAINER = ""
    MASTER_GLOBAL_DASHBOARD_DEFAULT_CLUSTER = ""
    MASTER_MERGE_JOBS_DASHBOARD_DEFAULT_CLUSTER = ""

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.specific_sensors.monitoring import MonitoringExpr
from yt_dashboard_generator.backends.monitoring import MonitoringTag, MonitoringLabelDashboardParameter
from yt_dashboard_generator.backends.grafana import GrafanaTag
from yt_dashboard_generator.sensor import MultiSensor, Text

import string


def common_sensors():
    total_cpu = (MasterCpu("yt.resource_tracker.total_cpu")
        .stack(False)
        .all(MonitoringTag("host"))
        .value("thread", "Automaton"))
    incremental_heartbeat_execution_time = (MasterRpc("yt.rpc.server.request_time.execution.max")
        .stack(False)
        .all(MonitoringTag("host"))
        .all("yt_service")
        .aggr("user")
        .value("method", "IncrementalHeartbeat"))
    tablet_store_updates = (Master("yt.tablet_server.update_tablet_stores.{}.rate")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("tablet_cell_bundle")
        .aggr("table_type")
        .aggr("update_reason"))
    tablet_store_update_store_count = tablet_store_updates("store_count")
    tablet_store_update_time = tablet_store_updates("cumulative_time")
    memory_usage = (MasterMemory("yt.resource_tracker.memory_usage.rss")
        .stack(False)
        .all(MonitoringTag("host")))
    rct = (MasterMemory("yt.ref_counted_tracker.total.{}_alive")
        .stack(False)
        .all(MonitoringTag("host")))
    rct_objects_alive = rct("objects")
    rct_bytes_alive = rct("bytes")
    utilization = (MasterCpu("yt.resource_tracker.utilization")
        .stack(False)
        .all(MonitoringTag("host")))
    rpc_light_utilization = (utilization
        .value("thread", "RpcLight"))
    rpc_heavy_utilization = (utilization
        .value("thread", "RpcHeavy"))
    blob_refresh_queue_size = (Master("yt.chunk_server.blob_refresh_queue_size")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    blob_requisition_update_queue_size = (Master("yt.chunk_server.blob_requisition_update_queue_size")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    underreplicated_chunk_count = (Master("yt.chunk_server.underreplicated_chunk_count")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    overreplicated_chunk_count = (Master("yt.chunk_server.overreplicated_chunk_count")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    data_missing_chunk_count = (Master("yt.chunk_server.data_missing_chunk_count")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    parity_missing_chunk_count = (Master("yt.chunk_server.parity_missing_chunk_count")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    unsafely_placed_chunk_count = (Master("yt.chunk_server.unsafely_placed_chunk_count")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    lost_vital_chunk_count = (Master("yt.chunk_server.lost_vital_chunk_count")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    leader_sync_time = (Master("yt.hydra.leader_sync_time.max")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_id"))
    chunk_trees = (Master("yt.chunk_server.chunk{}_{}.rate")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_tag"))
    chunks_created = chunk_trees("s", "created")
    chunks_destroyed = chunk_trees("s", "destroyed")
    chunk_lists_created = chunk_trees("_lists", "created")
    chunk_lists_destroyed = chunk_trees("_lists", "destroyed")
    hydra_restarts = (Master("yt.hydra.restart_count.rate")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_id")
        .all("reason"))
    fork_duration = (Master("yt.hydra.fork_executor.fork_duration.max")
        .stack(False)
        .all(MonitoringTag("host"))
        .aggr("cell_id"))

    return [
        ("Automaton CPU", total_cpu),
        ("Incremental HB duration", incremental_heartbeat_execution_time),
        ("Memory", memory_usage),
        ("RpcLight Utilization", rpc_light_utilization),
        ("RpcHeavy Utilization", rpc_heavy_utilization),
        ("Blob Refresh Queue Size", blob_refresh_queue_size),
        ("Blob Requisition Update Queue Size", blob_requisition_update_queue_size),
        ("URC Count", underreplicated_chunk_count),
        ("ORC Count", overreplicated_chunk_count),
        ("DMC Count", data_missing_chunk_count),
        ("PMC Count", parity_missing_chunk_count),
        ("Unsafely Placed Count", unsafely_placed_chunk_count),
        ("LVC Count", lost_vital_chunk_count),
        ("Leader Sync Time", leader_sync_time),
        ("Chunks Created", chunks_created),
        ("Chunks Destroyed", chunks_destroyed),
        ("Chunk Lists Created", chunk_lists_created),
        ("Chunks Lists Destroyed", chunk_lists_destroyed),
        ("Hydra Restarts", hydra_restarts),
        ("Fork Duration", fork_duration),
        ("RCT Objects Alive", rct_objects_alive),
        ("RCT Bytes Alive", rct_bytes_alive),
        ("Tablet Store Updates: Store Count", tablet_store_update_store_count),
        ("Tablet Store Updates: Cumulative Time", tablet_store_update_time),
    ]

def build_global_rowset():
    node_count = (Master("yt.node_tracker.{}_node_count")
        .stack(False)
        .all(MonitoringTag("host"))
        .all("flavor")
        .all("cell_tag"))
    banned_node_count = node_count("banned")
    decommissioned_node_count = node_count("decommissioned")
    offline_node_count = node_count("offline")
    full_node_count = node_count("full")
    with_alerts_node_count = node_count("with_alerts")
    job_rates = (MultiSensor(
        Master("yt.chunk_server.jobs_*.rate"),
        Master("yt.chunk_server.misscheduled_jobs.rate"))
            .stack(False)
            .aggr(MonitoringTag("host"))
            .aggr("cell_tag")
            .aggr("job_type"))
    running_jobs = (Master("yt.chunk_server.running_job_count")
        .stack(False)
        .aggr(MonitoringTag("host"))
        .aggr("cell_tag")
        .all("job_type"))
    data_nodes_being_disposed = (Master("yt.node_tracker.data_nodes_being_disposed")
        .stack(False)
        .all(MonitoringTag("host"))
        .all("cell_tag"))
    data_nodes_awaiting_for_being_disposed = (Master("yt.node_tracker.data_nodes_awaiting_for_being_disposed")
        .stack(False)
        .all(MonitoringTag("host"))
        .all("cell_tag"))

    rowset = Rowset()

    def by_cell_roles(title, sensor):
        (rowset.row()
            .cell(
                f"{title} (Primary Cell)",
                sensor
                    .value("container", "m0*"))
            .cell(
                f"{title} (Top by Portal Cells)",
                sensor
                    .top()
                    .value("container", "mp*"))
            .cell(
                f"{title} (Top by Chunk Cells)",
                sensor
                    .top()
                    .value("container", "mc*")))

    for sensor in common_sensors():
        by_cell_roles(*sensor)

    rowset = (rowset
        .row()
            .cell("Decommissioned Node Count", decommissioned_node_count)
            .cell("Banned Node Count", banned_node_count)
            .cell("Offline Node Count", offline_node_count)
        .row()
            .cell("Data Nodes Being Disposed", data_nodes_being_disposed)
            .cell("Data Nodes Awaiting For Being Disposed", data_nodes_awaiting_for_being_disposed)
            .cell("Full Node Count", full_node_count)
        .row()
            .cell("With Alerts Node Count", with_alerts_node_count)
            .cell("Job Rates", job_rates)
            .cell("Running Jobs", running_jobs)
    )

    return rowset.owner

def build_master_global():
    rowsets = [
        build_global_rowset(),
    ]

    d = Dashboard()
    for r in rowsets:
        d.add(r)

    d.set_title("Master Global")
    d.add_parameter("cluster", "YT cluster", MonitoringLabelDashboardParameter("yt", "cluster", MASTER_GLOBAL_DASHBOARD_DEFAULT_CLUSTER))

    return d

def build_local_rowset():
    rowset = Rowset()

    automaton_action_queue_cumulative_time = (MasterCpu("yt.action_queue.time.cumulative.rate")
        .stack(True)
        .all(MonitoringTag("host"))
        .value("thread", "Automaton")
        .all("bucket")
        .all("queue"))
    hydra_cumulative_mutation_time = (Master("yt.hydra.cumulative_mutation_time.rate")
        .stack(True)
        .all(MonitoringTag("host"))
        .all("type")
        .all("cell_id"))
    user_requests = (Master("yt.security.user_{}_{}.rate")
        .stack(False)
        .top()
        .all(MonitoringTag("host"))
        .all("user"))
    user_read_request_rate = user_requests("read", "request_count")
    user_write_request_rate = user_requests("write", "request_count")
    user_read_time = user_requests("read", "time")
    user_write_time = user_requests("write", "time")

    rpc_request_rate = (MasterRpc("yt.rpc.server.request_count.rate")
        .stack(False)
        .top()
        .all(MonitoringTag("host"))
        .all("queue")
        .aggr("user"))
    object_service_execute_rate = (rpc_request_rate
        .value("yt_service", "ObjectService")
        .value("method", "Execute"))
    chunk_service_execute_rate = (rpc_request_rate
        .value("yt_service", "ChunkService")
        .value("method", "ExecuteBatch"))
    chunk_service_weight_throttler = (Master("yt.chunk_service.weight_throttler.value.rate")
        .stack(False)
        .top()
        .all(MonitoringTag("host"))
        .all("user")
        .all("method")
        .all("cell_tag"))
    job_rates = (MultiSensor(
        Master("yt.chunk_server.jobs_*.rate"),
        Master("yt.chunk_server.misscheduled_jobs.rate"))
            .stack(False)
            .all(MonitoringTag("host"))
            .all("cell_tag")
            .all("job_type"))
    running_jobs = (Master("yt.chunk_server.running_job_count")
        .stack(False)
        .aggr("cell_tag")
        .all("job_type"))

    sensors = common_sensors()
    sensors.extend([
        ("Automaton Action Queue Cumulative Time", automaton_action_queue_cumulative_time),
        ("Hydra Cumulative Mutation Time", hydra_cumulative_mutation_time),
        ("User Read Request Rate", user_read_request_rate),
        ("User Write Request Rate", user_write_request_rate),
        ("User Read Time", user_read_time),
        ("User Write Time", user_write_time),
        ("User Chunk Service Weight Throttler", chunk_service_weight_throttler),
        ("Job Rates", job_rates),
        ("Running Jobs", running_jobs),
        ("ObjectService.Execute request rate", object_service_execute_rate),
        ("ChunkService.ExecuteBatch request rate", chunk_service_execute_rate),
    ])

    for i in range(0, len(sensors), 2):
        row = rowset.row()
        row.cell(*sensors[i])
        if i+1 < len(sensors):
            row.cell(*sensors[i+1])

    return rowset

def build_master_local():
    rowsets = [
        build_local_rowset(),
    ]

    d = Dashboard()
    for r in rowsets:
        d.add(r)

    d.set_title("Master Local")
    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", MASTER_LOCAL_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"])
    d.add_parameter(
        "container",
        "Container",
        MonitoringLabelDashboardParameter("yt", "container", MASTER_LOCAL_DASHBOARD_DEFAULT_CONTAINER),
        backends=["monitoring"])

    d.value(MonitoringTag("container"), TemplateTag("container"))
    d.value(GrafanaTag("pod"), TemplateTag("pod"))

    return d

def build_node_tracker_rowsets():
    online_node_count = Master("yt.node_tracker.online_node_count")
    offline_node_count = Master("yt.node_tracker.offline_node_count")
    decommissioned_node_count = Master("yt.node_tracker.decommissioned_node_count")
    banned_node_count = Master("yt.node_tracker.banned_node_count")

    nodes_being_disposed = Master("yt.node_tracker.data_nodes_being_disposed")
    nodes_awaiting_disposal = Master("yt.node_tracker.data_nodes_awaiting_for_being_disposed")

    throttled_node_registrations = (Master("yt.node_tracker.throttled_register_node_count")
        .value("flavor", "{{flavor}}")
        .aggr("container")
        .all("cell_tag"))
    data_node_heartbeat_time = (Master("yt.node_tracker.full_data_node_heartbeat_time.max")
        .all("container")
        .unit("UNIT_SECONDS"))

    node_dispose_time = Master("yt.node_tracker.node_dispose_time.max").unit("UNIT_SECONDS")
    node_unregister_time = Master("yt.node_tracker.node_unregister_time.max").unit("UNIT_SECONDS")

    return [
        Rowset().aggr("container").all("cell_tag")
            .row().value("flavor", "{{flavor}}")
                .cell("Online node count", online_node_count)
                .cell("Offline node count", offline_node_count)
            .row().value("flavor", "{{flavor}}")
                .cell("Decommissioned node count", decommissioned_node_count)
                .cell("Banned node count", banned_node_count)
            .row()
                .cell("Data nodes being disposed", nodes_being_disposed)
                .cell("Data node disposal queue", nodes_awaiting_disposal),
        Rowset()
            .row()
                .cell("Throttled node registrations", throttled_node_registrations)
                .cell("Data node heartbeat processing time", data_node_heartbeat_time),
        Rowset().all("container")
            .row()
                .cell("Node disposal time", node_dispose_time)
                .cell("Node unregistration time", node_unregister_time),
    ]

def build_master_node_tracker():
    rowsets = build_node_tracker_rowsets()

    dashboard = Dashboard()
    for rowset in rowsets:
        dashboard.add(rowset)

    dashboard.set_title("Master node tracker")
    dashboard.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", MASTER_LOCAL_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"])
    dashboard.add_parameter(
        "flavor",
        "Flavor",
        MonitoringLabelDashboardParameter("yt", "flavor", "*"),
        backends=["monitoring"])

    return dashboard.stack(False)

def build_segmented_user_sensors(sensor, group_size=12):
    sensors = []
    chars = [f"{elem}*" for elem in string.ascii_lowercase + string.digits]
    for i in range(0, len(chars), group_size):
        val = "|".join(chars[i:i+group_size])
        sensors.append(sensor.value("user", val))
    return MultiSensor(*sensors)

def build_cpu_rowsets():
    automaton_cpu = (MasterCpu("yt.resource_tracker.total_cpu")
        .value("thread", "Automaton")
        .unit("UNIT_PERCENT"))
    local_read_cpu = (MasterCpu("yt.resource_tracker.total_cpu")
        .value("thread", "LocalRead")
        .unit("UNIT_PERCENT"))

    user_requests = (Master("yt.security.user_{}_{}.rate")
        .all("cell_tag")
        .top())
    user_write_time = build_segmented_user_sensors(user_requests("write", "time").unit("UNIT_PERCENT_UNIT"))
    user_write_request_rate = build_segmented_user_sensors(user_requests("write", "request_count").unit("UNIT_WRITES_PER_SECOND"))
    user_read_time = build_segmented_user_sensors(user_requests("read", "time").unit("UNIT_PERCENT_UNIT"))
    user_read_request_rate = build_segmented_user_sensors(user_requests("read", "request_count").unit("UNIT_READS_PER_SECOND"))

    mutation_time_rate = (Master("yt.hydra.cumulative_mutation_time.rate")
        .all("cell_id")
        .all("type")
        .top(150)
        .unit("UNIT_PERCENT_UNIT"))

    create_chunk_requests = (Master("yt.chunk_service.weight_throttler.value.rate")
        .all("cell_tag")
        .all("user")
        .top(20)
        .value("method", "create_chunk")
        .unit("UNIT_REQUESTS_PER_SECOND"))

    memory_usage = (MasterMemory("yt.resource_tracker.memory_usage.rss")
        .unit("UNIT_BYTES_SI"))
    fork_duration = (Master("yt.hydra.fork_executor.fork_duration.max")
        .aggr("cell_id")
        .unit("UNIT_SECONDS"))

    return [
        Rowset().stack(False)
            .row()
                .cell("Automaton thread CPU usage", automaton_cpu)
                .cell("LocalRead thread CPU usage", local_read_cpu)
            .row()
                .cell("User write time", user_write_time)
                .cell("User write request rate", user_write_request_rate)
            .row()
                .cell("User read time", user_read_time)
                .cell("User read request rate", user_read_request_rate)
            .row()
                .cell("Create chunk requests", create_chunk_requests)
                .cell("Mutation rate", mutation_time_rate)
            .row()
                .cell("Master memory usage", memory_usage)
                .cell("Fork duration", fork_duration),
    ]

def build_master_cpu():
    rowsets = build_cpu_rowsets()

    dashboard = Dashboard()
    for rowset in rowsets:
        dashboard.add(rowset)

    dashboard.set_title("Master CPU")
    dashboard.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", MASTER_LOCAL_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"])
    dashboard.add_parameter(
        "container",
        "Container",
        MonitoringLabelDashboardParameter("yt", "container", MASTER_LOCAL_DASHBOARD_DEFAULT_CONTAINER),
        backends=["monitoring"])

    dashboard.value(MonitoringTag("container"), TemplateTag("container"))
    dashboard.value(GrafanaTag("pod"), TemplateTag("pod"))

    dashboard.all(MonitoringTag("host"))

    return dashboard

def build_merge_jobs_rowsets():
    nodes_being_merged = (Master("yt.chunk_server.chunk_merger.nodes_being_merged")
        .stack(True))
    account_queue_size = (Master("yt.chunk_server.chunk_merger.account_queue_size")
        .stack(False)
        .top(20, "avg"))
    jobs_awaiting_chunk_creation = (Master("yt.chunk_server.chunk_merger.jobs_awaiting_chunk_creation")
        .value("account", "{{account}}"))
    jobs_undergoing_chunk_creation = Master("yt.chunk_server.chunk_merger.jobs_undergoing_chunk_creation")
    jobs_awaiting_node_heartbeat = Master("yt.chunk_server.chunk_merger.jobs_awaiting_node_heartbeat")
    sessions_awaiting_finalization = Master("yt.chunk_server.chunk_merger.sessions_awaiting_finalization.rate")
    chunk_server_jobs_info = Master("*").value("job_type", "merge_chunks")
    completed_job_count = Master("yt.chunk_server.chunk_merger.completed_job_count.rate")
    chunk_count_saving = (Master("yt.chunk_server.chunk_merger.chunk_count_saving")
        .value("account", "{{account}}"))
    chunk_replacement_rate = (MultiSensor(
        Master("yt.chunk_server.chunk_merger.chunk_replacements_failed"),
        Master("yt.chunk_server.chunk_merger.chunk_replacements_succeeded"))
            .value("account", "{{account}}"))
    auto_merge_fallback_count = Master("yt.chunk_server.chunk_merger.auto_merge_fallback_count.rate")
    max_chunk_count_violated_criteria = (Master("yt.chunk_server.chunk_merger.max_chunk_count_violated_criteria")
        .value("account", "{{account}}"))
    max_row_count_violated_criteria = Master("yt.chunk_server.chunk_merger.max_row_count_violated_criteria")
    max_data_weight_violated_criteria = Master("yt.chunk_server.chunk_merger.max_data_weight_violated_criteria")
    max_uncompressed_data_violated_criteria = Master("yt.chunk_server.chunk_merger.max_uncompressed_data_violated_criteria")
    max_compressed_data_violated_criteria = Master("yt.chunk_server.chunk_merger.max_compressed_data_violated_criteria")
    max_input_chunk_data_weight_violated_criteria = Master("yt.chunk_server.chunk_merger.max_input_chunk_data_weight_violated_criteria")
    max_chunk_meta_size_violated_criteria = Master("yt.chunk_server.chunk_merger.max_chunk_meta_size_violated_criteria")
    stuck_nodes_count = Master("yt.chunk_server.chunk_merger.stuck_nodes_count")
    average_merge_duration = Master("yt.chunk_server.chunk_merger.average_merge_duration")

    return [
        Rowset().value("account", "{{account}}")
            .row()
                .cell("Nodes being merged", nodes_being_merged)
                .cell("Account queue size", account_queue_size),
        Rowset().stack(True)
            .row()
                .cell("Chunk server jobs info", chunk_server_jobs_info)
                .cell("Jobs awaiting chunk creation", jobs_awaiting_chunk_creation)
            .row().value("account", "{{account}}")
                .cell("Jobs undergoing chunk creation", jobs_undergoing_chunk_creation)
                .cell("Jobs awaiting node heartbeat", jobs_awaiting_node_heartbeat)
            .row()
                .cell("Sessions awaiting finalization", sessions_awaiting_finalization)
                .cell("Completed job count", completed_job_count)
            .row()
                .cell("Chunk count saving", chunk_count_saving)
                .cell("Chunk replacement rate", chunk_replacement_rate)
            .row()
                .cell("Auto merge fallback count", auto_merge_fallback_count)
                .cell("Max chunk count violated criteria", max_chunk_count_violated_criteria),
        Rowset().stack(True).value("account", "{{account}}")
            .row()
                .cell("Max row count violated criteria", max_row_count_violated_criteria)
                .cell("Max data weight violated criteria", max_data_weight_violated_criteria)
            .row()
                .cell("Max uncompressed data violated criteria", max_uncompressed_data_violated_criteria)
                .cell("Max compressed data violated criteria", max_compressed_data_violated_criteria)
            .row()
                .cell("Max input chunk data weight violated criteria", max_input_chunk_data_weight_violated_criteria)
                .cell("Max chunk meta size violated criteria", max_chunk_meta_size_violated_criteria)
            .row()
                .cell("Stuck nodes count", stuck_nodes_count)
                .cell("Average merge duration", average_merge_duration)
    ]

def build_master_merge_jobs():
    rowsets = build_merge_jobs_rowsets()

    dashboard = Dashboard()
    for rowset in rowsets:
        dashboard.add(rowset)

    dashboard.set_title("Master Merge Jobs")
    dashboard.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", MASTER_MERGE_JOBS_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"])
    dashboard.add_parameter(
        "cell_tag",
        "Cell Tag",
        MonitoringLabelDashboardParameter("yt", "cell_tag", "*"),
        backends=["monitoring"])
    dashboard.add_parameter(
        "cell_id",
        "Cell Id",
        MonitoringLabelDashboardParameter("yt", "cell_id", "-"),
        backends=["monitoring"])
    dashboard.add_parameter(
        "container",
        "Container",
        MonitoringLabelDashboardParameter("yt", "container", "-"),
        backends=["monitoring"])
    dashboard.add_parameter(
        "account",
        "Account",
        MonitoringLabelDashboardParameter("yt", "account", "-"),
        backends=["monitoring"])

    dashboard.value(MonitoringTag("container"), TemplateTag("container"))
    dashboard.value(MonitoringTag("cell_tag"), TemplateTag("cell_tag"))

    return dashboard

def build_accounts_rowsets():

    def _build_disk_usage(parameter_tag):
        return (MultiSensor(
            MonitoringExpr(Accounts("disk_space_limit_in_gb"))
                .alias(f"{{{{{parameter_tag}}}}} limit")
                .sensor_stack(False),
            MonitoringExpr(Accounts("detailed_disk_space_in_gb")
                .value("status", "committed"))
                .alias(f"{{{{{parameter_tag}}}}} usage committed"),
            MonitoringExpr(Accounts("detailed_disk_space_in_gb")
                .value("status", "uncommitted"))
                .alias(f"{{{{{parameter_tag}}}}} usage uncommitted"),
            )
        .value("medium", f"{{{{{parameter_tag}}}}}")
        .unit("UNIT_GIBIBYTES")
        .stack(True))

    left_disk_usage = _build_disk_usage("left_medium")
    right_disk_usage = _build_disk_usage("right_medium")

    def _build_account_multisensor(resource_name, metric_infix):
        return (MultiSensor(
                MonitoringExpr(Accounts(f"{metric_infix}_limit"))
                    .alias(f"{resource_name} limit")
                    .sensor_stack(False),
                MonitoringExpr(Accounts(f"detailed_{metric_infix}")
                    .value("status", "committed"))
                    .alias(f"{resource_name} usage committed"),
                MonitoringExpr(Accounts(f"detailed_{metric_infix}")
                    .value("status", "uncommitted"))
                    .alias(f"{resource_name} usage uncommitted"),
                )
            .unit("UNIT_COUNT")
            .stack(True))

    chunk_usage = _build_account_multisensor("Chunk", "chunk_count")
    nodes_usage = _build_account_multisensor("Nodes", "node_count")

    return [
        Rowset()
            .row(height=10)
                .cell("Disk usage/limit", left_disk_usage, yaxis_label="Disk space", display_legend=True)
                .cell("Disk usage/limit", right_disk_usage, yaxis_label="Disk space", display_legend=True),
        Rowset()
            .row(height=2)
                .cell("", Text("""Disk space usage and limit represents raw space available for account and total usage of this raw space.
                                  If you are running out of quota, then you can [order more via ABCD](https://yt.yandex-team.ru/docs/description/common/quota_request#zapros-na-rasshirenie-kvoty-pod-sushestvuyushij-akkaunt).""")),
        Rowset()
            .row(height=10)
                .cell("Chunk usage/limit", chunk_usage, yaxis_label="Chunks count", display_legend=True)
                .cell("Nodes usage/limit", nodes_usage, yaxis_label="Nodes count", display_legend=True),
        Rowset()
            .row(height=2)
                .cell("", Text("""All data stored in tables and files are split into parts called [chunks](https://yt.yandex-team.ru/docs/description/storage/chunks). **Nodes** - is a number of objects in the account (tables, files, folders, locks, etc.).
                                  Both resources are virtual and you do not need to order them. If you need to increase the limit, please follow these [instructions](https://yt.yandex-team.ru/docs/instructions/quota/get_more_node_chunks).""")),
    ]

def build_master_accounts():
    rowsets = build_accounts_rowsets()

    dashboard = Dashboard()
    for rowset in rowsets:
        dashboard.add(rowset)

    dashboard.set_title("Accounts dashboard")
    dashboard.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", MASTER_LOCAL_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"])
    dashboard.add_parameter(
        "account",
        "Account",
        MonitoringLabelDashboardParameter("yt", "account", "sys"),
        backends=["monitoring"])
    dashboard.add_parameter(
        "left_medium",
        "Left medium",
        MonitoringLabelDashboardParameter("yt", "medium", "default"),
        backends=["monitoring"])
    dashboard.add_parameter(
        "right_medium",
        "Right medium",
        MonitoringLabelDashboardParameter("yt", "medium", "ssd_blobs"),
        backends=["monitoring"])

    dashboard.value("host", "none")
    dashboard.value("account", "{{account}}")

    return dashboard
