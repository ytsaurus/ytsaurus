# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

try:
    from .constants import (
        ARTEMIS_DASHBOARD_DEFAULT_CLUSTER,
        ARTEMIS_DASHBOARD_DEFAULT_CONTAINER,
    )
except ImportError:
    ARTEMIS_DASHBOARD_DEFAULT_CLUSTER = ""
    ARTEMIS_DASHBOARD_DEFAULT_CONTAINER = ""

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, EmptyCell
from yt_dashboard_generator.taggable import NotEquals
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr
from yt_dashboard_generator.backends.grafana import GrafanaTag, GrafanaTextboxDashboardParameter

##################################################################


def build_read_write_rowset():
    top_rate = NodeTablet("yt.tablet_node.{}.{}.rate")
    reader_stats = NodeTablet(
        "yt.tablet_node.{}.chunk_reader_statistics.{}_bytes_read_from_disk.rate")
    transmitted_stats = NodeTablet(
        "yt.tablet_node.{}.chunk_reader_statistics.data_bytes_transmitted.rate")

    return (Rowset()
        .aggr(MonitoringTag("host"), "table_tag", "table_path")
        .all("#UB")
        .stack()
        .top()

        .row()
            .cell("Table write data weight rate", top_rate("write", "data_weight"))
            .cell("Table write row count rate", top_rate("write", "row_count"))
        .row()
            .cell("Table commit data weight rate", top_rate("commit", "data_weight"))
            .cell("Table commit row count rate", top_rate("commit", "row_count"))
        .row()
            .cell("Table lookup data weight rate", top_rate("lookup", "data_weight"))
            .cell("Table select data weight rate", top_rate("select", "data_weight"))
        .row()
            .cell("Table lookup unmerged data weight rate", top_rate("lookup", "unmerged_data_weight"))
            .cell("Table select unmerged data weight rate", top_rate("select", "unmerged_data_weight"))
        .row()
            .cell("Table lookup data bytes read from disk", reader_stats("lookup", "data"))
            .cell("Table select data bytes read from disk", reader_stats("select", "data"))
        .row()
            .cell("Table lookup data bytes transmitted", transmitted_stats("lookup"))
            .cell("Table select data bytes transmitted", transmitted_stats("select"))
        .row()
            .cell("Table lookup chunk meta bytes read from disk", reader_stats("lookup", "meta"))
            .cell("Table select chunk meta bytes read from disk", reader_stats("select", "meta"))
        .row()
            .cell("Table lookup row count rate", top_rate("lookup", "row_count"))
            .cell("Table select row count rate", top_rate("select", "row_count"))
        .row()
            .cell("Table lookup unmerged row count rate", top_rate("lookup", "unmerged_row_count"))
            .cell("Table select unmerged row count rate", top_rate("select", "unmerged_row_count"))
        .row()
            .cell("Table lookup CPU usage", top_rate("multiread", "cumulative_cpu_time"))
            .cell("Table select CPU usage", top_rate("execute", "cumulative_cpu_time"))
        .row()
            .cell("Table lookup decompression cpu time rate", top_rate("lookup", "decompression_cpu_time"))
            .cell("Table select decompression cpu time rate", top_rate("select", "decompression_cpu_time"))
        .row()
            .cell("Table lookup request count", top_rate("multiread", "request_count"))
            .cell("Table select request count", top_rate("execute", "request_count"))
    ).owner


def build_max_lookup_select_execute_time_per_host():
    return (Rowset()
        .aggr(MonitoringTag("host"), "table_tag", "table_path")
        .all("#B")
        .top()
        .stack(False)
        .row()
            .cell("Table lookup max duration", NodeTablet("yt.tablet_node.multiread.request_duration.max"))
            .cell("Table select max duration", NodeTablet("yt.tablet_node.execute.request_duration.max"))
    ).owner


def build_write_lookup_select_errors():
    s = (TabNodeRpc("yt.rpc.server.failed_request_count.rate")
        .aggr("user")
        .all(MonitoringTag("host"))
        .stack(False)
        .top())
    return (Rowset()
        .row()
            .cell(
                "Table write errors (per user)",
                s.service_method("TabletService", "Write").all("user").aggr(MonitoringTag("host")))
            .cell(
                "Table write errors (per host)",
                s.service_method("TabletService", "Write"))
        .row()
            .cell(
                "Table lookup (QueryService::Multiread) errors (per host)",
                s.service_method("QueryService", "Multiread"))
            .cell(
                "Table select (QueryService::Execute) errors (per host)",
                s.service_method("QueryService", "Execute"))
    ).owner


def build_lookup_select_ack_time():
    s = (TabNodeRpc("yt.rpc.server.request_time.remote_wait.max")
        .value("yt_service", "QueryService")
        .aggr("user")
        .all(MonitoringTag("host"))
        .stack(False)
        .top())
    return (Rowset()
        .row()
            .cell(
                "Table lookup (QueryService::Multiread) RPC acknowledge time",
                s.value("method", "Multiread").all("queue"))
            .cell(
                "Table select (QueryService::Execute) RPC acknowledge time",
                s.value("method", "Execute"))
    ).owner


def build_replicator():
    s = NodeTablet("yt.tablet_node.replica.{}").stack(False)
    return (Rowset()
        .aggr(MonitoringTag("host"), "table_path", "table_tag")
        .all("#B", "replica_cluster")
        .top()
        .row()
            .cell("Table replicator replica lag row count", s("lag_row_count"))
            .cell("Table replicator replica lag time", s("lag_time"))
        .row()
            .cell(
                "Table replicator replicated data weight",
                NodeTablet("yt.tablet_node.replica.replication_data_weight.rate"))
    ).owner


def build_background_resource_usage():
    top_disk = NodeTablet("yt.tablet_node.{}.{}.rate")

    return (Rowset()
        .all("#AB", "method", "medium")
        .aggr(MonitoringTag("host"), "table_tag", "table_path")
        .stack()
        .top()

        .row()
            .cell(
                "Tablet background data bytes read from disk",
                top_disk("chunk_reader_statistics", "data_bytes_read_from_disk"))
            .cell(
                "Tablet background chunk meta bytes read from disk",
                top_disk("chunk_reader_statistics", "meta_bytes_read_from_disk"))
        .row()
            .cell(
                "Tablet background data bytes read from cache",
                top_disk("chunk_reader_statistics", "data_bytes_read_from_cache"))
            .cell(
                "Tablet background unmerged data weight read rate",
                top_disk("chunk_reader", "unmerged_data_weight"))
        .row()
            .cell(
                "Tablet background decompression cpu time",
                top_disk("chunk_reader", "decompression_cpu_time"))
            .cell(
                "Tablet background compression cpu time",
                top_disk("chunk_writer", "compression_cpu_time"))
        .row()
            .cell(
                "Tablet background disk bytes written (with replication)",
                top_disk("chunk_writer", "disk_space"))
            .cell(
                "Tablet background data weight written (without replication)",
                top_disk("chunk_writer", "data_weight"))
    ).owner


def build_lsm(local=False):
    top_lsm = NodeTablet("yt.tablet_node.{}.{}")

    osc = NodeTablet("yt.tablet_node.tablet.overlapping_store_count.max")
    if local:
        osc = osc.all("table_path")
    else:
        osc = osc.aggr("table_path")

    return (Rowset()
        .all(MonitoringTag("host"))
        .stack(False)
        .top()

        .row()
            .cell(
                "Scheduled compactions rate",
                NodeTablet("yt.tablet_node.store_compactor.scheduled_compactions.rate"))
            .cell(
                "Scheduled partitionings rate",
                NodeTablet("yt.tablet_node.store_compactor.scheduled_partitionings.rate"))
        .row()
            .cell(
                "Running compactions",
                NodeTablet("yt.tablet_node.store_compactor.running_compactions"))
            .cell(
                "Running partitionings",
                NodeTablet("yt.tablet_node.store_compactor.running_partitionings"))
        .row()
            .cell(
                "Compaction queue size",
                NodeTablet("yt.tablet_node.store_compactor.feasible_compactions"))
            .cell(
                "Partitioning queue size",
                NodeTablet("yt.tablet_node.store_compactor.feasible_partitionings"))
        .row()
            .cell(
                "Running flushes",
                NodeTablet("yt.tablet_node.store_flusher.running_store_flushes"))
            .cell("Max overlapping store count", osc)
    ).owner


def build_data_node_disk_stuff():
    s = (DatNodeLocation("yt.location.blob_block_bytes.rate")
        .value("location_type", "Store|store")
        .all("medium", "category"))
    return (Rowset()
        .aggr(MonitoringTag("host"))
        .row()
            .cell("Disk bytes written per medium", s.value("direction", "write"))
            .cell("Disk bytes read per medium", s.value("direction", "read"))
        .row()
            .cell(
                "Data node disk throttlers",
                DatNodeLocation("yt.location.*.value.rate")
                    .aggr("location_id")
                    .all("medium")
                    .value("location_type", "Store|store"))
            .cell(
                "Tablet node preload throttler",
                NodeTablet("yt.tablet_node.throttlers.static_store_preload_in.value.rate"))
    ).owner

def build_tablet_node_preload_throttler():
    return (Rowset()
        .row()
            .cell(
                "Tablet node preload throttler",
                MultiSensor(
                    NodeTablet("yt.tablet_node.throttlers.static_store_preload_in.value.rate"),
                    TabNode("yt.cluster_node.in_throttler.value.rate").value("bucket", "static_store_preload_in")))
            .cell("", EmptyCell())
    ).owner


def build_cluster_node_throttlers_stuff():
    return (Rowset()
        .aggr(MonitoringTag("host"))
        .stack(False)
        .row()
            .cell(
                "Total in throttler queue size",
                Node("yt.cluster_node.throttlers.total_in.queue_size"))
            .cell(
                "Total out throttler queue size",
                Node("yt.cluster_node.throttlers.total_out.queue_size"))
    ).owner


def build_data_node_throttlers_stuff():
    return (Rowset()
        .aggr(MonitoringTag("host"))
        .stack(False)
        .row()
            .cell(
                "Data node in-throttlers queue size",
                Node("yt.data_node.throttlers.*_in.queue_size"))
            .cell(
                "Data node out-throttlers queue size",
                Node("yt.data_node.throttlers.*_out.queue_size"))
    ).owner


def build_tablet_node_throttlers_stuff():
    return (Rowset()
        .aggr(MonitoringTag("host"))
        .stack(False)
        .row()
            .cell(
                "Tablet node in-throttlers queue size",
                NodeTablet("yt.tablet_node.throttlers.*_in.queue_size"))
            .cell(
                "Tablet node out-throttlers queue size",
                NodeTablet("yt.tablet_node.throttlers.*_out.queue_size"))
    ).owner


def build_fair_throttler(cls):
    return (Rowset()
        .stack(False)
        .all("bucket")
        .min(0)
        .row()
            .cell(
                "In throttlers queue size",
                cls("yt.cluster_node.in_throttler.queue_size"))
            .cell(
                "Out throttlers queue size",
                cls("yt.cluster_node.out_throttler.queue_size"))
    ).owner


def build_tablet_balancer():
    s = Master("yt.tablet_server.tablet_balancer.{}.rate")
    return (Rowset()
        .all(MonitoringTag("host"), "#B", "container")
        .row()
            .cell("Tablet balancer moves", s("*_memory_moves"))
            .cell("Tablet balancer reshards", s("tablet_merges"))
    ).owner


def build_chunk_meta_caches_base():
    usage = TabNode("{}.weight")
    misses = TabNode("yt.{}_node.{}.missed_weight.rate")
    return (Rowset()
        .all(MonitoringTag("host"))
        .stack(False)
        .top()
        .min(0)

        .row()
            .cell(
                "Versioned chunk meta cache usage",
                NodeTablet("yt.tablet_node.versioned_chunk_meta_cache.weight").aggr("segment"))
            .cell(
                "Versioned chunk meta cache miss rate",
                NodeTablet("yt.tablet_node.versioned_chunk_meta_cache.missed_weight.rate"))
        .row()
            .cell("Block cache usage", usage("yt.data_node.block_cache.*compressed_data").aggr("segment"))
            .cell("Block cache miss rate", misses("data", "block_cache.*compressed_data"))
    )

def build_link_to_cache_dashboard_local():
    try:
        from .constants import CACHE_DASHBOARD_URL
    except ImportError:
        return

    def _make_url(service, cache_address):
        return CACHE_DASHBOARD_URL + "?" + "&".join((
            "p.cluster={{cluster}}",
            "p.container={{container}}",
            "p.host=!Aggr",
            "p.service=" + service,
            "p.cache=" + cache_address))

    text = f"""
See more information about caches at the separate dashboards.<EOLN>
- [Chunk meta cache]({_make_url("node_tablet", "yt.tablet_node.versioned_chunk_meta_cache")})
- [Compressed block cache]({_make_url("tab_node", "yt.data_node.block_cache.compressed_data")})
- [Uncompressed block cache]({_make_url("tab_node", "yt.data_node.block_cache.uncompressed_data")})
"""
    return Rowset().row(height=5).cell("", Text(text.strip()))


def build_chunk_meta_caches():
    usage = TabNode("{}.weight")
    misses = TabNode("yt.{}_node.{}.missed_weight.rate")
    return (build_chunk_meta_caches_base()
        .row()
            .cell("Chunk meta cache usage", usage("yt.data_node.chunk_meta_cache").aggr("segment"))
            .cell("Chunk meta cache miss rate", misses("data", "chunk_meta_cache"))
        .row()
            .cell("Block ext cache usage", usage("yt.data_node.blocks_ext_cache").aggr("segment"))
            .cell("Block ext cache miss rate", misses("data", "blocks_ext_cache"))
        .row()
            .cell(
                "Cached versioned chunk meta memory",
                TabNode("yt.cluster_node.memory_usage.used")
                    .value("category", "versioned_chunk_meta"))
    ).owner


def build_chunk_meta_caches_container():
    return build_chunk_meta_caches_base()


def build_memory():
    mem_used = TabNode("yt.cluster_node.memory_usage.used")
    return (Rowset()
        .stack(False)
        .all(MonitoringTag("host"))
        .top()

        .row()
            .cell(
                "Tablet dynamic memory",
                mem_used.value("category", "tablet_dynamic"))
            .cell(
                "Tablet static memory",
                mem_used.value("category", "tablet_static"))
        .row()
            .cell(
                "Query memory usage",
                mem_used.value("category", "query"))
            .cell(
                "Tracked memory usage",
                TabNode("yt.cluster_node.memory_usage.total_used"))
        .row()
            .cell(
                "Allocated memory",
                TabNodeMemory("yt.yt_alloc.total.bytes_used"))
            .cell(
                "Process memory usage (rss)",
                TabNodeMemory("yt.resource_tracker.memory_usage.rss"))
    ).owner


def build_memory_by_category():
    return (Rowset()
        .row()
            .cell(
                "Node memory categories",
                TabNode("yt.cluster_node.memory_usage.used")
                    .all("category")
                    .stack())
           .cell("", EmptyCell())
    ).owner


def build_thread_cpu():
    cpu = (lambda thread: TabNodeCpu("yt.resource_tracker.total_cpu")
        .value("thread", thread))
    return (Rowset()
        .stack(False)
        .all(MonitoringTag("host"))
        .top()

        .row()
            .cell("TabletLookup thread CPU", cpu("TabletLookup"))
            .cell("Query thread CPU", cpu("Query"))
        #  .row()
        #      .cell("StorageLookup thread user cpu per host", cpu("StorageLookup"))
        .row()
            .cell("TabletSlot threads CPU", cpu("TabletSlot*"))
            .cell("Compression thread pool CPU", cpu("Compression"))
        .row()
            .cell("ChunkReader thread CPU", cpu("ChunkReader"))
            .cell("ChunkWriter thread CPU", cpu("ChunkWriter"))
        .row()
            .cell("Control thread CPU", cpu("Control"))
            .cell(
                "Control thread action total time",
                TabNodeCpu("yt.action_queue.time.total.max").value("thread", "Control"))
        .row()
            .cell("CPU wait (all threads)", TabNodeCpu("yt.resource_tracker.cpu_wait")
                .aggr("thread"))
    ).owner

def build_thread_cpu_compressed():
    cpu = (lambda thread: (MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu")) / 100)
        .value("thread", thread))
    threads = "|".join([
        "TabletLookup",
        "Query",
        "ChunkReader",
        "ChunkWriter",
        "Compression",
        "Control",
        "StoreCompact",
        "BusXfer",
    ])
    return (Rowset()
        .stack(False)
        .min(0)

        .row()
            .cell(
                "Essential threads CPU usage (in cores)",
                cpu(threads))
            .cell(
                "TabletSlot threads CPU (in cores)",
                cpu("TabletSlot/*"))
        .row()
            .cell(
                "Essential threads action total time",
                TabNodeCpu("yt.action_queue.time.wait.max").value("thread", threads))
            .cell(
                "Top thread utilization",
                TabNodeCpu("yt.resource_tracker.utilization")
                    .value("thread", NotEquals("Logging|LogCompress"))
                    .top())
        .row()
            .cell("CPU wait (all threads)", TabNodeCpu("yt.resource_tracker.cpu_wait")
                .aggr("thread"))
    ).owner


def build_rpc_request_rate():
    request_rate = (TabNodeRpc("yt.rpc.server.{}.rate")
        .aggr(MonitoringTag("host"), "#U", "queue")
        .all("yt_service", "method")
        .stack(False)
        .top())
    return (Rowset()
        .row()
            .cell("RPC request rate", request_rate("request_count"))
            .cell("RPC failed request rate", request_rate("failed_request_count"))
    ).owner


def build_tx_stats():
    ping_time = (TabNodeRpcClient("yt.rpc.client.request_time.{}.max")
        .service_method("TransactionSupervisorService", "PingTransaction"))
    ack_time = (TabNodeRpc("yt.rpc.server.request_time.remote_wait.max")
        .all("method"))
    return (Rowset()
        .aggr("#U")
        .all(MonitoringTag("host"))
        .stack(False)
        .top()
        .row()
            .cell("PingTransaction acknowledge time", ping_time("ack"))
            .cell("PingTransaction total time", ping_time("total"))
        .row()
            .cell(
                "TransactionParticipantService RPC acknowledge time",
                ack_time.value("yt_service", "TransactionParticipantService"))
            .cell(
                "TransactionSupervisorService RPC acknowledge time",
                ack_time.value("yt_service", "TransactionSupervisorService"))
    ).owner


def build_hydra():
    return (Rowset()
        .aggr("cell_id")
        .row()
            .cell(
                "Hydra remote changelog replica quorum lag time",
                NodeTablet("yt.tablet_node.remote_changelog.write_quorum_lag.max")
                    .all(MonitoringTag("host"), "#B")
                    .stack(False)
                    .top())
            .cell(
                "Hydra restart rate",
                NodeTablet("yt.hydra.restart_count.rate")
                    .aggr(MonitoringTag("host"))
                    .all("#B", "reason"))
        .row()
            .cell(
                "Hydra mutation wait time",
                NodeTablet("yt.hydra.mutation_wait_time.max")
                    .all(MonitoringTag("host"), "#B")
                    .stack(False)
                    .top())
    ).owner


def build_changelogs():
    changelog = (NodeLocation("yt.location.{}")
        .aggr("location_id")
        .value("location_type", "Store|store")
        .value("medium", "ssd_journals"))
    return (Rowset()
        .all(MonitoringTag("host"))
        .stack(False)
        .top()

        .row()
            .cell(
                "Data Node put blocks RPC acknowledge time",
                NodeRpc("yt.rpc.server.request_time.remote_wait.max")
                    .service_method("DataNodeService", "PutBlocks")
                    .aggr("#U"))
            .cell(
                "Data Node changelogs put blocks wall time max",
                changelog("put_blocks_wall_time.max"))
        .row()
            .cell(
                "Data Node Multiplexed changelogs flush time",
                changelog("multiplexed_changelogs.changelog_flush_io_time.max"))
            .cell(
                "Data Node Split changelogs flush time",
                changelog("split_changelogs.changelog_flush_io_time.max"))
        .row()
            .cell(
                "Data node Multiplexed changelogs bytes rate",
                changelog("multiplexed_changelogs.bytes.rate"))
            .cell(
                "Data node Split changelogs bytes rate",
                changelog("split_changelogs.bytes.rate"))
        .row()
            .cell(
                "Data node Multiplexed changelogs records rate",
                changelog("multiplexed_changelogs.records.rate"))
            .cell(
                "Data node Split changelogs records rate",
                changelog("split_changelogs.records.rate"))
    ).owner


def build_rpc_message_size_stats_per_host(
    sensor_class, client_or_server, name_prefix, name_suffix=""
):
    s = (sensor_class("yt.rpc.{}.{{}}.rate".format(client_or_server))
        .all(MonitoringTag("host"))
        .aggr("method", "yt_service", "user", "queue")
        .stack(False)
        .top())
    if name_suffix:
        name_suffix = " " + name_suffix
    return (Rowset()
        .row()
            .cell(
                "{} request message body size{}".format(name_prefix, name_suffix),
                s("request_message_body_bytes"))
            .cell(
                "{} request message attachment size{}".format(name_prefix, name_suffix),
                s("request_message_attachment_bytes"))
        .row()
            .cell(
                "{} response message body size{}".format(name_prefix, name_suffix),
                s("response_message_body_bytes"))
            .cell(
                "{} response message attachment size{}".format(name_prefix, name_suffix),
                s("response_message_attachment_bytes"))
    ).owner


def build_server_rpc_message_size_stats_per_method(sensor_class, name_prefix):
    server_per_method = (sensor_class("yt.rpc.server.{}.rate")
        .all("yt_service", "method")
        .aggr(MonitoringTag("host"), "#U", "queue")
        .stack())
    return (Rowset()
        .row()
            .cell(
                "{} request message body size (per method)".format(name_prefix),
                server_per_method("request_message_body_bytes"))
            .cell(
                "{} request message attachment size (per method)".format(name_prefix),
                server_per_method("request_message_attachment_bytes"))
        .row()
            .cell(
                "{} response message body size (per method)".format(name_prefix),
                server_per_method("response_message_body_bytes"))
            .cell(
                "{} response message attachment size (per method)".format(name_prefix),
                server_per_method("response_message_attachment_bytes"))
    ).owner


def _get_network_sensor():
    return (Sensor("/Net/Ifs/{}Bytes", sensor_tag_name="path")
        .value("service", "sys|porto_iss")
        .stack(False)
        .top())


def build_network_global():
    s = _get_network_sensor().value("host", "*-*")
    r = Rowset()
    for i in range(3):
        (r.row()
            .cell(
                "Network received bytes (eth{})".format(i),
                s("Rx").value("intf", "eth" + str(i)))
            .cell(
                "Network transmitted bytes (eth{})".format(i),
                s("Tx").value("intf", "eth" + str(i)))
        )
    return r


def build_network_local():
    s = _get_network_sensor().all("intf")
    return (Rowset()
        .row()
            .cell("Network received bytes", s("Rx"))
            .cell("Network transmitted bytes", s("Tx"))
        .row()
            .cell(
                "Pending out bytes",
                TabNodeInternal("yt.bus.pending_out_bytes").all("network"))
    ).owner

def build_network_local_porto():
    s = (TabNodePorto("yt.porto.network.{}_bytes")
        .value("container_category", "pod"))
    return (Rowset()
        .min(0)
        .row()
            .cell("Network received bytes", s("rx"))
            .cell("Network transmitted bytes", s("tx"))
        .row()
            .cell(
                "Pending out bytes",
                TabNodeInternal("yt.bus.pending_out_bytes").all("network", "band", "encrypted"))
    ).owner


def build_job_cpu_usage():
    return (Rowset()
        .row()
            .cell("Job CPU usage", Node("yt.job_controller.resource_usage.cpu"))
    ).owner


def build_job_reporter():
    s = (Node("yt.job_reporter.{}")
        .aggr(MonitoringTag("host"), "#B")
        .all("reporter_type"))
    return (Rowset()
        .row()
            .cell("Jobs committed count", s("committed.rate"))
            .cell("Jobs committed data weight", s("committed_data_weight.rate"))
        .row()
            .cell("Jobs dropped", s("dropped.rate"))
            .cell("Jobs pending count", s("pending"))
    ).owner


def build_rpc_proxy_request_count_rate():
    request_count = (RpcProxyRpc("yt.rpc.server.request_count.rate")
        .value("yt_service", "ApiService")
        .stack(False)
        .top())
    return (Rowset()
        .row()
            .cell(
                "RPC Proxy requests",
                request_count
                    .value("yt_service", "ApiService")
                    .aggr(MonitoringTag("host"))
                    .all("#U", "method"))
            .cell(
                "RPC Proxy requests per host",
                request_count
                    .service_method("ApiService", "-")
                    .aggr("#U", "method")
                    .all(MonitoringTag("host")))
    ).owner


def build_http_proxy():
    errors_rate = (HttpProxy("yt.http_proxy.api_error_count.rate")
        .aggr(MonitoringTag("host"), "#U", "command", "error_code", "proxy_role")
        .stack(False)
        .top())
    return (Rowset()
        .row()
            .cell("HTTP proxy errors per user", errors_rate.all("#U"))
            .cell("HTTP proxy errors per error code", errors_rate.all("error_code"))
        .row()
            .cell("HTTP proxy errors per host", errors_rate.all(MonitoringTag("host"), "proxy_role"))
    ).owner


def build_tx_serialization_lag():
    return (Rowset()
        .row()
            .cell(
                "Transaction serialization lag",
                NodeTablet("yt.tablet_node.transaction_serialization_lag.max")
                    .all(MonitoringTag("host"))
                    .aggr("cell_id")
                    .stack(False)
                    .top())
    ).owner

##################################################################


def build_global_artemis():
    prefixes = ["tab", "exe", ""]
    with SplitNodeSensorsGuard(["dat", ""], prefixes, prefixes):
        rowsets = [
            build_read_write_rowset(),
            build_max_lookup_select_execute_time_per_host().aggr("#U").all(MonitoringTag("host")),
            build_write_lookup_select_errors(),
            build_lookup_select_ack_time(),
            build_replicator(),
            build_background_resource_usage(),
            build_lsm(),
            build_data_node_disk_stuff()
                .aggr("#B"),
            build_cluster_node_throttlers_stuff(),
            build_data_node_throttlers_stuff(),
            build_tablet_node_throttlers_stuff(),
            build_tablet_balancer(),
            build_chunk_meta_caches(),
            build_memory(),
            build_thread_cpu(),
            build_rpc_request_rate(),
            build_tx_stats(),
            build_hydra(),
            build_changelogs(),
            build_rpc_message_size_stats_per_host(
                TabNodeRpcClient, "client", "Rpc client (yt_node)", "(per host)"),
            build_rpc_message_size_stats_per_host(
                TabNodeRpc, "server", "Rpc server (yt_node)", "(per host)"),
            build_server_rpc_message_size_stats_per_method(TabNodeRpc, "Rpc server (yt_node)")
                .aggr("#B"),
            build_network_global(),
            build_job_cpu_usage()
                .aggr(MonitoringTag("host"), "#B"),
            build_job_reporter(),
            build_rpc_proxy_request_count_rate(),
            build_rpc_message_size_stats_per_host(
                RpcProxyRpc, "server", "RPC Proxy server", "(per host)"),
            build_server_rpc_message_size_stats_per_method(RpcProxyRpc, "RPC Proxy server")
                .aggr("proxy_role"),
            build_http_proxy(),
            build_tx_serialization_lag(),
        ]

        d = Dashboard()
        for r in rowsets:
            d.add(r)

        return d


def build_bundle_artemis():
    prefixes = ["tab", "exe", ""]
    with SplitNodeSensorsGuard(["dat", ""], prefixes, prefixes):
        # 1 means that the rowset should be tagged with tablet_cell_bundle={{bundle}}.
        rowsets = [
            (1, build_read_write_rowset()),
            (1, build_max_lookup_select_execute_time_per_host().aggr("#U").all(MonitoringTag("host"))),
            (1, build_replicator()),
            (1, build_background_resource_usage()),
            (1, build_tablet_balancer()),
            (1, build_thread_cpu()
                .top()),
            (1, build_hydra()),
            (1, build_cluster_node_throttlers_stuff()),
            (1, build_tablet_node_throttlers_stuff()),
        ]
        for i, (flag, rowset) in enumerate(rowsets):
            if flag == 1:
                rowsets[i] = rowset.value(
                    "tablet_cell_bundle",
                    TemplateTag("tablet_cell_bundle"))
            else:
                rowsets[i] = rowset

        d = Dashboard()
        for r in rowsets:
            d.add(r)

        d.add_parameter(
            "tablet_cell_bundle",
            "Tablet cell bundle",
            GrafanaTextboxDashboardParameter("default"))

        return d



def build_local_artemis_base():
    rowsets_local = [
        build_read_write_rowset(),
        build_max_lookup_select_execute_time_per_host().all("#U"),
        build_lookup_select_ack_time(),
        build_background_resource_usage(),
        build_lsm(local=True),
        build_memory(),
        build_memory_by_category(),
        build_thread_cpu(),
        build_rpc_request_rate(),
        build_tx_stats(),
        build_hydra(),
        build_changelogs(),
        build_chunk_meta_caches(),
        build_data_node_disk_stuff(),
        build_rpc_message_size_stats_per_host(
            TabNodeRpcClient, "client", "Rpc client (yt_node)"),
        build_rpc_message_size_stats_per_host(
            TabNodeRpc, "server", "Rpc server (yt_node)"),
        build_server_rpc_message_size_stats_per_method(TabNodeRpc, "Rpc server (yt_node)"),
        build_network_local(),
        build_job_cpu_usage(),
        build_cluster_node_throttlers_stuff(),
        build_data_node_throttlers_stuff(),
        build_tablet_node_throttlers_stuff(),
    ]

    d = Dashboard()
    for r in rowsets_local:
        d.add(r)
    return d

# TODO: remove grafana argument after system tags handing is fixed.
def build_local_artemis():
    prefixes = ["tab", "exe", ""]
    with SplitNodeSensorsGuard(["dat", ""], prefixes, prefixes):
        d = build_local_artemis_base()

        d.value(GrafanaTag("pod"), TemplateTag("pod"))
        d.value(MonitoringTag("host"), TemplateTag("host"))

        d.add_parameter(
            "pod",
            "Pod",
            GrafanaTextboxDashboardParameter("tnd-0"))

        return d

def build_local_artemis_container():
    rowsets = [
        build_read_write_rowset(),
        build_max_lookup_select_execute_time_per_host().all("#U"),
        build_lookup_select_ack_time(),
        build_background_resource_usage(),
        build_lsm(),
        build_memory(),
        build_memory_by_category(),
        build_thread_cpu_compressed(),
        build_rpc_request_rate(),
        build_tx_stats(),
        build_hydra(),
        build_chunk_meta_caches_container(),
        build_link_to_cache_dashboard_local(),
        build_tablet_node_preload_throttler(),
        build_rpc_message_size_stats_per_host(
            TabNodeRpcClient, "client", "Rpc client (yt_node)"),
        build_rpc_message_size_stats_per_host(
            TabNodeRpc, "server", "Rpc server (yt_node)"),
        build_server_rpc_message_size_stats_per_method(TabNodeRpc, "Rpc server (yt_node)"),
        build_network_local_porto(),
        build_fair_throttler(TabNode),
    ]

    try:
        from .constants import LEGACY_LOCAL_ARTEMIS_URL as legacy_url
        rowsets[:0] = [
            Rowset().row(height=2).cell("", Text(
                "This dashboard works for clusters with split tablet nodes "
                "(those under Bundle Controller). For other clusters please use "
                f"the [legacy dashboard]({legacy_url})."))
        ]
    except ImportError:
        pass

    d = Dashboard()
    for r in rowsets:
        if r is not None:
            d.add(r)

    d.value("container", TemplateTag("container"))
    d.all("#B", MonitoringTag("host"))
    d.set_title("Local Artemis (container)")
    d.set_description("Multi-purpose diagnostics dashboard for tablet node")
    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter(
            "yt",
            "cluster",
            ARTEMIS_DASHBOARD_DEFAULT_CLUSTER))
    d.add_parameter(
        "container",
        "Container",
        MonitoringLabelDashboardParameter(
            "yt",
            "container",
            ARTEMIS_DASHBOARD_DEFAULT_CONTAINER))

    d.set_monitoring_serializer_options(dict(halign="left"))

    return d
