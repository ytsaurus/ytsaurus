# flake8: noqa

from .common.sensors import *

from .key_filter import build_key_filter_rowset

try:
    from .constants import BUNDLE_UI_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    BUNDLE_UI_DASHBOARD_DEFAULT_CLUSTER = ""

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from yt_dashboard_generator.backends.monitoring.sensors import (
    MonitoringExpr)
from yt_dashboard_generator.backends.monitoring import (
    MonitoringTag, MonitoringLabelDashboardParameter)

from yt_dashboard_generator.sensor import (
    MultiSensor
)

##################################################################


def build_user_load():
    top_rate = NodeTablet("yt.tablet_node.{}.{}.rate")

    return (Rowset()
            .aggr(MonitoringTag("host"), "table_tag", "table_path")
            .all("#UB")
            .top()

            .row()
                .cell("Table write data weight rate", top_rate("write", "data_weight"))
                .cell("Table write row count rate", top_rate("write", "row_count"))
            .row()
                .cell("Table commit data weight rate", top_rate("commit", "data_weight"))
                .cell("Table commit row count rate", top_rate("commit", "row_count"))
            .row()
                .cell("Table lookup request count", top_rate("multiread", "request_count"))
                .cell("Table select request count", top_rate("execute", "request_count"))
            .row()
                .cell("Table lookup data weight rate", top_rate("lookup", "data_weight"))
                .cell("Table select data weight rate", top_rate("select", "data_weight"))
            .row()
                .cell("Table lookup unmerged data weight rate", top_rate("lookup", "unmerged_data_weight"))
                .cell("Table select unmerged data weight rate", top_rate("select", "unmerged_data_weight"))
            .row()
                .cell("Table lookup row count rate", top_rate("lookup", "row_count"))
                .cell("Table select row count rate", top_rate("select", "row_count"))
            .row()
                .cell("Table lookup unmerged row count rate", top_rate("lookup", "unmerged_row_count"))
                .cell("Table select unmerged row count rate", top_rate("select", "unmerged_row_count"))
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


def build_user_lsm():
    top_lsm = NodeTablet("yt.tablet_node.{}.{}")

    partitioning_dw = MonitoringExpr(NodeTablet(
        "yt.tablet_node.chunk_writer.data_weight.rate").value("method", "partitioning"))
    compaction_dw = MonitoringExpr(NodeTablet(
        "yt.tablet_node.chunk_writer.data_weight.rate").value("method", "compaction"))
    write_dw = MonitoringExpr(NodeTablet(
        "yt.tablet_node.write.data_weight.rate"))

    return (Rowset()
            .all(MonitoringTag("host"))
            .stack(False)
            .top()
            .row()
                .cell("Running compactions", top_lsm("store_compactor", "running_compactions"))
                .cell("Running partitionings", top_lsm("store_compactor", "running_partitionings"))
            .row()
                .cell("Feasible compactions", top_lsm("store_compactor", "feasible_compactions"))
                .cell("Feasible partitionings", top_lsm("store_compactor", "feasible_partitionings"))
            .row()
                .cell("Running flushes", top_lsm("store_flusher", "running_store_flushes"))
                .cell("Max overlapping store count", top_lsm("tablet", "overlapping_store_count.max"))
            .row()
                .aggr(MonitoringTag("host"), "table_tag", "table_path")
                .cell("LSM write amplification", (compaction_dw.moving_avg("10m") + partitioning_dw.moving_avg("10m")) / write_dw.moving_avg("10m"))
            ).owner


def build_user_thread_cpu():
    # total_cpu - cpu consumption in 100* of core
    # utilization is in 0..1
    # wait is in 100*core

    cpu_usage = (lambda thread: MultiSensor(
        TabNodeCpu("yt.resource_tracker.thread_count"),
        MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu")) / 100)
        .value("thread", thread))
    utilization = (MonitoringExpr(TabNodeCpu("yt.resource_tracker.utilization")).value("thread", "*").all(MonitoringTag("host")).alias("{{thread}} {{container}}") /
        MonitoringExpr(TabNodeCpu("yt.resource_tracker.thread_count")).value("thread", "Logging").aggr(MonitoringTag("host")))

    return (Rowset()
            .stack(False)
            # .all(MonitoringTag("host"))
            .top()
            .row()
                .cell("TabletLookup thread pool CPU usage", cpu_usage("TabletLookup"))
                .cell("Query thread pool CPU usage", cpu_usage("Query"))
            .row()
                .cell("TabletSlot thread pool CPU usage", cpu_usage("TabletSlot*"))
                .cell("Compression thread pool CPU usage", cpu_usage("Compression"))
            .row()
                .cell("StoreCompact thread pool CPU usage", cpu_usage("StoreCompact"))
                .cell("ChunkReader thread pool CPU usage", cpu_usage("ChunkReader"))
            .row()
                .cell("BusXfer thread pool CPU usage", cpu_usage("BusXfer"))
                .cell("Threads utilization", utilization)
            .row()
                .cell("CPU wait (all threads)", MonitoringExpr(TabNodeCpu("yt.resource_tracker.cpu_wait")
                                                            .aggr("thread"))/100)
                .cell("CPU usage (all threads)", MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu").aggr("thread")) / 100)
            ).owner

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
                .cell("Node restarts", MonitoringExpr(TabNode("yt.server.restarted")
                        .all(MonitoringTag("host"), "container")
                        .top()
                        .stack(True)
                        .value("window", "5min")).alias("{{container}}"))
                .cell("Node OOMs", MonitoringExpr(TabNodePorto("yt.porto.memory.oom_kills")
                        .value("container_category", "pod")
                        .all(MonitoringTag("host"), "container")
                        .top()
                        .stack(True))
                        .alias("{{container}}"))
            .row()
                .cell("Overload Controller", MonitoringExpr(NodeTablet("yt.tablet_node.overload_controller.overloaded.rate")
                    .all("tracker")
                    .stack(True)).alias("{{tracker}}"))
                .cell("Bundle Controller Alerts", MultiSensor(
                    MonitoringExpr(BundleController("yt.bundle_controller.scan_bundles_alarms_count.rate")
                        .all("alarm_id")).alias("{{alarm_id}}"))
                        .stack(True))
            .row()
                .cell("Target tablet node count", MonitoringExpr(bc("target_tablet_node_count")
                    .all("instance_size")).alias("target node count of size '{{instance_size}}'"))
                .cell("Alive tablet node count", MultiSensor(
                        MonitoringExpr(bc("alive_tablet_node_count")
                            .all("instance_size")).alias("alive bundle nodes of size '{{instance_size}}'"),
                        MonitoringExpr(bc("using_spare_node_count")).alias("assigned spare nodes")
                    ).stack(True))
            .row()
                .cell("Tablet node assignments", MultiSensor(
                        MonitoringExpr(bc("assigning_spare_nodes")).alias("assigning spare nodes"),
                        MonitoringExpr(bc("releasing_spare_nodes")).alias("releasing spare nodes"),
                        MonitoringExpr(bc("assigning_tablet_nodes")).alias("assigning new bundle nodes")
                    ).stack(True))
                .cell("Special tablet node states", MultiSensor(
                        MonitoringExpr(bc("maintenance_requested_node_count")).alias("maintenance requested nodes"),
                        MonitoringExpr(bc("decommissioned_node_count")).alias("decommissioned nodes"),
                        MonitoringExpr(bc("offline_node_count")).alias("offline nodes")
                    ).stack(True))
            .row()
                .cell("Inflight request count", MultiSensor(
                        MonitoringExpr(bc("inflight_node_allocations_count")).alias("inflight node allocations"),
                        MonitoringExpr(bc("inflight_node_deallocations_count")).alias("inflight node deallocations"),
                        MonitoringExpr(bc("inflight_cell_removal_count")).alias("inflight cell removal")
                    ).stack(True))
                .cell("Inflight request age", MultiSensor(
                        MonitoringExpr(bc("node_allocation_request_age")).alias("node allocation age max"),
                        MonitoringExpr(bc("node_deallocation_request_age")).alias("node deallocation age max"),
                        MonitoringExpr(bc("removing_cells_age")).alias("cell removal age max")
                    ).stack(False))
            ).owner


def build_user_memory():
    memory_usage = (lambda category: MultiSensor(
        Node("yt.cluster_node.memory_usage.used").value("category", category),
        Node("yt.cluster_node.memory_usage.limit").value("category", category)))

    return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("Tablet dynamic memory", memory_usage("tablet_dynamic"))
                .cell("Tablet static memory", memory_usage("tablet_static"))
            .row()
                .cell("Query memory usage", Node("yt.cluster_node.memory_usage.used").value("category", "query"))
                .cell("Tracked memory usage", Node("yt.cluster_node.memory_usage.total_used"))
            .row()
                .cell("Process memory usage (rss)", NodeMemory("yt.resource_tracker.memory_usage.rss"))
                .cell("Container (cgroup) memory usage", MultiSensor(NodeMemory("yt.memory.cgroup.rss"), NodeMemory("yt.memory.cgroup.memory_limit")))
            ).owner

def build_reserved_memory():
    TabNodeMemory = TabNode("yt.cluster_node.memory_usage.{}")
    user_categories = "block_cache|lookup_rows_cache|versioned_chunk_meta|tablet_dynamic|tablet_static"

    reserved_limit = (
        MonitoringExpr(TabNodeMemory("total_limit"))
        - MonitoringExpr(TabNodeMemory("limit").value("category", user_categories))
            .series_sum("container")
    ).top_max(1).alias("Limit")

    reserved_usage = (
        MonitoringExpr(TabNodeMemory("used")
            .value("category", "!{}".format(user_categories)))
            .series_sum("container")
            .alias("Usage {{container}}").top_max(10)
    )

    return (Rowset()
            .stack(False)
            .row()
                .cell("Reserved memory usage",  MultiSensor(reserved_limit, reserved_usage))
                .cell("Footprint and Fragmentation",  MultiSensor(
                        MonitoringExpr(TabNodeMemory("used").value("category", "footprint"))
                            .alias("footprint {{container}}"),
                        MonitoringExpr(TabNodeMemory("used").value("category", "alloc_fragmentation"))
                            .alias("fragmentation {{container}}"))
                      .top(1)
                      .stack(True))
            ).owner


def build_tablet_network():
    reader_stats = NodeTablet(
        "yt.tablet_node.{}.chunk_reader_statistics.{}.rate")

    return (Rowset()
            .aggr("table_tag", "table_path")
            .all("#UB")
            .top()
            .stack(True)
            .row()
                .cell("Table lookup bytes received", reader_stats("lookup", "data_bytes_transmitted"))
                .cell("Table select bytes received", reader_stats("select", "data_bytes_transmitted"))
            ).owner

def build_user_network():
    return (Rowset()
            .top()
            .aggr("network", "band", "encrypted")
            .row()
                .cell("Network received bytes (bus)", NodeInternal("yt.bus.in_bytes.rate"))
                .cell("Network transmitted bytes (bus)", NodeInternal("yt.bus.out_bytes.rate"))
            .row()
                .cell("Pending out bytes", NodeInternal("yt.bus.pending_out_bytes"))
                .cell("TCP retransmits rate", NodeInternal("yt.bus.retransmits.rate"))
            ).owner

def build_throttling():
    return (Rowset()
            .top()
            .row()
                .cell("Receive network throttled", TabNode("yt.cluster_node.in_throttler.throttled").all("bucket"))
                .cell("Receive network throttler bytes rate", MultiSensor(
                    TabNode("yt.cluster_node.in_throttler.value.rate"),
                    TabNode("yt.cluster_node.in_throttler.total_limit"))
                    .aggr("bucket"))
            .row()
                .cell("Transmit network throttled", TabNode("yt.cluster_node.out_throttler.throttled").all("bucket"))
                .cell("Transmit network throttler bytes rate", MultiSensor(
                    TabNode("yt.cluster_node.out_throttler.value.rate"),
                    TabNode("yt.cluster_node.out_throttler.total_limit"))
                    .aggr("bucket"))
            ).owner


def build_rpc_request_rate():
    request_rate = (NodeRpc("yt.rpc.server.{}.rate")
                    .aggr("#U")
                    .all("yt_service", "method")
                    .stack(False)
                    .top())
    return (Rowset()
            .row()
                .cell("RPC request rate", request_rate("request_count"))
                .cell("RPC rejected due to queue overflow", request_rate("request_queue_size_errors"))
            .row()
                .cell("RPC failed request rate", request_rate("failed_request_count"))
                .cell("RPC timed out request rate", request_rate("timed_out_request_count"))
            ).owner


def build_user_bus_cpu():
    return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("Bus thread pool CPU utilization", TabNodeCpu("yt.resource_tracker.utilization").value("thread", "Bus*"))
            ).owner


def build_lookup_select_ack_time():
    s = (NodeRpc("yt.rpc.server.request_time.remote_wait.max")
         .value("yt_service", "QueryService")
         .aggr("user")
         .stack(False)
         .top())
    return (Rowset()
            .row()
            .cell("Table lookup (QueryService::Multiread) RPC acknowledge time", s.value("method", "Multiread"))
            .cell("Table select (QueryService::Execute) RPC acknowledge time", s.value("method", "Execute"))
            ).owner


def build_rpc_message_size_stats_per_host(
    sensor_class, client_or_server, name_prefix, name_suffix=""
):
    s = (sensor_class("yt.rpc.{}.{{}}.rate".format(client_or_server))
         .aggr("method", "yt_service", "user")
         .stack(False)
         .top())
    if name_suffix:
        name_suffix = " " + name_suffix
    return (Rowset()
            .row()
                .cell("{} request message body size{}".format(name_prefix, name_suffix), s("request_message_body_bytes"))
                .cell("{} request message attachment size{}".format(name_prefix, name_suffix), s("request_message_attachment_bytes"))
            .row()
                .cell("{} response message body size{}".format(name_prefix, name_suffix), s("response_message_body_bytes"))
                .cell("{} response message attachment size{}".format(name_prefix, name_suffix), s("response_message_attachment_bytes"))
            ).owner

def build_user_caches():
    usage = Sensor("{}.hit_weight.rate")
    misses = Sensor("yt.{}_node.{}.missed_weight.rate")
    return (Rowset()
            .value("service", "*node")
            .stack(False)
            .top()
            .row()
                .value("service", "node_tablet")
                .cell("Versioned chunk meta cache hit weight rate", usage("yt.tablet_node.versioned_chunk_meta_cache"))
                .cell("Versioned chunk meta cache miss weight rate", misses("tablet", "versioned_chunk_meta_cache"))
            .row()
                .cell("Block cache hit weight rate", usage("yt.data_node.block_cache.*compressed_data"))
                .cell("Block cache miss weight rate", misses("data", "block_cache.*compressed_data"))
            .row()
                .cell("Block cache memory", Node("yt.cluster_node.memory_usage.used")
                    .value("category", "block_cache"))
                .cell("Cached versioned chunk meta memory", Node("yt.cluster_node.memory_usage.used")
                    .value("category", "versioned_chunk_meta"))
    ).owner

def build_block_cache_planning():
    miss_weight_rate = lambda name : MultiSensor(
        MonitoringExpr(Node("yt.data_node.block_cache.{}_data.missed_weight.rate".format(name))).alias("current missed weight rate"),
        MonitoringExpr(Node("yt.data_node.block_cache.{}_data.large_ghost_cache.missed_weight.rate".format(name))).alias("x2 larger cache missed weight rate"),
        MonitoringExpr(Node("yt.data_node.block_cache.{}_data.small_ghost_cache.missed_weight.rate".format(name))).alias("x/2 smaller cache missed weight rate"))

    return (Rowset()
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
            .stack(False)
            .top(1)
            .row()
                .cell("Compressed block cache size planning", miss_weight_rate("compressed"))
                .cell("Uncompressed block cache size planning", miss_weight_rate("uncompressed"))
    ).owner


def build_user_disk():
    reader_stats = MultiSensor(
        NodeTablet("yt.tablet_node.{}.chunk_reader_statistics.{}.rate"),
        NodeTablet("yt.tablet_node.{}.hunks.chunk_reader_statistics.{}.rate"))

    return (Rowset()
            .aggr("table_tag", "table_path", "user")
            .top()
            .row()
                .cell("Table lookup data bytes read from disk", reader_stats("lookup", "data_bytes_transmitted"))
                .cell("Table select data bytes read from disk", reader_stats("select", "data_bytes_transmitted"))
            .row()
                .cell("Table lookup chunk meta bytes read from disk", reader_stats("lookup", "meta_bytes_read_from_disk"))
                .cell("Table select chunk meta bytes read from disk", reader_stats("select", "meta_bytes_read_from_disk"))
            .row()
                .cell("Table lookup data wait time", reader_stats("lookup", "data_wait_time"))
                .cell("Table select data wait time", reader_stats("select", "data_wait_time"))
            .row()
                .cell("Table lookup meta wait time", reader_stats("lookup", "meta_wait_time"))
                .cell("Table select meta wait time", reader_stats("select", "meta_wait_time"))
            .row()
                .cell("Table lookup pick peer time", reader_stats("lookup", "pick_peer_wait_time"))
                .cell("Table select pick peer time", reader_stats("select", "pick_peer_wait_time"))
            .row()
                .cell("Table lookup meta disk read time", reader_stats("lookup", "meta_read_from_disk_time"))
                .cell("Table select meta disk read time", reader_stats("select", "meta_read_from_disk_time"))
            ).owner


def build_user_background_disk():
    top_disk = NodeTablet("yt.tablet_node.{}.{}.rate")

    return (Rowset()
            .all("#AB", "method", "medium")
            .aggr("table_tag", "table_path")
            .top()
            .row()
                .cell("Tablet background data bytes read from disk", top_disk("chunk_reader_statistics", "data_bytes_read_from_disk"))
                .cell("Tablet background chunk meta bytes read from disk", top_disk("chunk_reader_statistics", "meta_bytes_read_from_disk"))
            .row()
                .cell("Tablet background disk bytes written (with replication)", top_disk("chunk_writer", "disk_space"))
                .cell("Tablet background data weight written (without replication)", top_disk("chunk_writer", "data_weight"))
            ).owner


def build_user_resource_overview_rowset():
    def top_max_bottom_min(sensor):
        return [MonitoringExpr(TabNodePorto(sensor).value("container_category", "pod")
                                        .all(MonitoringTag("host")))
                                        .top_max(5)
                                        .alias("Usage {{container}}"),
                MonitoringExpr(TabNodePorto(sensor).value("container_category", "pod")
                    .all(MonitoringTag("host")))
                    .bottom_min(5)
                    .alias("Usage {{container}}")]

    return (Rowset()
            .stack(False)
            .row()
                .cell("CPU Total", MultiSensor(
                                    MonitoringExpr(TabNodePorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container CPU Guarantee")/100,
                                    MonitoringExpr(TabNodePorto("yt.porto.vcpu.total").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container CPU Usage")/100,
                                    MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu")
                                        .sensor_stack()
                                        .aggr(MonitoringTag("host"))
                                        .all("thread")).alias("{{thread}}")/100))
                .cell("CPU Per container", MultiSensor(
                                    MonitoringExpr(TabNodePorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .all(MonitoringTag("host")))
                                        .top(1)
                                        .alias("Guarantee {{container}}")/100,
                                    *[x / 100 for x in top_max_bottom_min("yt.porto.vcpu.total")]))
            .row()
                .cell("Memory Total", MultiSensor(
                                    MonitoringExpr(TabNodePorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Memory Guarantee"),
                                    MonitoringExpr(TabNodePorto("yt.porto.memory.anon_usage").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Memory Usage"),
                                    MonitoringExpr(Node("yt.cluster_node.memory_usage.used")
                                        .sensor_stack()
                                        .aggr(MonitoringTag("host"))
                                        .all("category")).alias("{{category}}")))
                .cell("Memory per container", MultiSensor(
                                    MonitoringExpr(TabNodePorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                    *top_max_bottom_min("yt.porto.memory.anon_usage")))
            .row()
                .cell("Net TX total", MultiSensor(
                                    MonitoringExpr(TabNodePorto("yt.porto.network.tx_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Tx Guarantee"),
                                    MonitoringExpr(TabNodePorto("yt.porto.network.tx_bytes").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Tx Bytes Rate"),
                                    MonitoringExpr(NodeInternal("yt.bus.out_bytes.rate")
                                        .aggr(MonitoringTag("host"), "band", "network")).alias("Node TX Bytes Rate")))
                .cell("Net TX per container", MultiSensor(MonitoringExpr(TabNodePorto("yt.porto.network.tx_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                        *top_max_bottom_min("yt.porto.network.tx_bytes")))
            .row()
                .cell("Net RX Total", MultiSensor(
                                    MonitoringExpr(TabNodePorto("yt.porto.network.rx_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Rx Guarantee"),
                                    MonitoringExpr(TabNodePorto("yt.porto.network.rx_bytes").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Rx Bytes Rate"),
                                    MonitoringExpr(NodeInternal("yt.bus.in_bytes.rate")
                                        .aggr(MonitoringTag("host"), "band", "network")).alias("Node RX Bytes Rate")))
                .cell("Net RX per container", MultiSensor(MonitoringExpr(TabNodePorto("yt.porto.network.rx_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                        *top_max_bottom_min("yt.porto.network.rx_bytes")))
            .row()
                .stack(True)
                .cell("Disk Write Total", MultiSensor(
                                    MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium")
                                        .all("method")).alias("{{method}}")))
                .cell("Disk Write per container", MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                                        .aggr("method", "table_path", "table_tag", "account", "medium"))
                                        .alias("{{container}}")
                                        .all(MonitoringTag("host")).top()
                                        .stack(False))
            .row()
                .stack(True)
                .cell("Disk Read Total", MultiSensor(
                                    MonitoringExpr(NodeTablet("yt.tablet_node.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium")
                                        .all("method")).alias("{{method}}"),
                                    MonitoringExpr(NodeTablet("yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                        .alias("lookup"),
                                    MonitoringExpr(NodeTablet("yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                        .alias("select")))
                .cell("Disk Read per container", (MonitoringExpr(NodeTablet("yt.tablet_node.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr("method", "table_path", "table_tag", "account", "medium") +
                                    MonitoringExpr(NodeTablet("yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr("method", "table_path", "table_tag", "user")) +
                                    MonitoringExpr(NodeTablet("yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr("method", "table_path", "table_tag", "user"))).alias("{{container}}"))
                                        .all(MonitoringTag("host")).top()
                                        .stack(False))
            .row()
                .stack(True)
                .cell("Master CPU", MonitoringExpr(Master("yt.tablet_server.update_tablet_stores.cumulative_time.rate")
                      .aggr(MonitoringTag("host"), "container", "table_type", "update_reason"))
                      .alias("Tablet Stores Update"))
            ).owner


def build_user_rpc_resource_overview_rowset():
    def top_max_bottom_min(sensor):
        return [MonitoringExpr(RpcProxyPorto(sensor).value("container_category", "pod")
                                        .all(MonitoringTag("host")))
                                        .top_max(5)
                                        .alias("Usage {{container}}"),
                MonitoringExpr(RpcProxyPorto(sensor).value("container_category", "pod")
                    .all(MonitoringTag("host")))
                    .bottom_min(5)
                    .alias("Usage {{container}}")]

    return (Rowset()
            .stack(False)
            .row()
                .cell("CPU Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container CPU Guarantee")/100,
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.total").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container CPU Usage")/100,
                                    MonitoringExpr(RpcProxyPorto("yt.resource_tracker.total_cpu")
                                        .sensor_stack()
                                        .aggr(MonitoringTag("host"))
                                        .all("thread")).alias("{{thread}}")/100))
                .cell("CPU Per container", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .all(MonitoringTag("host")))
                                        .top(1)
                                        .alias("Guarantee {{container}}")/100,
                                    *[x / 100 for x in top_max_bottom_min("yt.porto.vcpu.total")]))
            .row()
                .cell("Memory Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Memory Guarantee"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.anon_usage").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Memory Usage")))
                .cell("Memory per container", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                    *top_max_bottom_min("yt.porto.memory.anon_usage")))
            .row()
                .cell("Net TX total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Tx Guarantee"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_bytes").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Tx Bytes Rate"),
                                    MonitoringExpr(RpcProxyInternal("yt.bus.out_bytes.rate")
                                        .aggr(MonitoringTag("host"), "band", "network")).alias("Rpc Proxy TX Bytes Rate")))
                .cell("Net TX per container", MultiSensor(MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                        *top_max_bottom_min("yt.porto.network.tx_bytes")))
            .row()
                .cell("Net RX Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Rx Guarantee"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_bytes").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Rx Bytes Rate"),
                                    MonitoringExpr(RpcProxyInternal("yt.bus.in_bytes.rate")
                                        .aggr(MonitoringTag("host"), "band", "network")).alias("Rpc Proxy RX Bytes Rate")))
                .cell("Net RX per container", MultiSensor(MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                        *top_max_bottom_min("yt.porto.network.rx_bytes")))
    ).owner


def build_efficiency_rowset():
    return (Rowset()
            .stack(False)
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
            .row()
                .cell("Disk bytes written per user written byte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium", "method")) /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium", "method", "tablet_cell_bundle")) /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
                .cell("Compression CPU cores per user written gigabyte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.compression_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium", "method")) * 1073741824 /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.compression_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium", "method", "tablet_cell_bundle")) * 1073741824  /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
            .row()
                .cell("Disk bytes read per user lookup byte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user")) /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user", "tablet_cell_bundle")) /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
                .cell("Disk bytes read per user select byte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user")) /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user", "tablet_cell_bundle")) /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
            .row()
                .cell("Compression CPU cores per user lookup gigabyte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.decompression_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user")) * 1073741824 /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.decompression_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user", "tablet_cell_bundle")) * 1073741824  /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
                .cell("Compression CPU cores per user select gigabyte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.decompression_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user")) * 1073741824 /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.decompression_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user", "tablet_cell_bundle")) * 1073741824  /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
            .row()
                .cell("Lookup CPU cores per user lookup gigabyte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.multiread.cumulative_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user")) * 1073741824 /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.multiread.cumulative_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user", "tablet_cell_bundle")) * 1073741824  /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
                .cell("Select CPU cores per user select gigabyte", MultiSensor(
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.execute.cumulative_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user")) * 1073741824 /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                    ).alias("{{tablet_cell_bundle}}"),
                                    (
                                        MonitoringExpr(NodeTablet("yt.tablet_node.execute.cumulative_cpu_time.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_path", "table_tag", "user", "tablet_cell_bundle")) * 1073741824  /
                                        MonitoringExpr(NodeTablet("yt.tablet_node.select.data_weight.rate")
                                                .aggr(MonitoringTag("host"), "table_path", "table_tag", "user", "tablet_cell_bundle"))
                                    ).alias("whole {{cluster}}")))
            ).owner


def with_trend(sensor, limit_sensor):
    return MultiSensor(MonitoringExpr(sensor).alias("Usage"),
                MonitoringExpr(sensor).linear_trend("-45d", "60d").alias("Trend"),
                MonitoringExpr(limit_sensor).alias("Limit"))

def build_tablet_static_planning():
    memory_usage = (lambda category: with_trend(
        Node("yt.cluster_node.memory_usage.used").value("category", category),
        Node("yt.cluster_node.memory_usage.limit").value("category", category)))

    return (Rowset()
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
            .stack(False)
            .aggr(MonitoringTag("host"))
            .row()
                .cell("Tablet static memory", memory_usage("tablet_static")))

def build_node_resource_capacity_planning():
    per_container_limit=629145600
    node_count = MonitoringExpr(BundleController("yt.bundle_controller.resource.target_tablet_node_count")).all(MonitoringTag("host")).top(1)
    return (Rowset()
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
            .stack(False)
            .row()
                .cell("Tablet Node CPU Total", with_trend(
                                    MonitoringExpr(TabNodePorto("yt.porto.vcpu.total").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100,
                                    MonitoringExpr(TabNodePorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100))
                .cell("Tablet Node Memory Total", with_trend(
                                    MonitoringExpr(TabNodePorto("yt.porto.memory.anon_usage").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))),
                                    MonitoringExpr(TabNodePorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))))
            .row()
                .cell("Tablet Node Net TX total", with_trend(
                                    TabNodePorto("yt.porto.network.tx_bytes").value("container_category", "pod").aggr(MonitoringTag("host")),
                                    node_count * per_container_limit))
                .cell("Tablet Node Net RX Total", with_trend(
                                    TabNodePorto("yt.porto.network.rx_bytes").value("container_category", "pod").aggr(MonitoringTag("host")),
                                    node_count * per_container_limit))
            .row()
                .stack(True)
                .cell("Disk Write Total", MultiSensor(
                                    MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium")
                                        .all("method")).alias("{{method}}")))
                .cell("Disk Read Total", MultiSensor(
                                    MonitoringExpr(NodeTablet("yt.tablet_node.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium")
                                        .all("method")).alias("{{method}}"),
                                    MonitoringExpr(NodeTablet("yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                        .alias("lookup"),
                                    MonitoringExpr(NodeTablet("yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                        .alias("select")))
            ).owner

def build_rpc_proxy_resource_capacity_planning():
    per_container_limit=157286400
    rpc_proxy_count = MonitoringExpr(BundleController("yt.bundle_controller.resource.target_rpc_proxy_count")).all(MonitoringTag("host")).top(1).value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
    return (Rowset()
            .stack(False)
            .row()
                .value("proxy_role", TemplateTag("tablet_cell_bundle"))
                .cell("Rpc Proxy CPU Total", with_trend(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.total").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100,
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100))
                .cell("Rpc Proxy Memory Total", with_trend(
                                    RpcProxyPorto("yt.porto.memory.anon_usage").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")),
                                    RpcProxyPorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))))
            .row()
                .cell("Rpc Proxy Net TX total", with_trend(
                                    RpcProxyPorto("yt.porto.network.tx_bytes").value("container_category", "pod")
                                        .value("proxy_role", TemplateTag("tablet_cell_bundle"))
                                        .aggr(MonitoringTag("host")),
                                    rpc_proxy_count * per_container_limit))
                .cell("Rpc Proxy Net RX Total", with_trend(
                                    RpcProxyPorto("yt.porto.network.rx_bytes").value("container_category", "pod")
                                        .value("proxy_role", TemplateTag("tablet_cell_bundle"))
                                        .aggr(MonitoringTag("host")),
                                    rpc_proxy_count.alias("Container Net RX Limit") * per_container_limit))
            ).owner


##################################################################

def build_rpc_proxy_cpu():
    utilization = MonitoringExpr(RpcProxyCpu("yt.resource_tracker.utilization")).value("thread", "*").alias("{{thread}} {{container}}")
    cpu_usage = (lambda thread: MultiSensor(RpcProxyCpu("yt.resource_tracker.thread_count"),
        MonitoringExpr(RpcProxyCpu("yt.resource_tracker.total_cpu")) / 100)
        .value("thread", thread))

    return (Rowset()
            .stack(False)
            .value("proxy_role", TemplateTag("tablet_cell_bundle"))
            .top()
            .row()
                .cell("CPU Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod"))
                                        .top(3)
                                        .alias("Container CPU Guarantee {{container}}")/100,
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.total").value("container_category", "pod"))
                                        .alias("Container CPU Usage {{container}}")/100))
                .cell("Memory Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.memory_limit").value("container_category", "pod"))
                                        .alias("Container Memory Guarantee {{container}}"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.anon_usage").value("container_category", "pod"))
                                        .alias("Container Memory Usage {{container}}")))
            .row()
                .cell("Worker thread pool CPU usage", cpu_usage("Worker"))
                .cell("BusXfer thread pool CPU usage", cpu_usage("BusXfer"))
            .row()
                .cell("CPU wait (all threads)", MonitoringExpr(RpcProxyCpu("yt.resource_tracker.cpu_wait")
                                                            .aggr("thread"))/100)
                .cell("Threads utilization", utilization)
            ).owner


def build_rpc_proxy_network():
        return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("Net TX total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_limit").value("container_category", "pod"))
                                        .alias("Container Net Tx Guarantee {{container}}"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_bytes").value("container_category", "pod"))
                                        .alias("Container Net Tx Bytes Rate {{container}}"),
                                    MonitoringExpr(RpcProxyInternal("yt.bus.out_bytes.rate")
                                        .aggr("band", "network")).alias("Rpc Proxy TX Bytes Rate {{container}}")))
                .cell("Net RX Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_limit").value("container_category", "pod"))
                                        .alias("Container Net Rx Guarantee {{container}}"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_bytes").value("container_category", "pod"))
                                        .alias("Container Net Rx Bytes Rate {{container}}"),
                                    MonitoringExpr(RpcProxyInternal("yt.bus.in_bytes.rate")
                                        .aggr("band", "network")).alias("Rpc Proxy RX Bytes Rate {{container}}")))
            .row()
                .cell("Pending out bytes", RpcProxyInternal("yt.bus.pending_out_bytes"))
                .cell("TCP retransmits rate", RpcProxyInternal("yt.bus.retransmits.rate"))
    ).owner

def build_rpc_proxy_rpc_request_rate():
    request_rate = (RpcProxyRpc("yt.rpc.server.{}.rate")
                    .aggr("#U")
                    .all("yt_service", "method"))

    return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("RPC request rate", request_rate("request_count"))
                .cell("RPC inflight", RpcProxyRpc("yt.rpc.server.concurrency")
                    .aggr("#U")
                    .all("yt_service", "method"))
            .row()
                .cell("RPC failed request rate", request_rate("failed_request_count"))
                .cell("RPC timed out request rate", request_rate("timed_out_request_count"))
            ).owner

def build_rpc_proxy_rpc_request_wait():
    request_wait_max = (RpcProxyRpc("yt.rpc.server.request_time.{}.max")
                    .aggr("#U")
                    .all("yt_service", "method"))

    return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("RPC local wait max", request_wait_max("local_wait"))
                .cell("RPC remote wait max", request_wait_max("remote_wait"))
            ).owner

def build_rpc_proxy_maintenance():
    return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("Rpc Proxy restarts", MonitoringExpr(RpcProxy("yt.server.restarted")
                        .stack(True)
                        .value("window", "5min")).alias("{{container}}"))
                .cell("Rpc Proxy OOMs", MonitoringExpr(RpcProxyPorto("yt.porto.memory.oom_kills").value("container_category", "pod"))
                      .alias("{{container}}"))
            ).owner

##################################################################

def build_dashboard(rowsets):
    d = Dashboard()
    for r in rowsets:
        d.add(r)
    return d.value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))

##################################################################

def build_rpc_dashboard(rowsets):
    d = Dashboard()
    for r in rowsets:
        d.add(r)
    return d.value("proxy_role", TemplateTag("proxy_role"))

##################################################################

def build_bundle_ui_user_load():
    rowsets = [
        build_max_lookup_select_execute_time_per_host().aggr("#U").all(MonitoringTag("host")),
        build_user_load(),
    ]
    return build_dashboard(rowsets)


def build_bundle_ui_lsm():
    rowsets = [
        build_user_lsm(),
    ]
    return build_dashboard(rowsets)


def build_bundle_ui_downtime():
    rowsets = [
        build_user_hydra(),
        build_tablet_balancer(),
        build_bundle_controller(),
    ]
    return build_dashboard(rowsets)


def build_bundle_ui_cpu():
    rowsets = [
        build_user_thread_cpu(),
    ]
    return (build_dashboard(rowsets)
        .value(MonitoringTag("host"), TemplateTag("host"))
        .value(GrafanaTag("pod"), TemplateTag("pod")))


def build_bundle_ui_memory():
    rowsets = [
        build_user_memory(),
        build_reserved_memory()
    ]
    return (build_dashboard(rowsets)
        .value(MonitoringTag("host"), TemplateTag("host"))
        .value(GrafanaTag("pod"), TemplateTag("pod")))


def build_bundle_ui_network():
    rowsets = [
        build_tablet_network(),
        build_user_network(),
        build_throttling(),
        build_rpc_request_rate(),
        #build_user_bus_cpu(),
        build_lookup_select_ack_time(),
        build_rpc_message_size_stats_per_host(
            NodeRpcClient, "client", "Rpc client (yt_node)"),
        build_rpc_message_size_stats_per_host(
            NodeRpc, "server", "Rpc server (yt_node)"),
    ]
    return (build_dashboard(rowsets)
        .value(MonitoringTag("host"), TemplateTag("host"))
        .value(GrafanaTag("pod"), TemplateTag("pod")))


def build_bundle_ui_disk():
    rowsets = [
        build_user_disk(),
        build_user_background_disk(),
        build_user_caches(),
        build_block_cache_planning()
    ]
    return (build_dashboard(rowsets)
        .value(MonitoringTag("host"), TemplateTag("host"))
        .value(GrafanaTag("pod"), TemplateTag("pod")))


def build_bundle_ui_resource_overview():
    rowsets = [
        build_user_resource_overview_rowset()
    ]
    return build_dashboard(rowsets)

def build_bundle_ui_rpc_resource_overview():
    rowsets = [
        build_user_rpc_resource_overview_rowset()
    ]
    return build_rpc_dashboard(rowsets)

def build_bundle_rpc_proxy_dashboard():
    rowsets = [
        build_rpc_proxy_cpu(),
        build_rpc_proxy_network(),
        build_rpc_proxy_rpc_request_rate(),
        build_rpc_proxy_rpc_request_wait(),
        build_rpc_proxy_maintenance(),
    ]
    return (build_rpc_dashboard(rowsets)
        .value(MonitoringTag("host"), TemplateTag("host"))
        .value(GrafanaTag("pod"), TemplateTag("pod")))

def build_bundle_ui_efficiency():
    rowsets = [
        build_efficiency_rowset()
    ]

    d = Dashboard()
    for r in rowsets:
        d.add(r)
    return d


def build_bundle_capacity_planning():
    rowsets = [
        build_tablet_static_planning(),
        build_node_resource_capacity_planning(),
        build_block_cache_planning(),
        build_rpc_proxy_resource_capacity_planning(),
    ]

    d = Dashboard()
    for r in rowsets:
        d.add(r)
    return d

def build_bundle_ui_key_filter():
    filter_cache = TabNode("yt.data_node.block_cache.xor_filter.{}")

    d = Dashboard()

    d.set_title("Bundle UI Key Filter")
    d.add_parameter("cluster", "YT cluster", MonitoringLabelDashboardParameter("yt", "cluster", BUNDLE_UI_DASHBOARD_DEFAULT_CLUSTER))
    d.add_parameter("tablet_cell_bundle", "Tablet cell bundle", MonitoringLabelDashboardParameter("yt", "tablet_cell_bundle", "default"))

    d.add(build_key_filter_rowset())
    d.add(Rowset()
        .min(0)
        .row()
            .cell("Lookup key filter block cache usage", filter_cache("weight"))
            .cell("Lookup cumulative cpu time", NodeTablet("yt.tablet_node.lookup.cpu_time.rate"))
        .row()
            .cell("Key filter cache hit count rate", MultiSensor(
                filter_cache("hit_count.rate"),
                filter_cache("miss_count.rate"),
            ))
            .cell("Key filter cache hit weight rate", MultiSensor(
                filter_cache("hit_weight.rate"),
                filter_cache("miss_weight.rate"),
            )))

    return (d
        .value(MonitoringTag("cluster"), TemplateTag("cluster"))
        .value(MonitoringTag("tablet_cell_bundle"), TemplateTag("tablet_cell_bundle"))
        .aggr("user", "table_path", "table_tag", "host"))
