# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import (
    DatNodeAll, ExeNode, ExeNodeCpu, ExeNodeMemory, ExeNodePorto,
    CA, NodeMonitor, ProjectSensor,
    yt_host,
)

try:
    from .constants import (
        DATA_NODES_COMMON_DASHBOARD_DEFAULT_CLUSTER,
        DATA_NODES_COMMON_DASHBOARD_DEFAULT_HOST,
    )
except ImportError:
    from .yandex_constants import (
        DATA_NODES_COMMON_DASHBOARD_DEFAULT_CLUSTER,
        DATA_NODES_COMMON_DASHBOARD_DEFAULT_HOST,
    )

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text
from yt_dashboard_generator.taggable import NotEquals, SystemFields
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr, PlainMonitoringExpr

from functools import partial


def _build_sensor(name, sensor, hidden=False):
    return DatNodeAll(sensor)               \
        .value("host", TemplateTag("host")) \
        .name(name)


def _make_percentile_sensor(backend, sensor, percentiles=None):
    if percentiles is None:
        percentiles = [99.9, 99, 95, 90, 75, 50]

    if backend == "monitoring":
        sensor = sensor.hidden(True)
        return MultiSensor(
            sensor,
            MonitoringExpr(MonitoringExpr.NodeType.Terminal, sensor.get_tags()[SystemFields.Name])
                .name(str(percentiles))
                .series_percentile(percentiles)
        )

    return MultiSensor(*[
        MonitoringExpr(sensor)
            .series_percentile(percentile)
            .legend_format(f"p{percentile}")
        for percentile in percentiles
    ])


def _make_throttling_ratio(backend, throttled_name, throttled_sensor, total_name, total_sensor, ratio_name):
    if backend == "monitoring":
        return MultiSensor(
            _build_sensor(throttled_name, throttled_sensor)
                .hidden(True)
                .name(throttled_name),
            _build_sensor(total_name, total_sensor)
                .hidden(True)
                .name(total_name),
            (MonitoringExpr(MonitoringExpr.NodeType.Terminal, 100) * MonitoringExpr(MonitoringExpr.NodeType.Terminal, throttled_name).series_sum("medium") /
                (MonitoringExpr(MonitoringExpr.NodeType.Terminal, throttled_name).series_sum("medium") + MonitoringExpr(MonitoringExpr.NodeType.Terminal, total_name).series_sum("medium")))
                .name(ratio_name)
        )

    throttled = MonitoringExpr(_build_sensor(throttled_name, throttled_sensor)).series_sum("medium")
    total = MonitoringExpr(_build_sensor(total_name, total_sensor)).series_sum("medium")
    return (MonitoringExpr(MonitoringExpr.NodeType.Terminal, 100) * throttled / (throttled + total)) \
        .legend_format("{{medium}}")


def _make_memory_category(backend, name, category):
    return _make_percentile_sensor(
        backend,
        _build_sensor(name, "yt.cluster_node.memory_usage.used")
            .hidden(True)
            .value("category", category))


def _build_versions(d, backend):
    _build_percentile_sensor = partial(_make_percentile_sensor, backend)
    _build_memory_category = partial(_make_memory_category, backend)
    _build_throttling_ratio = partial(_make_throttling_ratio, backend)

    d.add(Rowset().row(height=3).cell("", Text("Memory")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("Versions",
                MonitoringExpr(_build_sensor("Versions", "yt.build.version"))
                    .series_sum("version")
                    .stack(True))
            .cell("Memory",
                MultiSensor(
                    MonitoringExpr(_build_sensor("Footprint", "yt.cluster_node.memory_usage.used")
                        .value("category", "footprint"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("BlockCache", "yt.cluster_node.memory_usage.used")
                        .value("category", "block_cache"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("ChunkMeta", "yt.cluster_node.memory_usage.used")
                        .value("category", "chunk_meta"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("ChunkBlocksExt", "yt.cluster_node.memory_usage.used")
                        .value("category", "chunk_blocks_ext"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("ChunkBlockMeta", "yt.cluster_node.memory_usage.used")
                        .value("category", "chunk_block_meta"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("P2P", "yt.cluster_node.memory_usage.used")
                        .value("category", "p2_p"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("VersionedChunkMeta", "yt.cluster_node.memory_usage.used")
                        .value("category", "versioned_chunk_meta"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("PendingDiskRead", "yt.cluster_node.memory_usage.used")
                        .value("category", "pending_disk_read"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("PendingDiskWrite", "yt.cluster_node.memory_usage.used")
                        .value("category", "pending_disk_write"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("RPC", "yt.cluster_node.memory_usage.used")
                        .value("category", "rpc"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("SystemJobs", "yt.cluster_node.memory_usage.used")
                        .value("category", "system_jobs"))
                        .series_sum("category")
                        .stack(True),
                    MonitoringExpr(_build_sensor("UserJobs", "yt.cluster_node.memory_usage.used")
                        .value("category", "user_jobs"))
                        .series_sum("category")
                        .stack(True)
                )
            )
            .cell("OOMs",
                MonitoringExpr(_build_sensor("OOMs", "yt.porto.memory.oom_kills_total")
                    .value("container_category", "pod"))
                    .series_sum()
                    .stack(True))
        .row()
            .cell("Footprint",
                _build_memory_category("Footprint", "footprint"))
            .cell("Alloc fragmentation",
                _build_memory_category("AllocFragmentation", "alloc_fragmentation"))
            .cell("RPC",
                _build_memory_category("RPC", "rpc"))
        .row()
            .cell("Pending disk read",
                _build_memory_category("PendingDiskRead", "pending_disk_read"))
            .cell("Pending disk write",
                _build_memory_category("PendingDiskWrite", "pending_disk_write"))
            .cell("System Jobs",
                _build_memory_category("SystemJobs", "system_jobs"))
    )

    d.add(Rowset().row(height=3).cell("", Text("Rpc server")))
    d.add(Rowset()
        .aggr("network", "encrypted", "band", "bucket")
        .stack(False)
        .row()
            .cell("Pending out bytes",
                _build_percentile_sensor(
                    _build_sensor("PendingOutBytes", "yt.bus.pending_out_bytes")))
            .cell("Out throttler value rate",
                _build_percentile_sensor(
                    _build_sensor("OutThrottlerValueRate", "yt.cluster_node.out_throttler.value.rate"))
            )
            .cell("Out throttler quota",
                _build_percentile_sensor(
                    _build_sensor("OutThrottlerQuota", "yt.cluster_node.out_throttler.quota"),
                    [0.1, 1, 5, 10, 25, 50])
            )
        .row()
            .cell("Client connections",
                _build_percentile_sensor(
                    _build_sensor("PendingOutBytes", "yt.bus.client_connections")))
            .cell("Server connections",
                _build_percentile_sensor(
                    _build_sensor("OutThrottlerValueRate", "yt.bus.server_connections")))
    )

    d.add(Rowset().row(height=3).cell("", Text("Rpc server requests")))
    d.add(Rowset()
        .aggr("network", "encrypted", "band", "queue")
        .stack(False)
        .row()
            .value("method", "ProbeBlockSet")
            .cell("ProbeBlockSet request count rate",
                _build_percentile_sensor(
                    _build_sensor("ProbeBlockSetRequestCountRate", "yt.rpc.server.request_count.rate"))
            )
            .cell("ProbeBlockSet failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("ProbeBlockSetFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate"))
            )
            .cell("ProbeBlockSet timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("ProbeBlockSetTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate"))
            )
        .row()
            .value("method", "GetBlockSet")
            .cell("GetBlockSet request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetBlockSetRequestCountRate", "yt.rpc.server.request_count.rate"))
            )
            .cell("GetBlockSet failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetBlockSetFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate"))
            )
            .cell("GetBlockSet timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetBlockSetTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate"))
            )
        .row()
            .value("method", "GetChunkMeta")
            .cell("GetChunkMeta request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetChunkMetaRequestCountRate", "yt.rpc.server.request_count.rate"))
            )
            .cell("GetChunkMeta failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetChunkMetaFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate"))
            )
            .cell("GetChunkMeta timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetChunkMetaTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate"))
            )
        .row()
            .value("method", "StartChunk")
            .cell("StartChunk request count rate",
                _build_percentile_sensor(
                    _build_sensor("StartChunkRequestCountRate", "yt.rpc.server.request_count.rate"))
            )
            .cell("StartChunk failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("StartChunkFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate"))
            ).cell("StartChunk timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("StartChunkTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate"))
            )
         .row()
            .value("method", "PutBlocks")
            .cell("PutBlocks request count rate",
                _build_percentile_sensor(
                    _build_sensor("PutBlocksRequestCountRate", "yt.rpc.server.request_count.rate"))
            )
            .cell("PutBlocks failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("PutBlocksFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate"))
            )
            .cell("PutBlocks timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("PutBlocksTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate"))
            )
        .row()
            .value("method", "SendBlocks")
            .cell("SendBlocks request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksRequestCountRate", "yt.rpc.server.request_count.rate"))
            )
            .cell("SendBlocks failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate"))
            )
            .cell("SendBlocks timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("Rpc server attachments")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("GetChunkMeta Rpc attachment size",
                MonitoringExpr(_build_sensor("GetChunkMetaRpcAttachmentSize", "yt.rpc.server.response_message_body_bytes.rate|yt.rpc.server.response_message_attachment_bytes.rate")
                    .value("method", "GetChunkMeta")
                    .all("queue"))
                    .series_sum("queue")
                    .stack(True)
            )
            .cell("GetBlockSet Rpc attachment size",
                MonitoringExpr(_build_sensor("GetBlockSetRpcAttachmentSize", "yt.rpc.server.response_message_body_bytes.rate|yt.rpc.server.response_message_attachment_bytes.rate")
                    .value("method", "GetBlockSet")
                    .all("queue"))
                    .series_sum("queue")
                    .stack(True)
            )
            .cell("PutBlocks Rpc attachment size",
                MonitoringExpr(_build_sensor("PutBlocksRpcAttachmentSize", "yt.rpc.server.request_message_attachment_bytes.rate|yt.rpc.server.request_message_body_bytes.rate")
                    .value("method", "PutBlocks")
                    .aggr("queue"))
                    .series_sum()
                    .stack(True)
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("CPU")))
    d.add(Rowset()
        .value("container_category", "pod")
        .stack(False)
        .row()
            .cell("Porto cpu total",
                _build_percentile_sensor(_build_sensor("PortoCpuTotal", "yt.porto.cpu.total"))
            )
            .cell("Porto cpu throttled",
                _build_percentile_sensor(_build_sensor("PortoCpuThrottled", "yt.porto.cpu.throttled"))
            )
            .cell("Porto cpu wait",
                _build_percentile_sensor(_build_sensor("PortoCpuWait", "yt.porto.cpu.wait"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("BusXferFS Thread")))
    d.add(Rowset()
        .value("thread", "BusXferFS")
        .stack(False)
        .row()
            .cell("BusXferFS cpu total",
                _build_percentile_sensor(_build_sensor("BusXferFSCpuTotal", "yt.resource_tracker.total_cpu"))
            )
            .cell("BusXferFS cpu util",
                _build_percentile_sensor(_build_sensor("BusXferFSCpuUtil", "yt.resource_tracker.utilization"))
            )
            .cell("BusXferFS cpu wait",
                _build_percentile_sensor(_build_sensor("BusXferFSCpuWait", "yt.resource_tracker.cpu_wait"))
            )
        .row()
            .cell("BusXferFS avg exec time",
                _build_percentile_sensor(_build_sensor("BusXferFSAvgExecTime", "yt.fair_share_queue.time.exec.avg"))
            )
            .cell("BusXferFS max exec time",
                _build_percentile_sensor(_build_sensor("BusXferFSMaxExecTime", "yt.fair_share_queue.time.exec.max"))
            )
        .row()
            .cell("BusXferFS avg wait time",
                _build_percentile_sensor(_build_sensor("BusXferFSAvgWaitTime", "yt.fair_share_queue.time.wait.avg"))
            )
            .cell("BusXferFS max wait time",
                _build_percentile_sensor(_build_sensor("BusXferFSMaxWaitTime", "yt.fair_share_queue.time.wait.max"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("StorageLight Thread")))
    d.add(Rowset()
        .value("thread", "StorageLight")
        .stack(False)
        .row()
            .cell("StorageLight cpu total",
                _build_percentile_sensor(_build_sensor("StorageLightCpuTotal", "yt.resource_tracker.total_cpu"))
            )
            .cell("StorageLight cpu util",
                _build_percentile_sensor(_build_sensor("StorageLightCpuUtil", "yt.resource_tracker.utilization"))
            )
            .cell("StorageLight cpu wait",
                _build_percentile_sensor(_build_sensor("StorageLightCpuWait", "yt.resource_tracker.cpu_wait"))
            )
        .row()
            .cell("StorageLight avg exec time",
                _build_percentile_sensor(_build_sensor("StorageLightAvgExecTime", "yt.action_queue.time.exec.avg"))
            )
            .cell("StorageLight max exec time",
                _build_percentile_sensor(_build_sensor("StorageLightMaxExecTime", "yt.action_queue.time.exec.max"))
            )
        .row()
            .cell("StorageLight avg wait time",
                _build_percentile_sensor(_build_sensor("StorageLightAvgWaitTime", "yt.action_queue.time.wait.avg"))
            )
            .cell("StorageLight max wait time",
                _build_percentile_sensor(_build_sensor("StorageLightMaxWaitTime", "yt.action_queue.time.wait.max"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("StorageHeavy Thread")))
    d.add(Rowset()
        .value("thread", "StorageHeavy")
        .stack(False)
        .row()
            .cell("StorageHeavy cpu total",
                _build_percentile_sensor(_build_sensor("StorageHeavyCpuTotal", "yt.resource_tracker.total_cpu"))
            )
            .cell("StorageHeavy cpu util",
                _build_percentile_sensor(_build_sensor("StorageHeavyCpuUtil", "yt.resource_tracker.utilization"))
            )
            .cell("StorageHeavy cpu wait",
                _build_percentile_sensor(_build_sensor("StorageHeavyCpuWait", "yt.resource_tracker.cpu_wait"))
            )
        .row()
            .cell("StorageHeavy avg exec time",
                _build_percentile_sensor(_build_sensor("StorageHeavyAvgExecTime", "yt.action_queue.time.exec.avg"))
            )
            .cell("StorageHeavy max exec time",
                _build_percentile_sensor(_build_sensor("StorageHeavyMaxExecTime", "yt.action_queue.time.exec.max"))
            )
        .row()
            .cell("StorageHeavy avg wait time",
                _build_percentile_sensor(_build_sensor("StorageHeavyAvgWaitTime", "yt.action_queue.time.wait.avg"))
            )
            .cell("StorageHeavy max wait time",
                _build_percentile_sensor(_build_sensor("StorageHeavyMaxWaitTime", "yt.action_queue.time.wait.max"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("System jobs")))
    d.add(Rowset()
        .value("state", "acquired")
        .stack(False)
        .row()
            .cell("Important system jobs",
                MultiSensor(
                    MonitoringExpr(_build_sensor("RepairJobs", "yt.job_controller.resource_usage.repair_slots"))
                        .series_sum()
                        .stack(True),
                    MonitoringExpr(_build_sensor("ReplicationJobs", "yt.job_controller.resource_usage.replication_slots"))
                        .series_sum()
                        .stack(True),
                    MonitoringExpr(_build_sensor("MergeJobs", "yt.job_controller.resource_usage.merge_slots"))
                        .series_sum()
                        .stack(True)
                )
            )
            .cell("System Jobs",
                _build_memory_category("SystemJobs", "system_jobs"))
        .row()
            .cell("Replication jobs",
                _build_percentile_sensor(_build_sensor("ReplicationJobs", "yt.job_controller.resource_usage.replication_slots")
                    .hidden(True))
            )
            .cell("Repair jobs",
                _build_percentile_sensor(_build_sensor("RepairJobs", "yt.job_controller.resource_usage.repair_slots")
                    .hidden(True))
            )
            .cell("Merge jobs",
                _build_percentile_sensor(_build_sensor("MergeJobs", "yt.job_controller.resource_usage.merge_slots")
                    .hidden(True))
            )
        .row()
            .cell("Removal jobs",
                _build_percentile_sensor(_build_sensor("RemovalJobs", "yt.job_controller.resource_usage.removal_slots")
                    .hidden(True))
            )
            .cell("Reincarnation jobs",
                _build_percentile_sensor(_build_sensor("ReincarnationJobs", "yt.job_controller.resource_usage.reincarnation_slots")
                    .hidden(True))
            )
            .cell("Autotomy jobs",
                _build_percentile_sensor(_build_sensor("AutotomyJobs", "yt.job_controller.resource_usage.autotomy_slots")
                    .hidden(True))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("IO")))
    d.add(Rowset()
        .all("medium")
        .aggr("location_id")
        .value("location_type", "store")
        .stack(False)
        .row()
            .cell("Throttling Writes",
                _build_throttling_ratio(
                    "ThrottlingWrites", "yt.location.throttled_writes.rate",
                    "Writes", "yt.location.write.request_count.rate",
                    "WritePercent"))
            .cell("Throttling Reads",
                _build_throttling_ratio(
                    "ThrottlingReads", "yt.location.throttled_reads.rate",
                    "Reads", "yt.location.read.request_count.rate",
                    "ReadPercent"))
        .row()
            .cell("Disk in queue size",
                MonitoringExpr(_build_sensor("DiskInQueueSize", "yt.location.disk_throttler.*in*.queue_size"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
            .cell("Disk out queue size",
                MonitoringExpr(_build_sensor("DiskOutQueueSize", "yt.location.disk_throttler.*out*.queue_size"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
    )

    d.add(Rowset()
        .all("medium")
        .value("location_type", "store")
        .stack(False)
        .row()
            .aggr("location_id")
            .all("disk_family")
            .cell("Disk in value rate",
                MonitoringExpr(_build_sensor("DiskInValueRate", "yt.location.disk_throttler.*in*.value.rate"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
            .cell("Disk out value rate",
                MonitoringExpr(_build_sensor("DiskOutValueRate", "yt.location.disk_throttler.*out*.value.rate"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
        .row()
            .all("category")
            .cell("Used memory for writes",
                MonitoringExpr(_build_sensor("UsedMemoryForWrites", "yt.location.used_memory")
                    .value("direction", "write"))
                    .series_sum("medium", "category", "direction", "sensor")
                    .stack(True)
            )
            .cell("Used memory for reads",
                MonitoringExpr(_build_sensor("UsedMemoryForReads", "yt.location.used_memory")
                    .value("direction", "read"))
                    .series_sum("medium", "category", "direction", "sensor")
                    .stack(True)
            )
    )


def build_data_nodes_common(backend="monitoring"):
    d = Dashboard()

    _build_versions(d, backend)

    d.set_monitoring_serializer_options(dict(default_row_height=8))
    d.set_grafana_serializer_options(dict(default_row_height=8))

    d.set_title("Data Nodes Common [Autogenerated]")

    d.add_parameter(
        "cluster",
        "cluster",
        MonitoringLabelDashboardParameter(
            "yt",
            "cluster",
            DATA_NODES_COMMON_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"],
    )

    d.add_parameter(
        "cluster",
        "Cluster",
        GrafanaTextboxDashboardParameter(DATA_NODES_COMMON_DASHBOARD_DEFAULT_CLUSTER),
        backends=["grafana"],
    )

    d.add_parameter(
        "host",
        "host",
        MonitoringLabelDashboardParameter(
            "yt",
            "host",
            DATA_NODES_COMMON_DASHBOARD_DEFAULT_HOST),
        backends=["monitoring"],
    )

    d.add_parameter(
        "host",
        "Host",
        GrafanaTextboxDashboardParameter(DATA_NODES_COMMON_DASHBOARD_DEFAULT_HOST),
        backends=["grafana"],
    )

    return d
