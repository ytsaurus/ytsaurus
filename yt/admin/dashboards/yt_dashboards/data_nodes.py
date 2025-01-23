# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import (
    ExeNode, ExeNodeCpu, ExeNodeMemory, ExeNodePorto,
    CA, NodeMonitor, ProjectSensor,
    yt_host,
)

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text
from yt_dashboard_generator.taggable import NotEquals

from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr, PlainMonitoringExpr

DataNode = ProjectSensor("dat_node*|node*", "yt-data-node.*")


def _build_sensor(name, sensor, hidden=False):
    return DataNode(sensor)        \
        .value("host", "{{host}}") \
        .name(name)


def _build_percentile_sensor(sensor, percentiles=[99.9, 99, 95, 90, 75, 50]):
    sensors = []

    sensors.append(sensor)

    for percentile in percentiles:
        sensors.append(MonitoringExpr(MonitoringExpr.NodeType.Terminal, sensor._name) \
            .name(str(percentile))                                                    \
            .series_percentile(percentile))

    return MultiSensor(*sensors)


def _build_memory_category(name, category):
    return _build_percentile_sensor(
        _build_sensor(name, "yt.cluster_node.memory_usage.used")
            .hidden(True)
            .value("category", category))


def _build_versions(d):
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
        .stack(False)
        .row()
            .cell("Pending out bytes",
                _build_percentile_sensor(
                    _build_sensor("PendingOutBytes", "yt.bus.pending_out_bytes")
                        .hidden(True)
                        .value("band", "-")
                        .value("encrypted", "-")
                        .value("network", "-")))
            .cell("Out throttler value rate",
                _build_percentile_sensor(
                    _build_sensor("OutThrottlerValueRate", "yt.cluster_node.out_throttler.value.rate")
                        .hidden(True)
                        .value("bucket", "-"))
            )
            .cell("Out throttler quota",
                _build_percentile_sensor(
                    _build_sensor("OutThrottlerQuota", "yt.cluster_node.out_throttler.quota")
                        .hidden(True)
                        .value("bucket", "-"),
                    [0.1, 1, 5, 10, 25, 50])
            )
        .row()
            .cell("Client connections",
                _build_percentile_sensor(
                    _build_sensor("PendingOutBytes", "yt.bus.client_connections")
                        .hidden(True)
                        .value("band", "-")
                        .value("encrypted", "-")
                        .value("network", "-")))
            .cell("Server connections",
                _build_percentile_sensor(
                    _build_sensor("OutThrottlerValueRate", "yt.bus.server_connections")
                        .hidden(True)
                        .value("band", "-")
                        .value("encrypted", "-")
                        .value("network", "-")))
    )

    d.add(Rowset().row(height=3).cell("", Text("Rpc server requests")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("ProbeBlockSet request count rate",
                _build_percentile_sensor(
                    _build_sensor("ProbeBlockSetRequestCountRate", "yt.rpc.server.request_count.rate")
                        .hidden(True)
                        .value("method", "ProbeBlockSet")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
            .cell("ProbeBlockSet failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("ProbeBlockSetFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate")
                        .hidden(True)
                        .value("method", "ProbeBlockSet")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            ).cell("ProbeBlockSet timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("ProbeBlockSetTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate")
                        .hidden(True)
                        .value("method", "ProbeBlockSet")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
        .row()
            .cell("GetBlockSet request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetBlockSetRequestCountRate", "yt.rpc.server.request_count.rate")
                        .hidden(True)
                        .value("method", "GetBlockSet")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
            .cell("GetBlockSet failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetBlockSetFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate")
                        .hidden(True)
                        .value("method", "GetBlockSet")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            ).cell("GetBlockSet timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetBlockSetTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate")
                        .hidden(True)
                        .value("method", "GetBlockSet")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
        .row()
            .cell("GetChunkMeta request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetChunkMetaRequestCountRate", "yt.rpc.server.request_count.rate")
                        .hidden(True)
                        .value("method", "GetChunkMeta")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
            .cell("GetChunkMeta failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetChunkMetaFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate")
                        .hidden(True)
                        .value("method", "GetChunkMeta")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            ).cell("GetChunkMeta timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("GetChunkMetaTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate")
                        .hidden(True)
                        .value("method", "GetChunkMeta")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
        .row()
            .cell("StartChunk request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksRequestCountRate", "yt.rpc.server.request_count.rate")
                        .hidden(True)
                        .value("method", "SendBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
            .cell("StartChunk failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate")
                        .hidden(True)
                        .value("method", "SendBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            ).cell("StartChunk timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate")
                        .hidden(True)
                        .value("method", "SendBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
         .row()
            .cell("PutBlocks request count rate",
                _build_percentile_sensor(
                    _build_sensor("PutBlocksRequestCountRate", "yt.rpc.server.request_count.rate")
                        .hidden(True)
                        .value("method", "PutBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
            .cell("PutBlocks failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("PutBlocksFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate")
                        .hidden(True)
                        .value("method", "PutBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            ).cell("PutBlocks timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("PutBlocksTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate")
                        .hidden(True)
                        .value("method", "PutBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
        .row()
            .cell("SendBlocks request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksRequestCountRate", "yt.rpc.server.request_count.rate")
                        .hidden(True)
                        .value("method", "SendBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
            .cell("SendBlocks failed request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksFailedRequestCountRate", "yt.rpc.server.failed_request_count.rate")
                        .hidden(True)
                        .value("method", "SendBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            ).cell("SendBlocks timed out request count rate",
                _build_percentile_sensor(
                    _build_sensor("SendBlocksTimedOutRequestCountRate", "yt.rpc.server.timed_out_request_count.rate")
                        .hidden(True)
                        .value("method", "SendBlocks")
                        .value("network", "-")
                        .value("encrypted", "-")
                        .value("band", "-")
                        .value("queue", "-"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("Rpc server attachments")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("GetChunkMeta Rpc attachment size",
                MonitoringExpr(_build_sensor("GetChunkMetaRpcAttachmentSize", "yt.rpc.server.response_message_body_bytes.rate|yt.rpc.server.response_message_attachment_bytes.rate")
                    .value("method", "GetChunkMeta")
                    .value("queue", "*"))
                    .series_sum(["queue"])
                    .stack(True)
            )
            .cell("GetBlockSet Rpc attachment size",
                MonitoringExpr(_build_sensor("GetBlockSetRpcAttachmentSize", "yt.rpc.server.response_message_body_bytes.rate|yt.rpc.server.response_message_attachment_bytes.rate")
                    .value("method", "GetBlockSet")
                    .value("queue", "*"))
                    .series_sum(["queue"])
                    .stack(True)
            )
            .cell("PutBlocks Rpc attachment size",
                MonitoringExpr(_build_sensor("PutBlocksRpcAttachmentSize", "yt.rpc.server.request_message_attachment_bytes.rate|yt.rpc.server.request_message_body_bytes.rate")
                    .value("method", "PutBlocks")
                    .value("queue", "-"))
                    .series_sum()
                    .stack(True)
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("CPU")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("Porto cpu total",
                _build_percentile_sensor(_build_sensor("PortoCpuTotal", "yt.porto.cpu.total")
                    .hidden(True)
                    .value("container_category", "pod"))
            )
            .cell("Porto cpu throttled",
                _build_percentile_sensor(_build_sensor("PortoCpuThrottled", "yt.porto.cpu.throttled")
                    .hidden(True)
                    .value("container_category", "pod"))
            )
            .cell("Porto cpu wait",
                _build_percentile_sensor(_build_sensor("PortoCpuWait", "yt.porto.cpu.wait")
                    .hidden(True)
                    .value("container_category", "pod"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("BusXferFS Thread")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("BusXferFS cpu total",
                _build_percentile_sensor(_build_sensor("BusXferFSCpuTotal", "yt.resource_tracker.total_cpu")
                    .hidden(True)
                    .value("thread", "BusXferFS"))
            )
            .cell("BusXferFS cpu util",
                _build_percentile_sensor(_build_sensor("BusXferFSCpuUtil", "yt.resource_tracker.utilization")
                    .hidden(True)
                    .value("thread", "BusXferFS"))
            )
            .cell("BusXferFS cpu wait",
                _build_percentile_sensor(_build_sensor("BusXferFSCpuWait", "yt.resource_tracker.cpu_wait")
                    .hidden(True)
                    .value("thread", "BusXferFS"))
            )
        .row()
            .cell("BusXferFS avg exec time",
                _build_percentile_sensor(_build_sensor("BusXferFSAvgExecTime", "yt.fair_share_queue.time.exec.avg")
                    .hidden(True)
                    .value("thread", "BusXferFS"))
            )
            .cell("BusXferFS max exec time",
                _build_percentile_sensor(_build_sensor("BusXferFSMaxExecTime", "yt.fair_share_queue.time.exec.max")
                    .hidden(True)
                    .value("thread", "BusXferFS"))
            )
        .row()
            .cell("BusXferFS avg wait time",
                _build_percentile_sensor(_build_sensor("BusXferFSAvgWaitTime", "yt.fair_share_queue.time.wait.avg")
                    .hidden(True)
                    .value("thread", "BusXferFS"))
            )
            .cell("BusXferFS max wait time",
                _build_percentile_sensor(_build_sensor("BusXferFSMaxWaitTime", "yt.fair_share_queue.time.wait.max")
                    .hidden(True)
                    .value("thread", "BusXferFS"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("StorageLight Thread")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("StorageLight cpu total",
                _build_percentile_sensor(_build_sensor("StorageLightCpuTotal", "yt.resource_tracker.total_cpu")
                    .hidden(True)
                    .value("thread", "StorageLight"))
            )
            .cell("StorageLight cpu util",
                _build_percentile_sensor(_build_sensor("StorageLightCpuUtil", "yt.resource_tracker.utilization")
                    .hidden(True)
                    .value("thread", "StorageLight"))
            )
            .cell("StorageLight cpu wait",
                _build_percentile_sensor(_build_sensor("StorageLightCpuWait", "yt.resource_tracker.cpu_wait")
                    .hidden(True)
                    .value("thread", "StorageLight"))
            )
        .row()
            .cell("StorageLight avg exec time",
                _build_percentile_sensor(_build_sensor("StorageLightAvgExecTime", "yt.action_queue.time.exec.avg")
                    .hidden(True)
                    .value("thread", "StorageLight"))
            )
            .cell("StorageLight max exec time",
                _build_percentile_sensor(_build_sensor("StorageLightMaxExecTime", "yt.action_queue.time.exec.max")
                    .hidden(True)
                    .value("thread", "StorageLight"))
            )
        .row()
            .cell("StorageLight avg wait time",
                _build_percentile_sensor(_build_sensor("StorageLightAvgWaitTime", "yt.action_queue.time.wait.avg")
                    .hidden(True)
                    .value("thread", "StorageLight"))
            )
            .cell("StorageLight max wait time",
                _build_percentile_sensor(_build_sensor("StorageLightMaxWaitTime", "yt.action_queue.time.wait.max")
                    .hidden(True)
                    .value("thread", "StorageLight"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("StorageHeavy Thread")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("StorageHeavy cpu total",
                _build_percentile_sensor(_build_sensor("StorageHeavyCpuTotal", "yt.resource_tracker.total_cpu")
                    .hidden(True)
                    .value("thread", "StorageHeavy"))
            )
            .cell("StorageHeavy cpu util",
                _build_percentile_sensor(_build_sensor("StorageHeavyCpuUtil", "yt.resource_tracker.utilization")
                    .hidden(True)
                    .value("thread", "StorageHeavy"))
            )
            .cell("StorageHeavy cpu wait",
                _build_percentile_sensor(_build_sensor("StorageHeavyCpuWait", "yt.resource_tracker.cpu_wait")
                    .hidden(True)
                    .value("thread", "StorageHeavy"))
            )
        .row()
            .cell("StorageHeavy avg exec time",
                _build_percentile_sensor(_build_sensor("StorageHeavyAvgExecTime", "yt.action_queue.time.exec.avg")
                    .hidden(True)
                    .value("thread", "StorageHeavy"))
            )
            .cell("StorageHeavy max exec time",
                _build_percentile_sensor(_build_sensor("StorageHeavyMaxExecTime", "yt.action_queue.time.exec.max")
                    .hidden(True)
                    .value("thread", "StorageHeavy"))
            )
        .row()
            .cell("StorageHeavy avg wait time",
                _build_percentile_sensor(_build_sensor("StorageHeavyAvgWaitTime", "yt.action_queue.time.wait.avg")
                    .hidden(True)
                    .value("thread", "StorageHeavy"))
            )
            .cell("StorageHeavy max wait time",
                _build_percentile_sensor(_build_sensor("StorageHeavyMaxWaitTime", "yt.action_queue.time.wait.max")
                    .hidden(True)
                    .value("thread", "StorageHeavy"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("System jobs")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("Important system jobs",
                MultiSensor(
                    MonitoringExpr(_build_sensor("Repair jobs", "yt.job_controller.resource_usage.repair_slots")
                        .value("state", "acquired"))
                        .series_sum()
                        .stack(True),
                    MonitoringExpr(_build_sensor("Replication jobs", "yt.job_controller.resource_usage.replication_slots")
                        .value("state", "acquired"))
                        .series_sum()
                        .stack(True),
                    MonitoringExpr(_build_sensor("Merge jobs", "yt.job_controller.resource_usage.merge_slots")
                        .value("state", "acquired"))
                        .series_sum()
                        .stack(True)
                )
            )
            .cell("System Jobs",
                _build_memory_category("SystemJobs", "system_jobs"))
        .row()
            .cell("Replication jobs",
                _build_percentile_sensor(_build_sensor("ReplicationJobs", "yt.job_controller.resource_usage.replication_slots")
                    .hidden(True)
                    .value("state", "acquired"))
            )
            .cell("Repair jobs",
                _build_percentile_sensor(_build_sensor("RepairJobs", "yt.job_controller.resource_usage.repair_slots")
                    .hidden(True)
                    .value("state", "acquired"))
            )
            .cell("Merge jobs",
                _build_percentile_sensor(_build_sensor("MergeJobs", "yt.job_controller.resource_usage.merge_slots")
                    .hidden(True)
                    .value("state", "acquired"))
            )
        .row()
            .cell("Removal jobs",
                _build_percentile_sensor(_build_sensor("RemovalJobs", "yt.job_controller.resource_usage.removal_slots")
                    .hidden(True)
                    .value("state", "acquired"))
            )
            .cell("Reincarnation jobs",
                _build_percentile_sensor(_build_sensor("ReincarnationJobs", "yt.job_controller.resource_usage.reincarnation_slots")
                    .hidden(True)
                    .value("state", "acquired"))
            )
            .cell("Autotomy jobs",
                _build_percentile_sensor(_build_sensor("AutotomyJobs", "yt.job_controller.resource_usage.autotomy_slots")
                    .hidden(True)
                    .value("state", "acquired"))
            )
    )

    d.add(Rowset().row(height=3).cell("", Text("IO")))
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("Throttling Writes",
                MultiSensor(
                    _build_sensor("ThrottlingWrites", "yt.location.throttled_writes.rate")
                        .hidden(True)
                        .name("ThrottlingWrites")
                        .value("medium", "*")
                        .value("location_id", "-")
                        .value("location_type", "store"),
                    _build_sensor("Writes", "yt.location.write.request_count.rate")
                        .hidden(True)
                        .name("Writes")
                        .value("medium", "*")
                        .value("location_id", "-")
                        .value("location_type", "store"),
                    (MonitoringExpr(MonitoringExpr.NodeType.Terminal, 100) * MonitoringExpr(MonitoringExpr.NodeType.Terminal, "ThrottlingWrites").series_sum("medium") /
                        (MonitoringExpr(MonitoringExpr.NodeType.Terminal, "ThrottlingWrites").series_sum("medium") + MonitoringExpr(MonitoringExpr.NodeType.Terminal, "Writes").series_sum("medium")))
                        .name("WritePercent")
                ))
            .cell("Throttling Reads",
                MultiSensor(
                    _build_sensor("ThrottlingReads", "yt.location.throttled_reads.rate")
                        .hidden(True)
                        .name("ThrottlingReads")
                        .value("medium", "*")
                        .value("location_id", "-")
                        .value("location_type", "store"),
                    _build_sensor("Reads", "yt.location.read.request_count.rate")
                        .hidden(True)
                        .name("Reads")
                        .value("medium", "*")
                        .value("location_id", "-")
                        .value("location_type", "store"),
                    (MonitoringExpr(MonitoringExpr.NodeType.Terminal, 100) * MonitoringExpr(MonitoringExpr.NodeType.Terminal, "ThrottlingReads").series_sum("medium") /
                        (MonitoringExpr(MonitoringExpr.NodeType.Terminal, "ThrottlingReads").series_sum("medium") + MonitoringExpr(MonitoringExpr.NodeType.Terminal, "Reads").series_sum("medium")))
                        .name("ReadPercent")
                ))
        .row()
            .cell("Disk in queue size",
                MonitoringExpr(_build_sensor("DiskInQueueSize", "yt.location.disk_throttler.*in*.queue_size")
                    .value("medium", "*")
                    .value("location_id", "-")
                    .value("location_type", "store"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
            .cell("Disk out queue size",
                MonitoringExpr(_build_sensor("DiskOutQueueSize", "yt.location.disk_throttler.*out*.queue_size")
                    .value("medium", "*")
                    .value("location_id", "-")
                    .value("location_type", "store"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
        .row()
            .cell("Disk in value rate",
                MonitoringExpr(_build_sensor("DiskInValueRate", "yt.location.disk_throttler.*in*.value.rate")
                    .value("medium", "*")
                    .value("location_id", "-")
                    .value("disk_family", "*")
                    .value("location_type", "store"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
            .cell("Disk out value rate",
                MonitoringExpr(_build_sensor("DiskOutValueRate", "yt.location.disk_throttler.*out*.value.rate")
                    .value("medium", "*")
                    .value("location_id", "-")
                    .value("disk_family", "*")
                    .value("location_type", "store"))
                    .series_sum("medium", "sensor")
                    .stack(True)
            )
        .row()
            .cell("Used memory for writes",
                MonitoringExpr(_build_sensor("UsedMemoryForWrites", "yt.location.used_memory")
                    .value("direction", "write")
                    .value("medium", "*")
                    .value("category", "*")
                    .value("location_type", "store"))
                    .series_sum("medium", "category", "direction", "sensor")
                    .stack(True)
            )
            .cell("Used memory for reads",
                MonitoringExpr(_build_sensor("UsedMemoryForReads", "yt.location.used_memory")
                    .value("direction", "read")
                    .value("medium", "*")
                    .value("category", "*")
                    .value("location_type", "store"))
                    .series_sum("medium", "category", "direction", "sensor")
                    .stack(True)
            )
    )


def build_data_nodes_common():
    d = Dashboard()

    _build_versions(d)

    d.set_monitoring_serializer_options(dict(default_row_height=8))

    d.set_title("Data Nodes Common [AUTOGENERATED]")

    d.add_parameter(
        "cluster",
        "cluster",
        MonitoringLabelDashboardParameter(
            "yt",
            "cluster",
            "freud")
    )

    d.add_parameter(
        "host",
        "host",
        MonitoringLabelDashboardParameter(
            "yt",
            "host",
            "Aggr")
    )

    return d
