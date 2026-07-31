import dataclasses
import datetime
import logging
import time

from collections import defaultdict
from copy import deepcopy
from typing import Optional

import yt.wrapper as yt

from yt.wrapper import yson

from yt.yt.flow.library.python.client.flow_view import get_flow_view


@dataclasses.dataclass
class Stream:
    stream_id: str
    reading_computations: set[str] = dataclasses.field(default_factory=set)
    source_stream_spec: Optional[dict] = None
    sink_stream_specs: list[dict] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class EpochPartsWallTimeInfo:
    max_time: dict[str, float] = dataclasses.field(default_factory=lambda: defaultdict(float))
    # Sum of times of all jobs.
    total_time: dict[str, float] = dataclasses.field(default_factory=lambda: defaultdict(float))


@dataclasses.dataclass
class ComputationSystemWatermarks:
    min_watermark: int = None
    min_watermark_partition_id: str = None
    max_watermark: int = None

    def update(self, watermark, partition_id):
        if self.min_watermark is None or watermark < self.min_watermark:
            self.min_watermark = watermark
            self.min_watermark_partition_id = partition_id
        if self.max_watermark is None or watermark > self.max_watermark:
            self.max_watermark = watermark


@dataclasses.dataclass
class Computation:
    computation_id: str
    spec: dict
    output_streams: set[str] = dataclasses.field(default_factory=set)  # output + timer
    input_streams: set[str] = dataclasses.field(default_factory=set)  # source + input + timer

    total_partition_count: int = 0
    total_job_count: int = 0
    unstable_job_count: int = 0  # recovering/warming
    retryable_errors_partition_count: int = 0

    retryable_errors: dict = dataclasses.field(default_factory=dict)
    max_cpu_usage: tuple[float, str] = dataclasses.field(default_factory=lambda: (0, ""))  # (cpu_usage, partition_id)

    epoch_parts_wall_time: EpochPartsWallTimeInfo = dataclasses.field(default_factory=EpochPartsWallTimeInfo)

    system_watermarks: ComputationSystemWatermarks = dataclasses.field(default_factory=ComputationSystemWatermarks)


@dataclasses.dataclass
class EntityLimitStatus:
    """Limit status for a single entity (buffer or store) on a single stream."""

    used: float = 0
    limit: float = 0
    pending: Optional[float] = None
    partition_id: str = ""
    blocked_time_share: float = 0

    def get_fill_rate(self):
        return self.used / max(1, self.limit)

    def get_fill_rate_with_pending(self):
        pending = self.pending if self.pending is not None else 0
        return (self.used + pending) / max(1, self.limit)


@dataclasses.dataclass
class EdgeLimitInfo:
    """Aggregated limit info for one (computation, stream) edge across all partitions.

    Stores the partition with the maximum fill rate for each limit type.
    limit_type -> EntityLimitStatus with the worst (highest fill rate) partition.
    """

    by_type: dict[str, EntityLimitStatus] = dataclasses.field(default_factory=dict)

    def get_max_fill_rate(self):
        if not self.by_type:
            return 0.0
        return max(s.get_fill_rate() for s in self.by_type.values())

    def get_max_blocked_time_share(self):
        if not self.by_type:
            return 0.0
        return max(s.blocked_time_share for s in self.by_type.values())

    def get_status(self, limit_type: str) -> EntityLimitStatus:
        return self.by_type.get(limit_type, EntityLimitStatus())


@dataclasses.dataclass
class Graph:
    streams: dict[str, Stream]
    computations: dict[str, Computation]

    # (computation_id, stream_id) -> EdgeLimitInfo
    input_edges: dict[tuple[str, str], EdgeLimitInfo] = dataclasses.field(default_factory=dict)
    output_edges: dict[tuple[str, str], EdgeLimitInfo] = dataclasses.field(default_factory=dict)

    monitoring_attributes: Optional[dict] = None
    cluster_name: Optional[str] = None
    pipeline_path: Optional[str] = None


def make_unique_stream_ids(spec):
    """Prefix internal stream IDs with their computation ID to make them globally unique.

    Returns (transformed_spec, raw_to_full_stream_id) where raw_to_full_stream_id is a
    per-computation mapping from raw stream ID (as used by the C++ worker) to the full
    prefixed stream ID used in the transformed spec.
    """
    spec = deepcopy(spec)
    computation_specs = spec["computations"]

    # computation_id -> {raw_stream_id -> full_stream_id}
    raw_to_full: dict[str, dict[str, str]] = {}

    for computation_id, computation_spec in computation_specs.items():
        external_streams = set(computation_spec["input_stream_ids"] + computation_spec["output_stream_ids"])

        def get_full_id(stream_id, _external=external_streams, _cid=computation_id):
            if stream_id in _external:
                return stream_id
            return f"{_cid}.{stream_id}"

        # Build raw -> full mapping before transforming the spec, while raw IDs are still available.
        mapping = {
            raw_id: get_full_id(raw_id)
            for raw_id in (
                computation_spec["input_stream_ids"]
                + computation_spec["output_stream_ids"]
                + list(computation_spec["timer_streams"].keys())
                + list(computation_spec["source_streams"].keys())
            )
        }
        raw_to_full[computation_id] = mapping

        streams_dependency = {}
        for stream_id, dependencies in computation_spec["streams_dependency"].items():
            streams_dependency[get_full_id(stream_id)] = [
                get_full_id(dependency_stream_id) for dependency_stream_id in dependencies
            ]
        computation_spec["streams_dependency"] = streams_dependency

        computation_spec["timer_streams"] = {
            get_full_id(stream_id): stream_spec for stream_id, stream_spec in computation_spec["timer_streams"].items()
        }

        computation_spec["source_streams"] = {
            get_full_id(stream_id): stream_spec for stream_id, stream_spec in computation_spec["source_streams"].items()
        }

    return spec, raw_to_full


def _update_edge_limit_info(
    edge_limit_info: EdgeLimitInfo, limit_type: str, stream_id: str, raw_status: dict, partition_id: str
):
    """Update EdgeLimitInfo for a given limit_type/stream_id with data from one partition.

    Keeps the partition with the highest fill rate.
    """
    new_status = EntityLimitStatus(
        used=raw_status.get("used", 0),
        limit=raw_status.get("limit", 0),
        pending=raw_status.get("pending"),
        partition_id=partition_id,
        blocked_time_share=raw_status.get("blocked_time_share", 0),
    )
    existing = edge_limit_info.by_type.get(limit_type)
    if existing is None or (new_status.blocked_time_share, new_status.get_fill_rate()) > (
        existing.blocked_time_share,
        existing.get_fill_rate(),
    ):
        edge_limit_info.by_type[limit_type] = new_status


def get_graph(args):
    if hasattr(args, "pipeline_path"):
        config = yt.default_config.get_config_from_env()
        config["backend"] = "rpc"
        yt_client = yt.YtClient(proxy=args.cluster_name, config=config)
        # Raise the per-request timeout; the default RPC timeout is too low for heavy pipelines.
        timeout_s = getattr(args, "timeout", None)
        if timeout_s is not None:
            yt.config.set_command_param("timeout", int(timeout_s * 1000), yt_client)
        monitoring_attributes = yt_client.get(
            args.pipeline_path, attributes=["monitoring_project", "monitoring_cluster"]
        ).attributes
        flow_view = get_flow_view(yt_client, args.pipeline_path, cache=True)
    else:
        monitoring_attributes = None
        with open(args.local_path, "rb") as f:
            flow_view = yson.load(f)

    logging.info("Loaded flow view")
    is_full = True

    transformed_spec, raw_to_full_stream_id = make_unique_stream_ids(flow_view["current_spec"]["value"])
    computation_specs = transformed_spec["computations"]
    layout = flow_view["state"]["execution_spec"]["layout"]
    partitions = layout["partitions"]

    streams = {}
    computations = {}

    for computation_id, computation_spec in computation_specs.items():
        computations[computation_id] = Computation(computation_id=computation_id, spec=computation_spec)
        for stream_id in (
            computation_spec["input_stream_ids"]
            + computation_spec["output_stream_ids"]
            + list(computation_spec["timer_streams"].keys())
            + list(computation_spec["source_streams"].keys())
        ):
            streams[stream_id] = Stream(stream_id=stream_id)

    for computation_id, computation_spec in computation_specs.items():
        computation = computations[computation_id]
        for stream_id in (
            computation_spec["input_stream_ids"]
            + list(computation_spec["source_streams"].keys())
            + list(computation_spec["timer_streams"].keys())
        ):
            computation.input_streams.add(stream_id)
            streams[stream_id].reading_computations.add(computation_id)
        for stream_id in computation_spec["output_stream_ids"] + list(computation_spec["timer_streams"].keys()):
            computation.output_streams.add(stream_id)
        for stream_id, source_spec in computation_spec["source_streams"].items():
            streams[stream_id].source_stream_spec = source_spec
        for sink_id, sink_spec in computation_spec["sinks"].items():
            for stream_id in sink_spec["input_stream_ids"]:
                spec_copy = dict(sink_spec)
                spec_copy.pop("input_stream_ids")
                streams[stream_id].sink_stream_specs.append(spec_copy)

    for partition_id, partition in partitions.items():
        if partition["state"] == "interrupted" or partition["state"] == "completed":
            continue
        computations[partition["computation_id"]].total_partition_count += 1

    now = time.time()
    now_iso8601 = datetime.datetime.fromtimestamp(now, datetime.UTC).isoformat()

    input_edges = {}
    output_edges = {}

    for partition_id, partition_job_status in flow_view["feedback"].get("partition_job_statuses", {}).items():
        current_job_status = partition_job_status.get("current_job_status")
        if current_job_status is None:
            if is_full:
                logging.warning("Graph is not full because partition %s has no current_job_status field", partition_id)
            is_full = False
            continue

        performance_metrics = current_job_status["performance_metrics"]
        cpu_usage_30s = performance_metrics.get("cpu_usage_30s", 0)
        inited_time = datetime.datetime.fromisoformat(current_job_status.get("inited_time", now_iso8601)).timestamp()
        retryable_errors = current_job_status.get("retryable_errors", {})

        computation_id = partitions[partition_id]["computation_id"]
        computation = computations[computation_id]
        computation.total_job_count += 1
        computation.unstable_job_count += now - inited_time < 5 * 60  # 5 min.
        computation.retryable_errors_partition_count += len(retryable_errors) > 0
        computation.max_cpu_usage = max(computation.max_cpu_usage, (cpu_usage_30s, partition_id))
        computation.retryable_errors.update(retryable_errors)

        for stream_id, stream_traverse in partition_job_status["last_traverse_data"]["node"]["streams"].items():
            if stream_id in computation.spec["output_stream_ids"] or stream_id in computation.spec["timer_streams"]:
                computation.system_watermarks.update(stream_traverse["system_watermark"], partition_id)

        # Epoch part times.
        epoch_part_times = current_job_status.get("epoch_part_times", {})
        if epoch_part_times:
            total_time = sum(epoch_part_times.values())
            if total_time > 0:
                job_aggr_times = defaultdict(float)
                for part_name, part_wall_time in epoch_part_times.items():
                    job_aggr_times[part_name.split(".")[0]] += part_wall_time / total_time

                wall_times = computation.epoch_parts_wall_time
                for part_name, part_wall_time in job_aggr_times.items():
                    wall_times.max_time[part_name] = max(wall_times.max_time[part_name], part_wall_time)
                    wall_times.total_time[part_name] += part_wall_time

        stream_id_map = raw_to_full_stream_id.get(computation_id, {})

        # Input limits (input_buffer_bytes, max_read_window_seconds, etc.).
        for limit_type, streams_statuses in current_job_status.get("input_limits", {}).items():
            for raw_stream_id, raw_status in streams_statuses.items():
                full_stream_id = stream_id_map.get(raw_stream_id, raw_stream_id)
                edge = input_edges.setdefault((computation_id, full_stream_id), EdgeLimitInfo())
                _update_edge_limit_info(edge, limit_type, full_stream_id, raw_status, partition_id)

        # Output limits (output_buffer_bytes, output_store_bytes, output_store_count,
        # controller, etc.).
        for limit_type, streams_statuses in current_job_status.get("output_limits", {}).items():
            for raw_stream_id, raw_status in streams_statuses.items():
                full_stream_id = stream_id_map.get(raw_stream_id, raw_stream_id)
                edge = output_edges.setdefault((computation_id, full_stream_id), EdgeLimitInfo())
                _update_edge_limit_info(edge, limit_type, full_stream_id, raw_status, partition_id)

    return (
        Graph(
            streams=streams,
            computations=computations,
            input_edges=input_edges,
            output_edges=output_edges,
            monitoring_attributes=monitoring_attributes,
            cluster_name=getattr(args, "cluster_name", None),
            pipeline_path=getattr(args, "pipeline_path", None),
        ),
        is_full,
    )
