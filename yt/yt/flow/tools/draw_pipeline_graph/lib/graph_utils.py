"""Shared helpers used by both dot_graph.py and mermaid_graph.py."""

from yt.wrapper.constants import UI_ADDRESS_PATTERN

_EDGE_WARNING_FILL_RATE_THRESHOLD = 0.5

# Mirrors WarningBlockedTimeShareThreshold in controller/describe/fill_graph_limits.h.
BLOCKED_TIME_SHARE_THRESHOLD = 0.3

MIN_VISIBLE_BLOCKED_TIME_SHARE = 0.01

# Mirrors ControllerLimitType in common/flow_view.h.
CONTROLLER_LIMIT_TYPE = "controller"

# Mirrors AsyncProcessorLimitTypePrefix in common/flow_view.h.
ASYNC_PROCESSOR_LIMIT_TYPE_PREFIX = "async_processor/"


def is_blocked_time_only_limit_type(limit_type):
    """Mirrors IsBlockedTimeOnlyLimitType in common/flow_view.h: such a limit is not a buffer."""
    return limit_type == CONTROLLER_LIMIT_TYPE or limit_type.startswith(ASYNC_PROCESSOR_LIMIT_TYPE_PREFIX)


def is_computation_warning(computation):
    """Return True if the computation has any diagnostic warning."""
    if computation.max_cpu_usage[0] >= 0.5:
        return True
    if computation.unstable_partition_count > 0:
        return True
    if computation.retryable_errors_partition_count > 0:
        return True
    system_watermarks = computation.system_watermarks
    if system_watermarks.min_watermark is not None:
        if system_watermarks.max_watermark - system_watermarks.min_watermark > 10 * 60:
            return True
    return False


def format_bytes_short(size):
    """20MB-style short byte size."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < 1024 or unit == "TB":
            return f"{value:.1f}{unit}" if unit != "B" and abs(value) < 10 else f"{value:.0f}{unit}"
        value /= 1024


def format_count_short(count):
    """50K-style short count."""
    value = float(count)
    for unit in ("", "K", "M", "G"):
        if abs(value) < 1000 or unit == "G":
            return f"{value:.1f}{unit}" if unit and abs(value) < 10 else f"{value:.0f}{unit}"
        value /= 1000


def format_seconds_short(seconds):
    """A whole number of hours, minutes or seconds: 3600 is 1h, 120 is 2m, 45 is 45s (mirrors fill_graph_limits.cpp)."""
    seconds = int(seconds)
    if seconds != 0 and seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds != 0 and seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _get_limit_value_formatter(limit_type):
    if limit_type.endswith("_count"):
        return format_count_short
    if limit_type.endswith("_seconds"):
        return format_seconds_short
    return format_bytes_short


def format_limit_entry(limit_type, status):
    """kind{blocked_share=, limit=, used=%, pending=%, backpressured_partition=} — mirrors the describe format."""
    parts = []
    if status.blocked_time_share >= MIN_VISIBLE_BLOCKED_TIME_SHARE:
        parts.append(f"blocked_share={status.blocked_time_share:.2f}")
    format_value = _get_limit_value_formatter(limit_type)

    def of_limit(value):
        # Percentages of a zero limit are meaningless: fall back to absolute values.
        return f"{value / status.limit * 100:.0f}%" if status.limit > 0 else format_value(value)

    # Such a limit type is not a buffer: it carries a blocked share and nothing else.
    blocked_time_only = is_blocked_time_only_limit_type(limit_type)
    if not blocked_time_only:
        parts.append(f"limit={format_value(status.limit)}")
        parts.append(f"used={of_limit(status.used)}")
        if status.pending is not None:
            parts.append(f"pending={of_limit(status.pending)}")
    fill_score = status.get_fill_rate() / _EDGE_WARNING_FILL_RATE_THRESHOLD
    blocked_score = status.blocked_time_share / BLOCKED_TIME_SHARE_THRESHOLD
    if fill_score >= 1 or blocked_score >= 1:
        # Like the describe format: the blocked-time attribution names the most blocked partition.
        blocked_partition_id = status.blocked_partition_id or status.partition_id
        if blocked_time_only or blocked_score >= fill_score:
            parts.append(f"backpressured_partition={blocked_partition_id}")
        else:
            parts.append(f"backpressured_partition={status.partition_id}")
    return f"{limit_type}{{{', '.join(parts)}}}"


def is_edge_warning(edge):
    """Return True if any limit on the edge is critically full or blocks the writer."""
    return (
        edge.get_max_fill_rate() >= _EDGE_WARNING_FILL_RATE_THRESHOLD
        or edge.get_max_blocked_time_share() >= BLOCKED_TIME_SHARE_THRESHOLD
    )


def make_computation_url(computation, graph):
    """Return the YT UI URL for a computation, or None if cluster/path are unknown."""
    if UI_ADDRESS_PATTERN and graph.cluster_name and graph.pipeline_path:
        ui_address = UI_ADDRESS_PATTERN.format(cluster_name=graph.cluster_name)
        return f"{ui_address}flows/computations/{computation.computation_id}/details?path={graph.pipeline_path}"
    return None


def iter_data_channel_node_edges(graph, with_computations):
    """Yield (from_node_id, to_node_id, label) for data-channel edges between stream and computation nodes: sinks and
    sources are replaced by their streams; edges from computations are skipped unless |with_computations|."""

    def resolve(kind, entity_id):
        if kind == "sink":
            return graph.sinks[entity_id].stream_ids
        if kind == "source":
            return graph.sources[entity_id].stream_ids
        if kind == "computation":
            return [entity_id] if with_computations else []
        return [entity_id]

    for edge in graph.data_channel_edges:
        for from_node_id in resolve(edge.from_kind, edge.from_id):
            if edge.from_kind != "computation" and from_node_id not in graph.streams:
                continue
            for to_node_id in resolve(edge.to_kind, edge.to_id):
                if to_node_id in graph.streams:
                    yield from_node_id, to_node_id, edge.label


def iter_read_delay_edges(graph):
    """Yield (blocker_stream_id, delayed_stream_id, delay_duration) for all read-delay pairs between known streams."""
    for blocker_stream_id, delayed_stream_id, delay_duration in graph.read_delay_edges:
        if blocker_stream_id in graph.streams and delayed_stream_id in graph.streams:
            yield blocker_stream_id, delayed_stream_id, delay_duration
