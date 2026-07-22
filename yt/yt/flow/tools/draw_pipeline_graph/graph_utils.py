"""Shared helpers used by both dot_graph.py and mermaid_graph.py."""

from yt.wrapper.constants import UI_ADDRESS_PATTERN

_EDGE_WARNING_FILL_RATE_THRESHOLD = 0.5

# Mirrors WarningBlockedTimeShareThreshold in controller/describe/fill_graph_limits.h.
BLOCKED_TIME_SHARE_THRESHOLD = 0.3

MIN_VISIBLE_BLOCKED_TIME_SHARE = 0.01

# Mirrors ControllerLimitType in common/flow_view.h.
CONTROLLER_LIMIT_TYPE = "controller"


def get_group_by_columns(computation):
    """Return list of group-by column expressions for a computation."""
    schema = computation.spec["group_by_schema"]
    if isinstance(schema, dict):
        schema = schema["$value"]
    return [c.get("expression", c["name"]) for c in schema]


def is_computation_warning(computation):
    """Return True if the computation has any diagnostic warning."""
    if computation.max_cpu_usage[0] >= 0.5:
        return True
    unstable_partition_count = computation.unstable_job_count + (
        computation.total_partition_count - computation.total_job_count
    )
    if unstable_partition_count > 0:
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


def format_limit_entry(limit_type, status):
    """kind{blocked_share=, limit=, used=%, pending=%, backpressured_partition=} — mirrors the describe format."""
    parts = []
    if status.blocked_time_share >= MIN_VISIBLE_BLOCKED_TIME_SHARE:
        parts.append(f"blocked_share={status.blocked_time_share:.2f}")
    # Count limits (e.g. output_store_count) are counts, not bytes.
    format_value = format_count_short if limit_type.endswith("_count") else format_bytes_short

    def of_limit(value):
        # Percentages of a zero limit are meaningless: fall back to absolute values.
        return f"{value / status.limit * 100:.0f}%" if status.limit > 0 else format_value(value)

    # The controller limit type is not a buffer: it carries a blocked share and nothing else.
    if limit_type != CONTROLLER_LIMIT_TYPE:
        parts.append(f"limit={format_value(status.limit)}")
        parts.append(f"used={of_limit(status.used)}")
        if status.pending is not None:
            parts.append(f"pending={of_limit(status.pending)}")
    if (
        status.get_fill_rate() >= _EDGE_WARNING_FILL_RATE_THRESHOLD
        or status.blocked_time_share >= BLOCKED_TIME_SHARE_THRESHOLD
    ):
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


def iter_read_delay_edges(graph):
    """Yield (blocker_stream_id, delayed_stream_id, delay_duration) for all read-delay pairs."""
    for computation in graph.computations.values():
        watermark_alignment = computation.spec.get("watermark_strategy", {}).get("watermark_alignment", {})
        read_delays = watermark_alignment.get("read_delays")
        if not read_delays:
            continue
        for delayed_stream_id in computation.spec.get("source_streams", {}).keys():
            for blocker_stream_id, delay_duration in read_delays.items():
                yield blocker_stream_id, delayed_stream_id, delay_duration
