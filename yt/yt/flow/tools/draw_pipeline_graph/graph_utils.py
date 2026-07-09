"""Shared helpers used by both dot_graph.py and mermaid_graph.py."""

from yt.wrapper.constants import UI_ADDRESS_PATTERN

_EDGE_WARNING_FILL_RATE_THRESHOLD = 0.5


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


def is_edge_warning(edge):
    """Return True if any limit on the edge is critically full."""
    return edge.get_max_fill_rate() >= _EDGE_WARNING_FILL_RATE_THRESHOLD


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
