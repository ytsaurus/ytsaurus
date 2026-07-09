import json
import re

from yt.yt.flow.tools.draw_pipeline_graph.graph_model import EdgeLimitInfo
from yt.yt.flow.tools.draw_pipeline_graph.graph_utils import (
    get_group_by_columns,
    is_computation_warning,
    is_edge_warning,
    iter_read_delay_edges,
    make_computation_url,
)

# Warning style: soft red background, dark text for readability.
_WARNING_NODE_STYLE = "fill:#ffcccc,color:#333,stroke:#cc0000"
_WARNING_LINK_STYLE = "stroke:#cc0000,stroke-width:2px"
# Read-delay edge style: dashed golden arrow (mirrors dot_graph.py).
_READ_DELAY_LINK_STYLE = "stroke:#ccaa00,stroke-width:1px,stroke-dasharray:5 5"


def _mermaid_safe_id(raw_id):
    """Convert an arbitrary string into a valid Mermaid node identifier."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", raw_id)


def _mermaid_safe_label(text):
    """Sanitize text for use inside a Mermaid node label (inside double-quotes).

    Mermaid's parser treats several characters as special even inside quoted
    strings: colons, parentheses, brackets, braces, equals signs, hash signs,
    and double-quotes.  We replace them with visually similar safe alternatives
    so the diagram renders without "Syntax error in text".
    """
    return (
        text.replace('"', "'")
        .replace("::", "\u2237")  # ∷ PROPORTION (C++ namespace separator) — must come before single ":"
        .replace(":", "\ufe13")  # ︓ PRESENTATION FORM FOR VERTICAL COLON
        .replace("(", "\uff08")  # （ FULLWIDTH LEFT PARENTHESIS
        .replace(")", "\uff09")  # ） FULLWIDTH RIGHT PARENTHESIS
        .replace("[", "\uff3b")  # ［ FULLWIDTH LEFT SQUARE BRACKET
        .replace("]", "\uff3d")  # ］ FULLWIDTH RIGHT SQUARE BRACKET
        .replace("{", "\uff5b")  # ｛ FULLWIDTH LEFT CURLY BRACKET
        .replace("}", "\uff5d")  # ｝ FULLWIDTH RIGHT CURLY BRACKET
        .replace("=", "\uff1d")  # ＝ FULLWIDTH EQUALS SIGN
        .replace("#", "\uff03")  # ＃ FULLWIDTH NUMBER SIGN
        .replace("<", "\uff1c")  # ＜ FULLWIDTH LESS-THAN SIGN (visually close to <)
        .replace(">", "\uff1e")  # ＞ FULLWIDTH GREATER-THAN SIGN (visually close to >)
    )


def _mermaid_safe_edge_label(text):
    """Sanitize text for use inside a Mermaid edge label (inside -->|...|).

    The pipe character | terminates the label, and double-quotes break parsing.
    """
    return text.replace("|", "/").replace('"', "'")


def _make_computation_label(computation):
    parts = [computation.computation_id, computation.spec["computation_class_name"]]

    group_by_columns = get_group_by_columns(computation)
    if group_by_columns:
        parts.append(f"GroupBy: {group_by_columns}")

    state = computation.spec["parameters"].get("state", {}).get("path")
    if state:
        parts.append(f"State: {state}")

    processing_mode = computation.spec["parameters"].get("processing_mode", "exactly_once")
    if processing_mode != "exactly_once":
        parts.append(f"ProcessingMode: {processing_mode}")

    watermark_alignment = computation.spec.get("watermark_strategy", {}).get("watermark_alignment", {})
    if "read_delays" in watermark_alignment:
        # Render read_delays as compact JSON (YSON not available in label context).
        delays_str = json.dumps(watermark_alignment["read_delays"], ensure_ascii=False)
        parts.append(f"ReadDelays: {delays_str}")

    if is_computation_warning(computation):
        if computation.max_cpu_usage[0] >= 0.5:
            usage, partition_id = computation.max_cpu_usage
            parts.append(f"⚠ MaxCpuUsage {usage:.3f}")

        unstable_partition_count = computation.unstable_job_count + (
            computation.total_partition_count - computation.total_job_count
        )
        if unstable_partition_count > 0:
            parts.append(f"⚠ UnstablePartitions: {unstable_partition_count}/{computation.total_partition_count}")

        if computation.retryable_errors_partition_count > 0:
            parts.append(
                f"⚠ RetryableErrors: {computation.retryable_errors_partition_count}/{computation.total_partition_count}"
            )

        system_watermarks = computation.system_watermarks
        if system_watermarks.min_watermark is not None:
            diff = system_watermarks.max_watermark - system_watermarks.min_watermark
            if diff > 10 * 60:
                parts.append(f"⚠ WatermarkDiff: {diff}")

    return _mermaid_safe_label("\n".join(parts))


def _make_stream_label(stream):
    parts = [stream.stream_id]
    if stream.source_stream_spec is not None:
        parts.append("(source)")
    if stream.sink_stream_specs:
        parts.append("(sink)")
    return _mermaid_safe_label("\n".join(parts))


def _make_edge_label(edge: EdgeLimitInfo):
    parts = []
    # Show all limit types with non-trivial fill rate, most critical first.
    sorted_statuses = sorted(
        edge.by_type.items(),
        key=lambda kv: kv[1].get_fill_rate(),
        reverse=True,
    )
    for limit_type, status in sorted_statuses:
        fill_rate = status.get_fill_rate()
        if fill_rate >= 1e-3:
            parts.append(f"{limit_type}={fill_rate:.3f}")
    return " ".join(parts)


def build_mermaid_computations_graph(graph, orientation="vertical"):
    """Build a Mermaid flowchart showing computations and streams (flat graph)."""
    direction = "TD" if orientation == "vertical" else "LR"
    lines = [f"flowchart {direction}"]

    # Stream nodes.
    lines.append("  %% Streams")
    for stream in graph.streams.values():
        node_id = "s_" + _mermaid_safe_id(stream.stream_id)
        label = _make_stream_label(stream)
        lines.append(f'  {node_id}(["{label}"])')

    # Computation nodes.
    lines.append("  %% Computations")
    for computation in graph.computations.values():
        node_id = "c_" + _mermaid_safe_id(computation.computation_id)
        label = _make_computation_label(computation)
        lines.append(f'  {node_id}["{label}"]')

    # Edges: stream -> computation (input) and computation -> stream (output).
    # Track edge index for linkStyle.
    lines.append("  %% Edges")
    edge_index = 0
    warning_edge_indices = []
    read_delay_edge_indices = []
    for computation in graph.computations.values():
        c_node = "c_" + _mermaid_safe_id(computation.computation_id)
        for stream_id in computation.input_streams:
            s_node = "s_" + _mermaid_safe_id(stream_id)
            edge = graph.input_edges.get((computation.computation_id, stream_id), EdgeLimitInfo())
            label = _make_edge_label(edge)
            arrow = "-->|" + label + "|" if label else "-->"
            lines.append(f"  {s_node} {arrow} {c_node}")
            if is_edge_warning(edge):
                warning_edge_indices.append(edge_index)
            edge_index += 1
        for stream_id in computation.output_streams:
            s_node = "s_" + _mermaid_safe_id(stream_id)
            edge = graph.output_edges.get((computation.computation_id, stream_id), EdgeLimitInfo())
            label = _make_edge_label(edge)
            arrow = "-->|" + label + "|" if label else "-->"
            lines.append(f"  {c_node} {arrow} {s_node}")
            if is_edge_warning(edge):
                warning_edge_indices.append(edge_index)
            edge_index += 1

    # Read-delay edges: dashed arrows between streams.
    lines.append("  %% Read-delay edges")
    for blocker_stream_id, delayed_stream_id, delay_duration in iter_read_delay_edges(graph):
        blocker_node = "s_" + _mermaid_safe_id(blocker_stream_id)
        delayed_node = "s_" + _mermaid_safe_id(delayed_stream_id)
        safe_delay = _mermaid_safe_edge_label(f"delay={delay_duration}")
        lines.append(f"  {blocker_node} -->|{safe_delay}| {delayed_node}")
        read_delay_edge_indices.append(edge_index)
        edge_index += 1

    # Style warning nodes.
    lines.append("  %% Styles")
    for computation in graph.computations.values():
        if is_computation_warning(computation):
            node_id = "c_" + _mermaid_safe_id(computation.computation_id)
            lines.append(f"  style {node_id} {_WARNING_NODE_STYLE}")

    # Style warning edges.
    for idx in warning_edge_indices:
        lines.append(f"  linkStyle {idx} {_WARNING_LINK_STYLE}")

    # Style read-delay edges.
    for idx in read_delay_edge_indices:
        lines.append(f"  linkStyle {idx} {_READ_DELAY_LINK_STYLE}")

    # Clickable computation nodes (open YT UI).
    lines.append("  %% Links")
    for computation in graph.computations.values():
        url = make_computation_url(computation, graph)
        if url:
            node_id = "c_" + _mermaid_safe_id(computation.computation_id)
            lines.append(f'  click {node_id} href "{url}" _blank')

    return "\n".join(lines)


def build_mermaid_streams_graph(graph, orientation="vertical"):
    """Build a Mermaid flowchart based on streams_dependency (acyclic unrolled graph)."""
    direction = "TD" if orientation == "vertical" else "LR"
    lines = [f"flowchart {direction}"]

    # Stream nodes.
    lines.append("  %% Streams")
    for stream in graph.streams.values():
        node_id = "s_" + _mermaid_safe_id(stream.stream_id)
        label = _make_stream_label(stream)
        lines.append(f'  {node_id}(["{label}"])')

    # Computation nodes: one node per (computation, set-of-outputs) pair, same as StreamClusterBuilder.
    lines.append("  %% Computations (unrolled)")
    outputs_to_computation_node_id = {}
    computation_node_ids = {}  # (computation_id, concatenated_sorted_outputs) -> mermaid_node_id

    edge_index = 0
    warning_edge_indices = []
    read_delay_edge_indices = []

    for stream in graph.streams.values():
        for computation_id in stream.reading_computations:
            computation = graph.computations[computation_id]
            outputs = [
                output_stream_id
                for output_stream_id in computation.output_streams
                if stream.stream_id in computation.spec["streams_dependency"][output_stream_id]
            ]
            concatenated_sorted_outputs = ",".join(sorted(outputs))
            key = (computation_id, concatenated_sorted_outputs)

            if key not in computation_node_ids:
                local_idx = len(outputs_to_computation_node_id)
                outputs_to_computation_node_id[concatenated_sorted_outputs] = local_idx
                raw_node_id = f"{computation_id}_{local_idx}"
                mermaid_node_id = "c_" + _mermaid_safe_id(raw_node_id)
                computation_node_ids[key] = mermaid_node_id

                label = _make_computation_label(computation)
                lines.append(f'  {mermaid_node_id}["{label}"]')

                for output_stream_id in outputs:
                    s_node = "s_" + _mermaid_safe_id(output_stream_id)
                    edge = graph.output_edges.get((computation_id, output_stream_id), EdgeLimitInfo())
                    edge_label = _make_edge_label(edge)
                    arrow = "-->|" + edge_label + "|" if edge_label else "-->"
                    lines.append(f"  {mermaid_node_id} {arrow} {s_node}")
                    if is_edge_warning(edge):
                        warning_edge_indices.append(edge_index)
                    edge_index += 1

            mermaid_node_id = computation_node_ids[key]
            s_node = "s_" + _mermaid_safe_id(stream.stream_id)
            edge = graph.input_edges.get((computation_id, stream.stream_id), EdgeLimitInfo())
            edge_label = _make_edge_label(edge)
            arrow = "-->|" + edge_label + "|" if edge_label else "-->"
            lines.append(f"  {s_node} {arrow} {mermaid_node_id}")
            if is_edge_warning(edge):
                warning_edge_indices.append(edge_index)
            edge_index += 1

    # Read-delay edges: dashed arrows between streams.
    lines.append("  %% Read-delay edges")
    for blocker_stream_id, delayed_stream_id, delay_duration in iter_read_delay_edges(graph):
        blocker_node = "s_" + _mermaid_safe_id(blocker_stream_id)
        delayed_node = "s_" + _mermaid_safe_id(delayed_stream_id)
        safe_delay = _mermaid_safe_edge_label(f"delay={delay_duration}")
        lines.append(f"  {blocker_node} -->|{safe_delay}| {delayed_node}")
        read_delay_edge_indices.append(edge_index)
        edge_index += 1

    # Style warning nodes.
    lines.append("  %% Styles")
    for (computation_id, _), mermaid_node_id in computation_node_ids.items():
        computation = graph.computations[computation_id]
        if is_computation_warning(computation):
            lines.append(f"  style {mermaid_node_id} {_WARNING_NODE_STYLE}")

    # Style warning edges.
    for idx in warning_edge_indices:
        lines.append(f"  linkStyle {idx} {_WARNING_LINK_STYLE}")

    # Style read-delay edges.
    for idx in read_delay_edge_indices:
        lines.append(f"  linkStyle {idx} {_READ_DELAY_LINK_STYLE}")

    # Clickable computation nodes (open YT UI).
    lines.append("  %% Links")
    for (computation_id, _), mermaid_node_id in computation_node_ids.items():
        computation = graph.computations[computation_id]
        url = make_computation_url(computation, graph)
        if url:
            lines.append(f'  click {mermaid_node_id} href "{url}" _blank')

    return "\n".join(lines)


def render_mermaid_graph(graph, output_path, orientation="vertical"):
    """Render both mermaid diagrams into a single HTML file at output_path."""
    computations_diagram = build_mermaid_computations_graph(graph, orientation)
    streams_diagram = build_mermaid_streams_graph(graph, orientation)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>Pipeline graph</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
  <script>mermaid.initialize({{startOnLoad: true, maxTextSize: 1000000}});</script>
  <style>
    body {{ font-family: sans-serif; padding: 1em; }}
    h2 {{ margin-top: 2em; }}
    .diagram-header {{ display: flex; align-items: center; gap: 1em; }}
    .mermaid {{ overflow: auto; border: 1px solid #ccc; padding: 1em; }}
    .copy-btn {{
      font-size: 2em; padding: 0.5em 1.4em; cursor: pointer;
      border: 1px solid #888; border-radius: 4px; background: #f5f5f5;
    }}
    .copy-btn:active {{ background: #ddd; }}
  </style>
</head>
<body>
  <h1>Pipeline graph</h1>

  <div class="diagram-header">
    <h2>Computations graph</h2>
    <button class="copy-btn" onclick="copyDiagram(this, 'comp-src')">Copy mermaid as text</button>
  </div>
  <textarea id="comp-src" style="display:none">{computations_diagram}</textarea>
  <div class="mermaid">
{computations_diagram}
  </div>

  <div class="diagram-header">
    <h2>Streams graph (acyclic unrolled)</h2>
    <button class="copy-btn" onclick="copyDiagram(this, 'streams-src')">Copy mermaid as text</button>
  </div>
  <textarea id="streams-src" style="display:none">{streams_diagram}</textarea>
  <div class="mermaid">
{streams_diagram}
  </div>

  <script>
    function copyDiagram(btn, srcId) {{
      var text = document.getElementById(srcId).value;
      navigator.clipboard.writeText(text).then(function() {{
        var orig = btn.textContent;
        btn.textContent = 'Copied!';
        setTimeout(function() {{ btn.textContent = orig; }}, 1500);
      }}).catch(function(err) {{
        alert('Copy failed: ' + err);
      }});
    }}
  </script>
</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
