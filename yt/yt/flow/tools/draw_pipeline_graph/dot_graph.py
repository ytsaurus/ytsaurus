import datetime
import html
import os
import tempfile

import pydot

from library.python import resource
from yt.wrapper import yson

from yt.yt.flow.tools.draw_pipeline_graph.graph_model import EdgeLimitInfo
from yt.yt.flow.tools.draw_pipeline_graph.graph_utils import (
    MIN_VISIBLE_BLOCKED_TIME_SHARE,
    format_limit_entry,
    get_group_by_columns,
    is_computation_warning,
    is_edge_warning,
    iter_read_delay_edges,
    make_computation_url,
)


def yson_to_html(yson_data):
    text = yson.dumps(yson_data, indent=4, yson_format="pretty").strip().decode("utf-8")
    return "".join(html.escape(line) + '<br ALIGN="LEFT"/>' for line in text.split("\n"))


def make_stream_label(stream):
    html_text = ""
    if stream.source_stream_spec is not None:
        html_text += "<br/>Source: " + yson_to_html(stream.source_stream_spec)
    if len(stream.sink_stream_specs) > 0:
        html_text += "<br/>Sinks: " + yson_to_html(stream.sink_stream_specs)
    return f"<<B>{stream.stream_id}</B>{html_text}>"


def make_dot_computation_attributes(computation, graph):
    label_html_text = f'ClassName: {html.escape(computation.spec["computation_class_name"])}'
    tooltip_text = ""

    group_by_columns = get_group_by_columns(computation)
    if group_by_columns:
        label_html_text += f'<br ALIGN="LEFT"/>GroupBy: {html.escape(repr(group_by_columns)).strip()}'

    state = computation.spec["parameters"].get("state", {}).get("path")
    if state:
        label_html_text += f'<br ALIGN="LEFT"/>State: {html.escape(repr(state).strip())}'

    processing_mode = computation.spec["parameters"].get("processing_mode", "exactly_once")
    if processing_mode != "exactly_once":
        label_html_text += f'<br ALIGN="LEFT"/>ProcessingMode: {processing_mode}'

    watermark_alignment = computation.spec.get("watermark_strategy", {}).get("watermark_alignment", {})
    if "read_delays" in watermark_alignment:
        label_html_text += '<br ALIGN="LEFT"/>ReadDelays:<br ALIGN="LEFT"/>'
        label_html_text += yson_to_html(watermark_alignment["read_delays"])

    if computation.max_cpu_usage[0] >= 0.5:
        usage, partition_id = computation.max_cpu_usage
        label_html_text += f'<br ALIGN="LEFT"/><B>MaxCpuUsage={usage:.3f}, PartitionId={partition_id}</B>'

    unstable_partition_count = computation.unstable_job_count + (
        computation.total_partition_count - computation.total_job_count
    )
    if unstable_partition_count > 0:
        label_html_text += (
            f'<br ALIGN="LEFT"/><B>UnstablePartitions: '
            f"{unstable_partition_count}/{computation.total_partition_count}</B>"
        )

    if computation.retryable_errors_partition_count > 0:
        label_html_text += (
            '<br ALIGN="LEFT"/><B>RetryableErrorsPartitions: '
            f'{computation.retryable_errors_partition_count}/{computation.total_partition_count}</B><br ALIGN="LEFT"/>'
        )
        for component, error in computation.retryable_errors.items():
            if "message" in error:
                label_html_text += f'<B>Error:</B>{component}: {error["message"]}<br ALIGN="LEFT"/>'

    def get_wall_time_text(times):
        sorted_times = sorted(times.items(), key=lambda x: x[1], reverse=True)
        return ", ".join(
            f"{part_name}({part_wall_time:.2f})" for part_name, part_wall_time in sorted_times if part_wall_time > 0.05
        )

    if computation.total_partition_count > 0:
        wall_times = computation.epoch_parts_wall_time
        avg_wall_times = get_wall_time_text(
            {k: v / computation.total_partition_count for k, v in wall_times.total_time.items()}
        )
        max_wall_times = get_wall_time_text(wall_times.max_time)
        tooltip_text += f"AvgWallTime: {html.escape(avg_wall_times)}\n"
        tooltip_text += f"MaxWallTime: {html.escape(max_wall_times)}\n"

    system_watermarks = computation.system_watermarks
    if system_watermarks.min_watermark is not None:
        system_watermark_difference = system_watermarks.max_watermark - system_watermarks.min_watermark
        tooltip_text += f"SystemWatermarkDifference: {system_watermark_difference}\n"
        tooltip_text += f"MinSystemWatermarkPartition: {system_watermarks.min_watermark_partition_id}\n"
        if system_watermark_difference > 10 * 60:
            label_html_text += f'<br ALIGN="LEFT"/>SystemWatermarkDifference: {system_watermark_difference}, '
            label_html_text += (
                f'MinSystemWatermarkPartition: {system_watermarks.min_watermark_partition_id}<br ALIGN="LEFT"/>'
            )

    attributes = {
        "color": "black",
        "label": f'<<B>{computation.computation_id}</B><br/>{label_html_text}<br ALIGN="LEFT"/>>',
        "shape": "box",
        "tooltip": tooltip_text,
    }

    # Has problems with graphviz of old versions.
    url = make_computation_url(computation, graph)
    if url:
        attributes["URL"] = url

    if is_computation_warning(computation):
        attributes["color"] = "red"
        attributes["style"] = "bold"

    return attributes


def make_dot_edge_attributes(edge: EdgeLimitInfo):
    attributes = {
        "color": "black",
    }
    label_parts = []

    # Show all limit types that have a non-trivial fill rate or blocked-time
    # share, most critical first.
    sorted_statuses = sorted(
        edge.by_type.items(),
        key=lambda kv: (kv[1].blocked_time_share, kv[1].get_fill_rate()),
        reverse=True,
    )
    for limit_type, status in sorted_statuses:
        if status.get_fill_rate() >= 1e-3 or status.blocked_time_share >= MIN_VISIBLE_BLOCKED_TIME_SHARE:
            label_parts.append(format_limit_entry(limit_type, status))

    attributes["label"] = "\n---------\n".join(label_parts)

    if is_edge_warning(edge):
        attributes["color"] = "red"
        attributes["style"] = "bold"

    return attributes


class ClusterBuilder:
    def __init__(self, cluster_id, cluster_title, graph):
        self._cluster_id = cluster_id
        self._cluster_title = cluster_title
        self._graph = graph
        self._entity_prefix = f"{cluster_id}_"
        self._cluster = pydot.Cluster(cluster_id, label=cluster_title, style="dotted")

    def make_stream_node(self, stream):
        node_id = self._entity_prefix + stream.stream_id
        return pydot.Node(node_id, color="black", label=make_stream_label(stream), shape="cds")

    def make_computation_node(self, computation, node_id=None):
        node_id = self._entity_prefix + (node_id or computation.computation_id)
        return pydot.Node(node_id, **make_dot_computation_attributes(computation, self._graph))

    def make_edge(self, a, b, edge: EdgeLimitInfo):
        return pydot.Edge(a, b, **make_dot_edge_attributes(edge))

    def make_input_edge(self, computation_id, stream_id, computation_node_id=None):
        computation_node_id = self._entity_prefix + (computation_node_id or computation_id)
        edge = self._graph.input_edges.get((computation_id, stream_id), EdgeLimitInfo())
        return self.make_edge(self._entity_prefix + stream_id, computation_node_id, edge)

    def make_output_edge(self, computation_id, stream_id, computation_node_id=None):
        computation_node_id = self._entity_prefix + (computation_node_id or computation_id)
        edge = self._graph.output_edges.get((computation_id, stream_id), EdgeLimitInfo())
        return self.make_edge(computation_node_id, self._entity_prefix + stream_id, edge)

    def make_read_delay_edge(self, blocker_stream_id, delayed_stream_id, delay_duration):
        """Arrow from the stream that blocks (blocker) to the stream that is delayed."""
        return pydot.Edge(
            self._entity_prefix + blocker_stream_id,
            self._entity_prefix + delayed_stream_id,
            style="dashed",
            color="#ccaa0099",
            fontcolor="#997700",
            label=f"delay={delay_duration}",
            constraint="true",
        )

    def add_read_delay_edges(self, cluster, graph):
        for blocker_stream_id, delayed_stream_id, delay_duration in iter_read_delay_edges(graph):
            cluster.add_edge(self.make_read_delay_edge(blocker_stream_id, delayed_stream_id, delay_duration))

    def build(self):
        self.do_build(self._graph, self._cluster)
        return self._cluster

    def do_build(self, graph, cluster):
        raise NotImplementedError()


class ComputationClusterBuilder(ClusterBuilder):
    def __init__(self, graph):
        super().__init__("computations", "Computations graph", graph)

    def do_build(self, graph, cluster):
        for stream in graph.streams.values():
            cluster.add_node(self.make_stream_node(stream))
        for computation in graph.computations.values():
            cluster.add_node(self.make_computation_node(computation))
            for stream_id in computation.input_streams:
                cluster.add_edge(self.make_input_edge(computation.computation_id, stream_id))
            for stream_id in computation.output_streams:
                cluster.add_edge(self.make_output_edge(computation.computation_id, stream_id))
        self.add_read_delay_edges(cluster, graph)


class StreamClusterBuilder(ClusterBuilder):
    def __init__(self, graph):
        super().__init__("streams", "Streams graph", graph)

    def do_build(self, graph, cluster):
        outputs_to_computation_node_id = dict()  # concatenated_sorted_outputs -> local_number

        for stream in graph.streams.values():
            cluster.add_node(self.make_stream_node(stream))
            for computation_id in stream.reading_computations:
                computation = graph.computations[computation_id]
                outputs = []
                for output_stream_id in computation.output_streams:
                    if stream.stream_id in computation.spec["streams_dependency"][output_stream_id]:
                        outputs.append(output_stream_id)
                concatenated_sorted_outputs = ",".join(sorted(outputs))

                computation_node_id = outputs_to_computation_node_id.get(concatenated_sorted_outputs)
                if computation_node_id is None:
                    computation_node_id = f"{computation.computation_id}_{len(outputs_to_computation_node_id)}"
                    outputs_to_computation_node_id[concatenated_sorted_outputs] = computation_node_id
                    cluster.add_node(self.make_computation_node(computation, node_id=computation_node_id))
                    for output_stream_id in outputs:
                        cluster.add_edge(self.make_output_edge(computation_id, output_stream_id, computation_node_id))

                cluster.add_edge(self.make_input_edge(computation_id, stream.stream_id, computation_node_id))

        self.add_read_delay_edges(cluster, graph)


def get_dot_binary_path():
    dir_path = tempfile.mkdtemp()
    dot_path = os.path.join(dir_path, "dot")
    with open(dot_path, "wb") as f:
        f.write(resource.find("dot_binary"))
    os.chmod(dot_path, os.stat(dot_path).st_mode | 0o111)

    # Provide fonts.
    with open(os.path.join(dir_path, "fonts.conf"), "w") as f:
        f.write(f'<?xml version="1.0"?>\n<fontconfig>\n\t<dir>{dir_path}</dir>\n</fontconfig>')
    for name in [
        "Crimson-BoldItalic.ttf",
        "Crimson-Bold.ttf",
        "Crimson-Italic.ttf",
        "Crimson-Roman.ttf",
        "Crimson-SemiboldItalic.ttf",
        "Crimson-Semibold.ttf",
    ]:
        with open(os.path.join(dir_path, name), "wb") as f:
            f.write(resource.find(name))

    # pydot doesn't propagate FONTCONFIG_PATH, so hack it.
    runner_path = os.path.join(dir_path, "runner")
    with open(runner_path, "w") as f:
        f.write(f'#!/usr/bin/bash\nexport FONTCONFIG_PATH={dir_path}\nexec {dot_path} "$@"\n')
    os.chmod(runner_path, os.stat(runner_path).st_mode | 0o111)

    return runner_path


def build_dot_graph(graph, args):
    rankdir = "TB" if args.orientation == "vertical" else "LR"
    full_graph_dot = pydot.Dot("full_graph", graph_type="digraph", rankdir=rankdir, splines="polyline")
    full_graph_dot.set_label(f"Tool arguments: {args}\nTime: {datetime.datetime.now().isoformat()}")

    full_graph_dot.add_subgraph(ComputationClusterBuilder(graph).build())
    full_graph_dot.add_subgraph(StreamClusterBuilder(graph).build())

    return full_graph_dot


def render_dot_graph(graph, args, dot_binary_path):
    full_graph_dot = build_dot_graph(graph, args)

    if args.dot_output:
        full_graph_dot.write_dot(args.dot_output, prog=dot_binary_path)

    file_path = args.output or os.path.join(tempfile.mkdtemp(), "pipeline_graph.svg")
    assert file_path.endswith(".svg"), "Output file must be .svg"
    full_graph_dot.write_svg(file_path, prog=dot_binary_path)

    return file_path
