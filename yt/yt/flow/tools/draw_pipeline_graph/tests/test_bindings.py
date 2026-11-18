import pathlib

import yatest.common as ycommon

from yt.wrapper import yson

from yt.yt.flow.tools.draw_pipeline_graph.bindings import (
    flow_view_yson_to_mermaid_computations_graph,
    flow_view_yson_to_mermaid_unrolled_graph,
)

# The working_healthy fixture of ui_test_controller: queue_writer/audit_sink writes the queue that
# audit_reader/queue_source reads.
EDGE = {
    "from": "snk-queue_writer/audit_sink",
    "to": "src-audit_reader/queue_source",
    "kind": "data_channel",
    "label": "yt:primary://tmp/audit_queue",
}
EDGE_LINE = (
    "e_snk_2dqueue__writer_2faudit__sink -.->|yt:primary://tmp/audit_queue| e_src_2daudit__reader_2fqueue__source"
)


def _load_flow_view_yson():
    extracted = pathlib.Path(ycommon.output_path("live_fixtures"))
    extracted.mkdir(exist_ok=True)
    ycommon.execute(["tar", "--zstd", "-xf", ycommon.work_path("flow-ui-live-fixtures.tar.zst"), "-C", str(extracted)])
    return (extracted / "fixtures" / "working_healthy.yson").read_bytes()


def _render(flow_view_yson, edges_yson):
    return [
        build(flow_view_yson, "vertical", "", "", edges_yson)
        for build in (flow_view_yson_to_mermaid_computations_graph, flow_view_yson_to_mermaid_unrolled_graph)
    ]


def test_edges_come_only_from_the_controller():
    flow_view_yson = _load_flow_view_yson()

    # Without the controller's describe edges, no data-channel edges are drawn.
    for diagram in _render(flow_view_yson, b""):
        assert "-.->" not in diagram, diagram
        assert "e_snk_2dqueue__writer_2faudit__sink" in diagram, diagram
    for diagram in _render(flow_view_yson, yson.dumps([])):
        assert "-.->" not in diagram, diagram

    # The controller's edges are drawn; kinds this build does not know are skipped.
    edges = [EDGE, dict(EDGE, kind="unknown_kind", label="ignored")]
    for diagram in _render(flow_view_yson, yson.dumps(edges)):
        assert diagram.count(EDGE_LINE) == 1, diagram
        assert "ignored" not in diagram, diagram


def test_buffer_details_reach_dot_tooltip_without_highlighting():
    from yt.yt.flow.tools.draw_pipeline_graph.lib.dot_graph import make_dot_edge_attributes
    from yt.yt.flow.tools.draw_pipeline_graph.lib.graph_model import _build_from_describe

    text = "buffers: Buffer sizing window: 6s (automatic); Cumulative batch duration: 5s"
    describe = {
        "streams": {"stream": {"id": "stm-stream"}},
        "computations": {
            "producer": {
                "output_streams": ["stm-stream"],
                "extended_output_streams": [
                    {"stream_graph_entity_id": "stm-stream", "messages": [{"text": text, "level": "info"}]}
                ],
            }
        },
    }
    flow_view = {
        "current_spec": {"value": {"computations": {"producer": {"source_streams": {}, "sinks": {}}}}},
        "state": {"execution_spec": {"layout": {"partitions": {}}}},
        "feedback": {},
    }
    graph, _ = _build_from_describe(describe, flow_view)
    attributes = make_dot_edge_attributes(graph.output_edges[("producer", "stream")])
    assert attributes["tooltip"] == text
    assert attributes["label"] == ""
    assert attributes["color"] == "black"
    assert "style" not in attributes
