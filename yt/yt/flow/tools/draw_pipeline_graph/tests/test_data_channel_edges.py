from yt.yt.flow.tools.draw_pipeline_graph.lib.dot_graph import ComputationClusterBuilder, StreamClusterBuilder
from yt.yt.flow.tools.draw_pipeline_graph.lib.graph_model import (
    Computation,
    Endpoint,
    Graph,
    Stream,
    attach_data_channel_edges,
)
from yt.yt.flow.tools.draw_pipeline_graph.mermaid_graph import (
    build_mermaid_computations_graph,
    build_mermaid_streams_graph,
)

LABEL = "yt:primary://sunk"


def _make_graph():
    """writer visits its own state through writer/visit, and its sink writes the table that reader/visit visits."""
    graph = Graph(
        streams={
            "writer/visit": Stream(stream_id="writer/visit", reading_computations={"writer"}),
            "out": Stream(stream_id="out"),
            "reader/visit": Stream(stream_id="reader/visit", reading_computations={"reader"}),
        },
        computations={
            "writer": Computation(
                computation_id="writer",
                input_streams={"writer/visit"},
                output_streams={"out"},
                streams_dependency={"out": {"writer/visit"}},
            ),
            "reader": Computation(computation_id="reader", input_streams={"reader/visit"}),
        },
        sinks={"snk-writer/sink": Endpoint("snk-writer/sink", "sink", "TSink", stream_ids=["out"])},
    )
    describe = {
        "computations": {"writer": {"id": "cmp-writer"}, "reader": {"id": "cmp-reader"}},
        "edges": [
            {"from": "cmp-writer", "to": "stm-writer/visit", "kind": "data_channel", "label": ""},
            {"from": "snk-writer/sink", "to": "stm-reader/visit", "kind": "data_channel", "label": LABEL},
        ],
    }
    attach_data_channel_edges(graph, describe, {"stm-writer/visit": "writer/visit", "stm-reader/visit": "reader/visit"})
    return graph


def _dashed_edges(cluster, prefix):
    edges = set()
    for edge in cluster.get_edges():
        if edge.get("style") == "dashed":
            edges.add(
                (
                    edge.get_source().strip('"').removeprefix(prefix),
                    edge.get_destination().strip('"').removeprefix(prefix),
                )
            )
    return edges


def test_dot():
    graph = _make_graph()
    sink_edge = ("out", "reader/visit")
    assert _dashed_edges(ComputationClusterBuilder(graph).build(), "computations_") == {
        ("writer", "writer/visit"),
        sink_edge,
    }
    # The streams graph has no single node per computation.
    assert _dashed_edges(StreamClusterBuilder(graph).build(), "streams_") == {sink_edge}


def test_dot_streams_graph_keeps_visitors_in_own_computations():
    graph = Graph(
        streams={
            "first/visit": Stream(stream_id="first/visit", reading_computations={"first"}),
            "second/visit": Stream(stream_id="second/visit", reading_computations={"second"}),
        },
        computations={
            "first": Computation(computation_id="first", input_streams={"first/visit"}),
            "second": Computation(computation_id="second", input_streams={"second/visit"}),
        },
    )
    cluster = StreamClusterBuilder(graph).build()
    input_edges = {
        edge.get_source()
        .strip('"')
        .removeprefix("streams_"): edge.get_destination()
        .strip('"')
        .removeprefix("streams_")
        for edge in cluster.get_edges()
        if edge.get("style") != "dashed"
    }
    assert input_edges.keys() == {"first/visit", "second/visit"}, input_edges
    assert input_edges["first/visit"].startswith("first_"), input_edges
    assert input_edges["second/visit"].startswith("second_"), input_edges


def test_mermaid():
    graph = _make_graph()
    computation_edge = "c_writer -.-> s_writer_2fvisit"
    sink_edge = f"e_snk_2dwriter_2fsink -.->|{LABEL}| s_reader_2fvisit"

    diagram = build_mermaid_computations_graph(graph)
    assert computation_edge in diagram, diagram
    assert sink_edge in diagram, diagram

    diagram = build_mermaid_streams_graph(graph)
    assert computation_edge not in diagram, diagram
    assert sink_edge in diagram, diagram
