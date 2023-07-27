import json
import pytest
from io import BytesIO

from exts.compress import ucopen
from exts.yjdump import dump_graph_as_json, dump_context_as_json
from yatest.common import work_path


one_node_graph = {'conf': {'p1': 'val1', 'p2': 'val2'}, 'graph': [{'a': 3}]}
two_node_graph = {'graph': [{'a': 3}, {'a': 4}], 'result': [1, 2]}
zero_node_graph = {'conf': {'p1': 'val1', 'p2': 'val2'}, 'graph': []}

one_node_context = {'configure_errors': {'a': 1}, 'lite_graph': one_node_graph}
two_node_context = {'lite_graph': two_node_graph, 'tests': {'u1': 'data1', 'u2': 'data2'}}
zero_node_context = {'configure_errors': [], 'lite_graph': zero_node_graph}


@pytest.mark.parametrize(
    "graph",
    [
        one_node_graph,
        two_node_graph,
        zero_node_graph,
        [],
        'abcdef',
        1337,
        {'conf': {'a': 1}},
        None,
    ],
)
def test_dump_graph_as_json(graph):
    fp = BytesIO()

    dump_graph_as_json(graph, fp)

    got = json.loads(fp.getvalue())
    assert got == graph


def test_dump_graph_as_json_from_file():
    graph = one_node_graph
    file_name = work_path("external_graph.json")
    with open(file_name, "w") as f:
        json.dump(graph, f)
    fp = BytesIO()

    dump_graph_as_json('file://{}'.format(file_name), fp)

    got = json.loads(fp.getvalue())

    assert got == graph


def test_dump_graph_as_json_from_compressed_file():
    graph = one_node_graph
    file_name = work_path("external_graph.json.uc")
    with ucopen(file_name, "w") as f:
        json.dump(graph, f)
    fp = BytesIO()

    dump_graph_as_json('file://{}'.format(file_name), fp)

    got = json.loads(fp.getvalue())

    assert got == graph


@pytest.mark.parametrize(
    "context",
    [
        one_node_context,
        two_node_context,
        zero_node_context,
        [],
        'abcdef',
        1337,
        {'conf': {'a': 1}},
        None,
    ],
)
def test_dump_context_as_json(context):
    fp = BytesIO()

    dump_context_as_json(context, fp)

    got = json.loads(fp.getvalue())
    assert got == context


def test_dump_context_as_json_with_graph_in_file():
    lite_graph = one_node_graph
    lite_graph_file_name = work_path("lite_graph.json")
    with open(lite_graph_file_name, "w") as f:
        json.dump(lite_graph, f)
    graph = two_node_graph
    graph_file_name = work_path("graph.json.uc")
    with ucopen(graph_file_name, "w") as f:
        json.dump(graph, f)
    context = {
        'configure_errors': {'a': 1},
        'lite_graph': 'file://{}'.format(lite_graph_file_name),
        'graph': 'file://{}'.format(graph_file_name),
    }
    fp = BytesIO()

    dump_context_as_json(context, fp)

    got = json.loads(fp.getvalue())

    expected = context.copy()
    expected.update(
        {
            'lite_graph': lite_graph,
            'graph': graph,
        }
    )
    print("GOT: {}".format(got))
    print("EXP: {}".format(expected))

    assert got == expected
