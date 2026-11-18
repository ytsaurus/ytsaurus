PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    dot_graph.py
    graph_model.py
    graph_utils.py
)

PEERDIR(
    contrib/python/pydot
    library/python/resource
    yt/python/yt/wrapper
    yt/yt/flow/library/python/client/flow_view
)

END()
