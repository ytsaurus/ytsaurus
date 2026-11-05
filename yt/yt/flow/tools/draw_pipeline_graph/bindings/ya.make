PY3_LIBRARY()

PY_REGISTER(yt.yt.flow.tools.draw_pipeline_graph.bindings)

SRCS(
    module.cpp
)

PEERDIR(
    library/cpp/pybind
    yt/yt/flow/library/cpp/controller/describe
)

END()
