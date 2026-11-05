PY3_LIBRARY()

PY_REGISTER(yt.yt.flow.library.python.client.flow_view.bindings)

SRCS(
    module.cpp
)

PEERDIR(
    library/cpp/pybind
    yt/yt/core
)

END()
