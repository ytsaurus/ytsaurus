PY3_LIBRARY()

PY_REGISTER(yt.yt.flow.library.python.describe)

SRCS(
    module.cpp
)

PEERDIR(
    library/cpp/pybind
    yt/yt/flow/library/cpp/controller/describe
)

END()
