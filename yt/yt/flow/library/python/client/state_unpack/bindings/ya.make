PY3_LIBRARY()

PY_REGISTER(yt.yt.flow.library.python.client.state_unpack.bindings)

SRCS(module.cpp)

PEERDIR(
    contrib/python/py3c
    yt/yt/flow/library/cpp/serializer
)

END()
