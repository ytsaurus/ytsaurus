PY3_PROGRAM()

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/yt_proto/yt/core
    yt/yt_proto/yt/client

    contrib/python/grpcio

    yt/python/client
)

END()
