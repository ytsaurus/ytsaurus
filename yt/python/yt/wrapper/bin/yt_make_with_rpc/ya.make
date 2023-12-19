PY3_PROGRAM(yt)

PEERDIR(
    yt/python/yt/cli
    yt/yt/python/yt_driver_rpc_bindings
)

COPY_FILE(yt/python/yt/wrapper/bin/yt __main__.py)

PY_SRCS(__main__.py)

END()
