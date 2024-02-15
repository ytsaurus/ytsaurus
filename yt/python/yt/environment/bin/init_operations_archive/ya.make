PY3_PROGRAM(init-operations-archive)

PEERDIR(
    yt/python/yt/environment
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
)

COPY_FILE(yt/python/yt/environment/init_operations_archive.py __main__.py)

PY_SRCS(__main__.py)

END()
