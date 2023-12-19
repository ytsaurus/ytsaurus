PY3_PROGRAM(yt-admin)

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/python/yt_driver_bindings
)

COPY_FILE(yt/python/yt/wrapper/bin/yt-admin __main__.py)

PY_SRCS(__main__.py)

END()
