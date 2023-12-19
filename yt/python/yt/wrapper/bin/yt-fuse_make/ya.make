PY3_PROGRAM(yt-fuse)

PEERDIR(
    yt/python/yt/wrapper
)

COPY_FILE(yt/python/yt/wrapper/bin/yt-fuse __main__.py)

PY_SRCS(__main__.py)

END()
