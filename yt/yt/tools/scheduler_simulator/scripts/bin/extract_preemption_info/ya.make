PY3_PROGRAM()

OWNER(g:yt)

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/yson
    yt/yt/tools/scheduler_simulator/scripts/lib
)

END()
