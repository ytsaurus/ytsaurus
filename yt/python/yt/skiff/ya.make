PY23_LIBRARY()

PEERDIR(
    yt/python/yt/yson

    contrib/python/six
)

PY_SRCS(
    NAMESPACE yt.skiff

    __init__.py
)

END()
