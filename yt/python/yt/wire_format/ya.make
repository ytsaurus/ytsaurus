PY23_LIBRARY()

PEERDIR(
    yt/python/yt/yson

    contrib/python/six
)

PY_SRCS(
    NAMESPACE yt.wire_format

    __init__.py
    wire_format.py
)

END()

