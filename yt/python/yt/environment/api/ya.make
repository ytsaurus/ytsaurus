PY23_LIBRARY()

PEERDIR(
    contrib/python/attrs
    yt/python/yt
)

PY_SRCS(
    NAMESPACE yt.environment.api

    __init__.py
)

END()
