PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/cachetools

    yt/python/client_with_rpc
)

END()
