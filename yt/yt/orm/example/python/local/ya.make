PY3_LIBRARY()

PEERDIR(
    yt/yt/orm/example/python/admin
    yt/yt/orm/example/python/client

    yt/yt/orm/python/orm/local

    yt/python/yt
)

PY_SRCS(
    __init__.py
    local.py
)

END()
