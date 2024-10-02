
PY3_LIBRARY()

PEERDIR(
    yt/yt/orm/python/orm/client
    yt/yt/orm/python/orm/library

    yt/python/yt
    yt/python/yt/packages
    yt/python/yt/wrapper
    yt/python/yt/yson

    contrib/python/prettytable
    contrib/python/termcolor
)

PY_SRCS(
    NAMESPACE yt.orm.cli.client

    client.py
)

END()
