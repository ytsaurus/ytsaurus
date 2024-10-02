PY3_LIBRARY()

PEERDIR(
    yt/yt/orm/python/orm/admin
    yt/yt/orm/python/orm/library

    yt/python/yt
    yt/python/yt/wrapper
)

PY_SRCS(
    NAMESPACE yt.orm.cli.admin

    admin.py
)

END()
