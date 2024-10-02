
PY3_LIBRARY()

PEERDIR(
    yt/yt/orm/python/orm/library
)

PY_SRCS(
    NAMESPACE yt.orm.cli.local

    local.py
)

END()
