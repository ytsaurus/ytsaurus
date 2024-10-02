PY23_LIBRARY()

PEERDIR(
    yt/yt/orm/python/orm/library
    yt/python/yt/local
    yt/python/yt/wrapper
    yt/python/yt/environment/arcadia_interop
    yt/python/yt/packages
    yt/python/yt
)

PY_SRCS(
    NAMESPACE yt.orm.yt_manager.local

    __init__.py
)

END()
